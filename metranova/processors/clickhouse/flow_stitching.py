import hashlib
import ipaddress
import logging
import os
import re
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from metranova.processors.clickhouse.base import BaseDataProcessor
from metranova.processors.clickhouse.pmacct import PMAcctFlowProcessor

logger = logging.getLogger(__name__)

_BYTE_SIZE_MULTIPLIERS = {
    "": 1, "b": 1,
    "k": 1024, "kb": 1024,
    "m": 1024 ** 2, "mb": 1024 ** 2,
    "g": 1024 ** 3, "gb": 1024 ** 3,
    "t": 1024 ** 4, "tb": 1024 ** 4,
}

_BYTE_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]*)\s*$")


def parse_byte_size(value: Any) -> int:
    """Parses a byte-size value into an integer number of bytes. Accepts a bare number
    (int/float, or a numeric string with no suffix) or a string with a case-insensitive
    binary-unit suffix -- K/KB, M/MB, G/GB, T/TB (1024-based, e.g. '10M' == 10*1024**2).
    Used for CLICKHOUSE_FLOW_STITCH_MIN_BYTES so it can be set like '10M' instead of a
    raw byte count."""
    if isinstance(value, (int, float)):
        return int(value)
    match = _BYTE_SIZE_RE.match(str(value))
    if not match:
        raise ValueError(f"Invalid byte size: {value!r}")
    number_str, suffix = match.groups()
    multiplier = _BYTE_SIZE_MULTIPLIERS.get(suffix.lower())
    if multiplier is None:
        raise ValueError(f"Invalid byte size suffix in {value!r}: {suffix!r}")
    return int(float(number_str) * multiplier)


def anonymize_ip(
    ip_str: Optional[str], ip_version: int, ipv4_prefix: int, ipv6_prefix: int
) -> Optional[str]:
    """Mask an IP address the same way MaterializedViewAnonymizedFlow's build_anon_ip()
    does in SQL: multicast (and 6to4/mapped) ranges are left unmodified, everything else
    is truncated to the configured prefix length. ipv4_prefix/ipv6_prefix are expressed
    as IPv6 prefix lengths (ClickHouse stores addresses as IPv6, with IPv4 mapped to
    ::ffff:0:0/96), matching CLICKHOUSE_FLOW_MV_ANONYMIZED_{WINDOW}_IPV{4,6}_PREFIX."""
    if not ip_str:
        return ip_str
    try:
        if ip_version == 4:
            addr = ipaddress.IPv4Address(ip_str)
            if addr in ipaddress.IPv4Network("224.0.0.0/4"):
                return str(addr)
            prefix = max(0, ipv4_prefix - 96)
            return str(ipaddress.ip_network(f"{addr}/{prefix}", strict=False).network_address)
        else:
            addr = ipaddress.IPv6Address(ip_str)
            if addr in ipaddress.IPv6Network("ff00::/8") or addr in ipaddress.IPv6Network("2002::/16"):
                return str(addr)
            return str(ipaddress.ip_network(f"{addr}/{ipv6_prefix}", strict=False).network_address)
    except ValueError:
        logger.debug(f"Could not parse IP '{ip_str}' (version {ip_version}) for anonymization; leaving unmodified")
        return ip_str


class FlowStitchingProcessor(BaseDataProcessor):
    """
    True flow stitching, analogous to the Logstash 'aggregate' filter used by the legacy
    NetSage pipeline (see 40-aggregation.conf): rather than periodically bucketing/summing
    flow-cache export slices in ClickHouse (see MaterializedViewAnonymizedFlow), this holds
    per-flow accumulator state in this process and emits exactly ONE row per real flow --
    once no new matching slice has arrived for CLICKHOUSE_FLOW_STITCH_INACTIVITY_TIMEOUT
    seconds (wall clock), or the flow's own reported duration (event time) exceeds
    CLICKHOUSE_FLOW_STITCH_MAX_TIMEOUT.

    Reuses PMAcctFlowProcessor.build_message() internally to turn each raw incoming
    pmacct/sflow message into a normalized `data_flow`-schema row (IP/AS/interface cache
    lookups, BGP/MPLS/VLAN extension parsing), then accumulates those instead of emitting
    them immediately. The inner PMAcctFlowProcessor instance is never registered as a
    pipeline processor itself, so it never creates a `data_flow` table/materialized views
    of its own -- it's purely reused for its per-slice normalization logic.

    A dedicated background thread periodically sweeps the accumulator for completed flows
    and writes them directly to ClickHouse via the client handed to us through
    set_clickhouse_client() (this processor's own build_message() always returns an empty
    list -- every incoming slice is absorbed, never written through the normal per-message
    batching path).

    NOTE: accumulator state is in-memory only. If this process restarts, any flows that
    were still in-progress at that moment are lost (their already-absorbed slices are not
    re-emitted). There is no persistence layer here, unlike Logstash's aggregate_maps_path
    save-on-shutdown -- add one (e.g. periodic snapshot to disk, or a Redis-backed store)
    if that gap matters for your deployment.
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)

        # Off by default -- this changes what gets written to ClickHouse, so it should be
        # an explicit opt-in per deployment rather than turning on the moment this
        # processor is added to a pipeline YAML.
        self.enabled = os.getenv("CLICKHOUSE_FLOW_STITCH_ENABLED", "false").lower() in ("true", "1", "yes")

        self.table = os.getenv("CLICKHOUSE_FLOW_STITCH_TABLE", "data_flow_stitched")
        self.table_ttl = os.getenv("CLICKHOUSE_FLOW_STITCH_TTL", "5 YEAR")
        self.table_ttl_column = os.getenv("CLICKHOUSE_FLOW_STITCH_TTL_COLUMN", "start_time")
        self.partition_by = os.getenv(
            "CLICKHOUSE_FLOW_STITCH_PARTITION_BY", "toYYYYMMDD(start_time)"
        )

        # Matches the Logstash aggregate filter's own semantics (see 40-aggregation.conf):
        # inactivity_timeout is wall-clock seconds since the last matching slice arrived;
        # max_flow_timeout is the flow's own reported duration (event time), not wall clock.
        self.inactivity_timeout = float(os.getenv("CLICKHOUSE_FLOW_STITCH_INACTIVITY_TIMEOUT", "630"))
        self.max_flow_timeout = float(os.getenv("CLICKHOUSE_FLOW_STITCH_MAX_TIMEOUT", "86400"))
        self.sweep_interval = float(os.getenv("CLICKHOUSE_FLOW_STITCH_SWEEP_INTERVAL", "30"))

        # Anonymize src_ip/dst_ip/peer_ip in stitched records by masking host bits
        # down to anon_ipv4_prefix/anon_ipv6_prefix below. On by default, matching this
        # processor's original always-anonymized behavior -- set to "no" to write full,
        # unmasked addresses instead.
        self.anonymize = os.getenv("CLICKHOUSE_FLOW_STITCH_ANONYMIZE", "yes").lower() in ("true", "1", "yes")

        # IP anonymization prefix lengths -- same meaning/defaults as
        # MaterializedViewAnonymizedFlow's CLICKHOUSE_FLOW_MV_ANONYMIZED_{WINDOW}_IPV{4,6}_PREFIX
        self.anon_ipv4_prefix = int(os.getenv("CLICKHOUSE_FLOW_STITCH_IPV4_PREFIX", "117"))
        self.anon_ipv6_prefix = int(os.getenv("CLICKHOUSE_FLOW_STITCH_IPV6_PREFIX", "48"))

        # Drop completed flows smaller than this many bytes rather than writing them --
        # 0 (default) disables filtering and keeps every flow, however small. Checked once
        # the flow is complete (its final size isn't known any earlier), so a small flow
        # still occupies an accumulator slot in memory until it times out/finishes.
        self.min_bytes = parse_byte_size(os.getenv("CLICKHOUSE_FLOW_STITCH_MIN_BYTES", "0"))

        # Drop completed flows whose duration is <= this many seconds -- filters out
        # single-sample/near-instant flows (a lone probe packet, a single netflow
        # sample with no measurable span) that are rarely useful for bitrate analysis.
        # Defaults to 0.1s rather than 0, unlike CLICKHOUSE_FLOW_STITCH_MIN_BYTES.
        self.min_duration = float(os.getenv("CLICKHOUSE_FLOW_STITCH_MIN_DURATION", "0.1"))

        # start_time/end_time here are the real min/max event times of the whole stitched
        # flow (not bucketed) -- kept at full DateTime64(3, 'UTC') precision, same as data_flow.
        self.column_defs.insert(0, ["start_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.insert(1, ["end_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.insert(2, ["duration", "Float64", True])
        # Stable hash of the flow fingerprint tuple (see _fingerprint()) -- lets
        # downstream consumers group/join rows belonging to the same flow without
        # reconstructing the tuple themselves.
        self.column_defs.insert(3, ["flow_hash", "UInt64", True])
        self.column_defs.append(["flow_type", "LowCardinality(String)", True])
        self.column_defs.append(["device_id", "LowCardinality(String)", True])
        self.column_defs.append(["device_ref", "Nullable(String)", True])
        self.column_defs.append(["src_as_id", "UInt32", True])
        self.column_defs.append(["src_as_ref", "Nullable(String)", True])
        self.column_defs.append(["src_ip", "IPv6", True])
        self.column_defs.append(["src_port", "UInt16", True])
        self.column_defs.append(["dst_as_id", "UInt32", True])
        self.column_defs.append(["dst_as_ref", "Nullable(String)", True])
        self.column_defs.append(["dst_ip", "IPv6", True])
        self.column_defs.append(["dst_port", "UInt16", True])
        self.column_defs.append(["protocol", "LowCardinality(String)", True])
        self.column_defs.append(["in_interface_id", "LowCardinality(Nullable(String))", True])
        self.column_defs.append(["in_interface_ref", "Nullable(String)", True])
        self.column_defs.append(["in_interface_edge", "Bool", True])
        self.column_defs.append(["out_interface_id", "LowCardinality(Nullable(String))", True])
        self.column_defs.append(["out_interface_ref", "Nullable(String)", True])
        self.column_defs.append(["out_interface_edge", "Bool", True])
        self.column_defs.append(["peer_as_id", "Nullable(UInt32)", True])
        self.column_defs.append(["peer_as_ref", "Nullable(String)", True])
        self.column_defs.append(["peer_ip", "Nullable(IPv6)", True])
        self.column_defs.append(["ip_version", "UInt8", True])
        self.column_defs.append(["application_port", "UInt16", True])
        # informational: how many flow-cache export slices got stitched into this one row
        self.column_defs.append(["flow_count", "UInt64", True])
        self.column_defs.append(["bit_count", "UInt64", True])
        self.column_defs.append(["packet_count", "UInt64", True])
        # Average rates over the whole stitched flow -- same formula and 0-on-zero-duration
        # fallback as the Logstash aggregate filter's packets_per_second/bits_per_second
        # (40-aggregation.conf): integer-truncated count / duration.
        self.column_defs.append(["packets_per_second", "UInt64", True])
        self.column_defs.append(["bits_per_second", "UInt64", True])

        # Same extension shape as MaterializedViewAnonymizedFlow's ext column.
        extension_options = {
            "bgp": [
                ["bgp_as_path_id", "Array(UInt32)"],
                ["bgp_as_path_padding", "Array(UInt16)"],
                ["bgp_community", "Array(LowCardinality(String))"],
                ["bgp_ext_community", "Array(LowCardinality(String))"],
                ["bgp_large_community", "Array(LowCardinality(String))"],
                ["bgp_local_pref", "Nullable(UInt32)"],
                ["bgp_med", "Nullable(UInt32)"],
            ],
            "ipv4": [
                ["ipv4_dscp", "Nullable(UInt8)"],
                ["ipv4_tos", "Nullable(UInt8)"],
            ],
            "ipv6": [
                ["ipv6_flow_label", "Nullable(UInt32)"],
            ],
            "mpls": [
                ["mpls_bottom_label", "Nullable(UInt32)"],
                ["mpls_exp", "Array(UInt8)"],
                ["mpls_label", "Array(UInt32)"],
                ["mpls_pw", "Nullable(UInt32)"],
                ["mpls_top_label_ip", "Nullable(IPv6)"],
                ["mpls_top_label_type", "Nullable(UInt32)"],
                ["mpls_vpn_rd", "LowCardinality(Nullable(String))"],
            ],
            "vlan": [
                ["vlan_id", "Nullable(UInt32)"],
                ["vlan_in_id", "Nullable(UInt32)"],
                ["vlan_out_id", "Nullable(UInt32)"],
                ["vlan_in_inner_id", "Nullable(UInt32)"],
                ["vlan_out_inner_id", "Nullable(UInt32)"],
            ],
        }
        self.extension_defs["ext"] = self.get_extension_defs("CLICKHOUSE_FLOW_EXTENSIONS", extension_options)

        # Plain MergeTree, not SummingMergeTree: each row written here is already the
        # final, complete flow -- there's nothing left for ClickHouse to merge/sum.
        self.table_engine = "MergeTree"
        self.table_engine_opts = ""
        self.order_by = ["src_as_id", "dst_as_id", "src_ip", "dst_ip", "start_time"]
        self.allow_nullable_key = True

        # Reused purely to normalize each raw incoming message into a `data_flow`-schema
        # row -- never registered as a pipeline processor itself, so it creates no table
        # or materialized views of its own (only a real, wired-up PMAcctFlowProcessor does).
        self._slice_processor = PMAcctFlowProcessor(pipeline)

        # Set via set_clickhouse_client() once ClickHouseBatcher creates our table -- the
        # background sweep thread uses this same client (see BaseClickHouseProcessor's
        # docstring: this hook exists precisely for processors that need direct access).
        self._client = None

        self._lock = threading.Lock()
        self._flows: Dict[Tuple, Dict[str, Any]] = {}

        self._sweep_count = 0

        self._sweep_thread = None
        if self.enabled:
            logger.info(
                "Flow stitching: enabled (table=%s, sweep_interval=%ss, "
                "inactivity_timeout=%ss, max_flow_timeout=%ss)",
                self.table, self.sweep_interval, self.inactivity_timeout, self.max_flow_timeout,
            )
            self._sweep_thread = threading.Thread(
                target=self._sweep_loop, name="FlowStitchingSweep", daemon=True
            )
            self._sweep_thread.start()
        else:
            logger.info("Flow stitching: disabled (CLICKHOUSE_FLOW_STITCH_ENABLED is not true)")

    def set_clickhouse_client(self, client) -> None:
        self._client = client

    def match_message(self, value: dict) -> bool:
        """Never match when disabled, so build_message() is never called and this
        processor absorbs nothing -- the single CLICKHOUSE_FLOW_STITCH_ENABLED switch."""
        if not self.enabled:
            return False
        return super().match_message(value)

    def create_table_command(self, table_name=None) -> str:
        """Skip creating the stitched-flow table entirely while disabled, same convention
        ClickHouseBatcher already uses elsewhere (a None command means "nothing to do")."""
        if not self.enabled:
            return None
        return super().create_table_command(table_name=table_name)

    def _fingerprint(self, row: Dict[str, Any]) -> Tuple:
        """Identifies 'the same flow' across export slices -- the usual 5-tuple plus
        device/interface, matching what NetFlow/pmacct considers one flow cache entry."""
        return (
            row.get("device_id"),
            row.get("src_ip"),
            row.get("src_port"),
            row.get("dst_ip"),
            row.get("dst_port"),
            row.get("protocol"),
            row.get("in_interface_id"),
            row.get("out_interface_id"),
        )

    def _hash_fingerprint(self, fp: Tuple) -> int:
        """Stable 64-bit hash of a flow fingerprint tuple, stored as flow_hash so
        downstream consumers can group/join stitched rows on 'the same flow' without
        reconstructing the fingerprint tuple themselves. Uses hashlib rather than
        Python's built-in hash(), which salts string hashing per-process
        (PYTHONHASHSEED) and would not be reproducible across restarts or between
        multiple pipeline processes writing to the same table. Fields are joined with
        an ASCII unit-separator character (rather than concatenated directly) so
        different tuples can't collide at a field boundary, e.g. ("1", "23") vs
        ("12", "3")."""
        key = "\x1f".join("" if v is None else str(v) for v in fp)
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big")

    def _accumulate(self, fp: Tuple, row: Dict[str, Any]) -> None:
        """Must be called with self._lock held."""
        entry = self._flows.get(fp)
        if entry is None:
            # First slice wins for identity/metadata fields (device, AS, interfaces,
            # extensions, policy, ...) -- matches Logstash's map['meta'] ||= event.get('meta'):
            # set once, never overwritten, since these dimensions shouldn't legitimately
            # change mid-flow.
            entry = {
                "template": row,
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "bit_count": 0,
                "packet_count": 0,
                "stitched_flows": 0,
            }
            self._flows[fp] = entry
        entry["start_time"] = min(entry["start_time"], row["start_time"])
        entry["end_time"] = max(entry["end_time"], row["end_time"])
        entry["bit_count"] += row.get("bit_count", 0) or 0
        entry["packet_count"] += row.get("packet_count", 0) or 0
        entry["stitched_flows"] += 1
        entry["last_seen_wallclock"] = time.time()

    def _finalize(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Builds the final stitched-flow row from an accumulator entry."""
        row = dict(entry["template"])
        # Computed from the template's original (pre-anonymization) fields, before
        # src_ip/dst_ip are masked below -- must match the same tuple _fingerprint()
        # used to key this flow in self._flows while it was accumulating.
        row["flow_hash"] = self._hash_fingerprint(self._fingerprint(row))
        row["start_time"] = entry["start_time"]
        row["end_time"] = entry["end_time"]
        # start_time/end_time are ms-since-epoch ints (see PMAcctFlowProcessor.build_message).
        row["duration"] = (entry["end_time"] - entry["start_time"]) / 1000.0
        row["bit_count"] = entry["bit_count"]
        row["packet_count"] = entry["packet_count"]
        row["flow_count"] = entry["stitched_flows"]
        # Matches Logstash: (count / duration).to_i, or 0 if duration isn't positive
        # (e.g. a single sample where start == end) rather than dividing by zero.
        if row["duration"] > 0:
            row["packets_per_second"] = int(row["packet_count"] / row["duration"])
            row["bits_per_second"] = int(row["bit_count"] / row["duration"])
        else:
            row["packets_per_second"] = 0
            row["bits_per_second"] = 0

        if self.anonymize:
            ip_version = row.get("ip_version", 4)
            row["src_ip"] = anonymize_ip(row.get("src_ip"), ip_version, self.anon_ipv4_prefix, self.anon_ipv6_prefix)
            row["dst_ip"] = anonymize_ip(row.get("dst_ip"), ip_version, self.anon_ipv4_prefix, self.anon_ipv6_prefix)
            if row.get("peer_ip"):
                row["peer_ip"] = anonymize_ip(row.get("peer_ip"), ip_version, self.anon_ipv4_prefix, self.anon_ipv6_prefix)

        return row

    def _sweep_once(self) -> List[Dict[str, Any]]:
        now = time.time()
        completed = []
        dropped_bytes = 0
        dropped_duration = 0
        with self._lock:
            expired_fps = []
            for fp, entry in self._flows.items():
                idle_seconds = now - entry["last_seen_wallclock"]
                span_seconds = (entry["end_time"] - entry["start_time"]) / 1000.0
                if idle_seconds >= self.inactivity_timeout or span_seconds >= self.max_flow_timeout:
                    expired_fps.append(fp)
            for fp in expired_fps:
                row = self._finalize(self._flows.pop(fp))
                # bit_count is in bits (see PMAcctFlowProcessor.build_message); compare in
                # bytes against CLICKHOUSE_FLOW_STITCH_MIN_BYTES. self.min_bytes == 0 (the
                # default) always passes, so nothing is filtered unless explicitly configured.
                if row["bit_count"] // 8 < self.min_bytes:
                    dropped_bytes += 1
                    continue
                # duration <= CLICKHOUSE_FLOW_STITCH_MIN_DURATION (default 0.1s) filters
                # out single-sample/near-instant flows. min_duration <= 0 disables this
                # filter entirely (including for a literal duration-0 flow), same
                # "0 means off" convention as CLICKHOUSE_FLOW_STITCH_MIN_BYTES.
                if self.min_duration > 0 and row["duration"] <= self.min_duration:
                    dropped_duration += 1
                    continue
                completed.append(row)
        if dropped_bytes:
            self.logger.debug(
                f"Flow stitching: dropped {dropped_bytes} completed flow(s) below "
                f"CLICKHOUSE_FLOW_STITCH_MIN_BYTES={self.min_bytes} bytes"
            )
        if dropped_duration:
            self.logger.debug(
                f"Flow stitching: dropped {dropped_duration} completed flow(s) with "
                f"duration <= CLICKHOUSE_FLOW_STITCH_MIN_DURATION={self.min_duration}s"
            )
        if completed:
            self._write_rows(completed)
        return completed

    def _sweep_loop(self) -> None:
        self.logger.info("Flow stitching: sweep thread started")
        while True:
            time.sleep(self.sweep_interval)
            try:
                completed = self._sweep_once()
                self._sweep_count += 1
                with self._lock:
                    in_flight = len(self._flows)
                # DEBUG only -- fires every sweep_interval forever, so it's not
                # something production logging should carry at INFO by default. Set
                # DEBUG=true (see docs/env_vars.md) to watch this live.
                self.logger.debug(
                    "Flow stitching: sweep #%d, %d flow(s) in-flight, %d completed this tick",
                    self._sweep_count, in_flight, len(completed),
                )
            except Exception as e:
                self.logger.error(f"Flow stitching sweep failed: {e}", exc_info=True)

    def _write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if self._client is None:
            self.logger.warning(
                f"Flow stitching: {len(rows)} completed flow(s) ready but no ClickHouse "
                "client yet (set_clickhouse_client() not called) -- dropping them"
            )
            return
        try:
            data = [self.message_to_columns(row, self.table) for row in rows]
            self._client.insert(table=self.table, data=data, column_names=self.column_names(self.table))
            self.logger.info(f"Flow stitching: wrote {len(rows)} completed flow(s) to {self.table}")
        except Exception as e:
            self.logger.error(f"Flow stitching: failed to write completed flows to {self.table}: {e}", exc_info=True)

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # Belt-and-suspenders alongside match_message()'s False: never accumulate while
        # disabled, even if something calls build_message() directly.
        if not self.enabled:
            return []
        try:
            slices = self._slice_processor.build_message(value, msg_metadata)
        except Exception as e:
            self.logger.error(f"Flow stitching: failed to normalize incoming message: {e}", exc_info=True)
            return []
        if not slices:
            return []
        with self._lock:
            for slice_row in slices:
                self._accumulate(self._fingerprint(slice_row), slice_row)
            in_flight = len(self._flows)
        self.logger.debug(
            "Flow stitching: absorbed %d slice(s), %d flow(s) now in-flight",
            len(slices), in_flight,
        )
        # Completed flows are written directly to ClickHouse by the background sweep
        # thread (_sweep_loop), not returned here -- every incoming slice is absorbed.
        return []
