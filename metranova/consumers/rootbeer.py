import logging
import os
from metranova.connectors.clickhouse import ClickHouseConnector
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)


class RootbeerClickHouseConsumer(TimedIntervalConsumer):
    """Periodically streams data_bmp_route_latest and emits rootbeer route-report chunks.

    Memory strategy
    ---------------
    The table can contain hundreds of millions of rows.  We never load the full
    result into memory.  Instead we:

    1. Stream the **accepted** rows (is_post=true, action='update') ordered by
       (device_id, peer_ip).  For each (device_id, peer_ip) group we hold only
       the list of accepted prefixes in memory.

    2. Stream the **pre-policy** rows (is_post=false, action='update') with the
       same ordering.  For each row we check whether its (device_id, peer_ip,
       prefix_key) was already accepted; if not, it goes into the rejected list.

    Because both streams are ordered the same way, we can process each
    (device_id, peer_ip) group as a unit, emit a chunk message, and discard it
    before moving on – so peak memory for the accepted-prefix lookup is bounded
    to at most one session's worth of prefixes at a time.

    After all groups have been emitted, a single flush message triggers the
    processor to assemble and return the final document.

    Environment variables
    ---------------------
    ROOTBEER_SOURCE_TABLE          – ClickHouse table/view to query
                                     (default: data_bmp_route_latest)
    ROOTBEER_IP_METADATA_TABLE     – meta_ip table used to resolve peer ASN
                                     (default: meta_ip)
    ROOTBEER_UPDATE_INTERVAL       – seconds between runs, -1 = run once
                                     (default: -1)
    ROOTBEER_QUERY_BLOCK_SIZE      – rows fetched per network block
                                     (default: 100000)
    """

    # Both queries return the same column shape:
    # (device_id, peer_ip, remote_as, ip_prefix_address, ip_prefix_length, add_path_id, as_path_id)
    #
    # peer_ip_ref is now a GROUP BY column in data_bmp_route_latest, so we can join directly
    # on peer_ip_ref = meta_ip.ref — the stable unique key for an IP metadata entry.
    _ROUTE_QUERY = """
        SELECT
            bmp.device_id                                         AS device_id,
            toString(bmp.peer_ip)                                 AS peer_ip,
            toString(m.as_id)                                     AS remote_as,
            toString(bmp.ip_prefix_address)                       AS ip_prefix_address,
            bmp.ip_prefix_length                                  AS ip_prefix_length,
            bmp.add_path_id                                       AS add_path_id,
            argMaxMerge(bmp.as_path_id_state)                     AS as_path_id
        FROM {table} AS bmp
        LEFT JOIN {ip_table} AS m ON bmp.peer_ip_ref = m.ref
        WHERE bmp.is_post = {is_post}
        GROUP BY device_id, peer_ip, peer_ip_ref, ip_prefix_address, ip_prefix_length, ip_prefix_ref, add_path_id, m.as_id
        HAVING argMaxMerge(bmp.action_state) = 'update'
        ORDER BY device_id, peer_ip, ip_prefix_address, ip_prefix_length, add_path_id
    """

    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.datasource = ClickHouseConnector()
        self.source_table = os.getenv('ROOTBEER_SOURCE_TABLE', 'data_bmp_route_latest')
        self.ip_metadata_table = os.getenv('ROOTBEER_IP_METADATA_TABLE', 'meta_ip')
        self.update_interval = int(os.getenv('ROOTBEER_UPDATE_INTERVAL', '-1'))
        self.block_size = int(os.getenv('ROOTBEER_QUERY_BLOCK_SIZE', '100000'))
        self.auto_prime_cachers = False  # no cachers needed

    def _build_query(self, is_post: bool) -> str:
        return self._ROUTE_QUERY.format(
            table=self.source_table,
            ip_table=self.ip_metadata_table,
            is_post='true' if is_post else 'false',
        )

    # ------------------------------------------------------------------
    # TimedIntervalConsumer interface
    # ------------------------------------------------------------------

    def consume_messages(self):
        from datetime import datetime, timezone
        self.logger.info("Rootbeer: starting route-report generation from %s", self.source_table)
        snapshot_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        accepted_query = self._build_query(is_post=True)
        pre_query = self._build_query(is_post=False)

        # Pass 1: stream accepted rows, group by (device_id, peer_ip), emit chunks.
        # Keep a set of accepted prefix keys per (device_id, peer_ip) to use in pass 2.
        # Because both passes are ordered identically we process groups sequentially.
        accepted_keys: dict[tuple, set] = {}   # (device_id, peer_ip) -> set of prefix keys

        self.logger.info("Rootbeer: pass 1 – streaming accepted routes")
        try:
            with self.datasource.client.query_row_block_stream(
                accepted_query,
                settings={"max_block_size": self.block_size},
            ) as stream:
                current_group: tuple | None = None
                current_accepted: list = []
                current_accepted_keys: set = set()

                for block in stream:
                    for row in block:
                        device_id, peer_ip, remote_as, prefix_addr, prefix_len, add_path_id, as_path_id = row
                        group = (device_id, peer_ip)
                        prefix_key = (prefix_addr, prefix_len, add_path_id)

                        if group != current_group:
                            if current_group is not None:
                                self._emit_chunk(current_group, current_remote_as, current_accepted, [], snapshot_ts)
                                accepted_keys[current_group] = current_accepted_keys
                            current_group = group
                            current_remote_as = remote_as
                            current_accepted = []
                            current_accepted_keys = set()

                        route = self._make_route(prefix_addr, prefix_len, as_path_id)
                        current_accepted.append(route)
                        current_accepted_keys.add(prefix_key)

                # flush last group
                if current_group is not None:
                    self._emit_chunk(current_group, current_remote_as, current_accepted, [], snapshot_ts)
                    accepted_keys[current_group] = current_accepted_keys

        except Exception as e:
            self.logger.error("Rootbeer: error streaming accepted routes: %s", e)
            return

        self.logger.info(
            "Rootbeer: pass 1 complete – %d (device, peer) groups", len(accepted_keys)
        )

        # Pass 2: stream pre-policy rows, emit rejected-only chunks for each group.
        # We only need to keep one group's rejected list in memory at a time.
        self.logger.info("Rootbeer: pass 2 – streaming pre-policy routes for rejection set")
        try:
            with self.datasource.client.query_row_block_stream(
                pre_query,
                settings={"max_block_size": self.block_size},
            ) as stream:
                current_group = None
                current_rejected: list = []

                for block in stream:
                    for row in block:
                        device_id, peer_ip, remote_as, prefix_addr, prefix_len, add_path_id, as_path_id = row
                        group = (device_id, peer_ip)
                        prefix_key = (prefix_addr, prefix_len, add_path_id)

                        if group != current_group:
                            if current_group is not None and current_rejected:
                                self._emit_chunk(current_group, current_remote_as, [], current_rejected, snapshot_ts)
                            current_group = group
                            current_remote_as = remote_as
                            current_rejected = []

                        # rejected = in pre-policy but NOT in post-policy
                        group_accepted = accepted_keys.get(group, set())
                        if prefix_key not in group_accepted:
                            route = self._make_route(prefix_addr, prefix_len, as_path_id)
                            current_rejected.append(route)

                # flush last group
                if current_group is not None and current_rejected:
                    self._emit_chunk(current_group, current_remote_as, [], current_rejected, snapshot_ts)

        except Exception as e:
            self.logger.error("Rootbeer: error streaming pre-policy routes: %s", e)
            return

        self.logger.info("Rootbeer: pass 2 complete – sending flush signal")

        # Signal the processor to assemble and write the final document
        self.pipeline.process_message({"rootbeer_flush": True, "timestamp": snapshot_ts})

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_route(self, prefix_addr: str, prefix_len: int, as_path_id: list) -> dict:
        return {
            "prefix": f"{prefix_addr}/{prefix_len}",
            "as-path": [str(asn) for asn in (as_path_id or [])],
        }

    def _emit_chunk(
        self,
        group: tuple,
        remote_as: str,
        accepted: list,
        rejected: list,
        timestamp: str,
    ) -> None:
        device_id, peer_ip = group
        msg = {
            "rootbeer_chunk": True,
            "device_id": device_id,
            "peer_ip": peer_ip,
            "remote_as": remote_as or "",
            "accepted": accepted,
            "rejected": rejected,
        }
        self.pipeline.process_message(msg)
