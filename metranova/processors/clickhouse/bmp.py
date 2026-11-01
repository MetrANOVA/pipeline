import orjson
import os
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.base import (
    BaseClickHouseMaterializedViewMixin,
    BaseClickHouseViewMixin,
    BaseDataProcessor,
)

# ---------------------------------------------------------------------------
# BMP Statistics Report (BMP message type 1)
# ---------------------------------------------------------------------------


class BaseBMPStatProcessor(BaseDataProcessor):
    """Processor for BMP Statistics Report messages (BMP message type 1).

    Each row records a single metric observation from a BMP statistics report
    message emitted by pmacct/pbmpd.  The source message carries one metric
    per JSON object, so a single statistics report from a router may produce
    many rows — one per ``metric_type``.

    Table: ``data_bmp_stat``

    Environment variables:
        CLICKHOUSE_BMP_STAT_TABLE        – target table name (default: ``data_bmp_stat``)
        CLICKHOUSE_BMP_STAT_TTL          – row TTL interval (default: ``7 DAY``)
        CLICKHOUSE_BMP_STAT_TTL_COLUMN   – TTL anchor column (default: ``observation_time``)
        CLICKHOUSE_BMP_STAT_PARTITION_BY – PARTITION BY expression
                                           (default: ``toYYYYMMDD(observation_time)``)
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)

        self.table = os.getenv('CLICKHOUSE_BMP_STAT_TABLE', 'data_bmp_stat')
        self.table_ttl = os.getenv('CLICKHOUSE_BMP_STAT_TTL', '7 DAY')
        self.table_ttl_column = os.getenv('CLICKHOUSE_BMP_STAT_TTL_COLUMN', 'observation_time')
        self.partition_by = os.getenv(
            'CLICKHOUSE_BMP_STAT_PARTITION_BY', 'toYYYYMMDD(observation_time)'
        )

        # observation_time maps to pbmpd `timestamp` — the time the BMP
        # statistics message was generated on the router.
        self.column_defs.insert(0, ["observation_time", "DateTime64(3, 'UTC')", True])

        # Device / router identity
        self.column_defs.append(['device_id', 'LowCardinality(String)', True])
        self.column_defs.append(['device_ref', 'Nullable(String)', True])

        # BGP peer identity
        self.column_defs.append(['peer_ip', 'IPv6', True])
        self.column_defs.append(['peer_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_as_id', 'Nullable(UInt32)', True])
        self.column_defs.append(['peer_as_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_type', 'Nullable(UInt8)', True])

        # Router BGP ID of the remote peer
        self.column_defs.append(['bgp_id', 'Nullable(IPv6)', True])

        # Route Distinguisher (https://tools.ietf.org/html/rfc7854#section-4.2)
        self.column_defs.append(['rd', 'Nullable(String)', True])

        # Scope flags present on stats messages
        self.column_defs.append(['is_loc', 'Bool', True])       # Loc-RIB data
        self.column_defs.append(['is_filtered', 'Bool', True])  # filtered Loc-RIB data

        # Statistics metric fields
        # metric_type: IANA BMP statistics type code
        #   https://www.iana.org/assignments/bmp-parameters/bmp-parameters.xhtml#statistics-types
        self.column_defs.append(['metric_type', 'UInt16', True])
        self.column_defs.append(['metric_type_str', 'LowCardinality(String)', True])
        # metric_value holds the raw 64-bit unsigned value from the BMP message.
        # All current IANA-assigned types use either a 4-byte or 8-byte value,
        # both representable as UInt64.
        self.column_defs.append(['metric_value', 'UInt64', True])

        # Primary key / ordering: one row per (device, peer, stat type, time).
        # metric_type is included so that time-range scans for a single stat
        # type are efficient (the sort order places all types for a given peer
        # together before splitting by time).
        self.primary_keys = [
            'device_id',
            'peer_ip',
            'metric_type',
            'observation_time',
        ]
        self.order_by = self.primary_keys.copy()

# ---------------------------------------------------------------------------
# BMP Peer Up events (BMP message type 3)
# ---------------------------------------------------------------------------


class BaseBMPPeerUpProcessor(BaseDataProcessor):
    """Processor for BMP Peer Up messages (BMP message type 3).

    Each row records a single peer session establishment event received
    from pmacct/pbmpd.

    Table: ``data_bmp_peer_up``

    Environment variables:
        CLICKHOUSE_BMP_PEER_UP_TABLE        – target table name (default: ``data_bmp_peer_up``)
        CLICKHOUSE_BMP_PEER_UP_TTL          – row TTL interval (default: ``90 DAY``)
        CLICKHOUSE_BMP_PEER_UP_TTL_COLUMN   – TTL anchor column (default: ``observation_time``)
        CLICKHOUSE_BMP_PEER_UP_PARTITION_BY – PARTITION BY expression
                                              (default: ``toYYYYMMDD(observation_time)``)
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)

        self.table = os.getenv('CLICKHOUSE_BMP_PEER_UP_TABLE', 'data_bmp_peer_up')
        self.table_ttl = os.getenv('CLICKHOUSE_BMP_PEER_UP_TTL', '90 DAY')
        self.table_ttl_column = os.getenv('CLICKHOUSE_BMP_PEER_UP_TTL_COLUMN', 'observation_time')
        self.partition_by = os.getenv(
            'CLICKHOUSE_BMP_PEER_UP_PARTITION_BY', 'toYYYYMMDD(observation_time)'
        )

        # observation_time maps to pbmpd `timestamp` — when the peer came up.
        self.column_defs.insert(0, ["observation_time", "DateTime64(3, 'UTC')", True])

        # Device / router identity
        self.column_defs.append(['device_id', 'LowCardinality(String)', True])
        self.column_defs.append(['device_ref', 'Nullable(String)', True])

        # BGP peer identity
        self.column_defs.append(['peer_ip', 'IPv6', True])
        self.column_defs.append(['peer_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_as_id', 'Nullable(UInt32)', True])
        self.column_defs.append(['peer_as_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_type', 'Nullable(UInt8)', True])

        # BGP router ID of the remote peer (from the BGP OPEN message)
        self.column_defs.append(['bgp_id', 'Nullable(IPv6)', True])

        # Route Distinguisher (https://tools.ietf.org/html/rfc7854#section-4.2)
        self.column_defs.append(['rd', 'Nullable(String)', True])

        # Local side of the session
        self.column_defs.append(['local_as', 'Nullable(UInt32)', True])
        self.column_defs.append(['local_port', 'Nullable(UInt16)', True])
        self.column_defs.append(['remote_port', 'Nullable(UInt16)', True])
        self.column_defs.append(['local_ip', 'Nullable(IPv6)', True])

        # Primary key: one event per (device, peer, timestamp).
        self.primary_keys = [
            'device_id',
            'peer_ip',
            'observation_time',
        ]
        self.order_by = self.primary_keys.copy()


# ---------------------------------------------------------------------------
# BMP Peer Down events (BMP message type 2)
# ---------------------------------------------------------------------------


class BaseBMPPeerDownProcessor(BaseDataProcessor):
    """Processor for BMP Peer Down messages (BMP message type 2).

    Each row records a single peer session termination event received
    from pmacct/pbmpd.

    Table: ``data_bmp_peer_down``

    Environment variables:
        CLICKHOUSE_BMP_PEER_DOWN_TABLE        – target table name (default: ``data_bmp_peer_down``)
        CLICKHOUSE_BMP_PEER_DOWN_TTL          – row TTL interval (default: ``90 DAY``)
        CLICKHOUSE_BMP_PEER_DOWN_TTL_COLUMN   – TTL anchor column (default: ``observation_time``)
        CLICKHOUSE_BMP_PEER_DOWN_PARTITION_BY – PARTITION BY expression
                                                (default: ``toYYYYMMDD(observation_time)``)
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)

        self.table = os.getenv('CLICKHOUSE_BMP_PEER_DOWN_TABLE', 'data_bmp_peer_down')
        self.table_ttl = os.getenv('CLICKHOUSE_BMP_PEER_DOWN_TTL', '90 DAY')
        self.table_ttl_column = os.getenv('CLICKHOUSE_BMP_PEER_DOWN_TTL_COLUMN', 'observation_time')
        self.partition_by = os.getenv(
            'CLICKHOUSE_BMP_PEER_DOWN_PARTITION_BY', 'toYYYYMMDD(observation_time)'
        )

        # observation_time maps to pbmpd `timestamp` — when the peer went down.
        self.column_defs.insert(0, ["observation_time", "DateTime64(3, 'UTC')", True])

        # Device / router identity
        self.column_defs.append(['device_id', 'LowCardinality(String)', True])
        self.column_defs.append(['device_ref', 'Nullable(String)', True])

        # BGP peer identity
        self.column_defs.append(['peer_ip', 'IPv6', True])
        self.column_defs.append(['peer_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_as_id', 'Nullable(UInt32)', True])
        self.column_defs.append(['peer_as_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_type', 'Nullable(UInt8)', True])

        # Route Distinguisher (https://tools.ietf.org/html/rfc7854#section-4.2)
        self.column_defs.append(['rd', 'Nullable(String)', True])

        # Reason for the session termination
        # reason_type: IANA BMP peer-down reason code
        #   https://www.iana.org/assignments/bmp-parameters/bmp-parameters.xhtml#peer-down-reason-codes
        self.column_defs.append(['reason_type', 'Nullable(UInt8)', True])
        self.column_defs.append(['reason_str', 'Nullable(String)', True])

        # Primary key: one event per (device, peer, timestamp).
        self.primary_keys = [
            'device_id',
            'peer_ip',
            'observation_time',
        ]
        self.order_by = self.primary_keys.copy()


class BaseBMPRouteProcessor(BaseDataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        # environment settings
        self.table = os.getenv('CLICKHOUSE_BMP_ROUTE_TABLE', 'data_bmp_route')
        self.table_ttl = os.getenv('CLICKHOUSE_BMP_ROUTE_TTL', '1 DAY')
        self.table_ttl_column = os.getenv('CLICKHOUSE_BMP_ROUTE_TTL_COLUMN', 'export_time')
        self.ip_to_as_lookup_order = os.getenv('CLICKHOUSE_BMP_ROUTE_IP_TO_AS_LOOKUP_ORDER', 'meta_ip').split(',')
        self.partition_by = os.getenv('CLICKHOUSE_BMP_ROUTE_PARTITION_BY', "toYYYYMMDD(export_time)")

        # add time fields to front of column names
        self.column_defs.insert(0, ["export_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.insert(1, ["observation_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.append(['device_id', 'LowCardinality(String)', True])
        self.column_defs.append(['device_ref', 'Nullable(String)', True])
        self.column_defs.append(['action', 'LowCardinality(String)', True])
        self.column_defs.append(['peer_ip', 'IPv6', True])
        self.column_defs.append(['peer_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['ip_prefix_address', 'IPv6', True])
        self.column_defs.append(['ip_prefix_length', 'UInt8', True])
        self.column_defs.append(['ip_prefix_range_start', 'IPv6', True])
        self.column_defs.append(['ip_prefix_range_end', 'IPv6', True])
        self.column_defs.append(['ip_prefix_ref', 'Nullable(String)', True])
        self.column_defs.append(['add_path_id', 'UInt32 DEFAULT 0', True])
        self.column_defs.append(['is_in', 'Bool', True])
        self.column_defs.append(['is_out', 'Bool', True])
        self.column_defs.append(['is_post', 'Bool', True])
        self.column_defs.append(['afi', 'Nullable(UInt8)', True])
        self.column_defs.append(['safi', 'Nullable(UInt8)', True])
        self.column_defs.append(['ip_version', 'Nullable(UInt8)', True])
        self.column_defs.append(['as_path_id', 'Array(UInt32)', True])
        self.column_defs.append(['as_path_ref', 'Array(Nullable(String))', True])
        self.column_defs.append(['bgp_nexthop', 'Nullable(IPv6)', True])
        self.column_defs.append(['bgp_nexthop_ref', 'Nullable(String)', True])
        self.column_defs.append(['community', 'Array(LowCardinality(String))', True])
        self.column_defs.append(['ext_community', 'Array(LowCardinality(String))', True])
        self.column_defs.append(['origin', 'LowCardinality(Nullable(String))', True])
        self.column_defs.append(['local_pref', 'Nullable(UInt32)', True])
        self.column_defs.append(['med', 'Nullable(UInt32)', True])
        self.column_defs.append(['rd', 'Nullable(String)', True])
        self.column_defs.append(['mpls_label', 'Nullable(UInt32)', True])
        self.column_defs.append(['aigp', 'Nullable(UInt64)', True])
        self.column_defs.append(['psid_li', 'Nullable(UInt64)', True])
        self.column_defs.append(['path_status', 'Array(LowCardinality(String))', True])
        self.column_defs.append(['reason_code', 'Array(LowCardinality(String))', True])
        self.primary_keys = ["device_id", "action", "peer_ip", "ip_prefix_range_start", "ip_prefix_range_end", "add_path_id", "export_time"]
        self.order_by = self.primary_keys.copy()
        self.load_materialized_views(
            'CLICKHOUSE_BMP_ROUTE_MATERIALIZED_VIEWS',
            MaterializedViewBMPRouteLatest,
            default='latest',
        )
        self.load_views(
            'CLICKHOUSE_BMP_ROUTE_VIEWS',
            {
                'adj_rib_in_pre': ViewBMPRouteLatestAdjRIBInPre,
                'adj_rib_in_post': ViewBMPRouteLatestAdjRIBInPost,
            },
            default='adj_rib_in_pre,adj_rib_in_post',
        )


class MaterializedViewBMPRouteLatest(BaseClickHouseMaterializedViewMixin):
    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        if agg_window and agg_window.lower() != 'latest':
            raise ValueError("MaterializedViewBMPRouteLatest only supports the 'latest' selector")
        super().__init__(source_table_name)
        self.table = 'data_bmp_route_latest'
        self.table_engine = 'AggregatingMergeTree'
        self.column_defs = [
            ['device_id', 'LowCardinality(String)', True],
            ['peer_ip', 'IPv6', True],
            ['peer_ip_ref', 'Nullable(String)', True],
            ['ip_prefix_address', 'IPv6', True],
            ['ip_prefix_length', 'UInt8', True],
            ['ip_prefix_range_start', 'IPv6', True],
            ['ip_prefix_range_end', 'IPv6', True],
            ['ip_prefix_ref', 'Nullable(String)', True],
            ['add_path_id', 'UInt32', True],
            ['is_post', 'Bool', True],
            ['action_state', "AggregateFunction(argMax, LowCardinality(String), DateTime64(3, 'UTC'))", True],
            ['export_time_state', "AggregateFunction(max, DateTime64(3, 'UTC'))", True],
            ['observation_time_state', "AggregateFunction(argMax, DateTime64(3, 'UTC'), DateTime64(3, 'UTC'))", True],
            ['bgp_nexthop_state', "AggregateFunction(argMax, Nullable(IPv6), DateTime64(3, 'UTC'))", True],
            ['as_path_id_state', "AggregateFunction(argMax, Array(UInt32), DateTime64(3, 'UTC'))", True],
            ['community_state', "AggregateFunction(argMax, Array(LowCardinality(String)), DateTime64(3, 'UTC'))", True],
            ['ext_community_state', "AggregateFunction(argMax, Array(LowCardinality(String)), DateTime64(3, 'UTC'))", True],
            ['origin_state', "AggregateFunction(argMax, LowCardinality(Nullable(String)), DateTime64(3, 'UTC'))", True],
            ['local_pref_state', "AggregateFunction(argMax, Nullable(UInt32), DateTime64(3, 'UTC'))", True],
            ['med_state', "AggregateFunction(argMax, Nullable(UInt32), DateTime64(3, 'UTC'))", True],
            ['rd_state', "AggregateFunction(argMax, Nullable(String), DateTime64(3, 'UTC'))", True],
            ['afi_state', "AggregateFunction(argMax, Nullable(UInt8), DateTime64(3, 'UTC'))", True],
            ['safi_state', "AggregateFunction(argMax, Nullable(UInt8), DateTime64(3, 'UTC'))", True],
            ['ip_version_state', "AggregateFunction(argMax, Nullable(UInt8), DateTime64(3, 'UTC'))", True],
            ['mpls_label_state', "AggregateFunction(argMax, Nullable(UInt32), DateTime64(3, 'UTC'))", True],
        ]
        self.primary_keys = [
            'device_id',
            'peer_ip',
            'peer_ip_ref',
            'ip_prefix_range_start',
            'ip_prefix_range_end',
            'ip_prefix_ref',
            'add_path_id',
            'is_post',
        ]
        self.order_by = self.primary_keys.copy()
        self.allow_nullable_key = True
        self.mv_name = 'data_bmp_route_latest_mv'
        self.mv_select_query = f"""
            SELECT
                device_id,
                peer_ip,
                peer_ip_ref,
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                ip_prefix_ref,
                add_path_id,
                is_post,
                argMaxState(action, export_time) AS action_state,
                maxState(export_time) AS export_time_state,
                argMaxState(observation_time, export_time) AS observation_time_state,
                argMaxState(bgp_nexthop, export_time) AS bgp_nexthop_state,
                argMaxState(as_path_id, export_time) AS as_path_id_state,
                argMaxState(community, export_time) AS community_state,
                argMaxState(ext_community, export_time) AS ext_community_state,
                argMaxState(origin, export_time) AS origin_state,
                argMaxState(local_pref, export_time) AS local_pref_state,
                argMaxState(med, export_time) AS med_state,
                argMaxState(rd, export_time) AS rd_state,
                argMaxState(afi, export_time) AS afi_state,
                argMaxState(safi, export_time) AS safi_state,
                argMaxState(ip_version, export_time) AS ip_version_state,
                argMaxState(mpls_label, export_time) AS mpls_label_state
            FROM {self.source_table_name}
            WHERE is_in = true
            GROUP BY
                device_id,
                peer_ip,
                peer_ip_ref,
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                ip_prefix_ref,
                add_path_id,
                is_post
        """


class _BaseBMPRouteLatestAdjRIBInView(BaseClickHouseViewMixin):
    def __init__(self, view_name: str, is_post: bool):
        super().__init__()
        self.view_name = view_name
        self.view_query = f"""
            SELECT
                device_id,
                peer_ip,
                peer_ip_ref,
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                ip_prefix_ref,
                add_path_id,
                argMaxMerge(action_state) AS action,
                maxMerge(export_time_state) AS export_time,
                argMaxMerge(observation_time_state) AS observation_time,
                argMaxMerge(bgp_nexthop_state) AS bgp_nexthop,
                argMaxMerge(as_path_id_state) AS as_path_id,
                argMaxMerge(community_state) AS community,
                argMaxMerge(ext_community_state) AS ext_community,
                argMaxMerge(origin_state) AS origin,
                argMaxMerge(local_pref_state) AS local_pref,
                argMaxMerge(med_state) AS med,
                argMaxMerge(rd_state) AS rd,
                argMaxMerge(afi_state) AS afi,
                argMaxMerge(safi_state) AS safi,
                argMaxMerge(ip_version_state) AS ip_version,
                argMaxMerge(mpls_label_state) AS mpls_label
            FROM data_bmp_route_latest
            WHERE is_post = {str(is_post).lower()}
            GROUP BY
                device_id,
                peer_ip,
                peer_ip_ref,
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                ip_prefix_ref,
                add_path_id
            HAVING argMaxMerge(action_state) = 'update'
        """


class ViewBMPRouteLatestAdjRIBInPre(_BaseBMPRouteLatestAdjRIBInView):
    def __init__(self):
        super().__init__('data_bmp_route_latest_adj_rib_in_pre_view', is_post=False)


class ViewBMPRouteLatestAdjRIBInPost(_BaseBMPRouteLatestAdjRIBInView):
    def __init__(self):
        super().__init__('data_bmp_route_latest_adj_rib_in_post_view', is_post=True)


