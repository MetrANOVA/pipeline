import orjson
import os
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.base import (
    BaseClickHouseMaterializedViewMixin,
    BaseClickHouseViewMixin,
    BaseDataProcessor,
)

class BaseBMPRouteProcessor(BaseDataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        # environment settings
        self.table = os.getenv('CLICKHOUSE_BMP_ROUTE_TABLE', 'data_bmp_route')
        self.table_ttl = os.getenv('CLICKHOUSE_BMP_ROUTE_TTL', '1 DAY')
        self.table_ttl_column = os.getenv('CLICKHOUSE_BMP_ROUTE_TTL_COLUMN', 'observation_time')
        self.ip_to_as_lookup_order = os.getenv('CLICKHOUSE_BMP_ROUTE_IP_TO_AS_LOOKUP_ORDER', 'meta_ip').split(',')
        self.partition_by = os.getenv('CLICKHOUSE_BMP_ROUTE_PARTITION_BY', "toYYYYMMDD(observation_time)")

        # add time fields to front of column names
        self.column_defs.insert(0, ["observation_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.insert(1, ["export_time", "DateTime64(3, 'UTC')", True])
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
        self.order_by = ["device_id", "action", "peer_ip", "ip_prefix_range_start", "ip_prefix_range_end", "add_path_id", "observation_time"]
        self.materialized_views.append(MaterializedViewBMPRouteLatest(source_table_name=self.table))
        self.views.extend([
            ViewBMPRouteLatestAdjRIBInPre(),
            ViewBMPRouteLatestAdjRIBInPost(),
        ])


class MaterializedViewBMPRouteLatest(BaseClickHouseMaterializedViewMixin):
    def __init__(self, source_table_name: str = ""):
        super().__init__(source_table_name)
        self.table = 'data_bmp_route_latest'
        self.table_engine = 'AggregatingMergeTree'
        self.column_defs = [
            ['device_id', 'LowCardinality(String)', True],
            ['peer_ip', 'IPv6', True],
            ['ip_prefix_address', 'IPv6', True],
            ['ip_prefix_length', 'UInt8', True],
            ['ip_prefix_range_start', 'IPv6', True],
            ['ip_prefix_range_end', 'IPv6', True],
            ['add_path_id', 'UInt32', True],
            ['is_post', 'Bool', True],
            ['action_state', "AggregateFunction(argMax, LowCardinality(String), DateTime64(3, 'UTC'))", True],
            ['observation_time_state', "AggregateFunction(max, DateTime64(3, 'UTC'))", True],
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
            'ip_prefix_range_start',
            'ip_prefix_range_end',
            'add_path_id',
            'is_post',
        ]
        self.order_by = self.primary_keys.copy()
        self.mv_name = 'data_bmp_route_latest_mv'
        self.mv_select_query = f"""
            SELECT
                device_id,
                peer_ip,
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                add_path_id,
                is_post,
                argMaxState(action, observation_time) AS action_state,
                maxState(observation_time) AS observation_time_state,
                argMaxState(bgp_nexthop, observation_time) AS bgp_nexthop_state,
                argMaxState(as_path_id, observation_time) AS as_path_id_state,
                argMaxState(community, observation_time) AS community_state,
                argMaxState(ext_community, observation_time) AS ext_community_state,
                argMaxState(origin, observation_time) AS origin_state,
                argMaxState(local_pref, observation_time) AS local_pref_state,
                argMaxState(med, observation_time) AS med_state,
                argMaxState(rd, observation_time) AS rd_state,
                argMaxState(afi, observation_time) AS afi_state,
                argMaxState(safi, observation_time) AS safi_state,
                argMaxState(ip_version, observation_time) AS ip_version_state,
                argMaxState(mpls_label, observation_time) AS mpls_label_state
            FROM {self.source_table_name}
            WHERE is_in = true
            GROUP BY
                device_id,
                peer_ip,
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
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
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                add_path_id,
                argMaxMerge(action_state) AS action,
                maxMerge(observation_time_state) AS observation_time,
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
                ip_prefix_address,
                ip_prefix_length,
                ip_prefix_range_start,
                ip_prefix_range_end,
                add_path_id
            HAVING argMaxMerge(action_state) = 'update'
        """


class ViewBMPRouteLatestAdjRIBInPre(_BaseBMPRouteLatestAdjRIBInView):
    def __init__(self):
        super().__init__('data_bmp_route_latest_adj_rib_in_pre_view', is_post=False)


class ViewBMPRouteLatestAdjRIBInPost(_BaseBMPRouteLatestAdjRIBInView):
    def __init__(self):
        super().__init__('data_bmp_route_latest_adj_rib_in_post_view', is_post=True)


