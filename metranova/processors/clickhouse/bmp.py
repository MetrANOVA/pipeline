import orjson
import os
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.base import BaseClickHouseMaterializedViewMixin, BaseDataProcessor

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
        self.column_defs.append(['ip_prefix', 'Tuple(IPv6,UInt8)', True])
        self.column_defs.append(['ip_prefix_ref', 'Nullable(String)', True])
        self.column_defs.append(['bgp_nexthop', 'Nullable(IPv6)', True])
        self.column_defs.append(['bgp_nexthop_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_ip', 'Nullable(IPv6)', True])
        self.column_defs.append(['peer_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['is_in', 'Bool', True])
        self.column_defs.append(['is_out', 'Bool', True])
        self.column_defs.append(['is_post', 'Bool', True])
        self.column_defs.append(['afi', 'Nullable(UInt8)', True])
        self.column_defs.append(['safi', 'Nullable(UInt8)', True])
        self.column_defs.append(['ip_version', 'Nullable(UInt8)', True])
        self.column_defs.append(['as_path_id', 'Array(UInt32)', True])
        self.column_defs.append(['as_path_ref', 'Array(Nullable(String))', True])
        self.column_defs.append(['community', 'Array(LowCardinality(String))', True])
        self.column_defs.append(['ext_community', 'Array(LowCardinality(String))', True])
        self.column_defs.append(['origin', 'LowCardinality(Nullable(String))', True])
        self.column_defs.append(['local_pref', 'Nullable(UInt32)', True])
        self.column_defs.append(['med', 'Nullable(UInt32)', True])
        self.column_defs.append(['rd', 'Nullable(String)', True])
        self.column_defs.append(['add_path_id', 'Nullable(UInt8)', True])
        self.column_defs.append(['mpls_label', 'Nullable(UInt32)', True])
        self.column_defs.append(['aigp', 'Nullable(UInt64)', True])
        self.column_defs.append(['psid_li', 'Nullable(UInt64)', True])
        self.column_defs.append(['path_status', 'Array(LowCardinality(String))', True])
        self.column_defs.append(['reason_code', 'Array(LowCardinality(String))', True])
        self.order_by = ["device_id", "action", "ip_prefix", "observation_time"]


