import logging
import orjson
import os
from metranova.processors.clickhouse.base import BaseDataProcessor, BaseMetadataProcessor

logger = logging.getLogger(__name__)

class InterfaceMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'meta_interface')
        self.boolean_fields = ['edge']
        self.int_fields = ['flow_index', 'speed', 'peer_as_id']
        self.array_fields = ['circuit_id', 'lag_member_interface_id']
        self.column_defs.extend([
            ['type', 'LowCardinality(String)', True],
            ['description', 'Nullable(String)', True],
            ['device_id', 'String', True],
            ['device_ref', 'Nullable(String)', True],
            ['edge', 'Bool', True],
            ['flow_index', 'Nullable(UInt32)', True],
            ['ipv4', 'Nullable(IPv4)', True],
            ['ipv6', 'Nullable(IPv6)', True],
            ['name', 'String', True],
            ['speed', 'Nullable(UInt64)', True],
            ['circuit_id', 'Array(String)', True],
            ['circuit_ref', 'Array(Nullable(String))', True],
            ['peer_as_id', 'Nullable(UInt32)', True],
            ['peer_as_ref', 'Nullable(String)', True],
            ['peer_interface_ipv4', 'Nullable(IPv4)', True],
            ['peer_interface_ipv6', 'Nullable(IPv6)', True],
            ['lag_member_interface_id', 'Array(LowCardinality(String))', True],
            ['lag_member_interface_ref', 'Array(Nullable(String))', True],
            ['port_interface_id', 'LowCardinality(Nullable(String))', True],
            ['port_interface_ref', 'Nullable(String)', True],
            ['remote_interface_id', 'LowCardinality(Nullable(String))', True],
            ['remote_interface_ref', 'Nullable(String)', True],
            ['remote_organization_id', 'LowCardinality(Nullable(String))', True],
            ['remote_organization_ref', 'Nullable(String)', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['device_id'], ['name'], ['type']]
        self.self_ref_fields = ['port_interface', 'remote_interface', 'lag_member_interface']

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        # lookup ref fields from redis cacher
        ref_fields = [
            ('device', 'device'), 
            ('peer_as', 'as'), 
            ('port_interface', 'interface'), 
            ('remote_interface', 'interface'), 
            ('remote_organization', 'organization')
        ]
        for ref_field, cacher_table in ref_fields:
            ref_id = formatted_record.get(f'{ref_field}_id', None)
            cached_info = self.pipeline.cacher("clickhouse").lookup(f"meta_{cacher_table}", ref_id)
            if cached_info:
                formatted_record[f"{ref_field}_ref"] = cached_info.get(self.db_ref_field, None)
            else:
                formatted_record[f"{ref_field}_ref"] = None

        #now lookup refs for json_array_fields
        formatted_record["circuit_ref"] = []
        for cid in formatted_record["circuit_id"]:
            cached_child_info = self.pipeline.cacher("clickhouse").lookup("meta_circuit", cid)
            if not cached_child_info:
                formatted_record["circuit_ref"].append(None)
            else:
                formatted_record["circuit_ref"].append(cached_child_info.get(self.db_ref_field, None))
        formatted_record["lag_member_interface_ref"] = []
        for lid in formatted_record["lag_member_interface_id"]:
            cached_parent_info = self.pipeline.cacher("clickhouse").lookup("meta_interface", lid)
            if not cached_parent_info:
                formatted_record["lag_member_interface_ref"].append(None)
            else:
                formatted_record["lag_member_interface_ref"].append(cached_parent_info.get(self.db_ref_field, None))

        #build a hash with all the keys and values from value['data']
        return formatted_record

class BaseInterfaceTrafficProcessor(BaseDataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_TRAFFIC_TABLE', 'data_interface_traffic')
        self.column_defs.insert(0, ["start_time", "DateTime('UTC') CODEC(Delta,ZSTD)", True])
        self.column_defs.insert(1, ["end_time", "DateTime('UTC') CODEC(Delta,ZSTD)", True])
        self.column_defs.extend([
            ['interface_id', 'String', True],
            ['interface_ref', 'Nullable(String)', True],
            ['admin_status', 'LowCardinality(Nullable(String))', True],
            ['oper_status', 'LowCardinality(Nullable(String))', True],
            ['in_bit_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['in_discard_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['in_error_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['in_bcast_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['in_ucast_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['in_mcast_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['out_bit_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['out_discard_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['out_error_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['out_bcast_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['out_ucast_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True],
            ['out_mcast_packet_count', 'Nullable(UInt64) CODEC(Delta,ZSTD)', True]
        ])
        self.table_engine = "CoalescingMergeTree"
        self.partition_by = 'toYYYYMMDD(start_time)'
        self.order_by = ['interface_id', 'policy_level', 'policy_scope', 'policy_originator', 'collector_id', 'start_time']
