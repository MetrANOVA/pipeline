import logging
import orjson
import os
from metranova.processors.clickhouse.base import BaseDataProcessor, BaseMetadataProcessor

logger = logging.getLogger(__name__)

class InterfaceMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'meta_interface')
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
            ['lag_member_interface_ref', 'Array(String)', True],
            ['port_interface_id', 'LowCardinality(Nullable(String))', True],
            ['port_interface_ref', 'Nullable(String)', True],
            ['remote_interface_id', 'LowCardinality(Nullable(String))', True],
            ['remote_interface_ref', 'Nullable(String)', True],
            ['remote_organization_id', 'LowCardinality(Nullable(String))', True],
            ['remote_organization_ref', 'Nullable(String)', True]
        ])
        self.val_id_field = ['data', 'id']
        self.required_fields = [ ['data', 'id'], ['data', 'device_id'], ['data', 'name'], ['data', 'type'] ]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)
        value_data = value.get('data', {})

        # lookup ref fields from redis cacher
        formatted_record.update({
            "device_ref": self.pipeline.cacher("redis").lookup("meta_device", value_data.get('device_id', None)),
            "peer_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value_data.get('peer_as_id', None)),
            "port_interface_ref": self.pipeline.cacher("redis").lookup("meta_interface", value_data.get('port_interface_id', None)),
            "remote_interface_ref": self.pipeline.cacher("redis").lookup("meta_interface", value_data.get('remote_interface_id', None)),
            "remote_organization_ref": self.pipeline.cacher("redis").lookup("meta_organization", value_data.get('remote_organization_id', None))
        })

        #format potential JSON strings from redis
        json_array_fields = ['circuit_id', 'lag_member_interface_id']
        for field in json_array_fields:
            if formatted_record[field] is None:
                formatted_record[field] = []
            elif isinstance(formatted_record[field], str):
                try:
                    formatted_record[field] = orjson.loads(formatted_record[field])
                except orjson.JSONDecodeError:
                    formatted_record[field] = []
        #now lookup refs for json_array_fields
        array_refs = ['circuit', 'lag_member_interface']
        for field in array_refs:
            formatted_record[f"{field}_ref"] = [self.pipeline.cacher("redis").lookup("meta_interface", iid) for iid in formatted_record[f"{field}_id"]]

        #format boolean
        formatted_record['edge'] = formatted_record['edge'] in [True, 'true', 'True', 1, '1']
        
        # format integers
        int_fields = ['flow_index', 'speed', 'peer_as_id']
        for field in int_fields:
            if formatted_record[field] is not None:
                try:
                    formatted_record[field] = int(formatted_record[field])
                except ValueError:
                    formatted_record[field] = None

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
