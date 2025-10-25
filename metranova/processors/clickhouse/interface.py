import hashlib
import logging
import orjson
import os
import re
from metranova.processors.clickhouse.base import BaseDataProcessor, BaseMetadataProcessor
from typing import Iterator, Dict, Any

logger = logging.getLogger(__name__)

class InterfaceMetadataProcessor(BaseMetadataProcessor):
    # Note does not use BaseInterfaceProcessor as sourced from redis not kafka-style
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'meta_if')
        self.val_id_field = ['data', 'id']
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            ref String,
            hash String,
            insert_time DateTime DEFAULT now(),
            description Nullable(String),
            device LowCardinality(Nullable(String)),
            id String,
            ifindex Nullable(UInt32),
            edge Bool,
            ipv4 Nullable(IPv4),
            ipv6 Nullable(IPv6),
            name Nullable(String),
            netflow_index Nullable(UInt32),
            peer_asn Nullable(UInt32),
            peer_ipv4 Nullable(IPv4),
            peer_ipv6 Nullable(IPv6),
            port_name Nullable(String),
            remote_device LowCardinality(Nullable(String)),
            remote_full_name LowCardinality(Nullable(String)),
            remote_id Nullable(String),
            remote_lldp_device LowCardinality(Nullable(String)),
            remote_lldp_port LowCardinality(Nullable(String)),
            remote_loc_name LowCardinality(Nullable(String)),
            remote_loc_type LowCardinality(Nullable(String)),
            remote_loc_lat Nullable(Decimal(8, 6)),
            remote_loc_lon Nullable(Decimal(9, 6)),
            remote_manufacturer LowCardinality(Nullable(String)),
            remote_model LowCardinality(Nullable(String)),
            remote_network LowCardinality(Nullable(String)),
            remote_os LowCardinality(Nullable(String)),
            remote_port LowCardinality(Nullable(String)),
            remote_role LowCardinality(Nullable(String)),
            remote_short_name LowCardinality(Nullable(String)),
            remote_state LowCardinality(Nullable(String)),
            site LowCardinality(Nullable(String)),
            speed Nullable(UInt32),
            visibility LowCardinality(Nullable(String)),
            vrtr_ifglobalindex Nullable(UInt32),
            vrtr_ifindex Nullable(String),
            vrtr_name LowCardinality(Nullable(String))
        ) ENGINE = MergeTree()
        ORDER BY (ref)
        """

        self.column_names = [
            'ref', 'hash', 'description', 'device', 'id', 'ifindex', 'edge',
            'ipv4', 'ipv6', 'name', 'netflow_index', 'peer_asn', 'peer_ipv4',
            'peer_ipv6', 'port_name', 'remote_device', 'remote_full_name',
            'remote_id', 'remote_lldp_device', 'remote_lldp_port', 'remote_loc_name',
            'remote_loc_type', 'remote_loc_lat', 'remote_loc_lon', 'remote_manufacturer',
            'remote_model', 'remote_network', 'remote_os', 'remote_port', 'remote_role',
            'remote_short_name', 'remote_state', 'site', 'speed', 'visibility',
            'vrtr_ifglobalindex', 'vrtr_ifindex', 'vrtr_name'
        ]

        self.required_fields = [ ['data', 'id'], ['data', 'device'], ['data', 'name'] ]

    def build_metadata_fields(self, value: dict) -> dict:
        formatted_record = {
            'description': value.get('data', {}).get('description', None),
            'device': value.get('data', {}).get('device', None),
            'ifindex': value.get('data', {}).get('ifindex', None),
            'edge': value.get('data', {}).get('edge', False),
            'ipv4': value.get('data', {}).get('ipv4', None),
            'ipv6': value.get('data', {}).get('ipv6', None),
            'name': value.get('data', {}).get('name', None),
            'netflow_index': value.get('data', {}).get('netflow_index', None),
            'peer_asn': value.get('data', {}).get('peer_asn', None),
            'peer_ipv4': value.get('data', {}).get('peer_ipv4', None),
            'peer_ipv6': value.get('data', {}).get('peer_ipv6', None),
            'port_name': value.get('data', {}).get('port_name', None),
            'remote_device': value.get('data', {}).get('remote_device', None),
            'remote_full_name': value.get('data', {}).get('remote_full_name', None),
            'remote_id': value.get('data', {}).get('remote_id', None),
            'remote_lldp_device': value.get('data', {}).get('remote_lldp_device', None),
            'remote_lldp_port': value.get('data', {}).get('remote_lldp_port', None),
            'remote_loc_name': value.get('data', {}).get('remote_loc_name', None),
            'remote_loc_type': value.get('data', {}).get('remote_loc_type', None),
            'remote_loc_lat': value.get('data', {}).get('remote_loc_lat', None),
            'remote_loc_lon': value.get('data', {}).get('remote_loc_lon', None),
            'remote_manufacturer': value.get('data', {}).get('remote_manufacturer', None),
            'remote_model': value.get('data', {}).get('remote_model', None),
            'remote_network': value.get('data', {}).get('remote_network', None),
            'remote_os': value.get('data', {}).get('remote_os', None),
            'remote_port': value.get('data', {}).get('remote_port', None),
            'remote_role': value.get('data', {}).get('remote_role', None),
            'remote_short_name': value.get('data', {}).get('remote_short_name', None),
            'remote_state': value.get('data', {}).get('remote_state', None),
            'site': value.get('data', {}).get('site', None),
            'speed': value.get('data', {}).get('speed', None),
            'visibility': value.get('data', {}).get('visibility', None),
            'vrtr_ifglobalindex': value.get('data', {}).get('vrtr_ifglobalindex', None),
            'vrtr_ifindex': value.get('data', {}).get('vrtr_ifindex', None),
            'vrtr_name': value.get('data', {}).get('vrtr_name', None)
        }

        #format boolean
        formatted_record['edge'] = formatted_record['edge'] in [True, 'true', 'True', 1, '1']

        #format floats
        float_fields = ['remote_loc_lat', 'remote_loc_lon']
        for field in float_fields:
            if formatted_record[field] is not None:
                try:
                    formatted_record[field] = float(formatted_record[field])
                except ValueError:
                    formatted_record[field] = None
        
        # format integers
        int_fields = ['ifindex', 'netflow_index', 'peer_asn', 'speed', 'vrtr_ifglobalindex']
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
        self.column_defs.insert(0, ["start_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.insert(1, ["end_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.extend([
            ['interface_id', 'String', True],
            ['interface_ref', 'Nullable(String)', True],
            ['admin_status', 'LowCardinality(Nullable(String))', True],
            ['oper_status', 'LowCardinality(Nullable(String))', True],
            ['in_bit_count', 'Nullable(UInt64)', True],
            ['in_packet_count', 'Nullable(UInt64)', True],
            ['in_discard_packet_count', 'Nullable(UInt64)', True],
            ['in_error_packet_count', 'Nullable(UInt64)', True],
            ['in_bcast_packet_count', 'Nullable(UInt64)', True],
            ['in_ucast_packet_count', 'Nullable(UInt64)', True],
            ['in_mcast_packet_count', 'Nullable(UInt64)', True],
            ['out_bit_count', 'Nullable(UInt64)', True],
            ['out_packet_count', 'Nullable(UInt64)', True],
            ['out_discard_packet_count', 'Nullable(UInt64)', True],
            ['out_error_packet_count', 'Nullable(UInt64)', True],
            ['out_bcast_packet_count', 'Nullable(UInt64)', True],
            ['out_ucast_packet_count', 'Nullable(UInt64)', True],
            ['out_mcast_packet_count', 'Nullable(UInt64)', True]
        ])
        self.table_engine = "CoalescingMergeTree"
        self.partition_by = 'toYYYYMMDD(start_time)'
        self.order_by = ['interface_id', 'policy_level', 'policy_scope', 'policy_originator', 'collector_id', 'start_time']
