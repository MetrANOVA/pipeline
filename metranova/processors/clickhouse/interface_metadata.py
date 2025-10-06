import hashlib
import logging
import os
import re
import orjson
from metranova.processors.clickhouse.base import BaseClickHouseProcessor


logger = logging.getLogger(__name__)

class InterfaceMetadataProcessor(BaseClickHouseProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'if_meta')
        self.force_update = os.getenv('CLICKHOUSE_IF_METADATA_FORCE_UPDATE', 'false').lower() in ['true', '1', 'yes']
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            ref String,
            hash String,
            insert_ts DateTime DEFAULT now(),
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
    
    def build_message(self, value: dict, msg_metadata: dict) -> list:
        # check required fields
        if not self.has_required_fields(value):
            return None

        #convert record to json, sort keys, and get md5 hash
        value_json = orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_md5 = hashlib.md5(value_json.encode('utf-8')).hexdigest()
        
        #determine ref and if we need new record
        ref = "{}__v1".format(value['data']['id'])
        cached_record = self.pipeline.cacher.lookup(self.table, value['data']['id']) if self.pipeline.cacher else None
        if not self.force_update and cached_record and cached_record['hash'] == record_md5:
            logger.debug(f"Record {value['data']['id']} unchanged, skipping")
            return None
        elif cached_record:
            #change so update
            logger.info(f"Record {value['data']['id']} changed, updating")
            #get latest version number from end of existing ref suffix ov __v{version_num} which may be multiple digits
            latest_ref = cached_record.get('ref', '')
            #use regex to extract version number
            match = re.search(r'__v(\d+)$', latest_ref)
            if match:
                version_num = int(match.group(1)) + 1
                ref = "{}__v{}".format(value['data']['id'], version_num)
            else:
                # If no version found, log a warning and skip
                logger.warning(f"No version found in ref {latest_ref} for record {value['data']['id']}, skipping version increment")
                return None
    
        #init hash
        formatted_record = {
            'ref': ref,
            'hash': record_md5,
            'description': value.get('data', {}).get('description', None),
            'device': value.get('data', {}).get('device', None),
            'id': value.get('data', {}).get('id', None),
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
        return [formatted_record]
