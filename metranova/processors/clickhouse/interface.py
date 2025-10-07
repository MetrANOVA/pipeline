import hashlib
import logging
import orjson
import os
import re
from metranova.processors.clickhouse.base import BaseClickHouseProcessor
from typing import Iterator, Dict, Any

logger = logging.getLogger(__name__)

class BaseInterfaceProcessor(BaseClickHouseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.meta_if_lookup_table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'meta_if')
        self.required_fields = [
            ['start'], 
            ['meta', 'id'], 
            ['meta', 'device']
        ]
        #set this to true in subclass if want to skip checking for meta.name field
        self.skip_required_name = False
        # Override in subclass
        self.table = None
        self.create_table_cmd = None
        self.column_names = None
    
    def has_required_fields(self, value):
        if not super().has_required_fields(value):
            return False
        
        #special case: check for missing name due to issue in source data
        if self.skip_required_name:
            return True
        if value.get('meta', {}).get('name', None) is None:
            meta_id = value.get('meta', {}).get('id', '')
            if '::' in meta_id:
                parts = meta_id.split('::', 1)
                if len(parts) == 2:
                    value.setdefault('meta', {})['name'] = parts[1]
        if value.get('meta', {}).get('name', None) is None:
            self.logger.error(f"Missing required field 'name' in message value and has no parsable meta.id: {value}")
            return False

        #everything is present so return true
        return True
    
class BaseInterfaceQueueProcessor(BaseInterfaceProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        #Override in subclass
        self.min_queue = 1
        self.max_queue = 16
        self.queue_values_fields = []

    def build_queue_value_columns(self):
        self.queue_values_fields.sort()
        #build column definition string for queue_value columns of type Nullable(UInt64) CODEC(Delta,ZSTD)
        self.queue_value_columns = ",\n            ".join([f"`{field}` Nullable(UInt64) CODEC(Delta,ZSTD)" for field in self.queue_values_fields])
       
    def has_queue_values(self, value):
        #check if has any queue between min and max
        for i in range(self.min_queue, self.max_queue + 1):
            if value.get('values', {}).get(f'queue{i}', None) is not None:
                return True
        return False

class InterfaceMetadataProcessor(BaseClickHouseProcessor):
    # Note does not use BaseInterfaceProcessor as sourced from redis not kafka-style
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
            self.logger.debug(f"Record {value['data']['id']} unchanged, skipping")
            return None
        elif cached_record:
            #change so update
            self.logger.info(f"Record {value['data']['id']} changed, updating")
            #get latest version number from end of existing ref suffix ov __v{version_num} which may be multiple digits
            latest_ref = cached_record.get('ref', '')
            #use regex to extract version number
            match = re.search(r'__v(\d+)$', latest_ref)
            if match:
                version_num = int(match.group(1)) + 1
                ref = "{}__v{}".format(value['data']['id'], version_num)
            else:
                # If no version found, log a warning and skip
                self.logger.warning(f"No version found in ref {latest_ref} for record {value['data']['id']}, skipping version increment")
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

class InterfaceTrafficProcessor(BaseInterfaceProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_BASE_TABLE', 'if_base')
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            `start_ts` DateTime64(3, 'UTC') CODEC(Delta,ZSTD),
            `insert_ts` DateTime64(3, 'UTC') DEFAULT now64() CODEC(Delta,ZSTD),
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `id` LowCardinality(String),
            `ref` Nullable(String),
            `device` LowCardinality(String),
            `name` LowCardinality(String),
            `collector_id` LowCardinality(Nullable(String)),
            `in_bits` Nullable(UInt64) CODEC(Delta,ZSTD),
            `in_pkts` Nullable(UInt64) CODEC(Delta,ZSTD),
            `in_errors` Nullable(UInt64) CODEC(Delta,ZSTD),
            `in_discards` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_bits` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_pkts` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_errors` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_discards` Nullable(UInt64) CODEC(Delta,ZSTD)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`id`, `start_ts`)
        TTL start_ts + INTERVAL 7 DAY
        SETTINGS index_granularity = 8192
        """

        self.column_names = [
            "start_ts",
            "policy_originator",
            "policy_level",
            "policy_scopes",
            "id",
            "ref",
            "device",
            "name",
            "collector_id",
            "in_bits",
            "in_pkts",
            "in_errors",
            "in_discards",
            "out_bits",
            "out_pkts",
            "out_errors",
            "out_discards"
        ]

    def match_message(self, value):
        #make sure we have at least one of values fields we care about
        values_fields = ['in_bits', 'in_pkts', 'in_errors', 'in_discards', 'out_bits', 'out_pkts', 'out_errors', 'out_discards']
        if not any(value.get('values', {}).get(f, {}).get("val", None) is not None for f in values_fields):
            self.logger.debug(f"Missing all values fields in message value")
            return False

        return True

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # Build message dictionary
        return [{
            "start_ts": value.get("start"),
            "policy_originator": value.get("policy", {}).get("originator", None),
            "policy_level": value.get("policy", {}).get("level", None),
            "policy_scopes": value.get("policy", {}).get("scopes", []),
            "id": value.get("meta", {}).get("id", None),
            "ref": self.pipeline.cacher.lookup(self.meta_if_lookup_table, value.get("meta", {}).get("id", None)) if self.pipeline.cacher else None,
            "device": value.get("meta", {}).get("device", None),
            "name": value.get("meta", {}).get("name", None),
            "collector_id": value.get("meta", {}).get("sensor_id", None)[0] if isinstance(value.get("meta", {}).get("sensor_id", None), list) else value.get("meta", {}).get("sensor_id", None),
            "in_bits": value.get("values", {}).get("in_bits", {}).get("val", None),
            "in_pkts": value.get("values", {}).get("in_pkts", {}).get("val", None),
            "in_errors": value.get("values", {}).get("in_errors", {}).get("val", None),
            "in_discards": value.get("values", {}).get("in_discards", {}).get("val", None),
            "out_bits": value.get("values", {}).get("out_bits", {}).get("val", None),
            "out_pkts": value.get("values", {}).get("out_pkts", {}).get("val", None),
            "out_errors": value.get("values", {}).get("out_errors", {}).get("val", None),
            "out_discards": value.get("values", {}).get("out_discards", {}).get("val", None)
        }]

class InterfacePortQueueProcessor(BaseInterfaceQueueProcessor):
     def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_PORT_QUEUE_TABLE', 'if_base')
        self.meta_if_lookup_table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'meta_if')
        self.min_queue = int(os.getenv('INTERFACE_PORT_MIN_QUEUE', '1'))
        self.max_queue = int(os.getenv('INTERFACE_PORT_MAX_QUEUE', '16'))
        self.queue_values_fields = [
            "in_inprof_dropped_bits",
            "in_inprof_dropped_pkts", 
            "in_inprof_fwd_bits",
            "in_inprof_fwd_pkts",
            "in_outprof_dropped_bits",
            "in_outprof_dropped_pkts",
            "in_outprof_fwd_bits",
            "in_outprof_fwd_pkts",
            "out_inprof_dropped_bits",
            "out_inprof_dropped_pkts",
            "out_inprof_fwd_bits", 
            "out_inprof_fwd_pkts",
            "out_outprof_dropped_bits",
            "out_outprof_dropped_pkts",
            "out_outprof_fwd_bits",
            "out_outprof_fwd_pkts"
        ]
        self.build_queue_value_columns()
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            `start_ts` DateTime64(3, 'UTC') CODEC(Delta,ZSTD),
            `insert_ts` DateTime64(3, 'UTC') DEFAULT now64() CODEC(Delta,ZSTD),
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `id` LowCardinality(String),
            `ref` Nullable(String),
            `device` LowCardinality(String),
            `name` LowCardinality(String),
            `queue_num` UInt8,
            `collector_id` LowCardinality(Nullable(String)),
            {self.queue_value_columns}
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`id`, `queue_num`, `start_ts`)
        TTL start_ts + INTERVAL 7 DAY
        SETTINGS index_granularity = 8192
        """

        self.column_names = [
            "start_ts",
            "policy_originator",
            "policy_level",
            "policy_scopes",
            "id",
            "ref",
            "device",
            "name",
            "queue_num",
            "collector_id"
        ]
        self.column_names.extend(self.queue_values_fields)

     def match_message(self, value):
        #make sure it is not service type field
        if value.get('meta', {}).get('service_type', None) is not None:
            return False

        return self.has_queue_values(value)
     
     def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # Build message dictionary
        for i in range(self.min_queue, self.max_queue + 1):
            queue_field = f'queue{i}'
            if value.get('values', {}).get(queue_field, None) is not None:
                msg = {
                    "start_ts": value.get("start"),
                    "policy_originator": value.get("policy", {}).get("originator", None),
                    "policy_level": value.get("policy", {}).get("level", None),
                    "policy_scopes": value.get("policy", {}).get("scopes", []),
                    "id": value.get("meta", {}).get("id", None),
                    "ref": self.pipeline.cacher.lookup(self.meta_if_lookup_table, value.get("meta", {}).get("id", None)) if self.pipeline.cacher else None,
                    "device": value.get("meta", {}).get("device", None),
                    "name": value.get("meta", {}).get("name", None),
                    "queue_num": i,
                    "collector_id": value.get("meta", {}).get("sensor_id", None)[0] if isinstance(value.get("meta", {}).get("sensor_id", None), list) else value.get("meta", {}).get("sensor_id", None)
                }
                for field in self.queue_values_fields:
                    if value.get('values', {}).get(queue_field, {}).get(field, {}).get("val", None) is not None:
                        msg[field] = value.get('values').get(queue_field).get(field)["val"]
                    else:
                        msg[field] = None
                yield msg

class InterfaceSAPQueueProcessor(BaseInterfaceQueueProcessor):
     def __init__(self, pipeline):
        super().__init__(pipeline)
        self.min_queue = int(os.getenv('INTERFACE_SAP_MIN_QUEUE', '1'))
        self.max_queue = int(os.getenv('INTERFACE_SAP_MAX_QUEUE', '16'))
        self.table = os.getenv('CLICKHOUSE_IF_SAP_QUEUE_TABLE', 'if_base')
        self.queue_values_fields = [
            "in_unicast_priority_hiprio_offered_pkts",
            "in_unicast_priority_hiprio_offered_bits",
            "in_unicast_priority_loprio_offered_pkts",
            "in_unicast_priority_loprio_offered_bits",
            "in_unicast_priority_hiprio_dropped_pkts",
            "in_unicast_priority_hiprio_dropped_bits",
            "in_unicast_priority_loprio_dropped_pkts",
            "in_unicast_priority_loprio_dropped_bits",
            "in_unicast_priority_inprof_fwd_pkts",
            "in_unicast_priority_inprof_fwd_bits",
            "in_unicast_priority_outprof_fwd_pkts",
            "in_unicast_priority_outprof_fwd_bits",
            "in_unicast_profile_color_in_offered_pkts",
            "in_unicast_profile_color_in_offered_bits",
            "in_unicast_profile_color_out_offered_pkts",
            "in_unicast_profile_color_out_offered_bits",
            "in_unicast_profile_uncolor_offered_pkts",
            "in_unicast_profile_uncolor_offered_bits",
            "in_unicast_profile_color_out_dropped_pkts",
            "in_unicast_profile_color_out_dropped_bits",
            "in_unicast_profile_color_in_uncolor_dropped_pkts",
            "in_unicast_profile_color_in_uncolor_dropped_bits",
            "in_unicast_inprofprof_fwd_pkts",
            "in_unicast_inprofprof_fwd_bits",
            "in_unicast_outprofprof_fwd_pkts",
            "in_unicast_outprofprof_fwd_bits",
            "in_multipoint_priority_hiprio_offered_pkts",
            "in_multipoint_priority_hiprio_offered_bits",
            "in_multipoint_priority_loprio_offered_pkts",
            "in_multipoint_priority_loprio_offered_bits",
            "in_multipoint_priority_combined_priority_offered_pkts",
            "in_multipoint_priority_combined_priority_offered_bits",
            "in_multipoint_priority_managed_priority_offered_pkts",
            "in_multipoint_priority_managed_priority_offered_bits",
            "in_multipoint_priority_hiprio_dropped_pkts",
            "in_multipoint_priority_hiprio_dropped_bits",
            "in_multipoint_priority_loprio_dropped_pkts",
            "in_multipoint_priority_loprio_dropped_bits",
            "in_multipoint_priority_inprof_fwd_pkts",
            "in_multipoint_priority_inprof_fwd_bits",
            "in_multipoint_priority_outprof_fwd_pkts",
            "in_multipoint_priority_outprof_fwd_bits",
            "in_multipoint_profile_color_in_offered_pkts",
            "in_multipoint_profile_color_in_offered_bits",
            "in_multipoint_profile_color_out_offered_pkts",
            "in_multipoint_profile_color_out_offered_bits",
            "in_multipoint_profile_uncolor_offered_pkts",
            "in_multipoint_profile_uncolor_offered_bits",
            "in_multipoint_profile_combined_offered_pkts",
            "in_multipoint_profile_combined_offered_bits",
            "in_multipoint_profile_managed_offered_pkts",
            "in_multipoint_profile_managed_offered_bits",
            "in_multipoint_profile_color_out_dropped_pkts",
            "in_multipoint_profile_color_out_dropped_bits",
            "in_multipoint_profile_color_in_uncolor_dropped_pkts",
            "in_multipoint_profile_color_in_uncolor_dropped_bits",
            "in_multipoint_inprofprof_fwd_pkts",
            "in_multipoint_inprofprof_fwd_bits",
            "in_multipoint_outprofprof_fwd_pkts",
            "in_multipoint_outprofprof_fwd_bits",
            "in_v4_v6_ipv4_offered_pkts",
            "in_v4_v6_ipv4_offered_bits",
            "in_v4_v6_ipv6_offered_pkts",
            "in_v4_v6_ipv6_offered_bits",
            "in_v4_v6_ipv4_dropped_pkts",
            "in_v4_v6_ipv4_dropped_bits",
            "in_v4_v6_ipv6_dropped_pkts",
            "in_v4_v6_ipv6_dropped_bits",
            "in_v4_v6_ipv4_fwd_pkts",
            "in_v4_v6_ipv4_fwd_bits",
            "in_v4_v6_ipv6_fwd_pkts",
            "in_v4_v6_ipv6_fwd_bits",
            "out_inprof_inplus_profile_dropped_bits",
            "out_inprof_inplus_profile_dropped_pkts",
            "out_inprof_inplus_profile_fwd_bits",
            "out_inprof_inplus_profile_fwd_pkts",
            "out_outprof_exceed_profile_dropped_bits",
            "out_outprof_exceed_profile_dropped_pkts",
            "out_outprof_exceed_profile_fwd_bits",
            "out_outprof_exceed_profile_fwd_pkts",
            "qos_sap_in_queue_hardware_queue_source_card",
            "qos_sap_in_queue_hardware_queue_source_fp", 
            "qos_sap_in_queue_hardware_queue_source_tap_offset",
            #"qos_sap_in_queue_hardware_queue_source_port",
            "qos_sap_in_queue_hardware_queue_dest_card",
            "qos_sap_in_queue_hardware_queue_dest_fp",
            "qos_sap_in_queue_hardware_queue_dest_tap_offset",
            "qos_sap_in_queue_hardware_queue_adapted_admin_mbs",
            "qos_sap_in_queue_hardware_queue_adapted_admin_cbs",
            "qos_sap_in_queue_hardware_queue_operational_mbs",
            "qos_sap_in_queue_hardware_queue_depth",
            "qos_sap_in_queue_hardware_queue_operational_cir",
            "qos_sap_in_queue_hardware_queue_operational_fir",
            #"qos_sap_in_queue_hardware_queue_operational_pir",
            "qos_sap_in_queue_hardware_queue_exceed_droptail",
            "qos_sap_in_queue_hardware_queue_high_droptail",
            "qos_sap_in_queue_hardware_queue_high_plus_droptail",
            "qos_sap_in_queue_hardware_queue_low_droptail",
            "qos_sap_in_queue_hardware_queue_operational_burst_fir"
        ]
        self.build_queue_value_columns()
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            `start_ts` DateTime64(3, 'UTC') CODEC(Delta,ZSTD),
            `insert_ts` DateTime64(3, 'UTC') DEFAULT now64() CODEC(Delta,ZSTD),
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `id` LowCardinality(String),
            `ref` Nullable(String),
            `device` LowCardinality(String),
            `name` LowCardinality(String),
            `queue_num` UInt8,
            `collector_id` LowCardinality(Nullable(String)),
            {self.queue_value_columns}
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`id`, `queue_num`, `start_ts`)
        TTL start_ts + INTERVAL 7 DAY
        SETTINGS index_granularity = 8192
        """

        self.column_names = [
            "start_ts",
            "policy_originator",
            "policy_level",
            "policy_scopes",
            "id",
            "ref",
            "device",
            "name",
            "queue_num",
            "collector_id"
        ]
        self.column_names.extend(self.queue_values_fields)

     def match_message(self, value):
        #check if has a service type field
        if value.get('meta', {}).get('service_type', None) is None:
            return False

        return self.has_queue_values(value)
     
     def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # Build message dictionary
        for i in range(self.min_queue, self.max_queue + 1):
            queue_field = f'queue{i}'
            if value.get('values', {}).get(queue_field, None) is not None:
                msg = {
                    "start_ts": value.get("start"),
                    "policy_originator": value.get("policy", {}).get("originator", None),
                    "policy_level": value.get("policy", {}).get("level", None),
                    "policy_scopes": value.get("policy", {}).get("scopes", []),
                    "id": value.get("meta", {}).get("id", None),
                    "ref": self.pipeline.cacher.lookup(self.meta_if_lookup_table, value.get("meta", {}).get("id", None)) if self.pipeline.cacher else None,
                    "device": value.get("meta", {}).get("device", None),
                    "name": value.get("meta", {}).get("name", None),
                    "queue_num": i,
                    "collector_id": value.get("meta", {}).get("sensor_id", None)[0] if isinstance(value.get("meta", {}).get("sensor_id", None), list) else value.get("meta", {}).get("sensor_id", None)
                }
                for field in self.queue_values_fields:
                    if value.get('values', {}).get(queue_field, {}).get(field, {}).get("val", None) is not None:
                        msg[field] = value.get('values').get(queue_field).get(field)["val"]
                    else:
                        msg[field] = None
                yield msg