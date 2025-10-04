from metranova.pipelines import BaseClickHouseProcessor
from typing import Iterator, Dict, Any
import os

class InterfaceSAPQueueProcessor(BaseClickHouseProcessor):
     def __init__(self, pipeline):
        self.min_queue = int(os.getenv('INTERFACE_SAP_MIN_QUEUE', '1'))
        self.max_queue = int(os.getenv('INTERFACE_SAP_MAX_QUEUE', '16'))
        super().__init__(pipeline)
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
        #sort the fields alphabetically for easier reading of create table statement
        self.queue_values_fields.sort()
        
        #build column definition string for queue_value columns of type Nullable(UInt64) CODEC(Delta,ZSTD)
        queue_value_columns = ",\n            ".join([f"`{field}` Nullable(UInt64) CODEC(Delta,ZSTD)" for field in self.queue_values_fields])
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            `start_ts` DateTime64(3, 'UTC') CODEC(Delta,ZSTD),
            `insert_ts` DateTime64(3, 'UTC') DEFAULT now64() CODEC(Delta,ZSTD),
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `id` LowCardinality(String),
            `device` LowCardinality(String),
            `name` LowCardinality(String),
            `queue_num` UInt8,
            `collector_id` LowCardinality(Nullable(String)),
            {queue_value_columns}
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
            "device",
            "name",
            "queue_num",
            "collector_id"
        ]
        self.column_names.extend(self.queue_values_fields)

        self.required_fields = [
            ['start'], 
            ['meta', 'id'], 
            ['meta', 'device'], 
            # name should be too but found some without, so handedle in build_message
        ]


     def match_message(self, value):
        #check if has a service type field
        if value.get('meta', {}).get('service_type', None) is None:
            return False

        #check if has any queue between min and max
        has_queue = False
        for i in range(1,self.max_queue + 1):
            if value.get('values', {}).get(f'queue{i}', None) is not None:
                has_queue = True
                break

        return has_queue
     
     def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # handle special case for name. If missing parse from meta.id which is a string of form <device>::<name>
        if value.get('meta', {}).get('name', None) is None:
            meta_id = value.get('meta', {}).get('id', '')
            if '::' in meta_id:
                parts = meta_id.split('::', 1)
                if len(parts) == 2:
                    value.setdefault('meta', {})['name'] = parts[1]
        
        # double check name is now present
        if value.get('meta', {}).get('name', None) is None:
            self.logger.error(f"Missing required field 'name' in message value and has no parsable meta.id")
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