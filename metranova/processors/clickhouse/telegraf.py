import logging
import os

from metranova.processors.clickhouse.interface import BaseInterfaceTrafficProcessor

logger = logging.getLogger(__name__)

class IFMIBInterfaceTrafficProcessor(BaseInterfaceTrafficProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.telegraf_name = os.getenv('TELEGRAF_IFMIB_NAME', 'snmp_if')
        self.ifname_lookup_table = os.getenv('TELEGRAF_IFMIB_IFNAME_LOOKUP_TABLE', 'ifindex_to_ifname')
        self.use_short_device_name = os.getenv('TELEGRAF_IFMIB_USE_SHORT_DEVICE_NAME', 'true').lower() in ('true', '1', 'yes')
        self.match_fields = [
            ["name"], 
            ["timestamp"],
            ["interval"],
            ["tags", "device"], 
            ["tags", "oidIndex"]
        ]
        self.metric_fields = {
            "admin_status": "SNMP_IF-MIB::ifAdminStatus",
            "oper_status": "SNMP_IF-MIB::ifOperStatus",
            "in_bcast_packet_count": 'SNMP_IF-MIB::ifHCInBroadcastPkts',
            "in_mcast_packet_count": 'SNMP_IF-MIB::ifHCInMulticastPkts',
            "in_bit_count": 'SNMP_IF-MIB::ifHCInOctets',
            "in_ucast_packet_count": 'SNMP_IF-MIB::ifHCInUcastPkts',
            "out_bcast_packet_count": 'SNMP_IF-MIB::ifHCOutBroadcastPkts',
            "out_mcast_packet_count": 'SNMP_IF-MIB::ifHCOutMulticastPkts',
            "out_bit_count": 'SNMP_IF-MIB::ifHCOutOctets',
            "out_ucast_packet_count": 'SNMP_IF-MIB::ifHCOutUcastPkts',
            "in_discard_packet_count": 'SNMP_IF-MIB::ifInDiscards',
            "in_error_packet_count": 'SNMP_IF-MIB::ifInErrors',
            "out_discard_packet_count": 'SNMP_IF-MIB::ifOutDiscards',
            "out_error_packet_count": 'SNMP_IF-MIB::ifOutErrors'
        }

    def match_message(self, value):
        #first check for basic existence of match fields
        if not self.has_match_field(value):
            return False
        
        # now check if name is snmp_if
        if value.get('name', '') != self.telegraf_name:
            return False
        
        #make sure it has at least one metric field
        for field in self.metric_fields.values():
            if field in value.get('fields', {}):
                return True

        return False

    def build_message(self, value: dict, msg_metadata: dict) -> list[dict]:
        # check required fields
        if not self.has_required_fields(value):
            return None
        
        #parse timestamp and interval to get start_time and end_time
        try:
            interval = int(value.get("tags", {}).get("interval", 0))
            start_time = int(value.get('timestamp', 0))
            if interval > 0:
                #round down to nearest interval
                start_time = (start_time // interval) * interval
            end_time = start_time + interval
        except (ValueError, TypeError):
            self.logger.warning("Invalid timestamp or interval")
            return None
        
        # Lookup interface name based on oidIndex and device
        device = value.get('tags', {}).get('device', None)
        oid_index = value.get('tags', {}).get('oidIndex', None)
        if device is None or oid_index is None:
            self.logger.warning("Missing device or oidIndex in tags")
            return None
        if self.use_short_device_name:
            device = str(device).split('.')[0]
        interface_name = self.pipeline.cacher("redis").lookup(self.ifname_lookup_table, f"{device}::{oid_index}")
        if interface_name is None:
            self.logger.debug(f"Interface name not found for device {device} and oidIndex {oid_index}")
            return None
        # Build interface_id and interface_ref
        interface_id = f"{device}::{interface_name}"
        interface_ref = self.pipeline.cacher("redis").lookup("meta_interface", interface_id)
        
        # Build the message
        formatted_record = {
            "start_time": start_time,
            "end_time": end_time,
            "collector_id": value.get("tags", {}).get("collector", "unknown"),
            "policy_originator": self.policy_originator,
            "policy_level": self.policy_level,
            "policy_scope": self.policy_scope,
            "ext": '{}',
            "interface_id": interface_id,
            "interface_ref": interface_ref
        }
        #add metric fields - set missing to null and CoalescingMergeTree will handle it
        for target_name, telegraf_name in self.metric_fields.items():
            formatted_record[target_name] = value.get("fields", {}).get(telegraf_name, None)

        #map admin_status and oper_status to string values
        admin_status_map = {1: 'up', 2: 'down', 3: 'testing'}
        if formatted_record['admin_status'] in admin_status_map:
            formatted_record['admin_status'] = admin_status_map[formatted_record['admin_status']]
        oper_status_map = {1: 'up', 2: 'down', 3: 'testing', 4: 'unknown', 5: 'dormant', 6: 'notPresent', 7: 'lowerLayerDown'}
        if formatted_record['oper_status'] in oper_status_map:
            formatted_record['oper_status'] = oper_status_map[formatted_record['oper_status']]

        return [formatted_record]