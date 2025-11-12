import logging
import os
import re

import orjson
import yaml

from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
from metranova.processors.clickhouse.interface import BaseInterfaceTrafficProcessor
from metranova.utils.snmp import TmnxPortId

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
            "admin_status": {"field": "SNMP_IF-MIB::ifAdminStatus"},
            "oper_status": {"field": "SNMP_IF-MIB::ifOperStatus"},
            "in_bcast_packet_count": {"field": 'SNMP_IF-MIB::ifHCInBroadcastPkts'},
            "in_mcast_packet_count": {"field": 'SNMP_IF-MIB::ifHCInMulticastPkts'},
            "in_bit_count": {"field": 'SNMP_IF-MIB::ifHCInOctets', "scale": 8},
            "in_ucast_packet_count": {"field": 'SNMP_IF-MIB::ifHCInUcastPkts'},
            "out_bcast_packet_count": {"field": 'SNMP_IF-MIB::ifHCOutBroadcastPkts'},
            "out_mcast_packet_count": {"field": 'SNMP_IF-MIB::ifHCOutMulticastPkts'},
            "out_bit_count": {"field": 'SNMP_IF-MIB::ifHCOutOctets', "scale": 8},
            "out_ucast_packet_count": {"field": 'SNMP_IF-MIB::ifHCOutUcastPkts'},
            "in_discard_packet_count": {"field": 'SNMP_IF-MIB::ifInDiscards'},
            "in_error_packet_count": {"field": 'SNMP_IF-MIB::ifInErrors'},
            "out_discard_packet_count": {"field": 'SNMP_IF-MIB::ifOutDiscards'},
            "out_error_packet_count": {"field": 'SNMP_IF-MIB::ifOutErrors'}
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
            if value.get('fields', {}).get(field['field'], None) is not None:
                return True

        return False

    def scale_value(self, value, scale):
        """Scale numeric value by scale factor."""
        if value is None:
            return None
        try:
            # note if we had float values this would round but only expect ints here
            return int(value) * scale
        except (ValueError, TypeError):
            self.logger.warning(f"Non-numeric value encountered for scaling: {value}")
            return None

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
        for target_name, telegraf_mapping in self.metric_fields.items():
            formatted_record[target_name] = value.get("fields", {}).get(telegraf_mapping["field"], None)
            if "scale" in telegraf_mapping:
                formatted_record[target_name] = self.scale_value(formatted_record[target_name], telegraf_mapping["scale"])

        #map admin_status and oper_status to string values
        admin_status_map = {1: 'up', 2: 'down', 3: 'testing'}
        if formatted_record['admin_status'] in admin_status_map:
            formatted_record['admin_status'] = admin_status_map[formatted_record['admin_status']]
        oper_status_map = {1: 'up', 2: 'down', 3: 'testing', 4: 'unknown', 5: 'dormant', 6: 'notPresent', 7: 'lowerLayerDown'}
        if formatted_record['oper_status'] in oper_status_map:
            formatted_record['oper_status'] = oper_status_map[formatted_record['oper_status']]

        return [formatted_record]

class DataGenericMetricProcessor(BaseDataGenericMetricProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.required_fields = [
            ["timestamp"], 
            ["fields"],
            ["tags"]
        ]
        #tags to always skip - these are added to skip_tags in rules
        self.default_skip_tags = ["date", "routing_tag", "collector"]
        # Load YAML configuration file
        yaml_path = os.getenv('TELEGRAF_MAPPINGS_PATH', '/etc/metranova_pipeline/telegraf_mappings.yml')
        try:
            with open(yaml_path, 'r') as file:
                self.rules = yaml.safe_load(file)
                self.logger.info(f"Loaded mappings from {yaml_path}")
        except FileNotFoundError:
            self.logger.error(f"Mappings file not found at {yaml_path}")
            self.rules = {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file {yaml_path}: {e}")
            self.rules = {}
        for rule_name, rule in self.rules.items():
            resource_type = rule.get('resource_type', None)
            if resource_type is None:
                self.logger.warning(f"Rule {rule_name} missing resource_type, skipping")
                continue
            # add to resource types if not already present
            if resource_type not in self.resource_types:
                self.resource_types.append(resource_type)
        #rebuild table name map
        self.table_name_map = {}
        self.resource_type_map = {}
        for resource_type in self.resource_types:
            self.resource_type_map[resource_type] = True
            for metric_type in self.metric_types:
                table_name = self.get_table_name(resource_type, metric_type)
                self.table_name_map[table_name] = True
    
    def match_message(self, value):
        val_name = value.get('name', None)
        if val_name is None:
            return False
        if self.rules.get(val_name, None) is None:
            return False
        if self.rules[val_name].get('resource_type', None) is None:
            return False
        #make sure it is a known resource type
        if self.resource_type_map.get(self.rules[val_name]['resource_type'], None) is None:
            return False
        return super().match_message(value)

    def format_value(self, value: str, format_type: str | None, format_opts: dict | None, ext: dict) -> str:
        """Format value based on specified format type."""
        if format_type is None:
            return value
        if format_type == 'short_hostname':
            return value.split('.')[0]
        elif format_type == 'tmnxsapid':
            formatted_val = TmnxPortId.decode_sap(value, ext)
            if formatted_val is not None:
                return formatted_val
        elif format_type == 'tmnxportid':
            formatted_val = TmnxPortId.decode(value)
            if formatted_val is not None:
                return formatted_val
        elif format_type == 'regex':
            # this is a regex pattern with named groups - we want to extract the named group "value"
            # any additional groups should be added to the dict ext
            regex_pattern = format_opts.get('pattern', None)
            if regex_pattern is None:
                return value
            match = re.match(regex_pattern, value)
            if not match:
                return value
            # Extract the named group "value"
            #if not present, return original value
            if "value" not in match.groupdict():
                return value
            extracted_value = match.group("value")
            # Add any additional named groups to the ext dict
            add_ext = {k: v for k, v in match.groupdict().items() if k != "value"}
            ext.update(add_ext)
            return extracted_value
            
        # Future: implement more formats as needed
        return value

    def lookup_value(self, value: str, prefix_parts: list[str], lookup_table: str | None) -> str:
        """Lookup value in specified lookup table."""
        if lookup_table is None:
            return value
        lookup_key = "::".join([*prefix_parts, value])
        looked_up = self.pipeline.cacher("redis").lookup(lookup_table, lookup_key)
        return looked_up if looked_up is not None else value

    def find_resource_id(self, value, rule, ext):
        #check rule
        if rule is None:
            return None
        #get rules for building resource id
        resource_id_rules = rule.get('resource_id', [])
        if not resource_id_rules:
            #check for default rules
            resource_id_rules = self.rules.get("_default", {}).get('resource_id', [])
        if not resource_id_rules:
            return None
        #build resource id parts
        resource_id_parts = []
        for rid_def in resource_id_rules:
            if rid_def.get('type') == 'field':
                # extract value from path
                current = value
                for key in rid_def.get('path', []):
                    if not isinstance(current, dict) or key not in current:
                        current = None
                        break
                    current = current[key]
                if current is not None:
                    # apply formatting if specified
                    format_type = rid_def.get('format', None)
                    current = self.format_value(str(current), format_type, rid_def.get('format_opts', None), ext)
                    lookup_table = rid_def.get('lookup', None)
                    current = self.lookup_value(current, resource_id_parts, lookup_table)
                    resource_id_parts.append(current)

        return '::'.join(resource_id_parts)

    def find_resource_ref(self, resource_id, rule):
        # check rule
        if rule is None:
            return None
        # get lookup table for resource ref
        lookup_table = rule.get('resource_ref', None)
        if lookup_table is None:
            return None
        # lookup resource ref
        return self.pipeline.cacher("redis").lookup(lookup_table, resource_id)

    def build_message(self, value, msg_metadata):
        # check required fields
        if not self.has_required_fields(value):
            return []

        #get rule for this telegraf name
        name = value.get('name', '')
        rule = self.rules.get(name, None)
        if rule is None:
            self.logger.debug(f"No rule found for telegraf name: {name}")
            return []

        #lookup resource id
        ext = {} # this may get additional fields during ID formatting
        resource_id = self.find_resource_id(value, rule, ext)
        if resource_id is None:
            self.logger.debug("Resource ID could not be determined from message")
            return []

        #lookup resource ref
        resource_ref = self.find_resource_ref(resource_id, rule)

        #grab the timestamp
        try:
            observation_time = int(value.get('timestamp', 0)) * 1000
        except (ValueError, TypeError):
            self.logger.error("Invalid timestamp format")
            return []

        #build ext from tags
        skip_tags = rule.get('skip_tags', []) + self.default_skip_tags
        skip_tags_map = {tag: True for tag in skip_tags}
        for tag_key, tag_value in value.get("tags", {}).items():
            if skip_tags_map.get(tag_key, False):
                continue
            ext[tag_key] = tag_value
        # build json here to save from doing it multiple times
        ext_json = orjson.dumps(ext).decode('utf-8')

        # build table name
        resource_type = rule.get('resource_type', None)
        if resource_type is None:
            self.logger.debug("No resource_type defined in rule, cannot determine table name")
            return []

        # loop through fields and build messages
        formatted_records = []
        for field, field_value in value.get("fields", {}).items():
            # we may have to change this so create var local to loop
            tmp_ext_json = ext_json
            table_name = None
            # If it is an int, make it a counter, if it is a float, make it a gauge, otherwise make it a counter by setting value to 1 and adding value to ext
            if isinstance(field_value, float):
                table_name = self.get_table_name(resource_type, 'gauge')
            elif not isinstance(field_value, int):
                # For non-numeric, set to 1 and add original value to ext
                table_name = self.get_table_name(resource_type, 'counter')
                tmp_ext = ext.copy()
                tmp_ext["metric_value"] = field_value
                tmp_ext_json = orjson.dumps(tmp_ext).decode('utf-8')
                field_value = 1
            else:
                table_name = self.get_table_name(resource_type, 'counter')

            # verify table name is valid
            if table_name not in self.table_name_map:
                self.logger.debug(f"Table name {table_name} not recognized, skipping field {field}")
                continue

            # build formatted record
            formatted_record = {
                "_clickhouse_table": table_name,
                "observation_time": observation_time,
                "collector_id": value.get("tags", {}).get("collector", "unknown"),
                "policy_originator": self.policy_originator,
                "policy_level": self.policy_level,
                "policy_scope": self.policy_scope,
                "ext": tmp_ext_json,
                "id": resource_id,
                "ref": resource_ref,
                "metric_name": field,
                "metric_value": field_value
            }
            formatted_records.append(formatted_record)

        return formatted_records