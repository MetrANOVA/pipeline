import logging
import yaml
import os
from metranova.processors.redis.base import BaseRedisProcessor

logger = logging.getLogger(__name__)

class LookupTableProcessor(BaseRedisProcessor):
    """
    Builds Redis lookup tables for mapping oidIndex from SNMP to id fields needed
    in clickhouse tables. It does this by reading in a yaml file that defines the
    mappings.
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.required_fields = [['name']]
        self.expires = int(os.getenv('REDIS_TELEGRAF_LOOKUP_TABLE_EXPIRES', '86400'))  # default 1 day

        # Load YAML configuration file
        yaml_path = os.getenv('TELEGRAF_MAPPINGS_PATH', '/app/conf/telegraf_mappings.yml')
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
    
    def _get_field_value(self, value: dict, path: list) -> any:
        """Extract field value from nested dict using path."""
        current = value
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current
    
    def match_message(self, value: dict) -> bool:
        """Match if name exists in rules and required paths are present."""
        # First check if we have required fields
        if not self.has_required_fields(value):
            return False
            
        name = value.get('name')
        if not name or name not in self.rules:
            return False
            
        rule = self.rules[name]
        cache_builders = rule.get('resource_lookup_tables', [])
        
        # Check if at least one cache lookup table builder has valid key and value paths
        for builder in cache_builders:
            # Check if value path exists
            value_config = builder.get('value', {})
            value_valid = True
            if value_config.get('type') == 'field':
                path = value_config.get('path', [])
                if self._get_field_value(value, path) is None:
                    value_valid = False
                    continue

            # Check if key paths exist
            key_configs = builder.get('key', [])
            key_valid = True
            for key_config in key_configs:
                if key_config.get('type') == 'field':
                    path = key_config.get('path', [])
                    if self._get_field_value(value, path) is None:
                        key_valid = False
                        break
            
            # If both key and value paths are valid for this builder, we match
            if key_valid and value_valid:
                return True
                
        return False
    
    def format_value(self, value: str, format_type: str | None) -> str:
        """Format value based on specified format type."""
        if format_type is None:
            return value
        if format_type == 'short_hostname':
            return value.split('.')[0]
        # Future: implement more formats as needed
        return value

    def build_message(self, value: dict, msg_metadata: dict) -> list[dict]:
        """Build lookup table entries from the message."""
        if not self.has_required_fields(value):
            return []
            
        name = value.get('name')
        if not name or name not in self.rules:
            return []
            
        rule = self.rules[name]
        cache_builders = rule.get('resource_lookup_tables', [])
        results = []
        
        for builder in cache_builders:
            table_name = builder.get('name')
            if not table_name:
                continue
                
            # Build key by concatenating key field values
            key_parts = []
            key_configs = builder.get('key', [])
            for key_config in key_configs:
                if key_config.get('type') == 'field':
                    path = key_config.get('path', [])
                    field_value = self._get_field_value(value, path)
                    if field_value is not None:
                        key_parts.append(self.format_value(str(field_value), key_config.get('format', None)))
                    else:
                        # If any key part is missing, skip this builder
                        break
            else:
                # Build value - else skips if break was hit in loop
                value_config = builder.get('value', {})
                lookup_value = None
                if value_config.get('type') == 'field':
                    path = value_config.get('path', [])
                    lookup_value = self._get_field_value(value, path)
                    
                if lookup_value is not None:
                    # Create the lookup table entry
                    formatted_record = {
                        "table": table_name,
                        "key": "::".join(key_parts),
                        "value": self.format_value(str(lookup_value), value_config.get('format', None)),
                        "expires": getattr(self, 'expires', None)
                    }
                    results.append(formatted_record)
                    self.logger.debug(f"Built lookup entry: {formatted_record}")
        
        return results