#!/usr/bin/env python3

import unittest
import tempfile
import os
import yaml
from unittest.mock import MagicMock, patch

# Add the project root to Python path for imports
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.redis.telegraf import LookupTableProcessor


class TestLookupTableProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Create test YAML configuration
        self.test_config = {
            'snmp_if': {
                'resource_type': 'interface',
                'cache_lookup_table_builder': [
                    {
                        'name': 'ifindex_to_ifname',
                        'key': [
                            {
                                'type': 'field',
                                'path': ['tags', 'device']
                            },
                            {
                                'type': 'field', 
                                'path': ['fields', 'SNMP_IF-MIB::ifIndex']
                            }
                        ],
                        'value': {
                            'type': 'field',
                            'path': ['fields', 'SNMP_IF-MIB::ifName']
                        }
                    }
                ]
            },
            'snmp_timetra_sap': {
                'type': 'interface',
                'cache_lookup_table_builder': [
                    {
                        'name': 'sapindex_to_sapname',
                        'key': [
                            {
                                'type': 'field',
                                'path': ['tags', 'device']
                            },
                            {
                                'type': 'field',
                                'path': ['tags', 'oidIndex']
                            }
                        ],
                        'value': {
                            'type': 'field',
                            'format': 'tmnxsapid',
                            'path': ['tags', 'oidIndex']
                        }
                    }
                ]
            }
        }

    def create_temp_yaml_file(self, config):
        """Create a temporary YAML file with the given configuration."""
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
        yaml.dump(config, temp_file, default_flow_style=False)
        temp_file.close()
        return temp_file.name

    @patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': '/path/to/snmp_mappings.yml'})
    @patch('builtins.open')
    @patch('yaml.safe_load')
    def test_init_loads_yaml_successfully(self, mock_yaml_load, mock_open):
        """Test that YAML configuration is loaded successfully during initialization."""
        mock_yaml_load.return_value = self.test_config
        
        processor = LookupTableProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.rules, self.test_config)
        mock_open.assert_called_once_with('/path/to/snmp_mappings.yml', 'r')
        mock_yaml_load.assert_called_once()

    @patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': '/path/to/missing.yml'})
    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_init_handles_missing_yaml_file(self, mock_open):
        """Test that missing YAML file is handled gracefully."""
        
        processor = LookupTableProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.rules, {})

    @patch.dict(os.environ, {}, clear=True)  # Clear environment to test default
    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_init_uses_default_path_when_env_not_set(self, mock_open):
        """Test that default path is used when environment variable is not set."""
        
        processor = LookupTableProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.rules, {})
        mock_open.assert_called_once_with('/etc/metranova_pipeline/telegraf_mappings.yml', 'r')

    def test_get_field_value_simple_path(self):
        """Test _get_field_value with simple path."""
        with patch.object(LookupTableProcessor, '__init__', lambda x, y: None):
            processor = LookupTableProcessor(None)
            
            test_data = {
                'tags': {
                    'device': 'router1'
                }
            }
            
            result = processor._get_field_value(test_data, ['tags', 'device'])
            self.assertEqual(result, 'router1')

    def test_get_field_value_missing_path(self):
        """Test _get_field_value with missing path."""
        with patch.object(LookupTableProcessor, '__init__', lambda x, y: None):
            processor = LookupTableProcessor(None)
            
            test_data = {
                'tags': {
                    'device': 'router1'
                }
            }
            
            result = processor._get_field_value(test_data, ['tags', 'missing'])
            self.assertIsNone(result)

    def test_get_field_value_nested_path(self):
        """Test _get_field_value with deeply nested path."""
        with patch.object(LookupTableProcessor, '__init__', lambda x, y: None):
            processor = LookupTableProcessor(None)
            
            test_data = {
                'fields': {
                    'SNMP_IF-MIB::ifName': 'eth0'
                }
            }
            
            result = processor._get_field_value(test_data, ['fields', 'SNMP_IF-MIB::ifName'])
            self.assertEqual(result, 'eth0')

    def test_match_message_valid_snmp_if(self):
        """Test match_message with valid snmp_if data."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'name': 'snmp_if',
                    'tags': {
                        'device': 'router1'
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifIndex': 1,
                        'SNMP_IF-MIB::ifName': 'eth0'
                    }
                }
                
                result = processor.match_message(test_value)
                self.assertTrue(result)
        finally:
            os.unlink(temp_file)

    def test_match_message_missing_name(self):
        """Test match_message with missing name field."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'tags': {
                        'device': 'router1'
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifIndex': 1,
                        'SNMP_IF-MIB::ifName': 'eth0'
                    }
                }
                
                result = processor.match_message(test_value)
                self.assertFalse(result)
        finally:
            os.unlink(temp_file)

    def test_match_message_unknown_name(self):
        """Test match_message with unknown name."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'name': 'unknown_type',
                    'tags': {
                        'device': 'router1'
                    }
                }
                
                result = processor.match_message(test_value)
                self.assertFalse(result)
        finally:
            os.unlink(temp_file)

    def test_match_message_missing_key_field(self):
        """Test match_message with missing key field."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'name': 'snmp_if',
                    'tags': {
                        'device': 'router1'
                        # Missing ifIndex which is needed for key
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifName': 'eth0'
                    }
                }
                
                result = processor.match_message(test_value)
                self.assertFalse(result)
        finally:
            os.unlink(temp_file)

    def test_match_message_missing_value_field(self):
        """Test match_message with missing value field."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'name': 'snmp_if',
                    'tags': {
                        'device': 'router1'
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifIndex': 1
                        # Missing ifName which is needed for value
                    }
                }
                
                result = processor.match_message(test_value)
                self.assertFalse(result)
        finally:
            os.unlink(temp_file)

    def test_build_message_valid_snmp_if(self):
        """Test build_message with valid snmp_if data."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                processor.expires = 3600  # Set expires for testing
                
                test_value = {
                    'name': 'snmp_if',
                    'tags': {
                        'device': 'router1'
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifIndex': 1,
                        'SNMP_IF-MIB::ifName': 'eth0'
                    }
                }
                
                result = processor.build_message(test_value, {})
                
                self.assertEqual(len(result), 1)
                record = result[0]
                self.assertEqual(record['table'], 'ifindex_to_ifname')
                self.assertEqual(record['key'], 'router1::1')
                self.assertEqual(record['value'], 'eth0')
                self.assertEqual(record['expires'], 3600)
        finally:
            os.unlink(temp_file)

    def test_build_message_with_format(self):
        """Test build_message with format specification."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                processor.expires = 7200
                
                test_value = {
                    'name': 'snmp_timetra_sap',
                    'tags': {
                        'device': 'nokia1',
                        'oidIndex': '1.2.3.4'
                    }
                }
                
                result = processor.build_message(test_value, {})
                
                self.assertEqual(len(result), 1)
                record = result[0]
                self.assertEqual(record['table'], 'sapindex_to_sapname')
                self.assertEqual(record['key'], 'nokia1::1.2.3.4')
                self.assertEqual(record['value'], '1.2.3.4')
                self.assertEqual(record['expires'], 7200)
        finally:
            os.unlink(temp_file)

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    # Missing 'name' field
                    'tags': {
                        'device': 'router1'
                    }
                }
                
                result = processor.build_message(test_value, {})
                self.assertEqual(result, [])
        finally:
            os.unlink(temp_file)

    def test_build_message_missing_key_field(self):
        """Test build_message with missing key field."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'name': 'snmp_if',
                    'tags': {
                        'device': 'router1'
                        # Missing required field for key
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifName': 'eth0'
                    }
                }
                
                result = processor.build_message(test_value, {})
                self.assertEqual(result, [])
        finally:
            os.unlink(temp_file)

    def test_build_message_missing_value_field(self):
        """Test build_message with missing value field."""
        temp_file = self.create_temp_yaml_file(self.test_config)
        
        try:
            with patch.dict(os.environ, {'REDIS_TELEGRAF_MAPPINGS_PATH': temp_file}):
                processor = LookupTableProcessor(self.mock_pipeline)
                
                test_value = {
                    'name': 'snmp_if',
                    'tags': {
                        'device': 'router1'
                    },
                    'fields': {
                        'SNMP_IF-MIB::ifIndex': 1
                        # Missing ifName value field
                    }
                }
                
                result = processor.build_message(test_value, {})
                self.assertEqual(result, [])
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)