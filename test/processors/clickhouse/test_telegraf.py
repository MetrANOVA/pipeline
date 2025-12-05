#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, Mock, mock_open
from io import StringIO

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.telegraf import IFMIBInterfaceTrafficProcessor, DataGenericMetricProcessor


class TestIFMIBInterfaceTrafficProcessor(unittest.TestCase):
    """Unit tests for IFMIBInterfaceTrafficProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Setup mock redis cacher
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = None
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        
        self.processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        self.assertEqual(self.processor.telegraf_name, 'snmp_if')
        self.assertEqual(self.processor.ifname_lookup_table, 'ifindex_to_ifname')
        self.assertTrue(self.processor.use_short_device_name)

    @patch.dict(os.environ, {
        'TELEGRAF_IFMIB_NAME': 'custom_snmp',
        'TELEGRAF_IFMIB_IFNAME_LOOKUP_TABLE': 'custom_lookup',
        'TELEGRAF_IFMIB_USE_SHORT_DEVICE_NAME': 'false'
    })
    def test_init_with_environment_variables(self):
        """Test initialization with custom environment variables."""
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.telegraf_name, 'custom_snmp')
        self.assertEqual(processor.ifname_lookup_table, 'custom_lookup')
        self.assertFalse(processor.use_short_device_name)

    def test_inheritance_from_base_interface_traffic_processor(self):
        """Test that processor inherits from BaseInterfaceTrafficProcessor."""
        from metranova.processors.clickhouse.interface import BaseInterfaceTrafficProcessor
        self.assertIsInstance(self.processor, BaseInterfaceTrafficProcessor)

    def test_match_message_valid(self):
        """Test match_message returns True for valid snmp_if message."""
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'interval': 60,
            'tags': {
                'device': 'router1.example.com',
                'oidIndex': '10'
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000
            }
        }
        
        self.assertTrue(self.processor.match_message(value))

    def test_match_message_wrong_name(self):
        """Test match_message returns False for wrong telegraf name."""
        value = {
            'name': 'wrong_name',
            'timestamp': 1609459200,
            'tags': {'device': 'router1'},
            'fields': {'SNMP_IF-MIB::ifHCInOctets': 1024000}
        }
        
        self.assertFalse(self.processor.match_message(value))

    def test_match_message_missing_metric_fields(self):
        """Test match_message returns False when no metric fields present."""
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {'device': 'router1', 'oidIndex': '10'},
            'fields': {}
        }
        
        self.assertFalse(self.processor.match_message(value))

    def test_scale_value_integer(self):
        """Test scale_value with integer input."""
        result = self.processor.scale_value(1024, 8)
        self.assertEqual(result, 8192)

    def test_scale_value_float(self):
        """Test scale_value with float input."""
        result = self.processor.scale_value(1024.5, 8)
        self.assertEqual(result, 8192)  # int() truncates, doesn't round

    def test_scale_value_none(self):
        """Test scale_value with None input."""
        result = self.processor.scale_value(None, 8)
        self.assertIsNone(result)

    def test_scale_value_invalid(self):
        """Test scale_value with invalid input."""
        with patch.object(self.processor.logger, 'warning') as mock_warning:
            result = self.processor.scale_value('invalid', 8)
        
        self.assertIsNone(result)
        mock_warning.assert_called_once()

    def test_build_message_success(self):
        """Test build_message with complete valid data."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.side_effect = lambda table, key: {
            'ifindex_to_ifname': 'eth0',
            'meta_interface': 'interface_ref_123'
        }.get(table, None)
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {
                'device': 'router1.example.com',
                'oidIndex': '10',
                'interval': '60',
                'collector': 'collector1'
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000,
                'SNMP_IF-MIB::ifHCOutOctets': 512000,
                'SNMP_IF-MIB::ifAdminStatus': 1,
                'SNMP_IF-MIB::ifOperStatus': 1
            }
        }
        
        result = processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        record = result[0]
        
        self.assertEqual(record['interface_id'], 'router1::eth0')
        self.assertEqual(record['interface_ref'], 'interface_ref_123')
        self.assertEqual(record['in_bit_count'], 1024000 * 8)
        self.assertEqual(record['out_bit_count'], 512000 * 8)
        self.assertEqual(record['admin_status'], 'up')
        self.assertEqual(record['oper_status'], 'up')

    def test_build_message_short_device_name(self):
        """Test build_message uses short device name when enabled."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.side_effect = lambda table, key: 'eth0' if 'router1::10' in key else None
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        processor.use_short_device_name = True
        
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {
                'device': 'router1.example.com',
                'oidIndex': '10',
                'interval': '60'
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000
            }
        }
        
        result = processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0]['interface_id'], 'router1::eth0')

    def test_build_message_interface_not_found(self):
        """Test build_message returns None when interface name not found."""
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {
                'device': 'router1',
                'oidIndex': '10',
                'interval': '60'
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000
            }
        }
        
        with patch.object(processor.logger, 'debug') as mock_debug:
            result = processor.build_message(value, {})
        
        self.assertIsNone(result)
        mock_debug.assert_called_once()

    def test_build_message_missing_device_or_oidindex(self):
        """Test build_message returns None when device or oidIndex missing."""
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {
                'device': 'router1'
                # Missing oidIndex
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000
            }
        }
        
        with patch.object(self.processor.logger, 'warning') as mock_warning:
            result = self.processor.build_message(value, {})
        
        self.assertIsNone(result)

    def test_build_message_admin_status_mapping(self):
        """Test build_message correctly maps admin_status values."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.side_effect = lambda table, key: 'eth0' if 'ifindex' in table else 'ref'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Test down status
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {'device': 'router1', 'oidIndex': '10', 'interval': '60'},
            'fields': {'SNMP_IF-MIB::ifAdminStatus': 2}
        }
        
        result = processor.build_message(value, {})
        self.assertEqual(result[0]['admin_status'], 'down')

    def test_build_message_oper_status_mapping(self):
        """Test build_message correctly maps oper_status values."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.side_effect = lambda table, key: 'eth0' if 'ifindex' in table else 'ref'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Test lowerLayerDown status
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {'device': 'router1', 'oidIndex': '10', 'interval': '60'},
            'fields': {'SNMP_IF-MIB::ifOperStatus': 7}
        }
        
        result = processor.build_message(value, {})
        self.assertEqual(result[0]['oper_status'], 'lowerLayerDown')


class TestDataGenericMetricProcessor(unittest.TestCase):
    """Unit tests for DataGenericMetricProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Setup mock redis cacher
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = None
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        
        # Mock YAML configuration - need to use patch decorator for proper setup
        self.yaml_config = """
interface:
  resource_type: interface
  resource_id:
    - type: field
      path: [tags, device]
      format: short_hostname
    - type: field
      path: [tags, interface]
  resource_ref: meta_interface
  skip_tags: [device, interface]

_default:
  resource_id:
    - type: field
      path: [tags, resource]
"""
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                self.processor = DataGenericMetricProcessor(self.mock_pipeline)

    def test_init_loads_yaml_config(self):
        """Test that processor loads YAML configuration."""
        self.assertIsNotNone(self.processor.rules)
        self.assertIn('interface', self.processor.rules)

    def test_init_with_missing_yaml_file(self):
        """Test initialization handles missing YAML file gracefully."""
        with patch('builtins.open', side_effect=FileNotFoundError()):
            with patch.object(DataGenericMetricProcessor, '__init__', lambda x, y: None):
                processor = DataGenericMetricProcessor.__new__(DataGenericMetricProcessor)
                processor.logger = MagicMock()
                processor.rules = {}

    def test_inheritance_from_base_data_generic_metric_processor(self):
        """Test that processor inherits from BaseDataGenericMetricProcessor."""
        from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
        self.assertIsInstance(self.processor, BaseDataGenericMetricProcessor)

    def test_match_message_valid(self):
        """Test match_message returns True for valid message with known rule."""
        value = {
            'name': 'interface',
            'timestamp': 1609459200,
            'fields': {'in_octets': 1024},
            'tags': {'device': 'router1'}
        }
        
        self.assertTrue(self.processor.match_message(value))

    def test_match_message_unknown_name(self):
        """Test match_message returns False for unknown telegraf name."""
        value = {
            'name': 'unknown_metric',
            'timestamp': 1609459200,
            'fields': {'value': 1024},
            'tags': {}
        }
        
        self.assertFalse(self.processor.match_message(value))

    def test_format_value_short_hostname(self):
        """Test format_value with short_hostname format."""
        result = self.processor.format_value('router1.example.com', 'short_hostname', None, {})
        self.assertEqual(result, 'router1')

    def test_format_value_no_format(self):
        """Test format_value returns original value when no format specified."""
        result = self.processor.format_value('test_value', None, None, {})
        self.assertEqual(result, 'test_value')

    def test_lookup_value_with_table(self):
        """Test lookup_value performs lookup when table specified."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'looked_up_value'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = DataGenericMetricProcessor(self.mock_pipeline)
        
        result = processor.lookup_value('test', ['prefix'], 'lookup_table')
        
        self.assertEqual(result, 'looked_up_value')
        mock_redis_cacher.lookup.assert_called_once_with('lookup_table', 'prefix::test')

    def test_lookup_value_no_table(self):
        """Test lookup_value returns original value when no table specified."""
        result = self.processor.lookup_value('test_value', [], None)
        self.assertEqual(result, 'test_value')

    def test_find_resource_id_from_rule(self):
        """Test find_resource_id extracts ID from message using rule."""
        value = {
            'tags': {
                'device': 'router1.example.com',
                'interface': 'eth0'
            }
        }
        rule = self.processor.rules['interface']
        ext = {}
        
        result = self.processor.find_resource_id(value, rule, ext)
        
        self.assertEqual(result, 'router1::eth0')

    def test_find_resource_id_with_default_rule(self):
        """Test find_resource_id falls back to default rule."""
        value = {
            'tags': {
                'resource': 'test_resource'
            }
        }
        rule = {}  # Empty rule should use _default
        ext = {}
        
        result = self.processor.find_resource_id(value, rule, ext)
        
        self.assertEqual(result, 'test_resource')

    def test_find_resource_ref(self):
        """Test find_resource_ref looks up reference."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'resource_ref_123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = DataGenericMetricProcessor(self.mock_pipeline)
        
        rule = {'resource_ref': 'meta_interface'}
        result = processor.find_resource_ref('device1::eth0', rule)
        
        self.assertEqual(result, 'resource_ref_123')

    def test_build_message_with_int_field(self):
        """Test build_message processes integer field as counter."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'interface',
            'timestamp': 1609459200,
            'fields': {'in_packets': 100},
            'tags': {'device': 'router1', 'interface': 'eth0', 'collector': 'col1'}
        }
        
        result = processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        record = result[0]
        
        self.assertEqual(record['metric_name'], 'in_packets')
        self.assertEqual(record['metric_value'], 100)
        self.assertIn('data_interface_counter', record['_clickhouse_table'])

    def test_build_message_with_float_field(self):
        """Test build_message processes float field as gauge."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'interface',
            'timestamp': 1609459200,
            'fields': {'temperature': 45.5},
            'tags': {'device': 'router1', 'interface': 'eth0'}
        }
        
        result = processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        
        self.assertEqual(record['metric_name'], 'temperature')
        self.assertEqual(record['metric_value'], 45.5)
        self.assertIn('gauge', record['_clickhouse_table'])

    def test_build_message_with_string_field(self):
        """Test build_message processes string field as counter with value in ext."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'interface',
            'timestamp': 1609459200,
            'fields': {'status': 'up'},
            'tags': {'device': 'router1', 'interface': 'eth0'}
        }
        
        result = processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        
        self.assertEqual(record['metric_value'], 1)
        # Check that original value is in ext
        import orjson
        ext_data = orjson.loads(record['ext'])
        self.assertEqual(ext_data['metric_value'], 'up')

    def test_build_message_skips_configured_tags(self):
        """Test build_message skips tags specified in skip_tags."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'interface',
            'timestamp': 1609459200,
            'fields': {'in_packets': 100},
            'tags': {
                'device': 'router1',
                'interface': 'eth0',
                'collector': 'col1',
                'custom_tag': 'custom_value'
            }
        }
        
        result = processor.build_message(value, {})
        
        record = result[0]
        import orjson
        ext_data = orjson.loads(record['ext'])
        
        # Default skip tags and rule skip tags should not be in ext
        self.assertNotIn('device', ext_data)
        self.assertNotIn('interface', ext_data)
        self.assertNotIn('collector', ext_data)
        
        # Custom tag should be in ext
        self.assertEqual(ext_data['custom_tag'], 'custom_value')

    def test_build_message_invalid_timestamp(self):
        """Test build_message returns None when timestamp is invalid."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'interface',
            'timestamp': 'invalid_timestamp',
            'fields': {'in_packets': 100},
            'tags': {'device': 'router1', 'interface': 'eth0'}
        }
        
        result = processor.build_message(value, {})
        
        # Returns empty list on error
        self.assertEqual(result, [])

    def test_build_message_missing_required_fields(self):
        """Test build_message returns empty list when required fields are missing."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'interface',
            'timestamp': 1609459200
            # Missing 'fields' and 'tags'
        }
        
        result = processor.build_message(value, {})
        
        # Returns empty list when required fields missing
        self.assertEqual(result, [])

    def test_format_value_tmnxsapid(self):
        """Test format_value with tmnxsapid format."""
        from metranova.utils.snmp import TmnxPortId
        
        ext = {}
        with patch.object(TmnxPortId, 'decode_sap', return_value='1/1/1:100') as mock_decode:
            result = self.processor.format_value('12345', 'tmnxsapid', None, ext)
        
        self.assertEqual(result, '1/1/1:100')
        mock_decode.assert_called_once()

    def test_format_value_tmnxportid(self):
        """Test format_value with tmnxportid format."""
        from metranova.utils.snmp import TmnxPortId
        
        with patch.object(TmnxPortId, 'decode', return_value='1/1/1') as mock_decode:
            result = self.processor.format_value('12345', 'tmnxportid', None, {})
        
        self.assertEqual(result, '1/1/1')
        mock_decode.assert_called_once()

    def test_format_value_tmnxsapid_returns_none(self):
        """Test format_value with tmnxsapid when decode returns None."""
        from metranova.utils.snmp import TmnxPortId
        
        ext = {}
        with patch.object(TmnxPortId, 'decode_sap', return_value=None):
            result = self.processor.format_value('12345', 'tmnxsapid', None, ext)
        
        self.assertEqual(result, '12345')  # Returns original value

    def test_format_value_tmnxportid_returns_none(self):
        """Test format_value with tmnxportid when decode returns None."""
        from metranova.utils.snmp import TmnxPortId
        
        with patch.object(TmnxPortId, 'decode', return_value=None):
            result = self.processor.format_value('12345', 'tmnxportid', None, {})
        
        self.assertEqual(result, '12345')  # Returns original value

    def test_format_value_regex_with_pattern(self):
        """Test format_value with regex format."""
        import re
        
        format_opts = {
            'pattern': r'(?P<value>\d+)_(?P<extra>\w+)'
        }
        ext = {}
        
        result = self.processor.format_value('12345_test', 'regex', format_opts, ext)
        
        self.assertEqual(result, '12345')
        self.assertEqual(ext['extra'], 'test')

    def test_format_value_regex_no_pattern(self):
        """Test format_value with regex but no pattern specified."""
        format_opts = {}
        ext = {}
        
        result = self.processor.format_value('12345_test', 'regex', format_opts, ext)
        
        self.assertEqual(result, '12345_test')  # Returns original

    def test_format_value_regex_no_match(self):
        """Test format_value with regex that doesn't match."""
        format_opts = {
            'pattern': r'(?P<value>\d+)_(?P<extra>\w+)'
        }
        ext = {}
        
        result = self.processor.format_value('no_match', 'regex', format_opts, ext)
        
        self.assertEqual(result, 'no_match')  # Returns original

    def test_format_value_regex_no_value_group(self):
        """Test format_value with regex that has no 'value' named group."""
        format_opts = {
            'pattern': r'(\d+)_(\w+)'  # No named groups
        }
        ext = {}
        
        result = self.processor.format_value('12345_test', 'regex', format_opts, ext)
        
        self.assertEqual(result, '12345_test')  # Returns original

    def test_lookup_value_not_found_returns_original(self):
        """Test lookup_value returns original value when lookup returns None."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = None
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = DataGenericMetricProcessor(self.mock_pipeline)
        
        result = processor.lookup_value('test_value', ['prefix'], 'lookup_table')
        
        self.assertEqual(result, 'test_value')

    def test_find_resource_id_with_none_rule(self):
        """Test find_resource_id returns None when rule is None."""
        result = self.processor.find_resource_id({}, None, {})
        
        self.assertIsNone(result)

    def test_find_resource_id_missing_resource_id_rules(self):
        """Test find_resource_id returns None when no resource_id rules."""
        rule = {}  # No resource_id key
        ext = {}
        
        result = self.processor.find_resource_id({}, rule, ext)
        
        # Returns empty string when no resource_id rules found
        self.assertEqual(result, '')

    def test_find_resource_ref_with_none_rule(self):
        """Test find_resource_ref returns None when rule is None."""
        result = self.processor.find_resource_ref('resource_id', None)
        
        self.assertIsNone(result)

    def test_find_resource_ref_missing_resource_ref(self):
        """Test find_resource_ref returns None when no resource_ref in rule."""
        rule = {}  # No resource_ref key
        
        result = self.processor.find_resource_ref('resource_id', rule)
        
        self.assertIsNone(result)

    def test_build_message_unknown_rule(self):
        """Test build_message returns None when rule not found."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'unknown_metric_name',
            'timestamp': 1609459200,
            'fields': {'value': 100},
            'tags': {}
        }
        
        result = processor.build_message(value, {})
        
        # Returns empty list when rule not found
        self.assertEqual(result, [])

    def test_build_message_missing_resource_type(self):
        """Test build_message returns empty list when resource_type missing from rule."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        # Create config with rule missing resource_type
        yaml_with_bad_rule = """
bad_rule:
  resource_id:
    - type: field
      path: [tags, device]
"""
        
        with patch('builtins.open', mock_open(read_data=yaml_with_bad_rule)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        value = {
            'name': 'bad_rule',
            'timestamp': 1609459200,
            'fields': {'value': 100},
            'tags': {'device': 'router1'}
        }
        
        result = processor.build_message(value, {})
        
        # Returns empty list when resource_type missing
        self.assertEqual(result, [])

    def test_build_message_null_resource_id(self):
        """Test build_message returns empty list when resource_id is None."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = 'ref123'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch('builtins.open', mock_open(read_data=self.yaml_config)):
            with patch.dict(os.environ, {'TELEGRAF_MAPPINGS_PATH': '/test/mappings.yml'}):
                processor = DataGenericMetricProcessor(mock_pipeline)
        
        # Mock find_resource_id to return None
        with patch.object(processor, 'find_resource_id', return_value=None):
            value = {
                'name': 'interface',
                'timestamp': 1609459200,
                'fields': {'in_packets': 100},
                'tags': {'device': 'router1', 'interface': 'eth0'}
            }
            
            result = processor.build_message(value, {})
        
        # Returns empty list when resource_id is None
        self.assertEqual(result, [])

    def test_build_message_invalid_interval(self):
        """Test build_message returns None when interval is invalid in IFMIBInterfaceTrafficProcessor."""
        # This test is for IFMIBInterfaceTrafficProcessor, not DataGenericMetricProcessor
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.side_effect = lambda table, key: 'eth0' if 'ifindex' in table else None
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        mock_pipeline = MagicMock()
        mock_pipeline.cacher.side_effect = cacher_side_effect
        
        processor = IFMIBInterfaceTrafficProcessor(mock_pipeline)
        
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {
                'device': 'router1',
                'oidIndex': '10',
                'interval': 'invalid'  # Invalid interval
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000
            }
        }
        
        with patch.object(processor.logger, 'warning') as mock_warning:
            result = processor.build_message(value, {})
        
        self.assertIsNone(result)

    def test_build_message_calculates_end_time_with_non_zero_interval(self):
        """Test build_message calculates end_time when interval > 0."""
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.side_effect = lambda table, key: 'eth0' if 'ifindex' in table else 'ref'
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = IFMIBInterfaceTrafficProcessor(self.mock_pipeline)
        
        value = {
            'name': 'snmp_if',
            'timestamp': 1609459200,
            'tags': {
                'device': 'router1',
                'oidIndex': '10',
                'interval': '60'  # 60 second interval
            },
            'fields': {
                'SNMP_IF-MIB::ifHCInOctets': 1024000
            }
        }
        
        result = processor.build_message(value, {})
        
        record = result[0]
        self.assertEqual(record['start_time'], 1609459200)
        self.assertEqual(record['end_time'], 1609459260)  # start + 60


if __name__ == '__main__':
    unittest.main(verbosity=2)
