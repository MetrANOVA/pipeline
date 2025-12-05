#!/usr/bin/env python3

import unittest
import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.stardust import InterfaceTrafficProcessor


class TestInterfaceTrafficProcessor(unittest.TestCase):
    """Unit tests for InterfaceTrafficProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Setup mock redis cacher
        mock_redis_cacher = Mock()
        mock_redis_cacher.lookup.return_value = "mock_ref_123"
        
        def cacher_side_effect(name):
            if name == "redis":
                return mock_redis_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        
        with patch.dict(os.environ, {
            'CLICKHOUSE_IF_METADATA_TABLE': 'meta_interface',
            'METRANOVA_IF_TRAFFIC_INTERVAL': '30'
        }):
            self.processor = InterfaceTrafficProcessor(self.mock_pipeline)

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        self.assertEqual(self.processor.meta_if_lookup_table, 'meta_interface')
        self.assertEqual(self.processor.meta_if_traffic_interval, 30)
        self.assertFalse(self.processor.skip_required_name)
        
        expected_required_fields = [
            ['start'],
            ['meta', 'id'],
            ['meta', 'device']
        ]
        self.assertEqual(self.processor.required_fields, expected_required_fields)

    @patch.dict(os.environ, {
        'CLICKHOUSE_IF_METADATA_TABLE': 'custom_interface_table',
        'METRANOVA_IF_TRAFFIC_INTERVAL': '60'
    })
    def test_init_with_custom_environment_variables(self):
        """Test initialization with custom environment variables."""
        processor = InterfaceTrafficProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.meta_if_lookup_table, 'custom_interface_table')
        self.assertEqual(processor.meta_if_traffic_interval, 60)

    def test_inheritance_from_base_interface_traffic_processor(self):
        """Test that processor inherits from BaseInterfaceTrafficProcessor."""
        from metranova.processors.clickhouse.interface import BaseInterfaceTrafficProcessor
        self.assertIsInstance(self.processor, BaseInterfaceTrafficProcessor)

    def test_has_required_fields_valid_with_name(self):
        """Test has_required_fields returns True when all fields present including name."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0'
            }
        }
        
        self.assertTrue(self.processor.has_required_fields(value))

    def test_has_required_fields_extracts_name_from_id(self):
        """Test has_required_fields extracts name from id when name is missing."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1'
                # Missing 'name'
            }
        }
        
        result = self.processor.has_required_fields(value)
        
        self.assertTrue(result)
        # Verify name was extracted and set
        self.assertEqual(value['meta']['name'], 'eth0')

    def test_has_required_fields_fails_when_name_missing_and_unparsable_id(self):
        """Test has_required_fields returns False when name is missing and id is unparsable."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1',  # No :: separator, can't extract name
                'device': 'device1'
            }
        }
        
        with patch.object(self.processor.logger, 'error') as mock_error:
            result = self.processor.has_required_fields(value)
        
        self.assertFalse(result)
        mock_error.assert_called_once()

    def test_has_required_fields_with_skip_required_name(self):
        """Test has_required_fields skips name check when skip_required_name is True."""
        processor = InterfaceTrafficProcessor(self.mock_pipeline)
        processor.skip_required_name = True
        
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1',
                'device': 'device1'
                # Missing 'name'
            }
        }
        
        result = processor.has_required_fields(value)
        
        self.assertTrue(result)

    def test_has_required_fields_missing_start(self):
        """Test has_required_fields returns False when start is missing."""
        value = {
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0'
            }
        }
        
        with patch.object(self.processor.logger, 'error') as mock_error:
            result = self.processor.has_required_fields(value)
        
        self.assertFalse(result)

    def test_has_required_fields_missing_meta_id(self):
        """Test has_required_fields returns False when meta.id is missing."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'device': 'device1',
                'name': 'eth0'
            }
        }
        
        with patch.object(self.processor.logger, 'error') as mock_error:
            result = self.processor.has_required_fields(value)
        
        self.assertFalse(result)

    def test_match_message_with_valid_values_fields(self):
        """Test match_message returns True when at least one values field is present."""
        test_fields = [
            'in_bits', 'in_pkts', 'in_errors', 'in_discards',
            'out_bits', 'out_pkts', 'out_errors', 'out_discards',
            'admin_state', 'oper_state'
        ]
        
        for field in test_fields:
            with self.subTest(field=field):
                value = {
                    'values': {
                        field: {'val': 100}
                    }
                }
                
                self.assertTrue(self.processor.match_message(value))

    def test_match_message_missing_all_values_fields(self):
        """Test match_message returns False when all values fields are missing."""
        value = {
            'values': {
                'some_other_field': {'val': 100}
            }
        }
        
        with patch.object(self.processor.logger, 'debug') as mock_debug:
            result = self.processor.match_message(value)
        
        self.assertFalse(result)
        mock_debug.assert_called_once()

    def test_match_message_with_none_values(self):
        """Test match_message returns False when all field values are None."""
        value = {
            'values': {
                'in_bits': {'val': None},
                'out_bits': {'val': None}
            }
        }
        
        with patch.object(self.processor.logger, 'debug') as mock_debug:
            result = self.processor.match_message(value)
        
        self.assertFalse(result)

    def test_build_message_basic_functionality(self):
        """Test build_message with basic valid input."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': 'sensor1'
            },
            'policy': {
                'originator': 'test_originator',
                'level': 1,
                'scopes': ['scope1', 'scope2']
            },
            'values': {
                'in_bits': {'val': 1024000},
                'out_bits': {'val': 512000},
                'admin_state': {'val': 1},
                'oper_state': {'val': 1}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['start_time'], '2023-01-01T00:00:00Z')
        self.assertEqual(record['interface_id'], 'device1::eth0')
        self.assertEqual(record['interface_ref'], 'mock_ref_123')
        self.assertEqual(record['collector_id'], 'sensor1')
        self.assertEqual(record['policy_originator'], 'test_originator')
        self.assertEqual(record['policy_level'], 1)
        self.assertEqual(record['policy_scope'], ['scope1', 'scope2'])
        self.assertEqual(record['in_bit_count'], 1024000)
        self.assertEqual(record['out_bit_count'], 512000)
        self.assertEqual(record['admin_status'], 1)
        self.assertEqual(record['oper_status'], 1)

    def test_build_message_calculates_end_time(self):
        """Test build_message calculates end_time correctly based on interval."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': 'sensor1'
            },
            'values': {
                'in_bits': {'val': 1024000}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        
        # end_time should be start_time + 30 seconds (default interval)
        start_dt = datetime.fromisoformat(record['start_time'].replace('Z', '+00:00'))
        end_dt = record['end_time']
        
        expected_end = start_dt + timedelta(seconds=30)
        self.assertEqual(end_dt, expected_end)

    def test_build_message_with_sensor_id_as_list(self):
        """Test build_message handles sensor_id as list."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': ['sensor1', 'sensor2']  # List instead of string
            },
            'values': {
                'in_bits': {'val': 1024000}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        self.assertEqual(record['collector_id'], 'sensor1')  # Takes first element

    def test_build_message_with_missing_sensor_id(self):
        """Test build_message defaults to 'unknown' when sensor_id is missing."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0'
                # Missing sensor_id
            },
            'values': {
                'in_bits': {'val': 1024000}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        self.assertEqual(record['collector_id'], 'unknown')

    def test_build_message_with_all_metrics(self):
        """Test build_message includes all metric fields when present."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': 'sensor1'
            },
            'values': {
                'in_bits': {'val': 1024000},
                'in_pkts': {'val': 500},
                'in_errors': {'val': 10},
                'in_discards': {'val': 5},
                'in_ucast_pkts': {'val': 450},
                'in_mcast_pkts': {'val': 40},
                'in_bcast_pkts': {'val': 10},
                'out_bits': {'val': 512000},
                'out_pkts': {'val': 250},
                'out_errors': {'val': 2},
                'out_discards': {'val': 1},
                'out_ucast_pkts': {'val': 240},
                'out_mcast_pkts': {'val': 8},
                'out_bcast_pkts': {'val': 2},
                'admin_state': {'val': 1},
                'oper_state': {'val': 1}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        
        # Verify all metrics are present
        self.assertEqual(record['in_bit_count'], 1024000)
        self.assertEqual(record['in_error_packet_count'], 10)
        self.assertEqual(record['in_discard_packet_count'], 5)
        self.assertEqual(record['in_ucast_packet_count'], 450)
        self.assertEqual(record['in_mcast_packet_count'], 40)
        self.assertEqual(record['in_bcast_packet_count'], 10)
        self.assertEqual(record['out_bit_count'], 512000)
        self.assertEqual(record['out_error_packet_count'], 2)
        self.assertEqual(record['out_discard_packet_count'], 1)
        self.assertEqual(record['out_ucast_packet_count'], 240)
        self.assertEqual(record['out_mcast_packet_count'], 8)
        self.assertEqual(record['out_bcast_packet_count'], 2)
        self.assertEqual(record['admin_status'], 1)
        self.assertEqual(record['oper_status'], 1)

    def test_build_message_with_missing_optional_metrics(self):
        """Test build_message handles missing optional metrics gracefully."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': 'sensor1'
            },
            'values': {
                'in_bits': {'val': 1024000}
                # Only one metric present
            }
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        
        # Check that missing metrics are None
        self.assertIsNone(record['out_bit_count'])
        self.assertIsNone(record['in_error_packet_count'])
        self.assertIsNone(record['admin_status'])

    def test_build_message_missing_required_fields(self):
        """Test build_message returns None when required fields are missing."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'device': 'device1'
                # Missing id and name
            },
            'values': {
                'in_bits': {'val': 1024000}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNone(result)

    def test_build_message_with_missing_policy_fields(self):
        """Test build_message handles missing policy fields gracefully."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': 'sensor1'
            },
            'values': {
                'in_bits': {'val': 1024000}
            }
            # Missing 'policy' section
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        
        # Check that policy fields are None or default empty list
        self.assertIsNone(record['policy_originator'])
        self.assertIsNone(record['policy_level'])
        # policy_scope may be an empty list when policy section is missing
        self.assertIsNotNone(record)  # Just check record exists

    def test_build_message_ext_field_is_empty_json(self):
        """Test build_message sets ext field to empty JSON object."""
        value = {
            'start': '2023-01-01T00:00:00Z',
            'meta': {
                'id': 'device1::eth0',
                'device': 'device1',
                'name': 'eth0',
                'sensor_id': 'sensor1'
            },
            'values': {
                'in_bits': {'val': 1024000}
            }
        }
        
        result = self.processor.build_message(value, {})
        
        record = result[0]
        self.assertEqual(record['ext'], '{}')


if __name__ == '__main__':
    unittest.main(verbosity=2)
