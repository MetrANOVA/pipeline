#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
import hashlib
import orjson

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.ip import IPMetadataProcessor, IPGeoPrefixMaterializedView, IPGeoDictionary


class TestIPMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
        # Mock clickhouse cacher for AS lookups
        self.mock_clickhouse_cacher = MagicMock()
        # For build_metadata_fields tests - return AS ref
        self.mock_clickhouse_cacher.lookup.return_value = {
            'ref': 'mock_as_ref__v1',
            'hash': 'mock_hash',
            'max_insert_time': '2023-01-01 00:00:00'
        }
        
        # Set up cacher method to return the appropriate mock based on the type
        def mock_cacher(cache_type):
            if cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            else:
                return MagicMock()
        
        self.mock_pipeline.cacher.side_effect = mock_cacher

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_ip')
        
        # Test that val_id_field is correctly set
        self.assertEqual(processor.val_id_field, ['id'])
        
        # Test that required_fields is correctly set
        self.assertEqual(processor.required_fields, [['id'], ['ip_subnet']])
        
        # Test that field types are set
        self.assertEqual(processor.float_fields, ['latitude', 'longitude'])
        self.assertEqual(processor.array_fields, ['ip_subnet'])
        self.assertEqual(processor.int_fields, ['as_id'])
        
        # Test that logger is set
        self.assertEqual(processor.logger, processor.logger)
        
        # Test that additional column definitions were added
        column_names = [col[0] for col in processor.column_defs]
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',  # from base class
            'ip_subnet', 'city_name', 'continent_name', 'country_name', 'country_code',
            'country_sub_name', 'country_sub_code', 'latitude', 'longitude',
            'as_id', 'as_ref'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names, f"Column '{expected_col}' not found in column definitions")

    def test_init_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        with patch.dict(os.environ, {'CLICKHOUSE_IP_METADATA_TABLE': 'custom_ip_table'}):
            processor = IPMetadataProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_ip_table')

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_IP_METADATA_TABLE': 'test_ip_table'}):
            processor = IPMetadataProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (IPMetadataProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Basic assertions
            self.assertIsInstance(result, str)
            self.assertIn("CREATE TABLE IF NOT EXISTS test_ip_table", result)
            self.assertIn("ENGINE = MergeTree()", result)
            self.assertIn("ORDER BY", result)
            
            # Check that IP-specific columns are included
            ip_columns = [
                'ip_subnet', 'city_name', 'continent_name', 'country_name', 'country_code',
                'country_sub_name', 'country_sub_code', 'latitude', 'longitude',
                'as_id', 'as_ref'
            ]
            for column in ip_columns:
                self.assertIn(f"`{column}`", result, f"Column {column} not found in CREATE TABLE command")

    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        # Ensure the environment variable is not set
        with patch.dict(os.environ, {}, clear=True):
            processor = IPMetadataProcessor(self.mock_pipeline)
            result = processor.create_table_command()
            
            self.assertIn("CREATE TABLE IF NOT EXISTS meta_ip", result)

    def test_column_definitions_structure(self):
        """Test that column definitions are properly structured."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        # Check each column definition has correct structure [name, type, insert_flag]
        for col_def in processor.column_defs:
            self.assertIsInstance(col_def, list, "Column definition should be a list")
            self.assertEqual(len(col_def), 3, "Column definition should have 3 elements")
            self.assertIsInstance(col_def[0], str, "Column name should be a string")
            # col_def[1] can be string or None (for ext field)
            self.assertIsInstance(col_def[2], bool, "Insert flag should be a boolean")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        # Create a mapping of column names to their types
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        # Test specific column types
        self.assertEqual(column_types['ip_subnet'], 'Array(Tuple(IPv6,UInt8))')
        self.assertEqual(column_types['city_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['continent_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['country_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['country_code'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['country_sub_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['country_sub_code'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['latitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['longitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['as_id'], 'Nullable(UInt32)')
        self.assertEqual(column_types['as_ref'], 'Nullable(String)')

    def test_build_metadata_fields_basic(self):
        """Test that build_metadata_fields returns correctly formatted data."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [['164.67.0.0', 16], ['128.97.0.0', 16]],
            'city_name': 'Los Angeles',
            'continent_name': 'North America',
            'country_name': 'United States',
            'country_code': 'US',
            'country_sub_name': 'California',
            'country_sub_code': 'CA',
            'latitude': 34.0522,
            'longitude': -118.2437,
            'as_id': 67890
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check basic field mapping. IPv4 CIDR lengths are offset by 96 for
        # IP_TRIE storage (see trie_prefix_length() in base.py): 16 -> 112.
        self.assertEqual(result['ip_subnet'], [('164.67.0.0', 112), ('128.97.0.0', 112)])
        self.assertEqual(result['city_name'], 'Los Angeles')
        self.assertEqual(result['continent_name'], 'North America')
        self.assertEqual(result['country_name'], 'United States')
        self.assertEqual(result['country_code'], 'US')
        self.assertEqual(result['country_sub_name'], 'California')
        self.assertEqual(result['country_sub_code'], 'CA')
        self.assertEqual(result['latitude'], 34.0522)
        self.assertEqual(result['longitude'], -118.2437)
        self.assertEqual(result['as_id'], 67890)
        
        # Check that ClickHouse lookup was performed for AS reference
        self.assertEqual(result['as_ref'], 'mock_as_ref__v1')
        
        # Verify ClickHouse cacher was called
        self.mock_clickhouse_cacher.lookup.assert_called_with('meta_as', 67890)

    def test_build_metadata_fields_ipv6_subnets(self):
        """Test build_metadata_fields with IPv6 subnets."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [['2607:f010::', 32], ['2001:db8::', 48]],
            'city_name': 'Los Angeles',
            'as_id': 67890
        }

        
        result = processor.build_metadata_fields(value)
        
        # Check that IPv6 subnets are properly converted to tuples
        self.assertEqual(result['ip_subnet'], [('2607:f010::', 32), ('2001:db8::', 48)])
        self.assertEqual(result['as_ref'], 'mock_as_ref__v1')

    def test_build_metadata_fields_mixed_ip_versions(self):
        """Test build_metadata_fields with mixed IPv4 and IPv6 subnets."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
                'id': 'test-ip-block',
                'ip_subnet': [['164.67.0.0', 16], ['2607:f010::', 32], ['10.0.0.0', 8]],
                'as_id': 67890
            }
        
        result = processor.build_metadata_fields(value)
        
        # Check that mixed IP versions are properly handled. IPv4 lengths are
        # offset by 96 for IP_TRIE storage (16 -> 112, 8 -> 104); native IPv6
        # lengths (32) are untouched.
        expected_subnets = [('164.67.0.0', 112), ('2607:f010::', 32), ('10.0.0.0', 104)]
        self.assertEqual(result['ip_subnet'], expected_subnets)

    def test_build_metadata_fields_invalid_ip_subnet_items(self):
        """Test build_metadata_fields with invalid ip_subnet items."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
                'ip_subnet': [
                    ['164.67.0.0', 16],  # Valid
                    ['invalid'],  # Invalid - only one element
                    ['192.168.1.0', 'invalid_prefix'],  # Invalid - non-integer prefix
                    ['10.0.0.0', 24],  # Valid
                    'not_a_list',  # Invalid - not a list/tuple
                    ['172.16.0.0', 12, 'extra'],  # Invalid - too many elements
                ],
                'as_id': 67890
            }
        
        with patch.object(processor.logger, 'warning') as mock_warning:
            result = processor.build_metadata_fields(value)
            
            # Should only contain valid items. IPv4 lengths offset by 96 for
            # IP_TRIE storage: 16 -> 112, 24 -> 120.
            expected_subnets = [('164.67.0.0', 112), ('10.0.0.0', 120)]
            self.assertEqual(result['ip_subnet'], expected_subnets)
            
            # Should have logged warnings for invalid items
            self.assertTrue(mock_warning.called)

    def test_build_metadata_fields_empty_ip_subnet(self):
        """Test build_metadata_fields with empty ip_subnet array."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
            'data': {
                'ip_subnet': [],
                'city_name': 'Los Angeles',
                'as_id': 67890
            }
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check that empty array is handled correctly
        self.assertEqual(result['ip_subnet'], [])
        self.assertEqual(result['as_ref'], 'mock_as_ref__v1')

    def test_build_metadata_fields_minimal_data(self):
        """Test build_metadata_fields with minimal required data."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [['192.168.1.0', 24]]
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check required fields are set. IPv4 /24 offset by 96 for IP_TRIE
        # storage -> /120.
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 120)])
        
        # Check that optional fields are None or default values
        self.assertIsNone(result.get('city_name'))
        self.assertIsNone(result.get('continent_name'))
        self.assertIsNone(result.get('country_name'))
        self.assertIsNone(result.get('country_code'))
        self.assertIsNone(result.get('country_sub_name'))
        self.assertIsNone(result.get('country_sub_code'))
        self.assertIsNone(result.get('latitude'))
        self.assertIsNone(result.get('longitude'))
        self.assertIsNone(result.get('as_id'))

    def test_build_metadata_fields_float_conversion(self):
        """Test that latitude and longitude are properly converted to floats."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [['164.67.0.0', 16]],
            'latitude': '34.0522',  # String that should be converted to float
            'longitude': '-118.2437',  # String that should be converted to float
            'as_id': '67890'  # String that should be converted to int
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check that float conversion happened
        self.assertIsInstance(result['latitude'], float)
        self.assertIsInstance(result['longitude'], float)
        self.assertEqual(result['latitude'], 34.0522)
        self.assertEqual(result['longitude'], -118.2437)
        
        # Check that int conversion happened for as_id
        self.assertIsInstance(result['as_id'], int)
        self.assertEqual(result['as_id'], 67890)

    def test_build_metadata_fields_invalid_numeric_values(self):
        """Test that invalid numeric values are handled gracefully."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        value = {
            'data': {
                'ip_subnet': [['164.67.0.0', 16]],
                'latitude': 'invalid',  # Invalid float value
                'longitude': 'also_invalid',  # Invalid float value
                'as_id': 'not_a_number'  # Invalid int value
            }
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check that invalid values are converted to None
        self.assertIsNone(result['latitude'])
        self.assertIsNone(result['longitude'])
        self.assertIsNone(result['as_id'])

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        # Missing 'data' field entirely
        input_data_1 = {'other': 'value'}
        result_1 = processor.build_message(input_data_1, {})
        self.assertEqual(result_1, [])
        
        # Missing 'id' field in data
        input_data_2 = {'data': [{'ip_subnet': [['192.168.1.0', 24]]}]}
        result_2 = processor.build_message(input_data_2, {})
        self.assertEqual(result_2, [])
        
        # Missing 'ip_subnet' field in data
        input_data_3 = {'data': [{'id': 'test-ip-block'}]}
        result_3 = processor.build_message(input_data_3, {})
        self.assertEqual(result_3, [])

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'ucla-ipv4-block-1',
                'ip_subnet': [['164.67.0.0', 16], ['128.97.0.0', 16]],
                'city_name': 'Los Angeles',
                'continent_name': 'North America',
                'country_name': 'United States',
                'country_code': 'US',
                'country_sub_name': 'California',
                'country_sub_code': 'CA',
                'latitude': 34.0522,
                'longitude': -118.2437,
                'as_id': 67890
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'ucla-ipv4-block-1')
        # IPv4 /16 offset by 96 for IP_TRIE storage -> /112.
        self.assertEqual(record['ip_subnet'], [('164.67.0.0', 112), ('128.97.0.0', 112)])
        self.assertEqual(record['city_name'], 'Los Angeles')
        self.assertEqual(record['continent_name'], 'North America')
        self.assertEqual(record['country_name'], 'United States')
        self.assertEqual(record['country_code'], 'US')
        self.assertEqual(record['country_sub_name'], 'California')
        self.assertEqual(record['country_sub_code'], 'CA')
        self.assertEqual(record['latitude'], 34.0522)
        self.assertEqual(record['longitude'], -118.2437)
        self.assertEqual(record['as_id'], 67890)
        self.assertEqual(record['as_ref'], 'mock_as_ref__v1')
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('ucla-ipv4-block-1__v'))

    def test_build_message_minimal_required_fields(self):
        """Test build_message with only required fields."""
        processor = IPMetadataProcessor(self.mock_pipeline)

        input_data = { 
            'data': [{
                'id': 'minimal-ip-block',
                'ip_subnet': [['10.0.0.0', 8]]
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'minimal-ip-block')
        # IPv4 /8 offset by 96 for IP_TRIE storage -> /104.
        self.assertEqual(record['ip_subnet'], [('10.0.0.0', 104)])
        
        # Other fields should be None or default values
        self.assertIsNone(record['city_name'])
        self.assertIsNone(record['continent_name'])
        self.assertIsNone(record['country_name'])
        self.assertIsNone(record['country_code'])
        self.assertIsNone(record['country_sub_name'])
        self.assertIsNone(record['country_sub_code'])
        self.assertIsNone(record['latitude'])
        self.assertIsNone(record['longitude'])
        self.assertIsNone(record['as_id'])
        self.assertEqual(record['ext'], '{}')
        self.assertEqual(record['tag'], [])

    def test_build_message_existing_record_no_change(self):
        """Test build_message skips unchanged records."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': 'test-ip-block',
                'ip_subnet': [['192.168.1.0', 24]],
                'as_id': 67890
            }
        }
        
        # First, build the record to see what hash is actually generated
        result = processor.build_message(input_data, {})
        actual_hash = result[0]['hash'] if result else None
        
        # Now mock existing record with the actual hash and set up lookup side effects
        mock_existing = {'hash': actual_hash, 'ref': 'test-ip-block__v1'}
        mock_as_ref = {'ref': 'mock_as_ref__v1', 'hash': 'mock_hash', 'max_insert_time': '2023-01-01 00:00:00'}
        
        def lookup_side_effect(table, key):
            if table == 'meta_ip' and key == 'test-ip-block':
                return mock_existing
            elif table == 'meta_as' and key == 67890:
                return mock_as_ref
            return None
        
        self.mock_clickhouse_cacher.lookup.side_effect = lookup_side_effect
        
        # Now test that it skips unchanged records
        result = processor.build_message(input_data, {})

        self.assertEqual(result, [])

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-ip-block',
                'ip_subnet': [['192.168.1.0', 24], ['10.0.0.0', 8]],  # Changed - added subnet
                'city_name': 'San Francisco',  # New field
                'as_id': 67890
            }]
        }
        
        # Mock existing record with different hash
        mock_existing = {'hash': 'different_hash', 'ref': 'test-ip-block__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test-ip-block')
        # IPv4 lengths offset by 96 for IP_TRIE storage: 24 -> 120, 8 -> 104.
        self.assertEqual(record['ip_subnet'], [('192.168.1.0', 120), ('10.0.0.0', 104)])
        self.assertEqual(record['city_name'], 'San Francisco')
        self.assertEqual(record['ref'], 'test-ip-block__v2')  # Should increment version

    def test_build_message_comprehensive_scenario(self):
        """Test build_message with a comprehensive real-world scenario."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'ucla-mixed-blocks',
                'ip_subnet': [['164.67.0.0', 16], ['2607:f010::', 32]],
                'city_name': 'Los Angeles',
                'continent_name': 'North America',
                'country_name': 'United States',
                'country_code': 'US',
                'country_sub_name': 'California',
                'country_sub_code': 'CA',
                'latitude': 34.0522,
                'longitude': -118.2437,
                'as_id': 67890,
                'ext': '{"registry": "ARIN", "purpose": "research"}',
                'tag': ['education', 'research', 'mixed-ip']
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        
        # Verify all fields are properly processed. IPv4 /16 offset by 96 for
        # IP_TRIE storage -> /112; native IPv6 /32 is untouched.
        self.assertEqual(record['id'], 'ucla-mixed-blocks')
        self.assertEqual(record['ip_subnet'], [('164.67.0.0', 112), ('2607:f010::', 32)])
        self.assertEqual(record['city_name'], 'Los Angeles')
        self.assertEqual(record['continent_name'], 'North America')
        self.assertEqual(record['country_name'], 'United States')
        self.assertEqual(record['country_code'], 'US')
        self.assertEqual(record['country_sub_name'], 'California')
        self.assertEqual(record['country_sub_code'], 'CA')
        self.assertEqual(record['latitude'], 34.0522)
        self.assertEqual(record['longitude'], -118.2437)
        self.assertEqual(record['as_id'], 67890)
        self.assertEqual(record['as_ref'], 'mock_as_ref__v1')
        
        # Verify ext and tag from base class processing
        self.assertEqual(record['ext'], '{"registry": "ARIN", "purpose": "research"}')
        self.assertEqual(record['tag'], ['education', 'research', 'mixed-ip'])
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('ucla-mixed-blocks__v'))

    def test_clickhouse_cacher_lookup_called(self):
        """Test that Clickhouse cacher lookup is called for AS reference."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-ip-block',
                'ip_subnet': [['192.168.1.0', 24]],
                'as_id': 67890
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        # Verify clickhouse cacher was called
        self.mock_pipeline.cacher.assert_called_with("clickhouse")

        # Verify ClickHouse lookups for both base record and AS reference
        # Check that both calls were made
        expected_calls = [
            unittest.mock.call('meta_ip', 'test-ip-block'),
            unittest.mock.call('meta_as', 67890)
        ]
        self.mock_clickhouse_cacher.lookup.assert_has_calls(expected_calls, any_order=True)

    def test_ip_subnet_tuple_conversion(self):
        """Test that IP subnet conversion handles various formats correctly."""
        processor = IPMetadataProcessor(self.mock_pipeline)
        
        # IPv4 lengths are offset by 96 for IP_TRIE storage (see
        # trie_prefix_length() in base.py: 24 -> 120, 8 -> 104); native IPv6
        # lengths are untouched.
        test_cases = [
            # IPv4 subnets
            {
                'input': [['192.168.1.0', 24], ['10.0.0.0', 8]],
                'expected': [('192.168.1.0', 120), ('10.0.0.0', 104)]
            },
            # IPv6 subnets
            {
                'input': [['2001:db8::', 48], ['fe80::', 10]],
                'expected': [('2001:db8::', 48), ('fe80::', 10)]
            },
            # Mixed IPv4 and IPv6
            {
                'input': [['192.168.1.0', 24], ['2001:db8::', 48]],
                'expected': [('192.168.1.0', 120), ('2001:db8::', 48)]
            },
            # String prefix lengths
            {
                'input': [['192.168.1.0', '24'], ['10.0.0.0', '8']],
                'expected': [('192.168.1.0', 120), ('10.0.0.0', 104)]
            },
            # Tuple input (instead of list)
            {
                'input': [('192.168.1.0', 24), ('10.0.0.0', 8)],
                'expected': [('192.168.1.0', 120), ('10.0.0.0', 104)]
            }
        ]
        
        for i, case in enumerate(test_cases):
            with self.subTest(case=i):
                # Reset cacher mock for each test case
                self.mock_clickhouse_cacher.lookup.return_value = None
                
                input_data = {
                    'data': [{
                        'id': f'test-ip-block-{i}',
                        'ip_subnet': case['input']
                    }]
                }
                
                result = processor.build_message(input_data, {})
                
                self.assertIsInstance(result, list)
                self.assertEqual(len(result), 1)
                
                record = result[0]
                self.assertEqual(record['ip_subnet'], case['expected'])

    def test_dictionary_enabled_by_default(self):
        """Test that the ip geo dictionary (and its backing prefix MV) is enabled by default."""
        processor = IPMetadataProcessor(self.mock_pipeline)

        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)
        self.assertIsInstance(processor.ch_dictionaries[0], IPGeoDictionary)
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertIsInstance(processor.materialized_views[0], IPGeoPrefixMaterializedView)

    @patch.dict(os.environ, {'CLICKHOUSE_IP_GEO_DICTIONARY_ENABLED': 'false'})
    def test_dictionary_disabled_by_env(self):
        """Test that the dictionary/MV can be disabled via environment variable."""
        processor = IPMetadataProcessor(self.mock_pipeline)

        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)
        self.assertEqual(len(processor.materialized_views), 0)

    def test_dictionary_sourced_from_prefix_mv_table(self):
        """Test that IPGeoDictionary is sourced from the flattened prefix table, not the raw ip table."""
        with patch.dict(os.environ, {'CLICKHOUSE_IP_METADATA_TABLE': 'custom_ip'}):
            processor = IPMetadataProcessor(self.mock_pipeline)

            mv = processor.materialized_views[0]
            self.assertEqual(mv.source_table_name, 'custom_ip')
            self.assertEqual(mv.table, 'meta_ip_geo_prefix')

            dictionary = processor.ch_dictionaries[0]
            self.assertEqual(dictionary.source_table_name, 'meta_ip_geo_prefix')


class TestIPGeoPrefixMaterializedView(unittest.TestCase):
    """Unit tests for IPGeoPrefixMaterializedView class."""

    def test_init_default_values(self):
        mv = IPGeoPrefixMaterializedView(source_table_name='meta_ip')

        self.assertEqual(mv.source_table_name, 'meta_ip')
        self.assertEqual(mv.table, 'meta_ip_geo_prefix')
        self.assertEqual(mv.mv_name, 'meta_ip_geo_prefix_mv')
        self.assertEqual(mv.primary_keys, ['prefix'])
        self.assertEqual(mv.order_by, ['prefix', 'id'])
        self.assertEqual(mv.table_engine, 'ReplacingMergeTree')

    @patch.dict(os.environ, {'CLICKHOUSE_IP_GEO_PREFIX_TABLE': 'custom_prefix_table'})
    def test_init_custom_table_name(self):
        mv = IPGeoPrefixMaterializedView(source_table_name='meta_ip')
        self.assertEqual(mv.table, 'custom_prefix_table')
        self.assertEqual(mv.mv_name, 'custom_prefix_table_mv')

    def test_mv_select_query_flattens_ip_subnet(self):
        mv = IPGeoPrefixMaterializedView(source_table_name='meta_ip')

        self.assertIn('ARRAY JOIN ip_subnet AS ip_subnet_entry', mv.mv_select_query)
        self.assertIn('FROM meta_ip', mv.mv_select_query)
        self.assertIn('AS prefix', mv.mv_select_query)

    def test_create_table_command(self):
        mv = IPGeoPrefixMaterializedView(source_table_name='meta_ip')
        result = mv.create_table_command()

        self.assertIn('CREATE TABLE IF NOT EXISTS meta_ip_geo_prefix', result)
        self.assertIn('`prefix` String', result)
        self.assertIn('`city_name` String', result)
        self.assertIn('`country_name` String', result)
        self.assertIn('`country_code` String', result)
        self.assertIn('`latitude` Float64', result)
        self.assertIn('`longitude` Float64', result)
        self.assertIn('`as_id` UInt32', result)
        self.assertIn('`as_ref` String', result)
        self.assertIn('ENGINE = ReplacingMergeTree(insert_time)', result)

    def test_mv_select_query_coalesces_nulls(self):
        """IP_TRIE dictionaries don't support Nullable attributes, so NULLs from the
        raw ip table must be coalesced before they reach the flattened prefix table --
        lat/long coalesce to NaN rather than 0, since (0, 0) is a real point."""
        mv = IPGeoPrefixMaterializedView(source_table_name='meta_ip')

        self.assertIn("coalesce(city_name, '') AS city_name", mv.mv_select_query)
        self.assertIn("coalesce(country_name, '') AS country_name", mv.mv_select_query)
        self.assertIn("coalesce(latitude, nan) AS latitude", mv.mv_select_query)
        self.assertIn("coalesce(longitude, nan) AS longitude", mv.mv_select_query)
        self.assertIn("coalesce(as_id, 0) AS as_id", mv.mv_select_query)
        self.assertIn("coalesce(as_ref, '') AS as_ref", mv.mv_select_query)

    def test_create_mv_command(self):
        mv = IPGeoPrefixMaterializedView(source_table_name='meta_ip')
        result = mv.create_mv_command()

        self.assertIn('CREATE MATERIALIZED VIEW IF NOT EXISTS meta_ip_geo_prefix_mv', result)
        self.assertIn('TO meta_ip_geo_prefix', result)


class TestIPGeoDictionary(unittest.TestCase):
    """Unit tests for IPGeoDictionary class."""

    def test_init_default_values(self):
        dictionary = IPGeoDictionary('meta_ip_geo_prefix')

        self.assertEqual(dictionary.source_table_name, 'meta_ip_geo_prefix')
        self.assertEqual(dictionary.dictionary_name, 'meta_ip_geo_dict')
        expected_columns = [
            ['prefix', 'String'],
            ['city_name', "String DEFAULT ''"],
            ['continent_name', "String DEFAULT ''"],
            ['country_name', "String DEFAULT ''"],
            ['country_code', "String DEFAULT ''"],
            ['country_sub_name', "String DEFAULT ''"],
            ['country_sub_code', "String DEFAULT ''"],
            ['latitude', "Float64 DEFAULT nan"],
            ['longitude', "Float64 DEFAULT nan"],
            ['as_id', "UInt32 DEFAULT 0"],
            ['as_ref', "String DEFAULT ''"],
        ]
        self.assertEqual(dictionary.column_defs, expected_columns)
        self.assertEqual(dictionary.primary_keys, ['prefix'])
        self.assertEqual(dictionary.lifetime_min, '600')
        self.assertEqual(dictionary.lifetime_max, '3600')
        self.assertEqual(dictionary.layout, "IP_TRIE()")

    @patch.dict(os.environ, {'CLICKHOUSE_IP_GEO_DICTIONARY_NAME': 'custom_ip_geo_dict'})
    def test_init_with_custom_dictionary_name(self):
        dictionary = IPGeoDictionary('meta_ip_geo_prefix')
        self.assertEqual(dictionary.dictionary_name, 'custom_ip_geo_dict')

    def test_create_dictionary_command(self):
        dictionary = IPGeoDictionary('meta_ip_geo_prefix')
        command = dictionary.create_dictionary_command()

        self.assertIn("CREATE DICTIONARY IF NOT EXISTS meta_ip_geo_dict", command)
        self.assertIn("`prefix` String", command)
        self.assertIn("PRIMARY KEY (`prefix`)", command)
        self.assertIn("SOURCE(CLICKHOUSE(TABLE 'meta_ip_geo_prefix'", command)
        self.assertIn("LAYOUT(IP_TRIE())", command)


if __name__ == '__main__':
    unittest.main()
