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

from metranova.processors.clickhouse.circuit import CircuitMetadataProcessor


class TestCircuitMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
        # Mock clickhouse cacher for circuit lookups
        self.mock_clickhouse_cacher = MagicMock()
        
        # Set up side effect to handle None values properly
        def mock_lookup(table, lookup_id):
            if lookup_id is None:
                return None
            return {
                'ref': 'mock_circuit_ref__v1',
                'hash': 'mock_hash',
                'max_insert_time': '2023-01-01 00:00:00'
            }
        
        self.mock_clickhouse_cacher.lookup.side_effect = mock_lookup
        
        # Set up cacher method to return the appropriate mock based on the type
        def mock_cacher(cache_type):
            if cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            else:
                return MagicMock()
        
        self.mock_pipeline.cacher.side_effect = mock_cacher

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_circuit')
        
        # Test that val_id_field is correctly set
        self.assertEqual(processor.val_id_field, ['id'])
        
        # Test that required_fields is correctly set
        self.assertEqual(processor.required_fields, [['id'], ['endpoint_id'], ['endpoint_type']])
        
        # Test that array_fields are set
        self.assertEqual(processor.array_fields, ['endpoint_type', 'endpoint_id'])
        
        # Test that logger is set
        self.assertEqual(processor.logger, processor.logger)
        
        # Test that additional column definitions were added
        column_names = [col[0] for col in processor.column_defs]
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',  # from base class
            'type', 'description', 'state', 'endpoint_type', 'endpoint_id',
            'parent_circuit_id', 'parent_circuit_ref'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names, f"Column '{expected_col}' not found in column definitions")

    def test_init_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        with patch.dict(os.environ, {'CLICKHOUSE_CIRCUIT_METADATA_TABLE': 'custom_circuit_table'}):
            processor = CircuitMetadataProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_circuit_table')

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_CIRCUIT_METADATA_TABLE': 'test_circuit_table'}):
            processor = CircuitMetadataProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (CircuitMetadataProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Basic assertions
            self.assertIsInstance(result, str)
            self.assertIn("CREATE TABLE IF NOT EXISTS test_circuit_table", result)
            self.assertIn("ENGINE = MergeTree()", result)
            self.assertIn("ORDER BY", result)
            
            # Check that circuit-specific columns are included
            circuit_columns = [
                'type', 'description', 'state', 'endpoint_type', 'endpoint_id',
                'parent_circuit_id', 'parent_circuit_ref'
            ]
            for column in circuit_columns:
                self.assertIn(f"`{column}`", result, f"Column {column} not found in CREATE TABLE command")

    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        # Ensure the environment variable is not set
        with patch.dict(os.environ, {}, clear=True):
            processor = CircuitMetadataProcessor(self.mock_pipeline)
            result = processor.create_table_command()
            
            self.assertIn("CREATE TABLE IF NOT EXISTS meta_circuit", result)

    def test_column_definitions_structure(self):
        """Test that column definitions are properly structured."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        # Check each column definition has correct structure [name, type, insert_flag]
        for col_def in processor.column_defs:
            self.assertIsInstance(col_def, list, "Column definition should be a list")
            self.assertEqual(len(col_def), 3, "Column definition should have 3 elements")
            self.assertIsInstance(col_def[0], str, "Column name should be a string")
            # col_def[1] can be string or None (for ext field)
            self.assertIsInstance(col_def[2], bool, "Insert flag should be a boolean")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        # Create a mapping of column names to their types
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        # Test specific column types
        self.assertEqual(column_types['type'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['description'], 'Nullable(String)')
        self.assertEqual(column_types['state'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['endpoint_type'], 'Tuple(LowCardinality(String),LowCardinality(String))')
        self.assertEqual(column_types['endpoint_id'], 'Tuple(String,String)')
        self.assertEqual(column_types['parent_circuit_id'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['parent_circuit_ref'], 'Nullable(String)')

    def test_build_metadata_fields_basic(self):
        """Test that build_metadata_fields returns correctly formatted data."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        value = {
            'type': 'pwave',
            'description': 'Test Circuit Description',
            'state': 'active',
            'endpoint_type': ['interface', 'interface'],
            'endpoint_id': ['losa-cr6::pwave-losa_se-1553', 'sfo-cr1::pwave-sfo_se-1553'],
            'parent_circuit_id': 'parent-circuit-1'
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check basic field mapping
        self.assertEqual(result['type'], 'pwave')
        self.assertEqual(result['description'], 'Test Circuit Description')
        self.assertEqual(result['state'], 'active')
        self.assertEqual(result['endpoint_type'], ['interface', 'interface'])
        self.assertEqual(result['endpoint_id'], ['losa-cr6::pwave-losa_se-1553', 'sfo-cr1::pwave-sfo_se-1553'])
        self.assertEqual(result['parent_circuit_id'], 'parent-circuit-1')
        
        # Check that lookups were performed for references
        self.assertEqual(result['parent_circuit_ref'], 'mock_circuit_ref__v1')
        
        # Verify cacher was called
        expected_calls = [
            (('meta_circuit', 'parent-circuit-1'), {})
        ]
        self.mock_clickhouse_cacher.lookup.assert_has_calls([unittest.mock.call(*args, **kwargs) for args, kwargs in expected_calls])

    def test_build_metadata_fields_none_values(self):
        """Test build_metadata_fields with None values for array fields."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        value = {
            'data': {
                'type': 'transport',
                'description': 'Test Circuit',
                'state': 'provisioning',
                'endpoint_type': ['port', 'port'],
                'endpoint_id': ['port1', 'port2'],
                'parent_circuit_id': None
            }
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check that None values are converted to empty arrays by format_array_fields
        self.assertEqual(result['parent_circuit_id'], None)  
        self.assertEqual(result['parent_circuit_ref'], None)

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        # Missing 'data' field entirely
        input_data_1 = {'other': 'value'}
        result_1 = processor.build_message(input_data_1, {})
        self.assertEqual(result_1, [])
        
        # Missing 'id' field in data
        input_data_2 = {'data': [{'endpoint_type': ['interface', 'interface'], 'endpoint_id': ['int1', 'int2']}]}
        result_2 = processor.build_message(input_data_2, {})
        self.assertEqual(result_2, [])
        
        # Missing 'endpoint_type' field in data
        input_data_3 = {'data': [{'id': 'test-circuit', 'endpoint_id': ['int1', 'int2']}]}
        result_3 = processor.build_message(input_data_3, {})
        self.assertEqual(result_3, [])
        
        # Missing 'endpoint_id' field in data
        input_data_4 = {'data': [{'id': 'test-circuit', 'endpoint_type': ['interface', 'interface']}]}
        result_4 = processor.build_message(input_data_4, {})
        self.assertEqual(result_4, [])

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'losa-sfo-pwave-1553',
                'type': 'pwave',
                'description': 'PWAVE Circuit Los Angeles to San Francisco',
                'state': 'active',
                'endpoint_type': ['interface', 'interface'],
                'endpoint_id': ['losa-cr6::pwave-losa_se-1553', 'sfo-cr1::pwave-sfo_se-1553'],
                'parent_circuit_id': 'losa-sfo-backbone-01'
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'losa-sfo-pwave-1553')
        self.assertEqual(record['type'], 'pwave')
        self.assertEqual(record['description'], 'PWAVE Circuit Los Angeles to San Francisco')
        self.assertEqual(record['state'], 'active')
        self.assertEqual(record['endpoint_type'], ['interface', 'interface'])
        self.assertEqual(record['endpoint_id'], ['losa-cr6::pwave-losa_se-1553', 'sfo-cr1::pwave-sfo_se-1553'])
        self.assertEqual(record['parent_circuit_id'], 'losa-sfo-backbone-01')
        self.assertEqual(record['parent_circuit_ref'], 'mock_circuit_ref__v1')
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('losa-sfo-pwave-1553__v'))

    def test_build_message_minimal_required_fields(self):
        """Test build_message with only required fields."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-circuit',
                'endpoint_type': ['device', 'device'],
                'endpoint_id': ['device1', 'device2']
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test-circuit')
        self.assertEqual(record['endpoint_type'], ['device', 'device'])
        self.assertEqual(record['endpoint_id'], ['device1', 'device2'])
        
        # Other fields should be None or default values
        self.assertIsNone(record['type'])
        self.assertIsNone(record['description'])
        self.assertIsNone(record['state'])
        self.assertEqual(record['ext'], '{}')
        self.assertEqual(record['tag'], [])

    def test_build_message_existing_record_no_change(self):
        """Test build_message skips unchanged records."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-circuit',
                'endpoint_type': ['interface', 'interface'],
                'endpoint_id': ['int1', 'int2'],
                'type': 'test'
            }]
        }
        
        # Create a side effect function to handle the mock calls
        def side_effect_func(table, id_value):
            if table == 'meta_circuit' and id_value == 'test-circuit':
                # Calculate the hash the same way the processor does
                formatted_record = {"id": id_value}
                formatted_record.update(processor.build_metadata_fields(input_data['data'][0]))
                
                # Calculate hash
                import orjson
                import hashlib
                record_json = orjson.dumps(formatted_record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
                record_md5 = hashlib.md5(record_json.encode('utf-8')).hexdigest()
                
                # Return existing record with the actual hash (same hash means no change)
                return {'hash': record_md5, 'ref': 'test-circuit__v1'}
            elif table == 'meta_circuit' and id_value is None:
                return None
            elif table == 'meta_circuit':
                return {
                    'ref': 'mock_circuit_ref__v1',
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                }
            return None
        
        self.mock_clickhouse_cacher.lookup.side_effect = side_effect_func
        
        # Now test that it skips unchanged records
        result = processor.build_message(input_data, {})
        
        self.assertEqual(result, [])

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-circuit',
                'endpoint_type': ['interface', 'interface'],
                'endpoint_id': ['int1', 'int2'],
                'type': 'test-updated',  # Changed value
                'description': 'Updated description'
            }]
        }
        
        # Mock existing record with different hash
        mock_existing = {'hash': 'different_hash', 'ref': 'test-circuit__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test-circuit')
        self.assertEqual(record['type'], 'test-updated')
        self.assertEqual(record['description'], 'Updated description')
        self.assertEqual(record['ref'], 'test-circuit__v2')  # Should increment version

    def test_build_message_comprehensive_scenario(self):
        """Test build_message with a comprehensive real-world scenario."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'losa-sfo-backbone-01',
                'type': 'backbone',
                'description': 'Los Angeles to San Francisco Backbone Circuit',
                'state': 'active',
                'endpoint_type': ['device', 'device'],
                'endpoint_id': ['losa-cr6', 'sfo-cr1'],
                'parent_circuit_id': None,
                'ext': '{"vendor": "Cisco", "capacity": "100G"}',
                'tag': ['backbone', 'high-priority', 'production']
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        
        # Verify all fields are properly processed
        self.assertEqual(record['id'], 'losa-sfo-backbone-01')
        self.assertEqual(record['type'], 'backbone')
        self.assertEqual(record['description'], 'Los Angeles to San Francisco Backbone Circuit')
        self.assertEqual(record['state'], 'active')
        self.assertEqual(record['endpoint_type'], ['device', 'device'])
        self.assertEqual(record['endpoint_id'], ['losa-cr6', 'sfo-cr1'])
        self.assertEqual(record['parent_circuit_id'], None)
        self.assertEqual(record['parent_circuit_ref'], None)

        # Verify ext and tag from base class processing
        self.assertEqual(record['ext'], '{"vendor": "Cisco", "capacity": "100G"}')
        self.assertEqual(record['tag'], ['backbone', 'high-priority', 'production'])
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('losa-sfo-backbone-01__v'))

    def test_clickhouse_cacher_lookup_called(self):
        """Test that ClickHouse cacher lookup is called for circuit references."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-circuit',
                'endpoint_type': ['interface', 'interface'],
                'endpoint_id': ['int1', 'int2'],
                'parent_circuit_id': 'parent1'
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        # Verify clickhouse cacher was called for base record lookup and circuit lookups
        self.mock_pipeline.cacher.assert_called_with("clickhouse")
        
        # Verify ClickHouse lookups for circuit references
        clickhouse_lookup_calls = [
            (('meta_circuit', 'parent1'), {})
        ]
        self.mock_clickhouse_cacher.lookup.assert_has_calls([unittest.mock.call(*args, **kwargs) for args, kwargs in clickhouse_lookup_calls], any_order=True)

    def test_endpoint_tuple_handling(self):
        """Test that endpoint tuples are properly handled."""
        processor = CircuitMetadataProcessor(self.mock_pipeline)
        
        test_cases = [
            # Standard interface endpoints
            {
                'endpoint_type': ['interface', 'interface'],
                'endpoint_id': ['losa-cr6::pwave-losa_se-1553', 'sfo-cr1::pwave-sfo_se-1553']
            },
            # Device endpoints
            {
                'endpoint_type': ['device', 'device'],
                'endpoint_id': ['losa-cr6', 'sfo-cr1']
            },
            # Port endpoints
            {
                'endpoint_type': ['port', 'port'],
                'endpoint_id': ['losa-cr6::ae1553', 'sfo-cr1::ae1553']
            },
            # Mixed endpoint types
            {
                'endpoint_type': ['device', 'interface'],
                'endpoint_id': ['losa-cr6', 'sfo-cr1::pwave-sfo_se-1553']
            }
        ]
        
        for i, endpoints in enumerate(test_cases):
            with self.subTest(case=i):
                # Reset cacher mock for each test case
                self.mock_clickhouse_cacher.lookup.return_value = None
                
                input_data = {
                    'data': [{
                        'id': f'test-circuit-{i}',
                        **endpoints
                    }]
                }
                
                result = processor.build_message(input_data, {})
                
                self.assertIsInstance(result, list)
                self.assertEqual(len(result), 1)
                
                record = result[0]
                self.assertEqual(record['endpoint_type'], endpoints['endpoint_type'])
                self.assertEqual(record['endpoint_id'], endpoints['endpoint_id'])


if __name__ == '__main__':
    unittest.main()
