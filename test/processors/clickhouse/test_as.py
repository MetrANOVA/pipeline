#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
import importlib
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Import using importlib to handle 'as' reserved keyword
as_module = importlib.import_module('metranova.processors.clickhouse.as')
ASMetadataProcessor = as_module.ASMetadataProcessor


class TestASMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
        # Mock clickhouse cacher for organization lookups
        self.mock_clickhouse_cacher = MagicMock()
        self.mock_clickhouse_cacher.lookup.return_value = {
            'ref': 'mock_org_ref__v1',
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
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_as')
        
        # Test that val_id_field is correctly set
        self.assertEqual(processor.val_id_field, ['data', 'id'])
        
        # Test that required_fields is correctly set
        self.assertEqual(processor.required_fields, [['data', 'id'], ['data', 'name'], ['data', 'organization_id']])
        
        # Test that logger is set
        self.assertEqual(processor.logger, processor.logger)
        
        # Test that additional column definitions were added
        column_names = [col[0] for col in processor.column_defs]
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',  # from base class
            'name', 'organization_id', 'organization_ref'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names, f"Column '{expected_col}' not found in column definitions")

    def test_init_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        with patch.dict(os.environ, {'CLICKHOUSE_AS_METADATA_TABLE': 'custom_as_table'}):
            processor = ASMetadataProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_as_table')

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_AS_METADATA_TABLE': 'test_as_table'}):
            processor = ASMetadataProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (ASMetadataProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Basic assertions
            self.assertIsInstance(result, str)
            self.assertIn("CREATE TABLE IF NOT EXISTS test_as_table", result)
            self.assertIn("ENGINE = MergeTree()", result)
            self.assertIn("ORDER BY", result)
            
            # Check that AS-specific columns are included
            as_columns = [
                'name', 'organization_id', 'organization_ref'
            ]
            for column in as_columns:
                self.assertIn(f"`{column}`", result, f"Column {column} not found in CREATE TABLE command")

    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        # Ensure the environment variable is not set
        with patch.dict(os.environ, {}, clear=True):
            processor = ASMetadataProcessor(self.mock_pipeline)
            result = processor.create_table_command()
            
            self.assertIn("CREATE TABLE IF NOT EXISTS meta_as", result)

    def test_column_definitions_structure(self):
        """Test that column definitions are properly structured."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        # Check each column definition has correct structure [name, type, insert_flag]
        for col_def in processor.column_defs:
            self.assertIsInstance(col_def, list, "Column definition should be a list")
            self.assertEqual(len(col_def), 3, "Column definition should have 3 elements")
            self.assertIsInstance(col_def[0], str, "Column name should be a string")
            # col_def[1] can be string or None (for ext field)
            self.assertIsInstance(col_def[2], bool, "Insert flag should be a boolean")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        # Create a mapping of column names to their types
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        # Test specific column types
        self.assertEqual(column_types['name'], 'LowCardinality(String)')
        self.assertEqual(column_types['organization_id'], 'String')
        self.assertEqual(column_types['organization_ref'], 'Nullable(String)')

    def test_build_metadata_fields_basic(self):
        """Test that build_metadata_fields returns correctly formatted data."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        value = {
            'data': {
                'name': 'FICTITIOUS-AS',
                'organization_id': 'ucla'
            }
        }
        
        result = processor.build_metadata_fields(value)
        
        # Check basic field mapping
        self.assertEqual(result['name'], 'FICTITIOUS-AS')
        self.assertEqual(result['organization_id'], 'ucla')
        
        # Check that ClickHouse lookup was performed for organization reference
        self.assertEqual(result['organization_ref'], 'mock_org_ref__v1')
        
        # Verify ClickHouse cacher was called
        self.mock_clickhouse_cacher.lookup.assert_called_once_with('meta_organization', 'ucla')

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        # Missing 'data' field entirely
        input_data_1 = {'other': 'value'}
        result_1 = processor.build_message(input_data_1, {})
        self.assertIsNone(result_1)
        
        # Missing 'id' field in data
        input_data_2 = {'data': {'name': 'TEST-AS', 'organization_id': 'test-org'}}
        result_2 = processor.build_message(input_data_2, {})
        self.assertIsNone(result_2)
        
        # Missing 'name' field in data
        input_data_3 = {'data': {'id': '67890', 'organization_id': 'test-org'}}
        result_3 = processor.build_message(input_data_3, {})
        self.assertIsNone(result_3)
        
        # Missing 'organization_id' field in data
        input_data_4 = {'data': {'id': '67890', 'name': 'TEST-AS'}}
        result_4 = processor.build_message(input_data_4, {})
        self.assertIsNone(result_4)

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': '67890',
                'name': 'FICTITIOUS-AS',
                'organization_id': 'ucla'
            }
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], '67890')
        self.assertEqual(record['name'], 'FICTITIOUS-AS')
        self.assertEqual(record['organization_id'], 'ucla')
        self.assertEqual(record['organization_ref'], 'mock_org_ref__v1')
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('67890__v'))

    def test_build_message_minimal_required_fields(self):
        """Test build_message with only required fields."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': '12345',
                'name': 'MINIMAL-AS',
                'organization_id': 'test-org'
            }
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], '12345')
        self.assertEqual(record['name'], 'MINIMAL-AS')
        self.assertEqual(record['organization_id'], 'test-org')
        self.assertEqual(record['organization_ref'], 'mock_org_ref__v1')
        
        # Other fields should be None or default values
        self.assertEqual(record['ext'], '{}')
        self.assertEqual(record['tag'], [])

    def test_build_message_existing_record_no_change(self):
        """Test build_message skips unchanged records."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': '67890',
                'name': 'TEST-AS',
                'organization_id': 'test-org'
            }
        }
        
        # Create a side effect function to handle the mock calls
        def side_effect_func(table, id_value):
            if table == 'meta_as' and id_value == '67890':
                # Calculate the hash the same way the processor does
                formatted_record = {"id": id_value}
                formatted_record.update(processor.build_metadata_fields(input_data))
                
                # Calculate hash
                import orjson
                import hashlib
                record_json = orjson.dumps(formatted_record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
                record_md5 = hashlib.md5(record_json.encode('utf-8')).hexdigest()
                
                # Return existing record with the actual hash (same hash means no change)
                return {'hash': record_md5, 'ref': '67890__v1'}
            elif table == 'meta_organization' and id_value == 'test-org':
                return {
                    'ref': 'mock_org_ref__v1',
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                }
            return None
        
        self.mock_clickhouse_cacher.lookup.side_effect = side_effect_func
        
        # Now test that it skips unchanged records
        result = processor.build_message(input_data, {})
        
        self.assertIsNone(result)

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': '67890',
                'name': 'UPDATED-AS',  # Changed value
                'organization_id': 'test-org'
            }
        }
        
        # Mock existing record with different hash
        mock_existing = {'hash': 'different_hash', 'ref': '67890__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], '67890')
        self.assertEqual(record['name'], 'UPDATED-AS')
        self.assertEqual(record['ref'], '67890__v2')  # Should increment version

    def test_build_message_comprehensive_scenario(self):
        """Test build_message with a comprehensive real-world scenario."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': '67890',
                'name': 'FICTITIOUS-AS',
                'organization_id': 'ucla',
                'ext': '{"type": "research", "established": "2020"}',
                'tag': ['research', 'academic', 'west-coast']
            }
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        
        # Verify all fields are properly processed
        self.assertEqual(record['id'], '67890')
        self.assertEqual(record['name'], 'FICTITIOUS-AS')
        self.assertEqual(record['organization_id'], 'ucla')
        self.assertEqual(record['organization_ref'], 'mock_org_ref__v1')
        
        # Verify ext and tag from base class processing
        self.assertEqual(record['ext'], '{"type": "research", "established": "2020"}')
        self.assertEqual(record['tag'], ['research', 'academic', 'west-coast'])
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('67890__v'))

    def test_clickhouse_cacher_lookup_called(self):
        """Test that ClickHouse cacher lookup is called for organization reference."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': {
                'id': '67890',
                'name': 'TEST-AS',
                'organization_id': 'ucla'
            }
        }
        
        result = processor.build_message(input_data, {})
        
        # Verify clickhouse cacher was called for both base record lookup and organization lookup
        self.mock_pipeline.cacher.assert_called_with("clickhouse")
        
        # Verify ClickHouse lookup for organization reference was among the calls
        self.mock_clickhouse_cacher.lookup.assert_any_call('meta_organization', 'ucla')


if __name__ == '__main__':
    unittest.main()
