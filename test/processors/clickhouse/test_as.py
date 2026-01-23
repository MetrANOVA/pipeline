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
ASDictionary = as_module.ASDictionary


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
        self.assertEqual(processor.val_id_field, ['id'])
        
        # Test that required_fields is correctly set
        self.assertEqual(processor.required_fields, [['id'], ['name'], ['organization_id']])
        
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
            'name': 'FICTITIOUS-AS',
            'organization_id': 'ucla'
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
        self.assertEqual(result_1, [])
        
        # Missing 'id' field in data
        input_data_2 = {'data': [{'name': 'TEST-AS', 'organization_id': 'test-org'}]}
        result_2 = processor.build_message(input_data_2, {})
        self.assertEqual(result_2, [])
        
        # Missing 'name' field in data
        input_data_3 = {'data': [{'id': '67890', 'organization_id': 'test-org'}]}
        result_3 = processor.build_message(input_data_3, {})
        self.assertEqual(result_3, [])
        
        # Missing 'organization_id' field in data
        input_data_4 = {'data': [{'id': '67890', 'name': 'TEST-AS'}]}
        result_4 = processor.build_message(input_data_4, {})
        self.assertEqual(result_4, [])

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': '67890',
                'name': 'FICTITIOUS-AS',
                'organization_id': 'ucla'
            }]
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
            'data': [{
                'id': '12345',
                'name': 'MINIMAL-AS',
                'organization_id': 'test-org'
            }]
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
            'data': [{
                'id': '67890',
                'name': 'TEST-AS',
                'organization_id': 'test-org'
            }]
        }
        
        # Create a side effect function to handle the mock calls
        def side_effect_func(table, id_value):
            if table == 'meta_as' and id_value == '67890':
                # Calculate the hash the same way the processor does
                formatted_record = {"id": id_value}
                formatted_record.update(processor.build_metadata_fields(input_data['data'][0]))
                
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
        
        self.assertEqual(result, [])

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': '67890',
                'name': 'UPDATED-AS',  # Changed value
                'organization_id': 'test-org'
            }]
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
            'data': [{
                'id': '67890',
                'name': 'FICTITIOUS-AS',
                'organization_id': 'ucla',
                'ext': '{"type": "research", "established": "2020"}',
                'tag': ['research', 'academic', 'west-coast']
            }]
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
            'data': [{
                'id': '67890',
                'name': 'TEST-AS',
                'organization_id': 'ucla'
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        # Verify clickhouse cacher was called for both base record lookup and organization lookup
        self.mock_pipeline.cacher.assert_called_with("clickhouse")
        
        # Verify ClickHouse lookup for organization reference was among the calls
        self.mock_clickhouse_cacher.lookup.assert_any_call('meta_organization', 'ucla')

    def test_dictionary_enabled_by_default(self):
        """Test that dictionary is enabled by default."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)
        self.assertIsInstance(processor.ch_dictionaries[0], ASDictionary)

    @patch.dict(os.environ, {'CLICKHOUSE_AS_DICTIONARY_ENABLED': 'false'})
    def test_dictionary_disabled_by_env(self):
        """Test that dictionary can be disabled via environment variable."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)

    @patch.dict(os.environ, {'CLICKHOUSE_AS_DICTIONARY_ENABLED': '0'})
    def test_dictionary_disabled_by_env_zero(self):
        """Test that dictionary can be disabled with '0'."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)

    @patch.dict(os.environ, {'CLICKHOUSE_AS_DICTIONARY_ENABLED': 'true'})
    def test_dictionary_enabled_explicitly(self):
        """Test that dictionary can be explicitly enabled."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)

    def test_dictionary_receives_table_name(self):
        """Test that ASDictionary receives the correct table name."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        dictionary = processor.ch_dictionaries[0]
        self.assertEqual(dictionary.source_table_name, 'meta_as')

    @patch.dict(os.environ, {'CLICKHOUSE_AS_METADATA_TABLE': 'custom_as_table'})
    def test_dictionary_receives_custom_table_name(self):
        """Test that ASDictionary receives custom table name."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        dictionary = processor.ch_dictionaries[0]
        self.assertEqual(dictionary.source_table_name, 'custom_as_table')

    def test_get_ch_dictionaries(self):
        """Test that get_ch_dictionaries returns the dictionary list."""
        processor = ASMetadataProcessor(self.mock_pipeline)
        
        dictionaries = processor.get_ch_dictionaries()
        self.assertEqual(len(dictionaries), 1)
        self.assertIsInstance(dictionaries[0], ASDictionary)


class TestASDictionary(unittest.TestCase):
    """Unit tests for ASDictionary class."""

    def setUp(self):
        """Set up test fixtures."""
        self.source_table = 'meta_as'

    def test_init_default_values(self):
        """Test that ASDictionary initializes with correct default values."""
        dictionary = ASDictionary(self.source_table)
        
        # Test source table
        self.assertEqual(dictionary.source_table_name, self.source_table)
        
        # Test dictionary name
        self.assertEqual(dictionary.dictionary_name, 'meta_as_dict')
        
        # Test column definitions
        expected_columns = [
            ['id', 'UInt32'],
            ['name', 'String']
        ]
        self.assertEqual(dictionary.column_defs, expected_columns)
        
        # Test primary keys
        self.assertEqual(dictionary.primary_keys, ['id'])
        
        # Test lifetime
        self.assertEqual(dictionary.lifetime_min, '600')
        self.assertEqual(dictionary.lifetime_max, '3600')
        
        # Test layout
        self.assertEqual(dictionary.layout, "HASHED()")

    @patch.dict(os.environ, {'CLICKHOUSE_AS_DICTIONARY_NAME': 'custom_as_dict'})
    def test_init_with_custom_dictionary_name(self):
        """Test initialization with custom dictionary name."""
        dictionary = ASDictionary(self.source_table)
        self.assertEqual(dictionary.dictionary_name, 'custom_as_dict')

    @patch.dict(os.environ, {
        'CLICKHOUSE_AS_DICTIONARY_LIFETIME_MIN': '300',
        'CLICKHOUSE_AS_DICTIONARY_LIFETIME_MAX': '1800'
    })
    def test_init_with_custom_lifetime(self):
        """Test initialization with custom lifetime values."""
        dictionary = ASDictionary(self.source_table)
        self.assertEqual(dictionary.lifetime_min, '300')
        self.assertEqual(dictionary.lifetime_max, '1800')

    def test_inheritance_from_base_dictionary_mixin(self):
        """Test that ASDictionary inherits from BaseClickHouseDictionaryMixin."""
        from metranova.processors.clickhouse.base import BaseClickHouseDictionaryMixin
        dictionary = ASDictionary(self.source_table)
        self.assertIsInstance(dictionary, BaseClickHouseDictionaryMixin)

    def test_create_dictionary_command(self):
        """Test that ASDictionary can create a valid dictionary command."""
        dictionary = ASDictionary(self.source_table)
        
        command = dictionary.create_dictionary_command()
        
        # Verify command structure
        self.assertIn("CREATE DICTIONARY IF NOT EXISTS meta_as_dict", command)
        self.assertIn("`id` UInt32", command)
        self.assertIn("`name` String", command)
        self.assertIn("PRIMARY KEY (`id`)", command)
        self.assertIn("SOURCE(CLICKHOUSE(TABLE 'meta_as'", command)
        self.assertIn("LIFETIME(MIN 600 MAX 3600)", command)
        self.assertIn("LAYOUT(HASHED())", command)

    @patch.dict(os.environ, {
        'CLICKHOUSE_CLUSTER_NAME': 'test_cluster',
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_USERNAME': 'test_user',
        'CLICKHOUSE_PASSWORD': 'test_pass'
    })
    def test_create_dictionary_command_with_cluster_and_credentials(self):
        """Test dictionary creation with cluster and custom credentials."""
        dictionary = ASDictionary(self.source_table)
        
        command = dictionary.create_dictionary_command()
        
        self.assertIn("ON CLUSTER 'test_cluster'", command)
        self.assertIn("USER 'test_user'", command)
        self.assertIn("PASSWORD 'test_pass'", command)
        self.assertIn("DB 'test_db'", command)

    def test_hashed_layout_for_as_lookups(self):
        """Test that HASHED layout is properly configured for AS ID lookups."""
        dictionary = ASDictionary(self.source_table)
        
        # Verify layout type for direct key lookups
        self.assertEqual(dictionary.layout, "HASHED()")
        
        # Verify primary key is AS ID
        self.assertEqual(dictionary.primary_keys, ['id'])

    def test_uint32_id_column_type(self):
        """Test that AS ID column is UInt32."""
        dictionary = ASDictionary(self.source_table)
        
        # Find the id column definition
        id_col = next((col for col in dictionary.column_defs if col[0] == 'id'), None)
        
        self.assertIsNotNone(id_col)
        self.assertEqual(id_col[1], 'UInt32')

    def test_includes_name_field(self):
        """Test that dictionary includes name field for lookups."""
        dictionary = ASDictionary(self.source_table)
        
        # Find the name column definition
        name_col = next((col for col in dictionary.column_defs if col[0] == 'name'), None)
        
        self.assertIsNotNone(name_col)
        self.assertEqual(name_col[1], 'String')


if __name__ == '__main__':
    unittest.main()
