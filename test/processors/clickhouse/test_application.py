#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.application import ApplicationMetadataProcessor, ApplicationDictionary


class TestApplicationMetadataProcessor(unittest.TestCase):
    """Unit tests for ApplicationMetadataProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_application')
        
        # Test versioned is False
        self.assertFalse(processor.versioned)
        
        # Test table engine
        self.assertEqual(processor.table_engine, 'ReplacingMergeTree')
        
        # Test order_by
        self.assertEqual(processor.order_by, ['protocol', 'id', 'port_range_min', 'port_range_max'])
        
        # Test val_id_field
        self.assertEqual(processor.val_id_field, ['id'])
        
        # Test int_fields
        self.assertEqual(processor.int_fields, ['port_range_min', 'port_range_max'])
        
        # Test required_fields
        expected_required = [['id'], ['protocol'], ['port_range_min'], ['port_range_max']]
        self.assertEqual(processor.required_fields, expected_required)

    @patch.dict(os.environ, {'CLICKHOUSE_APPLICATION_METADATA_TABLE': 'custom_app_table'})
    def test_init_with_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        self.assertEqual(processor.table, 'custom_app_table')

    def test_column_definitions(self):
        """Test that column definitions are properly configured."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        # Verify expected columns exist
        column_names = [col[0] for col in processor.column_defs]
        
        self.assertIn('id', column_names)
        self.assertIn('insert_time', column_names)
        self.assertIn('ext', column_names)
        self.assertIn('tag', column_names)
        self.assertIn('protocol', column_names)
        self.assertIn('port_range_min', column_names)
        self.assertIn('port_range_max', column_names)

    def test_inheritance_from_base_metadata_processor(self):
        """Test that ApplicationMetadataProcessor inherits from BaseMetadataProcessor."""
        from metranova.processors.clickhouse.base import BaseMetadataProcessor
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        self.assertIsInstance(processor, BaseMetadataProcessor)

    def test_table_configuration_for_non_versioned(self):
        """Test that non-versioned configuration is correct."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        # Test that versioned is False
        self.assertFalse(processor.versioned)
        
        # Test that ReplacingMergeTree is used (appropriate for non-versioned data)
        self.assertEqual(processor.table_engine, 'ReplacingMergeTree')

    def test_dictionary_enabled_by_default(self):
        """Test that dictionary is enabled by default."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)
        self.assertIsInstance(processor.ch_dictionaries[0], ApplicationDictionary)

    @patch.dict(os.environ, {'CLICKHOUSE_APPLICATION_DICTIONARY_ENABLED': 'false'})
    def test_dictionary_disabled_by_env(self):
        """Test that dictionary can be disabled via environment variable."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)

    @patch.dict(os.environ, {'CLICKHOUSE_APPLICATION_DICTIONARY_ENABLED': '0'})
    def test_dictionary_disabled_by_env_zero(self):
        """Test that dictionary can be disabled with '0'."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)

    @patch.dict(os.environ, {'CLICKHOUSE_APPLICATION_DICTIONARY_ENABLED': 'true'})
    def test_dictionary_enabled_explicitly(self):
        """Test that dictionary can be explicitly enabled."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)

    def test_dictionary_receives_table_name(self):
        """Test that ApplicationDictionary receives the correct table name."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        dictionary = processor.ch_dictionaries[0]
        self.assertEqual(dictionary.source_table_name, 'meta_application')

    @patch.dict(os.environ, {'CLICKHOUSE_APPLICATION_METADATA_TABLE': 'custom_app_table'})
    def test_dictionary_receives_custom_table_name(self):
        """Test that ApplicationDictionary receives custom table name."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        dictionary = processor.ch_dictionaries[0]
        self.assertEqual(dictionary.source_table_name, 'custom_app_table')

    def test_get_ch_dictionaries(self):
        """Test that get_ch_dictionaries returns the dictionary list."""
        processor = ApplicationMetadataProcessor(self.mock_pipeline)
        
        dictionaries = processor.get_ch_dictionaries()
        self.assertEqual(len(dictionaries), 1)
        self.assertIsInstance(dictionaries[0], ApplicationDictionary)


class TestApplicationDictionary(unittest.TestCase):
    """Unit tests for ApplicationDictionary class."""

    def setUp(self):
        """Set up test fixtures."""
        self.source_table = 'meta_application'

    def test_init_default_values(self):
        """Test that ApplicationDictionary initializes with correct default values."""
        dictionary = ApplicationDictionary(self.source_table)
        
        # Test source table
        self.assertEqual(dictionary.source_table_name, self.source_table)
        
        # Test dictionary name
        self.assertEqual(dictionary.dictionary_name, 'meta_application_dict')
        
        # Test column definitions
        expected_columns = [
            ['id', 'String'],
            ['protocol', 'String'],
            ['port_range_min', 'UInt16'],
            ['port_range_max', 'UInt16']
        ]
        self.assertEqual(dictionary.column_defs, expected_columns)
        
        # Test primary keys
        self.assertEqual(dictionary.primary_keys, ['protocol'])
        
        # Test lifetime
        self.assertEqual(dictionary.lifetime_min, '600')
        self.assertEqual(dictionary.lifetime_max, '3600')
        
        # Test layout
        self.assertEqual(dictionary.layout, "RANGE_HASHED(range_lookup_strategy 'min')")
        
        # Test range fields
        self.assertEqual(dictionary.layout_range_min, 'port_range_min')
        self.assertEqual(dictionary.layout_range_max, 'port_range_max')

    @patch.dict(os.environ, {'CLICKHOUSE_APPLICATION_DICTIONARY_NAME': 'custom_app_dict'})
    def test_init_with_custom_dictionary_name(self):
        """Test initialization with custom dictionary name."""
        dictionary = ApplicationDictionary(self.source_table)
        self.assertEqual(dictionary.dictionary_name, 'custom_app_dict')

    @patch.dict(os.environ, {
        'CLICKHOUSE_APPLICATION_DICTIONARY_LIFETIME_MIN': '300',
        'CLICKHOUSE_APPLICATION_DICTIONARY_LIFETIME_MAX': '1800'
    })
    def test_init_with_custom_lifetime(self):
        """Test initialization with custom lifetime values."""
        dictionary = ApplicationDictionary(self.source_table)
        self.assertEqual(dictionary.lifetime_min, '300')
        self.assertEqual(dictionary.lifetime_max, '1800')

    def test_inheritance_from_base_dictionary_mixin(self):
        """Test that ApplicationDictionary inherits from BaseClickHouseDictionaryMixin."""
        from metranova.processors.clickhouse.base import BaseClickHouseDictionaryMixin
        dictionary = ApplicationDictionary(self.source_table)
        self.assertIsInstance(dictionary, BaseClickHouseDictionaryMixin)

    def test_create_dictionary_command(self):
        """Test that ApplicationDictionary can create a valid dictionary command."""
        dictionary = ApplicationDictionary(self.source_table)
        
        command = dictionary.create_dictionary_command()
        
        # Verify command structure
        self.assertIn("CREATE DICTIONARY IF NOT EXISTS meta_application_dict", command)
        self.assertIn("`id` String", command)
        self.assertIn("`protocol` String", command)
        self.assertIn("`port_range_min` UInt16", command)
        self.assertIn("`port_range_max` UInt16", command)
        self.assertIn("PRIMARY KEY (`protocol`)", command)
        self.assertIn("SOURCE(CLICKHOUSE(TABLE 'meta_application'", command)
        self.assertIn("LIFETIME(MIN 600 MAX 3600)", command)
        self.assertIn("LAYOUT(RANGE_HASHED(range_lookup_strategy 'min'))", command)
        self.assertIn("RANGE(MIN port_range_min MAX port_range_max)", command)

    @patch.dict(os.environ, {
        'CLICKHOUSE_CLUSTER_NAME': 'test_cluster',
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_USERNAME': 'test_user',
        'CLICKHOUSE_PASSWORD': 'test_pass'
    })
    def test_create_dictionary_command_with_cluster_and_credentials(self):
        """Test dictionary creation with cluster and custom credentials."""
        dictionary = ApplicationDictionary(self.source_table)
        
        command = dictionary.create_dictionary_command()
        
        self.assertIn("ON CLUSTER 'test_cluster'", command)
        self.assertIn("USER 'test_user'", command)
        self.assertIn("PASSWORD 'test_pass'", command)
        self.assertIn("DB 'test_db'", command)

    def test_range_hashed_layout_for_port_ranges(self):
        """Test that RANGE_HASHED layout is properly configured for port range lookups."""
        dictionary = ApplicationDictionary(self.source_table)
        
        # Verify layout type for range-based lookups
        self.assertIn("RANGE_HASHED", dictionary.layout)
        self.assertIn("range_lookup_strategy", dictionary.layout)
        
        # Verify range fields are set
        self.assertEqual(dictionary.layout_range_min, 'port_range_min')
        self.assertEqual(dictionary.layout_range_max, 'port_range_max')


if __name__ == '__main__':
    unittest.main(verbosity=2)
