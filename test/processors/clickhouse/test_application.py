#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.application import ApplicationMetadataProcessor


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


if __name__ == '__main__':
    unittest.main(verbosity=2)
