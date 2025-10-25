#!/usr/bin/env python3

import unittest
from unittest.mock import MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.redis.base import BaseRedisProcessor


class TestBaseRedisProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        
        # Check that it inherits from BaseProcessor correctly
        self.assertEqual(processor.pipeline, self.mock_pipeline)
        self.assertIsNotNone(processor.logger)
        self.assertIsInstance(processor.required_fields, list)
        
        # Check Redis-specific initialization
        self.assertIsInstance(processor.match_fields, list)
        self.assertEqual(len(processor.match_fields), 0)  # Should be empty initially

    def test_match_fields_initialization(self):
        """Test that match_fields is properly initialized as an empty list."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        
        # Should be an empty list
        self.assertEqual(processor.match_fields, [])
        self.assertIsInstance(processor.match_fields, list)

    def test_has_match_field_with_empty_match_fields(self):
        """Test has_match_field when match_fields is empty."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        
        # Empty match_fields should return False for any input
        test_value = {
            "field1": "value1",
            "nested": {
                "field2": "value2"
            }
        }
        
        result = processor.has_match_field(test_value)
        self.assertFalse(result)

    def test_has_match_field_single_level_match(self):
        """Test has_match_field with single-level field path that matches."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field1']]
        
        test_value = {
            "field1": "value1",
            "field2": "value2"
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_single_level_no_match(self):
        """Test has_match_field with single-level field path that doesn't match."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field3']]
        
        test_value = {
            "field1": "value1",
            "field2": "value2"
        }
        
        result = processor.has_match_field(test_value)
        self.assertFalse(result)

    def test_has_match_field_nested_match(self):
        """Test has_match_field with nested field path that matches."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['nested', 'deep', 'field']]
        
        test_value = {
            "nested": {
                "deep": {
                    "field": "target_value"
                }
            },
            "other": "value"
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_nested_partial_path(self):
        """Test has_match_field when nested path is incomplete."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['nested', 'deep', 'missing']]
        
        test_value = {
            "nested": {
                "deep": {
                    "field": "target_value"
                    # Missing 'missing' key
                }
            }
        }
        
        result = processor.has_match_field(test_value)
        self.assertFalse(result)

    def test_has_match_field_non_dict_intermediate(self):
        """Test has_match_field when intermediate value is not a dict."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['nested', 'field', 'subfield']]
        
        test_value = {
            "nested": {
                "field": "string_not_dict"  # Should be dict but is string
            }
        }
        
        result = processor.has_match_field(test_value)
        self.assertFalse(result)

    def test_has_match_field_null_value_no_match(self):
        """Test has_match_field when target field exists but is None."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        test_value = {
            "field": None
        }
        
        result = processor.has_match_field(test_value)
        self.assertFalse(result)  # None values should not match

    def test_has_match_field_multiple_paths_first_matches(self):
        """Test has_match_field with multiple paths where first one matches."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [
            ['existing_field'],
            ['non_existing_field'],
            ['another_missing']
        ]
        
        test_value = {
            "existing_field": "value"
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_multiple_paths_last_matches(self):
        """Test has_match_field with multiple paths where last one matches."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [
            ['missing1'],
            ['missing2'],
            ['existing_field']
        ]
        
        test_value = {
            "existing_field": "value"
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_multiple_paths_none_match(self):
        """Test has_match_field with multiple paths where none match."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [
            ['missing1'],
            ['missing2'],
            ['missing3']
        ]
        
        test_value = {
            "existing_field": "value"
        }
        
        result = processor.has_match_field(test_value)
        self.assertFalse(result)

    def test_has_match_field_complex_nested_structure(self):
        """Test has_match_field with complex nested data structure."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['data', 'metadata', 'interface', 'id']]
        
        test_value = {
            "data": {
                "type": "interface",
                "metadata": {
                    "interface": {
                        "id": "eth0",
                        "name": "ethernet0",
                        "status": "up"
                    },
                    "device": {
                        "id": "router1"
                    }
                }
            },
            "timestamp": "2023-01-01T00:00:00Z"
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_empty_string_value(self):
        """Test has_match_field when target field is an empty string."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        test_value = {
            "field": ""
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)  # Empty string is not None, so should match

    def test_has_match_field_zero_value(self):
        """Test has_match_field when target field is zero."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        test_value = {
            "field": 0
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)  # Zero is not None, so should match

    def test_has_match_field_false_value(self):
        """Test has_match_field when target field is False."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        test_value = {
            "field": False
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)  # False is not None, so should match

    def test_has_match_field_list_value(self):
        """Test has_match_field when target field is a list."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        test_value = {
            "field": ["item1", "item2"]
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_dict_value(self):
        """Test has_match_field when target field is a dict."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        test_value = {
            "field": {"nested": "value"}
        }
        
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_has_match_field_with_non_dict_input(self):
        """Test has_match_field behavior when input is not a dict."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        processor.match_fields = [['field']]
        
        # Test with string input
        result = processor.has_match_field("not_a_dict")
        self.assertFalse(result)
        
        # Test with list input
        result = processor.has_match_field(["not", "a", "dict"])
        self.assertFalse(result)
        
        # Test with None input
        result = processor.has_match_field(None)
        self.assertFalse(result)

    def test_match_fields_modification(self):
        """Test that match_fields can be modified after initialization."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        
        # Initially empty
        self.assertEqual(processor.match_fields, [])
        
        # Add match fields
        processor.match_fields = [['field1'], ['nested', 'field2']]
        
        # Test that modification worked
        test_value = {"field1": "value"}
        result = processor.has_match_field(test_value)
        self.assertTrue(result)

    def test_inheritance_from_base_processor(self):
        """Test that BaseRedisProcessor properly inherits from BaseProcessor."""
        
        processor = BaseRedisProcessor(self.mock_pipeline)
        
        # Should have BaseProcessor attributes
        self.assertTrue(hasattr(processor, 'pipeline'))
        self.assertTrue(hasattr(processor, 'logger'))
        self.assertTrue(hasattr(processor, 'required_fields'))
        self.assertTrue(hasattr(processor, 'has_required_fields'))
        
        # Should have Redis-specific attributes
        self.assertTrue(hasattr(processor, 'match_fields'))
        self.assertTrue(hasattr(processor, 'has_match_field'))


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
