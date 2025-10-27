#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.redis.interface import BaseInterfaceMetadataProcessor


class TestBaseInterfaceMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
    def test_init_with_default_values(self):
        """Test that the processor initializes with default environment values."""
        
        with patch.dict(os.environ, {}, clear=True):
            processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
            
            # Check default values
            self.assertEqual(processor.table, 'meta_interface_cache')
            self.assertEqual(processor.expires, 86400)  # 1 day in seconds
            self.assertEqual(processor.pipeline, self.mock_pipeline)
            self.assertIsInstance(processor.match_fields, list)

    @patch.dict(os.environ, {
        'REDIS_IF_METADATA_TABLE': 'custom_interface_cache',
        'REDIS_IF_METADATA_EXPIRES': '3600'
    })
    def test_init_with_custom_environment_values(self):
        """Test that the processor respects custom environment variables."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Check custom values
        self.assertEqual(processor.table, 'custom_interface_cache')
        self.assertEqual(processor.expires, 3600)  # 1 hour in seconds

    @patch.dict(os.environ, {
        'REDIS_IF_METADATA_EXPIRES': 'invalid_number'
    })
    def test_init_with_invalid_expires_value(self):
        """Test that invalid expires value raises ValueError."""
        
        with self.assertRaises(ValueError):
            BaseInterfaceMetadataProcessor(self.mock_pipeline)

    def test_match_message_with_empty_match_fields(self):
        """Test match_message when match_fields is empty (should return False)."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Empty match_fields should return False for any message
        test_message = {
            "data": {
                "id": "interface1",
                "name": "eth0"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertTrue(result)

    def test_match_message_with_valid_match_fields(self):
        """Test match_message when match_fields are configured and data matches."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure match fields to look for ['data', 'id']
        processor.match_fields = [['data', 'id']]
        
        # Test with matching message
        test_message = {
            "data": {
                "id": "interface1",
                "name": "eth0"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertTrue(result)

    def test_match_message_with_multiple_match_fields(self):
        """Test match_message with multiple match field paths."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure multiple match fields
        processor.match_fields = [
            ['data', 'id'],
            ['metadata', 'interface_id'],
            ['info', 'name']
        ]
        
        # Test message that matches the second path
        test_message = {
            "metadata": {
                "interface_id": "eth0",
                "device": "router1"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertTrue(result)

    def test_match_message_with_no_matching_fields(self):
        """Test match_message when configured fields don't match the message."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure match fields that won't match the test message
        processor.match_fields = [
            ['data', 'id'],
            ['metadata', 'interface_id']
        ]
        
        # Test message that doesn't match any configured paths
        test_message = {
            "info": {
                "name": "eth0",
                "device": "router1"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertFalse(result)

    def test_match_message_with_null_values(self):
        """Test match_message when target field exists but is None."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure match fields
        processor.match_fields = [['data', 'id']]
        
        # Test message where the target field is None
        test_message = {
            "data": {
                "id": None,
                "name": "eth0"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertFalse(result)  # None values should not match

    def test_match_message_with_partial_path(self):
        """Test match_message when the path partially exists but not completely."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure match fields with deeper path
        processor.match_fields = [['data', 'interface', 'id']]
        
        # Test message where only part of the path exists
        test_message = {
            "data": {
                "name": "eth0"
                # Missing 'interface' key
            }
        }
        
        result = processor.match_message(test_message)
        self.assertFalse(result)

    def test_match_message_with_non_dict_intermediate_value(self):
        """Test match_message when an intermediate path value is not a dict."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure match fields
        processor.match_fields = [['data', 'interface', 'id']]
        
        # Test message where intermediate value is not a dict
        test_message = {
            "data": {
                "interface": "not_a_dict",  # Should be dict but is string
                "name": "eth0"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertFalse(result)

    def test_match_message_with_deep_nested_path(self):
        """Test match_message with deeply nested field paths."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure match fields with deep nesting
        processor.match_fields = [['network', 'devices', 'interfaces', 'physical', 'id']]
        
        # Test message with deep nesting that matches
        test_message = {
            "network": {
                "devices": {
                    "interfaces": {
                        "physical": {
                            "id": "eth0",
                            "type": "ethernet"
                        }
                    }
                }
            }
        }
        
        result = processor.match_message(test_message)
        self.assertTrue(result)

    def test_match_message_with_multiple_paths_one_matches(self):
        """Test match_message where only one of multiple configured paths matches."""
        
        processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
        
        # Configure multiple match fields
        processor.match_fields = [
            ['missing', 'field'],  # Won't match
            ['data', 'id'],        # Will match
            ['another', 'missing'] # Won't match
        ]
        
        # Test message that matches only the middle path
        test_message = {
            "data": {
                "id": "interface1",
                "name": "eth0"
            }
        }
        
        result = processor.match_message(test_message)
        self.assertTrue(result)  # Should return True if any path matches

    def test_table_and_expires_properties(self):
        """Test that table and expires properties are accessible."""
        
        with patch.dict(os.environ, {
            'REDIS_IF_METADATA_TABLE': 'test_table',
            'REDIS_IF_METADATA_EXPIRES': '7200'
        }):
            processor = BaseInterfaceMetadataProcessor(self.mock_pipeline)
            
            # Test that properties are accessible and correct
            self.assertEqual(processor.table, 'test_table')
            self.assertEqual(processor.expires, 7200)
            
            # Test that these are instance attributes, not class attributes
            processor2 = BaseInterfaceMetadataProcessor(self.mock_pipeline)
            processor2.table = 'different_table'
            processor2.expires = 1800
            
            # Original processor should be unchanged
            self.assertEqual(processor.table, 'test_table')
            self.assertEqual(processor.expires, 7200)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
