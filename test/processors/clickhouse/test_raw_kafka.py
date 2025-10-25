#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.raw_kafka import RawKafkaProcessor


class TestRawKafkaProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_RAW_KAFKA_TABLE': 'test_kafka_table'}):
            processor = RawKafkaProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (RawKafkaProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Verify basic structure
            self.assertIn('CREATE TABLE IF NOT EXISTS test_kafka_table', result)
            self.assertIn('ENGINE = MergeTree()', result)
            
            # Check for key columns
            self.assertIn('`timestamp` DateTime64(3)', result)
            self.assertIn('`topic` String', result)
            self.assertIn('`partition` UInt32', result)
            self.assertIn('`offset` UInt64', result)
            self.assertIn('`key` Nullable(String)', result)
            self.assertIn('`value` String', result)
            
            # Check ORDER BY clause
            self.assertIn('ORDER BY (`timestamp`,`topic`,`partition`)', result)
    
    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            processor = RawKafkaProcessor(self.mock_pipeline)
            
            result = processor.create_table_command()
            
            # Should use default table name
            self.assertIn('CREATE TABLE IF NOT EXISTS data_kafka_message', result)
    
    def test_build_message_basic(self):
        """Test build_message with basic input data and metadata."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "message_id": "12345",
            "user": "test_user",
            "action": "login",
            "timestamp": "2023-10-24T10:30:00Z"
        }
        
        msg_metadata = {
            "topic": "user_events",
            "partition": 1,
            "offset": 1234567,
            "key": "user_12345"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        # Should return a list with one dict
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        message = result[0]
        
        # Verify field mappings
        self.assertEqual(message["topic"], "user_events")
        self.assertEqual(message["partition"], 1)
        self.assertEqual(message["offset"], 1234567)
        self.assertEqual(message["key"], "user_12345")
        
        # Verify value is JSON serialized and sorted
        import orjson
        expected_value = orjson.dumps(input_value, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        self.assertEqual(message["value"], expected_value)
    
    def test_build_message_no_metadata(self):
        """Test build_message with None metadata."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "test": "data"
        }
        
        result = processor.build_message(input_value, None)
        
        message = result[0]
        
        # Should use default values when metadata is None
        self.assertEqual(message["topic"], "")
        self.assertEqual(message["partition"], 0)
        self.assertEqual(message["offset"], 0)
        self.assertIsNone(message["key"])
        
        # Value should still be serialized
        import orjson
        expected_value = orjson.dumps(input_value, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        self.assertEqual(message["value"], expected_value)
    
    def test_build_message_empty_metadata(self):
        """Test build_message with empty metadata dict."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "test": "data"
        }
        
        msg_metadata = {}
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Should use default values when metadata is empty
        self.assertEqual(message["topic"], "")
        self.assertEqual(message["partition"], 0)
        self.assertEqual(message["offset"], 0)
        self.assertIsNone(message["key"])
    
    def test_build_message_partial_metadata(self):
        """Test build_message with partial metadata."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "test": "data"
        }
        
        msg_metadata = {
            "topic": "test_topic",
            "partition": 5
            # Missing offset and key
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Should use provided values and defaults for missing ones
        self.assertEqual(message["topic"], "test_topic")
        self.assertEqual(message["partition"], 5)
        self.assertEqual(message["offset"], 0)  # Default
        self.assertIsNone(message["key"])  # Default
    
    def test_build_message_complex_value(self):
        """Test build_message with complex nested data structures."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "nested": {
                "array": [1, 2, 3],
                "object": {
                    "boolean": True,
                    "null": None,
                    "number": 42.5
                }
            },
            "string": "test",
            "unicode": "测试"
        }
        
        msg_metadata = {
            "topic": "complex_topic",
            "partition": 0,
            "offset": 999,
            "key": "complex_key"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Verify metadata fields
        self.assertEqual(message["topic"], "complex_topic")
        self.assertEqual(message["partition"], 0)
        self.assertEqual(message["offset"], 999)
        self.assertEqual(message["key"], "complex_key")
        
        # Verify value serialization preserves structure
        import orjson
        deserialized_value = orjson.loads(message["value"])
        self.assertEqual(deserialized_value, input_value)
        
        # Verify keys are sorted in the JSON output
        self.assertTrue(message["value"].startswith('{"nested":'))
    
    def test_build_message_special_characters(self):
        """Test build_message handles special characters and encoding properly."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "special_chars": "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./",
            "newlines": "line1\nline2\rline3\r\n",
            "tabs": "col1\tcol2\ttab",
            "unicode": "émojis: 🎉🚀💻",
            "quotes": 'He said "Hello" and she replied \'Hi\''
        }
        
        msg_metadata = {
            "topic": "special_chars",
            "partition": 0,
            "offset": 1,
            "key": "special_key"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Verify serialization handles special characters
        import orjson
        deserialized_value = orjson.loads(message["value"])
        self.assertEqual(deserialized_value, input_value)
    
    def test_build_message_numeric_types(self):
        """Test build_message handles various numeric types properly."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "integer": 42,
            "float": 3.14159,
            "negative": -123,
            "zero": 0,
            "large_number": 9999999999999999,
            "scientific": 1.23e10
        }
        
        msg_metadata = {
            "topic": "numeric_topic",
            "partition": 2,
            "offset": 5000,
            "key": "numeric_key"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Verify numeric types are preserved
        import orjson
        deserialized_value = orjson.loads(message["value"])
        self.assertEqual(deserialized_value, input_value)
    
    def test_build_message_null_key(self):
        """Test build_message when key is explicitly None in metadata."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {"test": "data"}
        
        msg_metadata = {
            "topic": "test_topic",
            "partition": 0,
            "offset": 100,
            "key": None
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Key should be None
        self.assertIsNone(message["key"])
    
    def test_build_message_empty_value(self):
        """Test build_message with empty input value."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {}
        
        msg_metadata = {
            "topic": "empty_topic",
            "partition": 0,
            "offset": 1,
            "key": "empty_key"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Should serialize empty dict
        self.assertEqual(message["value"], "{}")
    
    def test_build_message_zero_offset_partition(self):
        """Test build_message with zero values for offset and partition."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {"test": "data"}
        
        msg_metadata = {
            "topic": "zero_topic",
            "partition": 0,
            "offset": 0,
            "key": "zero_key"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Zero values should be preserved
        self.assertEqual(message["partition"], 0)
        self.assertEqual(message["offset"], 0)
    
    def test_build_message_large_offset(self):
        """Test build_message with large offset values."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {"test": "data"}
        
        msg_metadata = {
            "topic": "large_topic",
            "partition": 99,
            "offset": 18446744073709551615,  # Max UInt64
            "key": "large_key"
        }
        
        result = processor.build_message(input_value, msg_metadata)
        
        message = result[0]
        
        # Large values should be preserved
        self.assertEqual(message["partition"], 99)
        self.assertEqual(message["offset"], 18446744073709551615)
    
    def test_json_serialization_deterministic(self):
        """Test that JSON serialization is deterministic due to sorted keys."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        input_value = {
            "z_last": "value",
            "a_first": "value",
            "m_middle": "value"
        }
        
        msg_metadata = {
            "topic": "test",
            "partition": 0,
            "offset": 1,
            "key": "test"
        }
        
        # Serialize multiple times
        result1 = processor.build_message(input_value, msg_metadata)
        result2 = processor.build_message(input_value, msg_metadata)
        
        # Should be identical due to sorted keys
        self.assertEqual(result1[0]["value"], result2[0]["value"])
        
        # Should start with the alphabetically first key
        self.assertTrue(result1[0]["value"].startswith('{"a_first"'))
    
    def test_column_definitions(self):
        """Test that column definitions are correctly set."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        expected_columns = [
            ['timestamp', 'DateTime64(3)', False],
            ['topic', 'String', True],
            ['partition', 'UInt32', True],
            ['offset', 'UInt64', True],
            ['key', 'Nullable(String)', True],
            ['value', 'String', True]
        ]
        
        self.assertEqual(processor.column_defs, expected_columns)
    
    def test_order_by_definition(self):
        """Test that ORDER BY clause is correctly set."""
        processor = RawKafkaProcessor(self.mock_pipeline)
        
        expected_order_by = ['timestamp', 'topic', 'partition']
        
        self.assertEqual(processor.order_by, expected_order_by)
    
    def test_table_name_configuration(self):
        """Test table name configuration via environment variable."""
        # Test with custom table name
        with patch.dict(os.environ, {'CLICKHOUSE_RAW_KAFKA_TABLE': 'custom_kafka_table'}):
            processor = RawKafkaProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_kafka_table')
        
        # Test with default table name
        with patch.dict(os.environ, {}, clear=True):
            processor = RawKafkaProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'data_kafka_message')


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
