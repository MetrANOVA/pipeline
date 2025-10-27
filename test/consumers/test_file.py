#!/usr/bin/env python3

import unittest
import sys
import os
import tempfile
import yaml
import orjson
from unittest.mock import Mock, patch, mock_open
from io import StringIO

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.file import BaseFileConsumer, YAMLFileConsumer, MetadataYAMLFileConsumer, JSONFileConsumer


class TestBaseFileConsumer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
    def test_init_default_values(self):
        """Test that the consumer initializes with correct default values."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        # Check default values
        self.assertEqual(consumer.file_paths, [])
        self.assertEqual(consumer.update_interval, -1)
        self.assertIsNone(consumer.datasource)

    @patch.dict(os.environ, {'FILE_CONSUMER_PATHS': '/path/to/file1.txt,/path/to/file2.txt'})
    def test_init_with_file_paths(self):
        """Test initialization with file paths from environment variable."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        self.assertEqual(consumer.file_paths, ['/path/to/file1.txt', '/path/to/file2.txt'])

    @patch.dict(os.environ, {'FILE_CONSUMER_PATHS': '/path/to/file1.txt, /path/to/file2.txt , /path/to/file3.txt'})
    def test_init_with_file_paths_whitespace(self):
        """Test initialization handles whitespace in file paths correctly."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        self.assertEqual(consumer.file_paths, ['/path/to/file1.txt', '/path/to/file2.txt', '/path/to/file3.txt'])

    @patch.dict(os.environ, {'PREFIX_FILE_CONSUMER_PATHS': '/prefixed/file.txt'})
    def test_init_with_env_prefix(self):
        """Test initialization with environment variable prefix."""
        
        consumer = BaseFileConsumer(self.mock_pipeline, env_prefix='PREFIX')
        
        self.assertEqual(consumer.file_paths, ['/prefixed/file.txt'])

    @patch.dict(os.environ, {'PREFIX_FILE_CONSUMER_PATHS': '/prefixed/file.txt'})
    def test_init_with_env_prefix_underscore(self):
        """Test initialization with environment variable prefix that already has underscore."""
        
        consumer = BaseFileConsumer(self.mock_pipeline, env_prefix='PREFIX_')
        
        self.assertEqual(consumer.file_paths, ['/prefixed/file.txt'])

    @patch.dict(os.environ, {'FILE_CONSUMER_UPDATE_INTERVAL': '300'})
    def test_init_with_update_interval(self):
        """Test initialization with custom update interval."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        self.assertEqual(consumer.update_interval, 300)

    def test_init_empty_file_paths(self):
        """Test initialization when no file paths are provided."""
        
        # Ensure the environment variable is not set
        if 'FILE_CONSUMER_PATHS' in os.environ:
            del os.environ['FILE_CONSUMER_PATHS']
            
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        self.assertEqual(consumer.file_paths, [])

    def test_load_file_data_default(self):
        """Test default load_file_data method."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        # Mock file object
        mock_file = Mock()
        mock_file.read.return_value = "test content"
        
        result = consumer.load_file_data(mock_file)
        
        self.assertEqual(result, "test content")
        mock_file.read.assert_called_once()

    def test_handle_file_data(self):
        """Test handle_file_data method."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data("/path/to/file.txt", "test data")
        
        self.mock_pipeline.process_message.assert_called_once_with({
            'file_path': '/path/to/file.txt',
            'data': 'test data'
        })

    @patch('builtins.open', new_callable=mock_open, read_data="test file content")
    def test_consume_messages_success(self, mock_file_open):
        """Test consume_messages with successful file reading."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        consumer.file_paths = ['/path/to/test.txt']
        
        consumer.consume_messages()
        
        # Verify file was opened and processed
        self.mock_pipeline.process_message.assert_called_once_with({
            'file_path': '/path/to/test.txt',
            'data': 'test file content'
        })

    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_consume_messages_file_not_found(self, mock_file_open):
        """Test consume_messages handles FileNotFoundError gracefully."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        consumer.file_paths = ['/path/to/nonexistent.txt']
        
        with patch.object(consumer.logger, 'error') as mock_error:
            consumer.consume_messages()
            
            mock_error.assert_called_once()
            self.assertIn("File not found", mock_error.call_args[0][0])

        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    @patch('builtins.open', side_effect=PermissionError("Permission denied"))
    def test_consume_messages_permission_error(self, mock_file_open):
        """Test consume_messages handles general exceptions gracefully."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        consumer.file_paths = ['/path/to/protected.txt']
        
        with patch.object(consumer.logger, 'error') as mock_error:
            consumer.consume_messages()
            
            mock_error.assert_called_once()
            self.assertIn("Error processing file", mock_error.call_args[0][0])

        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    @patch('builtins.open', new_callable=mock_open, read_data="content1")
    def test_consume_messages_multiple_files(self, mock_file_open):
        """Test consume_messages with multiple files."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        consumer.file_paths = ['/path/to/file1.txt', '/path/to/file2.txt']
        
        consumer.consume_messages()
        
        # Should process both files
        self.assertEqual(self.mock_pipeline.process_message.call_count, 2)
        
        # Check the calls
        calls = self.mock_pipeline.process_message.call_args_list
        self.assertEqual(calls[0][0][0]['file_path'], '/path/to/file1.txt')
        self.assertEqual(calls[1][0][0]['file_path'], '/path/to/file2.txt')

    def test_consume_messages_empty_file_paths(self):
        """Test consume_messages with no file paths."""
        
        consumer = BaseFileConsumer(self.mock_pipeline)
        consumer.file_paths = []
        
        consumer.consume_messages()
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()


class TestYAMLFileConsumer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_load_file_data_valid_yaml(self):
        """Test load_file_data with valid YAML content."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        yaml_content = """
        key1: value1
        key2:
          - item1
          - item2
        key3:
          nested_key: nested_value
        """
        
        mock_file = StringIO(yaml_content)
        result = consumer.load_file_data(mock_file)
        
        expected = {
            'key1': 'value1',
            'key2': ['item1', 'item2'],
            'key3': {'nested_key': 'nested_value'}
        }
        
        self.assertEqual(result, expected)

    def test_load_file_data_invalid_yaml(self):
        """Test load_file_data with invalid YAML content."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        # Invalid YAML with improper indentation
        invalid_yaml = """
        key1: value1
      invalid_indentation: value2
        """
        
        mock_file = StringIO(invalid_yaml)
        
        with patch.object(consumer.logger, 'error') as mock_error:
            result = consumer.load_file_data(mock_file)
            
            self.assertIsNone(result)
            mock_error.assert_called_once()
            self.assertIn("Error parsing YAML file", mock_error.call_args[0][0])

    def test_load_file_data_empty_yaml(self):
        """Test load_file_data with empty YAML file."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        mock_file = StringIO("")
        result = consumer.load_file_data(mock_file)
        
        self.assertIsNone(result)

    def test_load_file_data_yaml_with_special_characters(self):
        """Test load_file_data with YAML containing special characters."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        yaml_content = """
        unicode_key: "Special chars: áéíóú 中文 🚀"
        quoted_string: "This is a 'quoted' string"
        multiline: |
          This is a
          multiline string
        """
        
        mock_file = StringIO(yaml_content)
        result = consumer.load_file_data(mock_file)
        
        self.assertIsInstance(result, dict)
        self.assertIn('unicode_key', result)
        self.assertIn('Special chars', result['unicode_key'])


class TestMetadataYAMLFileConsumer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_handle_file_data_valid_metadata(self):
        """Test handle_file_data with valid metadata structure."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'test_table',
            'data': [
                {'id': 'record1', 'name': 'Test Record 1'},
                {'id': 'record2', 'name': 'Test Record 2'}
            ]
        }
        
        consumer.handle_file_data('/path/to/file.yml', data)
        
        # Should process each record
        self.assertEqual(self.mock_pipeline.process_message.call_count, 2)
        
        calls = self.mock_pipeline.process_message.call_args_list
        self.assertEqual(calls[0][0][0], {'table': 'test_table', 'data': {'id': 'record1', 'name': 'Test Record 1'}})
        self.assertEqual(calls[1][0][0], {'table': 'test_table', 'data': {'id': 'record2', 'name': 'Test Record 2'}})

    def test_handle_file_data_no_table(self):
        """Test handle_file_data when table field is missing."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'data': [{'id': 'record1', 'name': 'Test Record 1'}]
        }
        
        with patch.object(consumer.logger, 'error') as mock_error:
            consumer.handle_file_data('/path/to/file.yml', data)
            
            mock_error.assert_called_once()
            self.assertIn("No table found in file", mock_error.call_args[0][0])

        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_empty_data(self):
        """Test handle_file_data with empty data array."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'test_table',
            'data': []
        }
        
        consumer.handle_file_data('/path/to/file.yml', data)
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_no_data_field(self):
        """Test handle_file_data when data field is missing."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'test_table'
        }
        
        consumer.handle_file_data('/path/to/file.yml', data)
        
        # Should not process any messages (empty list default)
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_none_input(self):
        """Test handle_file_data with None input data."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_single_record(self):
        """Test handle_file_data with single record."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'interface_table',
            'data': [
                {
                    'id': 'intf-001',
                    'name': 'eth0',
                    'type': 'ethernet',
                    'description': 'Management interface'
                }
            ]
        }
        
        consumer.handle_file_data('/path/to/interfaces.yml', data)
        
        self.mock_pipeline.process_message.assert_called_once_with({
            'table': 'interface_table',
            'data': {
                'id': 'intf-001',
                'name': 'eth0',
                'type': 'ethernet',
                'description': 'Management interface'
            }
        })

    def test_handle_file_data_complex_records(self):
        """Test handle_file_data with complex nested record structures."""
        
        consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'complex_table',
            'data': [
                {
                    'id': 'complex-001',
                    'metadata': {
                        'tags': ['production', 'critical'],
                        'properties': {
                            'region': 'us-west-2',
                            'environment': 'prod'
                        }
                    },
                    'array_field': [1, 2, 3, 4]
                }
            ]
        }
        
        consumer.handle_file_data('/path/to/complex.yml', data)
        
        expected_call = {
            'table': 'complex_table',
            'data': {
                'id': 'complex-001',
                'metadata': {
                    'tags': ['production', 'critical'],
                    'properties': {
                        'region': 'us-west-2',
                        'environment': 'prod'
                    }
                },
                'array_field': [1, 2, 3, 4]
            }
        }
        
        self.mock_pipeline.process_message.assert_called_once_with(expected_call)


class TestJSONFileConsumer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_load_file_data_valid_json(self):
        """Test load_file_data with valid JSON content."""
        
        consumer = JSONFileConsumer(self.mock_pipeline)
        
        json_data = {
            'key1': 'value1',
            'key2': ['item1', 'item2'],
            'key3': {'nested_key': 'nested_value'}
        }
        
        json_string = orjson.dumps(json_data).decode('utf-8')
        mock_file = Mock()
        mock_file.read.return_value = json_string
        
        result = consumer.load_file_data(mock_file)
        
        self.assertEqual(result, json_data)

    def test_load_file_data_invalid_json(self):
        """Test load_file_data with invalid JSON content."""
        
        consumer = JSONFileConsumer(self.mock_pipeline)
        
        # Invalid JSON with trailing comma
        invalid_json = '{"key1": "value1", "key2": "value2",}'
        
        mock_file = Mock()
        mock_file.read.return_value = invalid_json
        
        with patch.object(consumer.logger, 'error') as mock_error:
            result = consumer.load_file_data(mock_file)
            
            self.assertIsNone(result)
            mock_error.assert_called_once()
            self.assertIn("Error parsing JSON file", mock_error.call_args[0][0])

    def test_load_file_data_empty_json(self):
        """Test load_file_data with empty JSON file."""
        
        consumer = JSONFileConsumer(self.mock_pipeline)
        
        mock_file = Mock()
        mock_file.read.return_value = ""
        
        with patch.object(consumer.logger, 'error') as mock_error:
            result = consumer.load_file_data(mock_file)
            
            self.assertIsNone(result)
            mock_error.assert_called_once()

    def test_load_file_data_json_with_unicode(self):
        """Test load_file_data with JSON containing Unicode characters."""
        
        consumer = JSONFileConsumer(self.mock_pipeline)
        
        json_data = {
            'unicode_text': 'Special chars: áéíóú 中文 🚀',
            'emoji': '🐍🔥',
            'accents': 'café résumé naïve'
        }
        
        json_string = orjson.dumps(json_data).decode('utf-8')
        mock_file = Mock()
        mock_file.read.return_value = json_string
        
        result = consumer.load_file_data(mock_file)
        
        self.assertEqual(result, json_data)
        self.assertEqual(result['unicode_text'], 'Special chars: áéíóú 中文 🚀')

    def test_load_file_data_json_array(self):
        """Test load_file_data with JSON array as root element."""
        
        consumer = JSONFileConsumer(self.mock_pipeline)
        
        json_data = [
            {'id': 1, 'name': 'Item 1'},
            {'id': 2, 'name': 'Item 2'},
            {'id': 3, 'name': 'Item 3'}
        ]
        
        json_string = orjson.dumps(json_data).decode('utf-8')
        mock_file = Mock()
        mock_file.read.return_value = json_string
        
        result = consumer.load_file_data(mock_file)
        
        self.assertEqual(result, json_data)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)

    def test_load_file_data_json_numbers_and_booleans(self):
        """Test load_file_data with various JSON data types."""
        
        consumer = JSONFileConsumer(self.mock_pipeline)
        
        json_data = {
            'string': 'test',
            'integer': 42,
            'float': 3.14159,
            'boolean_true': True,
            'boolean_false': False,
            'null_value': None,
            'array': [1, 2, 3],
            'object': {'nested': 'value'}
        }
        
        json_string = orjson.dumps(json_data).decode('utf-8')
        mock_file = Mock()
        mock_file.read.return_value = json_string
        
        result = consumer.load_file_data(mock_file)
        
        self.assertEqual(result, json_data)
        self.assertEqual(result['integer'], 42)
        self.assertEqual(result['float'], 3.14159)
        self.assertTrue(result['boolean_true'])
        self.assertFalse(result['boolean_false'])
        self.assertIsNone(result['null_value'])


class TestFileConsumersIntegration(unittest.TestCase):
    """Integration tests for file consumers."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_yaml_consumer_with_real_file(self):
        """Test YAMLFileConsumer with a real temporary file."""
        
        yaml_content = """
table: meta_interface
data:
  - id: interface-001
    name: eth0
    type: ethernet
    description: Management interface
    properties:
      speed: 1000
      duplex: full
  - id: interface-002
    name: eth1
    type: ethernet
    description: Data interface
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as temp_file:
            temp_file.write(yaml_content)
            temp_file_path = temp_file.name
        
        try:
            # Test YAMLFileConsumer
            consumer = YAMLFileConsumer(self.mock_pipeline)
            consumer.file_paths = [temp_file_path]
            
            consumer.consume_messages()
            
            # Should process the file
            self.mock_pipeline.process_message.assert_called_once()
            
            # Test MetadataYAMLFileConsumer
            self.mock_pipeline.reset_mock()
            metadata_consumer = MetadataYAMLFileConsumer(self.mock_pipeline)
            metadata_consumer.file_paths = [temp_file_path]
            
            metadata_consumer.consume_messages()
            
            # Should process each record in the data array
            self.assertEqual(self.mock_pipeline.process_message.call_count, 2)
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)

    def test_json_consumer_with_real_file(self):
        """Test JSONFileConsumer with a real temporary file."""
        
        json_data = {
            "metadata": [
                {
                    "id": "device-001",
                    "name": "router-01",
                    "type": "router",
                    "interfaces": ["eth0", "eth1"]
                },
                {
                    "id": "device-002", 
                    "name": "switch-01",
                    "type": "switch",
                    "interfaces": ["ge-0/0/0", "ge-0/0/1"]
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write(orjson.dumps(json_data).decode('utf-8'))
            temp_file_path = temp_file.name
        
        try:
            consumer = JSONFileConsumer(self.mock_pipeline)
            consumer.file_paths = [temp_file_path]
            
            consumer.consume_messages()
            
            # Should process the file
            self.mock_pipeline.process_message.assert_called_once()
            
            # Check the processed data
            call_args = self.mock_pipeline.process_message.call_args[0][0]
            self.assertEqual(call_args['file_path'], temp_file_path)
            self.assertEqual(call_args['data'], json_data)
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)