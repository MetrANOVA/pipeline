import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from metranova.consumers.metadata import YAMLFileConsumer

class TestYAMLFileConsumer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_handle_file_data_valid_metadata(self):
        """Test handle_file_data with valid metadata structure."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'test_table',
            'data': [
                {'id': 'record1', 'name': 'Test Record 1'},
                {'id': 'record2', 'name': 'Test Record 2'}
            ]
        }
        
        consumer.handle_file_data('/path/to/file.yml', data)
        
        # Records are handles all at once
        self.assertEqual(self.mock_pipeline.process_message.call_count, 1)
        
        calls = self.mock_pipeline.process_message.call_args_list
        self.assertEqual(calls[0][0][0], {'table': 'test_table', 'data': [{'id': 'record1', 'name': 'Test Record 1'},{'id': 'record2', 'name': 'Test Record 2'}]})
    def test_handle_file_data_no_table(self):
        """Test handle_file_data when table field is missing."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
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
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_no_data_field(self):
        """Test handle_file_data when data field is missing."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages (empty list default)
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_none_input(self):
        """Test handle_file_data with None input data."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_single_record(self):
        """Test handle_file_data with single record."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
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
            'data': [{
                'id': 'intf-001',
                'name': 'eth0',
                'type': 'ethernet',
                'description': 'Management interface'
            }]
        })

    def test_handle_file_data_complex_records(self):
        """Test handle_file_data with complex nested record structures."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
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
            'data': [{
                'id': 'complex-001',
                'metadata': {
                    'tags': ['production', 'critical'],
                    'properties': {
                        'region': 'us-west-2',
                        'environment': 'prod'
                    }
                },
                'array_field': [1, 2, 3, 4]
            }]
        }
        
        self.mock_pipeline.process_message.assert_called_once_with(expected_call)

class TestMetadataConsumersIntegration(unittest.TestCase):
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
            
            # Should process each record in the data array
            self.assertEqual(self.mock_pipeline.process_message.call_count, 1)
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)

if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)