#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.kafka import KafkaConsumer
from confluent_kafka import KafkaError


class TestKafkaConsumer(unittest.TestCase):
    """Unit tests for KafkaConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        # Patch KafkaConnector to avoid actual Kafka connection
        with patch('metranova.consumers.kafka.KafkaConnector') as mock_connector_class:
            self.mock_connector = Mock()
            self.mock_client = Mock()
            self.mock_connector.client = self.mock_client
            mock_connector_class.return_value = self.mock_connector
            
            self.consumer = KafkaConsumer(self.mock_pipeline)

    def test_initialization(self):
        """Test that KafkaConsumer initializes correctly."""
        self.assertIsNotNone(self.consumer.datasource)
        self.assertEqual(self.consumer.pipeline, self.mock_pipeline)

    def test_consume_messages_no_message(self):
        """Test consume_messages when poll returns None."""
        self.mock_client.poll.return_value = None
        
        self.consumer.consume_messages()
        
        # Verify poll was called
        self.mock_client.poll.assert_called_once_with(timeout=1.0)
        # Verify no message processing occurred
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_partition_eof(self):
        """Test consume_messages handles partition EOF correctly."""
        mock_msg = Mock()
        mock_error = Mock()
        mock_error.code.return_value = KafkaError._PARTITION_EOF
        mock_msg.error.return_value = mock_error
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0
        
        self.mock_client.poll.return_value = mock_msg
        
        with patch.object(self.consumer.logger, 'debug') as mock_debug:
            self.consumer.consume_messages()
            
            # Verify debug log was called
            mock_debug.assert_called_once()
            self.assertIn('End of partition', mock_debug.call_args[0][0])
        
        # Verify no message processing occurred
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_kafka_error(self):
        """Test consume_messages handles Kafka errors correctly."""
        mock_msg = Mock()
        mock_error = Mock()
        mock_error.code.return_value = KafkaError.UNKNOWN
        mock_msg.error.return_value = mock_error
        
        self.mock_client.poll.return_value = mock_msg
        
        with patch.object(self.consumer.logger, 'error') as mock_error_log:
            self.consumer.consume_messages()
            
            # Verify error log was called
            mock_error_log.assert_called_once()
            self.assertIn('Kafka error', mock_error_log.call_args[0][0])
        
        # Verify no message processing occurred
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_success(self):
        """Test consume_messages processes valid message successfully."""
        import orjson
        
        test_data = {'key': 'value', 'number': 123}
        test_value = orjson.dumps(test_data)
        
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = test_value
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.timestamp.return_value = (1, 1638360000000)
        mock_msg.key.return_value = b'test_key'
        
        self.mock_client.poll.return_value = mock_msg
        
        self.consumer.consume_messages()
        
        # Verify message was processed
        self.mock_pipeline.process_message.assert_called_once()
        
        # Verify message data
        call_args = self.mock_pipeline.process_message.call_args
        msg_data = call_args[0][0]
        msg_metadata = call_args[1]['consumer_metadata']
        
        self.assertEqual(msg_data, test_data)
        self.assertEqual(msg_metadata['topic'], 'test_topic')
        self.assertEqual(msg_metadata['partition'], 0)
        self.assertEqual(msg_metadata['offset'], 100)
        self.assertEqual(msg_metadata['timestamp'], 1638360000000)
        self.assertEqual(msg_metadata['key'], 'test_key')

    def test_consume_messages_null_value(self):
        """Test consume_messages handles null message value."""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = None
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.timestamp.return_value = (1, 1638360000000)
        mock_msg.key.return_value = None
        
        self.mock_client.poll.return_value = mock_msg
        
        self.consumer.consume_messages()
        
        # Verify message was processed with None data
        self.mock_pipeline.process_message.assert_called_once()
        call_args = self.mock_pipeline.process_message.call_args
        msg_data = call_args[0][0]
        self.assertIsNone(msg_data)

    def test_consume_messages_no_key(self):
        """Test consume_messages handles missing message key."""
        import orjson
        
        test_data = {'test': 'data'}
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps(test_data)
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.timestamp.return_value = None
        mock_msg.key.return_value = None
        
        self.mock_client.poll.return_value = mock_msg
        
        self.consumer.consume_messages()
        
        # Verify metadata has None for key and timestamp
        call_args = self.mock_pipeline.process_message.call_args
        msg_metadata = call_args[1]['consumer_metadata']
        self.assertIsNone(msg_metadata['key'])
        self.assertIsNone(msg_metadata['timestamp'])

    def test_consume_messages_processing_error(self):
        """Test consume_messages handles processing errors gracefully."""
        import orjson
        
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps({'test': 'data'})
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.timestamp.return_value = (1, 1638360000000)
        mock_msg.key.return_value = b'test_key'
        
        self.mock_client.poll.return_value = mock_msg
        self.mock_pipeline.process_message.side_effect = Exception("Processing error")
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called_once()
            self.assertIn('Error processing message', mock_error.call_args[0][0])

    def test_close_with_client(self):
        """Test close method closes Kafka client."""
        self.consumer.close()
        
        self.mock_client.close.assert_called_once_with(timeout=5.0)

    def test_close_without_client(self):
        """Test close method handles missing client gracefully."""
        self.consumer.datasource.client = None
        
        # Should not raise an exception
        self.consumer.close()

    def test_close_with_exception(self):
        """Test close method handles exceptions during close."""
        self.mock_client.close.side_effect = Exception("Close error")
        
        with patch.object(self.consumer.logger, 'info') as mock_info:
            self.consumer.close()
            
            # Verify exception was logged
            self.assertEqual(mock_info.call_count, 2)  # One for "Closing..." and one for error
            self.assertIn('Kafka consumer closed with message', mock_info.call_args[0][0])

    def test_consume_messages_invalid_json(self):
        """Test consume_messages handles invalid JSON gracefully."""
        mock_msg = Mock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b'invalid json {'
        mock_msg.topic.return_value = 'test_topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        mock_msg.timestamp.return_value = (1, 1638360000000)
        mock_msg.key.return_value = b'test_key'
        
        self.mock_client.poll.return_value = mock_msg
        
        # Should raise an exception during orjson.loads
        with self.assertRaises(Exception):
            self.consumer.consume_messages()


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
