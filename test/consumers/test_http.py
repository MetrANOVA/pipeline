#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import HTTPError, RequestException, ConnectionError

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.http import HTTPConsumer


class TestHTTPConsumer(unittest.TestCase):
    """Unit tests for HTTPConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'HTTP_CONSUMER_UPDATE_INTERVAL': '3600',
            'HTTP_CONSUMER_URLS': 'http://example.com/api/data1,http://example.com/api/data2'
        })
        self.env_patcher.start()
        
        with patch('metranova.consumers.http.HTTPConnector') as mock_connector_class:
            self.mock_connector = Mock()
            self.mock_client = Mock()
            self.mock_connector.client = self.mock_client
            mock_connector_class.return_value = self.mock_connector
            
            self.consumer = HTTPConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that HTTPConsumer initializes correctly."""
        self.assertEqual(self.consumer.update_interval, 3600)
        self.assertEqual(len(self.consumer.urls), 2)
        self.assertIn('http://example.com/api/data1', self.consumer.urls)
        self.assertIn('http://example.com/api/data2', self.consumer.urls)
        self.assertIsNotNone(self.consumer.datasource)

    def test_initialization_with_prefix(self):
        """Test initialization with environment variable prefix."""
        with patch.dict(os.environ, {
            'SCIREG_HTTP_CONSUMER_UPDATE_INTERVAL': '7200',
            'SCIREG_HTTP_CONSUMER_URLS': 'http://scireg.example.com/api',
            'HTTP_CONSUMER_ENV_PREFIX': 'SCIREG'
        }):
            with patch('metranova.consumers.http.HTTPConnector'):
                consumer = HTTPConsumer(self.mock_pipeline)
                
                self.assertEqual(consumer.update_interval, 7200)
                self.assertEqual(len(consumer.urls), 1)
                self.assertIn('http://scireg.example.com/api', consumer.urls)

    def test_initialization_empty_urls(self):
        """Test initialization with empty URLs."""
        with patch.dict(os.environ, {'HTTP_CONSUMER_URLS': ''}):
            with patch('metranova.consumers.http.HTTPConnector'):
                consumer = HTTPConsumer(self.mock_pipeline)
                
                self.assertEqual(len(consumer.urls), 0)

    def test_consume_messages_success(self):
        """Test consume_messages successfully fetches and processes data."""
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {'data': 'test_data_1'}
        
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {'data': 'test_data_2'}
        
        self.mock_client.get.side_effect = [mock_response1, mock_response2]
        
        self.consumer.consume_messages()
        
        # Verify both URLs were fetched
        self.assertEqual(self.mock_client.get.call_count, 2)
        self.mock_client.get.assert_any_call('http://example.com/api/data1')
        self.mock_client.get.assert_any_call('http://example.com/api/data2')
        
        # Verify both responses raised for status
        self.assertEqual(mock_response1.raise_for_status.call_count, 1)
        self.assertEqual(mock_response2.raise_for_status.call_count, 1)
        
        # Verify both messages were processed
        self.assertEqual(self.mock_pipeline.process_message.call_count, 2)
        
        # Verify message structure
        call_args1 = self.mock_pipeline.process_message.call_args_list[0][0][0]
        self.assertEqual(call_args1['url'], 'http://example.com/api/data1')
        self.assertEqual(call_args1['status_code'], 200)
        self.assertEqual(call_args1['data'], {'data': 'test_data_1'})

    def test_consume_messages_http_error(self):
        """Test consume_messages handles HTTP errors."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = HTTPError('404 Not Found')
        
        self.mock_client.get.return_value = mock_response
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged for each URL
            self.assertEqual(mock_error.call_count, 2)
            self.assertIn('HTTP error', mock_error.call_args[0][0])
        
        # Verify no messages were processed
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_request_exception(self):
        """Test consume_messages handles request exceptions."""
        self.mock_client.get.side_effect = RequestException('Request failed')
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged for each URL
            self.assertEqual(mock_error.call_count, 2)
            self.assertIn('Request error', mock_error.call_args[0][0])

    def test_consume_messages_connection_error(self):
        """Test consume_messages handles connection errors."""
        # ConnectionError is a subclass of RequestException, so it gets caught by that handler
        self.mock_client.get.side_effect = ConnectionError('Connection refused')
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged for each URL
            self.assertEqual(mock_error.call_count, 2)
            # Connection errors may be logged as "Request error" or "Connection error"
            self.assertTrue('error' in mock_error.call_args[0][0].lower())

    def test_consume_messages_invalid_json(self):
        """Test consume_messages handles invalid JSON."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError('Invalid JSON')
        
        self.mock_client.get.return_value = mock_response
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged for each URL
            self.assertEqual(mock_error.call_count, 2)
            self.assertIn('Invalid JSON', mock_error.call_args[0][0])

    def test_consume_messages_unexpected_exception(self):
        """Test consume_messages handles unexpected exceptions."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception('Unexpected error')
        
        self.mock_client.get.return_value = mock_response
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged for each URL
            self.assertEqual(mock_error.call_count, 2)
            self.assertIn('Error processing message', mock_error.call_args[0][0])

    def test_consume_messages_partial_failure(self):
        """Test consume_messages continues after one URL fails."""
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {'data': 'success'}
        
        self.mock_client.get.side_effect = [
            HTTPError('First URL failed'),
            mock_response_success
        ]
        
        with patch.object(self.consumer.logger, 'error'):
            self.consumer.consume_messages()
        
        # Verify second URL was still processed
        self.mock_pipeline.process_message.assert_called_once()
        call_args = self.mock_pipeline.process_message.call_args[0][0]
        self.assertEqual(call_args['data'], {'data': 'success'})

    def test_consume_messages_empty_urls(self):
        """Test consume_messages with no URLs configured."""
        self.consumer.urls = []
        
        self.consumer.consume_messages()
        
        # No HTTP requests should be made
        self.mock_client.get.assert_not_called()
        self.mock_pipeline.process_message.assert_not_called()

    def test_env_prefix_adds_underscore(self):
        """Test that env prefix automatically adds underscore if missing."""
        with patch.dict(os.environ, {
            'HTTP_CONSUMER_ENV_PREFIX': 'TEST',
            'TEST_HTTP_CONSUMER_URLS': 'http://test.com'
        }):
            with patch('metranova.consumers.http.HTTPConnector'):
                consumer = HTTPConsumer(self.mock_pipeline)
                
                self.assertEqual(len(consumer.urls), 1)
                self.assertIn('http://test.com', consumer.urls)

    def test_urls_whitespace_handling(self):
        """Test that URLs are properly trimmed of whitespace."""
        with patch.dict(os.environ, {
            'HTTP_CONSUMER_URLS': ' http://url1.com , http://url2.com , http://url3.com '
        }):
            with patch('metranova.consumers.http.HTTPConnector'):
                consumer = HTTPConsumer(self.mock_pipeline)
                
                self.assertEqual(len(consumer.urls), 3)
                self.assertEqual(consumer.urls[0], 'http://url1.com')
                self.assertEqual(consumer.urls[1], 'http://url2.com')
                self.assertEqual(consumer.urls[2], 'http://url3.com')


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
