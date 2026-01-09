#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import HTTPError, RequestException, ConnectionError

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.alcf import StatusConsumer


class TestStatusConsumer(unittest.TestCase):
    """Unit tests for StatusConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'ALCF_STATUS_CONSUMER_UPDATE_INTERVAL': '3600',
            'ALCF_STATUS_CONSUMER_URLS': 'http://example.com/api/polaris/status,http://example.com/api/aurora/status'
        })
        self.env_patcher.start()
        
        with patch('metranova.consumers.alcf.HTTPConnector') as mock_connector_class:
            self.mock_connector = Mock()
            self.mock_client = Mock()
            self.mock_connector.client = self.mock_client
            mock_connector_class.return_value = self.mock_connector
            
            self.consumer = StatusConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that StatusConsumer initializes correctly."""
        self.assertEqual(self.consumer.update_interval, 3600)
        self.assertEqual(len(self.consumer.urls), 2)
        self.assertIn('http://example.com/api/polaris/status', self.consumer.urls)
        self.assertIn('http://example.com/api/aurora/status', self.consumer.urls)
        self.assertIsNotNone(self.consumer.datasource)

    def test_initialization_empty_urls(self):
        """Test initialization with empty URLs."""
        with patch.dict(os.environ, {
            'ALCF_STATUS_CONSUMER_UPDATE_INTERVAL': '3600',
            'ALCF_STATUS_CONSUMER_URLS': ''
        }):
            with patch('metranova.consumers.alcf.HTTPConnector'):
                consumer = StatusConsumer(self.mock_pipeline)
                self.assertEqual(len(consumer.urls), 0)

    def test_initialization_default_interval(self):
        """Test initialization with default update interval."""
        with patch.dict(os.environ, {
            'ALCF_STATUS_CONSUMER_URLS': 'http://example.com/api/status'
        }, clear=True):
            with patch('metranova.consumers.alcf.HTTPConnector'):
                consumer = StatusConsumer(self.mock_pipeline)
                self.assertEqual(consumer.update_interval, -1)

    def test_consume_messages_success(self):
        """Test consume_messages successfully fetches and processes data."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'queued': [
                {'jobid': 'job1', 'queue': 'default', 'nodes': 2, 'project': 'proj1'},
                {'jobid': 'job2', 'queue': 'gpu', 'nodes': 4, 'project': 'proj2'}
            ],
            'running': [
                {'jobid': 'job3', 'queue': 'default', 'nodes': 1, 'project': 'proj1'},
                {'jobid': 'job4', 'queue': 'gpu', 'nodes': 8, 'project': 'proj3'}
            ]
        }
        
        self.mock_client.get.return_value = mock_response
        
        # Call consume_messages
        self.consumer.consume_messages()
        
        # Verify HTTP GET was called for each URL
        self.assertEqual(self.mock_client.get.call_count, 2)
        
        # Verify process_message was called for each job type (queued and running) per URL
        # 2 URLs * 2 job types = 4 calls
        self.assertEqual(self.mock_pipeline.process_message.call_count, 4)
        
        # Check the structure of processed messages
        calls = self.mock_pipeline.process_message.call_args_list
        
        # First call should be for queued jobs from first URL
        first_call_msg = calls[0][0][0]
        self.assertEqual(first_call_msg['type'], 'job')
        self.assertEqual(first_call_msg['status_code'], 200)
        self.assertIn('url', first_call_msg)
        self.assertIn('data', first_call_msg)
        
        # Verify compute_resource_id was added to each record
        queued_data = first_call_msg['data']
        for record in queued_data:
            self.assertIn('compute_resource_id', record)
            # Should be either 'polaris' or 'aurora' depending on URL order
            self.assertIn(record['compute_resource_id'], ['polaris', 'aurora'])

    def test_consume_messages_http_error(self):
        """Test consume_messages handles HTTP errors gracefully."""
        self.mock_client.get.side_effect = HTTPError("404 Not Found")
        
        # Should not raise exception
        try:
            self.consumer.consume_messages()
        except HTTPError:
            self.fail("consume_messages raised HTTPError unexpectedly")
        
        # Verify process_message was not called
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_request_exception(self):
        """Test consume_messages handles request exceptions gracefully."""
        self.mock_client.get.side_effect = RequestException("Network error")
        
        # Should not raise exception
        try:
            self.consumer.consume_messages()
        except RequestException:
            self.fail("consume_messages raised RequestException unexpectedly")
        
        # Verify process_message was not called
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_connection_error(self):
        """Test consume_messages handles connection errors gracefully."""
        self.mock_client.get.side_effect = ConnectionError("Connection refused")
        
        # Should not raise exception
        try:
            self.consumer.consume_messages()
        except ConnectionError:
            self.fail("consume_messages raised ConnectionError unexpectedly")
        
        # Verify process_message was not called
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_invalid_json(self):
        """Test consume_messages handles invalid JSON gracefully."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        
        self.mock_client.get.return_value = mock_response
        
        # Should not raise exception
        try:
            self.consumer.consume_messages()
        except ValueError:
            self.fail("consume_messages raised ValueError unexpectedly")
        
        # Verify process_message was not called
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_generic_exception(self):
        """Test consume_messages handles generic exceptions gracefully."""
        self.mock_client.get.side_effect = Exception("Unexpected error")
        
        # Should not raise exception
        try:
            self.consumer.consume_messages()
        except Exception:
            self.fail("consume_messages raised Exception unexpectedly")
        
        # Verify process_message was not called
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_extracts_compute_resource_id_correctly(self):
        """Test that compute_resource_id is correctly extracted from URL."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'queued': [{'jobid': 'job1', 'nodes': 1}],
            'running': [{'jobid': 'job2', 'nodes': 2}]
        }
        
        self.mock_client.get.return_value = mock_response
        
        self.consumer.consume_messages()
        
        # Verify that compute_resource_id was extracted and added to records
        calls = self.mock_pipeline.process_message.call_args_list
        
        # There should be 4 calls (2 URLs * 2 job types)
        self.assertEqual(len(calls), 4)
        
        # Check that all messages have the compute_resource_id field
        for call in calls:
            msg = call[0][0]
            if msg['data']:  # Only check non-empty data
                for record in msg['data']:
                    self.assertIn('compute_resource_id', record)
                    # Should be either 'polaris' or 'aurora'
                    self.assertIn(record['compute_resource_id'], ['polaris', 'aurora'])

    def test_consume_messages_handles_missing_job_types(self):
        """Test consume_messages handles missing queued or running keys."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'queued': [{'jobid': 'job1', 'nodes': 1}]
            # 'running' key is missing
        }
        
        self.mock_client.get.return_value = mock_response
        
        # Should not raise exception
        try:
            self.consumer.consume_messages()
        except KeyError:
            self.fail("consume_messages raised KeyError unexpectedly")
        
        # Verify process_message was called for both job types
        # queued should have data, running should be empty list
        self.assertEqual(self.mock_pipeline.process_message.call_count, 4)

    def test_consume_messages_multiple_urls(self):
        """Test consume_messages processes multiple URLs correctly."""
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            'queued': [{'jobid': 'job1'}],
            'running': [{'jobid': 'job2'}]
        }
        
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            'queued': [{'jobid': 'job3'}],
            'running': [{'jobid': 'job4'}]
        }
        
        self.mock_client.get.side_effect = [mock_response1, mock_response2]
        
        self.consumer.consume_messages()
        
        # Verify both URLs were called
        self.assertEqual(self.mock_client.get.call_count, 2)
        
        # Verify process_message was called for each job type per URL
        self.assertEqual(self.mock_pipeline.process_message.call_count, 4)


if __name__ == '__main__':
    unittest.main()
