import unittest
import os
from unittest.mock import Mock, patch, MagicMock, call
from ipaddress import IPv6Address
from metranova.writers.redis import RedisWriter, RedisMetadataRefWriter, RedisHashWriter


class TestRedisWriter(unittest.TestCase):
    """Test the RedisWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_processor = Mock()
        
        with patch('metranova.writers.redis.RedisConnector') as mock_connector:
            self.mock_redis_client = Mock()
            mock_connector.return_value.client = self.mock_redis_client
            self.writer = RedisWriter([self.mock_processor])
    
    def test_initialization(self):
        """Test RedisWriter initialization."""
        self.assertEqual(len(self.writer.processors), 1)
        self.assertIsNotNone(self.writer.datastore)
    
    def test_write_message_valid(self):
        """Test writing a valid message."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'value': 'test_value',
            'expires': 3600
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_called_once_with('test_table:test_key', 'test_value')
        self.mock_redis_client.expire.assert_called_once_with('test_table:test_key', 3600)
    
    def test_write_message_no_expires(self):
        """Test writing message without expires."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'value': 'test_value'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_called_once_with('test_table:test_key', 'test_value')
        self.mock_redis_client.expire.assert_not_called()
    
    def test_write_message_expires_zero(self):
        """Test writing message with expires=0 (no expiration)."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'value': 'test_value',
            'expires': 0
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_called_once()
        self.mock_redis_client.expire.assert_not_called()
    
    def test_write_message_expires_string(self):
        """Test writing message with string expires value."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'value': 'test_value',
            'expires': '7200'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.expire.assert_called_once_with('test_table:test_key', 7200)
    
    def test_write_message_expires_invalid(self):
        """Test writing message with invalid expires value."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'value': 'test_value',
            'expires': 'invalid'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_called_once()
        self.mock_redis_client.expire.assert_not_called()
    
    def test_write_message_none(self):
        """Test write_message with None."""
        self.writer.write_message(None)
        
        self.mock_redis_client.set.assert_not_called()
    
    def test_write_message_missing_table(self):
        """Test write_message with missing table field."""
        msg = {
            'key': 'test_key',
            'value': 'test_value'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_not_called()
    
    def test_write_message_missing_key(self):
        """Test write_message with missing key field."""
        msg = {
            'table': 'test_table',
            'value': 'test_value'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_not_called()
    
    def test_write_message_missing_value(self):
        """Test write_message with missing value field."""
        msg = {
            'table': 'test_table',
            'key': 'test_key'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.set.assert_not_called()
    
    def test_write_message_redis_exception(self):
        """Test write_message handles Redis exceptions."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'value': 'test_value'
        }
        
        self.mock_redis_client.set.side_effect = Exception("Redis error")
        
        with self.assertRaises(Exception):
            self.writer.write_message(msg)


class TestRedisMetadataRefWriter(unittest.TestCase):
    """Test the RedisMetadataRefWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_processor = Mock()
        
        with patch('metranova.writers.redis.RedisConnector') as mock_connector:
            self.mock_redis_client = Mock()
            self.mock_pipeline = Mock()
            self.mock_redis_client.pipeline.return_value = self.mock_pipeline
            mock_connector.return_value.client = self.mock_redis_client
            self.writer = RedisMetadataRefWriter([self.mock_processor])
    
    def test_initialization(self):
        """Test RedisMetadataRefWriter initialization."""
        self.assertEqual(len(self.writer.processors), 1)
        self.assertIsNotNone(self.writer.datastore)
    
    def test_process_message_valid(self):
        """Test processing a valid message."""
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1'],
                [2, 'ref2'],
                [3, 'ref3']
            ]
        }
        
        self.writer.process_message(msg)
        
        # Verify pipeline was used
        self.mock_redis_client.pipeline.assert_called()
        self.mock_pipeline.execute.assert_called()
        
        # Verify set calls
        expected_calls = [
            call.set('test_table:1', 'ref1'),
            call.set('test_table:2', 'ref2'),
            call.set('test_table:3', 'ref3')
        ]
        self.mock_pipeline.assert_has_calls(expected_calls, any_order=True)
    
    def test_process_message_with_additional_fields(self):
        """Test processing message with additional index fields."""
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1', 'index1'],
                [2, 'ref2', 'index2']
            ]
        }
        
        self.writer.process_message(msg)
        
        # Should use the additional field in the key
        expected_calls = [
            call.set('test_table:index1', 1),
            call.set('test_table:index2', 2)
        ]
        self.mock_pipeline.assert_has_calls(expected_calls, any_order=True)
    
    def test_process_message_with_ipv6_mapped_ipv4(self):
        """Test processing with IPv6-mapped IPv4 addresses."""
        ipv6_mapped = IPv6Address('::ffff:192.168.1.1')
        
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1', ipv6_mapped]
            ]
        }
        
        self.writer.process_message(msg)
        
        # Should extract IPv4 address
        self.mock_pipeline.set.assert_called_with('test_table:192.168.1.1', 1)
    
    def test_process_message_with_regular_ipv6(self):
        """Test processing with regular IPv6 addresses."""
        ipv6 = IPv6Address('2001:db8::1')
        
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1', ipv6]
            ]
        }
        
        self.writer.process_message(msg)
        
        # Should use string representation
        self.mock_pipeline.set.assert_called_with('test_table:2001:db8::1', 1)
    
    def test_process_message_skips_null_values(self):
        """Test that rows with null values are skipped."""
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1'],
                [None, 'ref2'],
                [3, None],
                [None, None]
            ]
        }
        
        self.writer.process_message(msg)
        
        # Only first row should be processed
        self.mock_pipeline.set.assert_called_once_with('test_table:1', 'ref1')
    
    def test_process_message_skips_insufficient_columns(self):
        """Test that rows with insufficient columns are skipped."""
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1'],
                [2],  # Only one column
                []    # No columns
            ]
        }
        
        self.writer.process_message(msg)
        
        # Only first row should be processed
        self.mock_pipeline.set.assert_called_once_with('test_table:1', 'ref1')
    
    def test_process_message_batch_execution(self):
        """Test that pipeline executes in batches of 1000."""
        # Create 2500 rows to test batching
        rows = [[i, f'ref{i}'] for i in range(2500)]
        msg = {
            'table': 'test_table',
            'rows': rows
        }
        
        self.writer.process_message(msg)
        
        # Should execute 3 times: at 1000, 2000, and final batch
        self.assertEqual(self.mock_pipeline.execute.call_count, 3)
    
    def test_process_message_sets_metadata(self):
        """Test that metadata is set after loading."""
        msg = {
            'table': 'test_table',
            'rows': [
                [1, 'ref1'],
                [2, 'ref2']
            ]
        }
        
        with patch('metranova.writers.redis.time.time', return_value=1234567890):
            self.writer.process_message(msg)
        
        # Verify metadata was set
        self.mock_redis_client.hset.assert_called_once()
        call_args = self.mock_redis_client.hset.call_args
        self.assertEqual(call_args[0][0], 'test_table:_metadata')
        self.assertIn('last_updated', call_args[1]['mapping'])
        self.assertIn('record_count', call_args[1]['mapping'])
    
    def test_process_message_none(self):
        """Test process_message with None."""
        self.writer.process_message(None)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_process_message_no_rows(self):
        """Test process_message with no rows."""
        msg = {
            'table': 'test_table',
            'rows': []
        }
        
        self.writer.process_message(msg)
        
        # Pipeline should not be used for empty rows
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_process_message_missing_table(self):
        """Test process_message with missing table field."""
        msg = {
            'rows': [[1, 'ref1']]
        }
        
        self.writer.process_message(msg)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_process_message_redis_exception(self):
        """Test process_message handles Redis exceptions."""
        msg = {
            'table': 'test_table',
            'rows': [[1, 'ref1']]
        }
        
        self.mock_pipeline.execute.side_effect = Exception("Redis error")
        
        with self.assertRaises(Exception):
            self.writer.process_message(msg)


class TestRedisHashWriter(unittest.TestCase):
    """Test the RedisHashWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_processor = Mock()
        
        with patch('metranova.writers.redis.RedisConnector') as mock_connector:
            self.mock_redis_client = Mock()
            self.mock_pipeline = Mock()
            self.mock_redis_client.pipeline.return_value = self.mock_pipeline
            mock_connector.return_value.client = self.mock_redis_client
            self.writer = RedisHashWriter([self.mock_processor])
    
    def test_initialization(self):
        """Test RedisHashWriter initialization."""
        self.assertEqual(len(self.writer.processors), 1)
        self.assertIsNotNone(self.writer.datastore)
    
    def test_write_message_valid(self):
        """Test writing a valid hash message."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1', 'field2': 'value2'},
            'expires': 3600
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.pipeline.assert_called_once()
        self.mock_pipeline.hset.assert_called_once_with(
            'test_table:test_key',
            mapping={'field1': 'value1', 'field2': 'value2'}
        )
        self.mock_pipeline.hexpire.assert_called_once_with(
            'test_table:test_key',
            3600,
            'field1', 'field2'
        )
        self.mock_pipeline.execute.assert_called_once()
    
    def test_write_message_no_expires(self):
        """Test writing hash message without expires."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1'}
        }
        
        self.writer.write_message(msg)
        
        self.mock_pipeline.hset.assert_called_once()
        self.mock_pipeline.hexpire.assert_not_called()
    
    def test_write_message_expires_zero(self):
        """Test writing hash message with expires=0."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1'},
            'expires': 0
        }
        
        self.writer.write_message(msg)
        
        self.mock_pipeline.hset.assert_called_once()
        self.mock_pipeline.hexpire.assert_not_called()
    
    def test_write_message_filters_none_values(self):
        """Test that None values are filtered out."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {
                'field1': 'value1',
                'field2': None,
                'field3': 'value3',
                'field4': None
            }
        }
        
        self.writer.write_message(msg)
        
        # Should only write non-None values
        self.mock_pipeline.hset.assert_called_once_with(
            'test_table:test_key',
            mapping={'field1': 'value1', 'field3': 'value3'}
        )
    
    def test_write_message_all_none_values(self):
        """Test that message with all None values is skipped."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {
                'field1': None,
                'field2': None
            }
        }
        
        self.writer.write_message(msg)
        
        self.mock_pipeline.hset.assert_not_called()
    
    def test_write_message_expires_string(self):
        """Test writing with string expires value."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1'},
            'expires': '7200'
        }
        
        self.writer.write_message(msg)
        
        self.mock_pipeline.hexpire.assert_called_once()
    
    def test_write_message_expires_invalid(self):
        """Test writing with invalid expires value."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1'},
            'expires': 'invalid'
        }
        
        self.writer.write_message(msg)
        
        self.mock_pipeline.hexpire.assert_not_called()
    
    def test_write_message_none(self):
        """Test write_message with None."""
        self.writer.write_message(None)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_write_message_missing_table(self):
        """Test write_message with missing table field."""
        msg = {
            'key': 'test_key',
            'data': {'field1': 'value1'}
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_write_message_missing_key(self):
        """Test write_message with missing key field."""
        msg = {
            'table': 'test_table',
            'data': {'field1': 'value1'}
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_write_message_missing_data(self):
        """Test write_message with missing data field."""
        msg = {
            'table': 'test_table',
            'key': 'test_key'
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_write_message_empty_data(self):
        """Test write_message with empty data dict."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {}
        }
        
        self.writer.write_message(msg)
        
        self.mock_redis_client.pipeline.assert_not_called()
    
    def test_write_message_redis_exception(self):
        """Test write_message handles Redis exceptions."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1'}
        }
        
        self.mock_pipeline.execute.side_effect = Exception("Redis error")
        
        with self.assertRaises(Exception):
            self.writer.write_message(msg)
    
    def test_write_message_with_consumer_metadata(self):
        """Test write_message with consumer_metadata (should work normally)."""
        msg = {
            'table': 'test_table',
            'key': 'test_key',
            'data': {'field1': 'value1'}
        }
        metadata = {'offset': 123}
        
        self.writer.write_message(msg, metadata)
        
        self.mock_pipeline.hset.assert_called_once()


if __name__ == '__main__':
    unittest.main()
