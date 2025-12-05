#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.redis import RedisConsumer, RedisHashConsumer


class TestRedisConsumer(unittest.TestCase):
    """Unit tests for RedisConsumer base class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        with patch('metranova.consumers.redis.RedisConnector') as mock_connector_class:
            self.mock_connector = Mock()
            self.mock_client = Mock()
            self.mock_connector.client = self.mock_client
            mock_connector_class.return_value = self.mock_connector
            
            self.consumer = RedisConsumer(self.mock_pipeline)

    def test_initialization(self):
        """Test that RedisConsumer initializes correctly."""
        self.assertEqual(self.consumer.tables, [])
        self.assertIsNotNone(self.consumer.datasource)
        self.assertEqual(self.consumer.pipeline, self.mock_pipeline)

    def test_consume_messages_no_tables(self):
        """Test consume_messages logs error when no tables specified."""
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            mock_error.assert_called_once()
            self.assertIn('No tables specified', mock_error.call_args[0][0])
        
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_with_tables(self):
        """Test consume_messages processes all tables."""
        self.consumer.tables = ['table1', 'table2']
        
        # Mock query_table to return an iterator
        def query_table_side_effect(table):
            if table == 'table1':
                return [{'data': 'data1_1'}, {'data': 'data1_2'}]
            elif table == 'table2':
                return [{'data': 'data2_1'}]
            return []
        
        self.consumer.query_table = Mock(side_effect=query_table_side_effect)
        
        self.consumer.consume_messages()
        
        # Verify all tables were queried
        self.assertEqual(self.consumer.query_table.call_count, 2)
        self.consumer.query_table.assert_any_call('table1')
        self.consumer.query_table.assert_any_call('table2')
        
        # Verify all messages were processed (3 total from both tables)
        self.assertEqual(self.mock_pipeline.process_message.call_count, 3)

    def test_consume_messages_handles_error(self):
        """Test consume_messages continues after error."""
        self.consumer.tables = ['table1', 'table2']
        
        def query_table_side_effect(table):
            if table == 'table1':
                raise Exception('Query error')
            return [{'data': 'data2'}]
        
        self.consumer.query_table = Mock(side_effect=query_table_side_effect)
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            # Verify error was logged
            mock_error.assert_called_once()
            self.assertIn('Error processing message', mock_error.call_args[0][0])
        
        # Verify second table was still processed
        self.assertEqual(self.consumer.query_table.call_count, 2)

    def test_query_table_not_implemented(self):
        """Test that query_table raises NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.consumer.query_table('test_table')


class TestRedisHashConsumer(unittest.TestCase):
    """Unit tests for RedisHashConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'REDIS_CONSUMER_UPDATE_INTERVAL': '600',
            'REDIS_CONSUMER_TABLES': 'meta_interface_cache,meta_device_cache'
        })
        self.env_patcher.start()
        
        with patch('metranova.consumers.redis.RedisConnector') as mock_connector_class:
            self.mock_connector = Mock()
            self.mock_client = Mock()
            self.mock_connector.client = self.mock_client
            mock_connector_class.return_value = self.mock_connector
            
            self.consumer = RedisHashConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that RedisHashConsumer initializes correctly."""
        self.assertEqual(self.consumer.update_interval, 600)
        self.assertEqual(len(self.consumer.tables), 2)
        self.assertIn('meta_interface_cache', self.consumer.tables)
        self.assertIn('meta_device_cache', self.consumer.tables)

    def test_initialization_empty_tables(self):
        """Test initialization with empty REDIS_CONSUMER_TABLES."""
        with patch.dict(os.environ, {'REDIS_CONSUMER_TABLES': ''}):
            with patch('metranova.consumers.redis.RedisConnector'):
                consumer = RedisHashConsumer(self.mock_pipeline)
                
                self.assertEqual(len(consumer.tables), 0)

    def test_query_table_success(self):
        """Test query_table successfully retrieves hash data."""
        # Mock Redis scan_iter to return hash keys
        self.mock_client.scan_iter.return_value = [
            'meta_interface_cache:key1',
            'meta_interface_cache:key2',
            'meta_interface_cache:key3'
        ]
        
        # Mock hgetall to return hash data
        def hgetall_side_effect(key):
            if key == 'meta_interface_cache:key1':
                return {b'field1': b'value1', b'field2': b'value2'}
            elif key == 'meta_interface_cache:key2':
                return {b'field3': b'value3'}
            elif key == 'meta_interface_cache:key3':
                return {}  # Empty hash
            return {}
        
        self.mock_client.hgetall.side_effect = hgetall_side_effect
        
        result = list(self.consumer.query_table('meta_interface_cache'))
        
        # Verify scan_iter was called with correct pattern
        self.mock_client.scan_iter.assert_called_once_with(
            match='meta_interface_cache:*',
            _type='hash'
        )
        
        # Verify hgetall was called for each key
        self.assertEqual(self.mock_client.hgetall.call_count, 3)
        
        # Verify results (empty hash should be skipped)
        self.assertEqual(len(result), 2)
        
        # Verify message structure
        self.assertEqual(result[0]['table'], 'meta_interface_cache')
        self.assertEqual(result[0]['key'], 'meta_interface_cache:key1')
        self.assertEqual(result[0]['data'], {b'field1': b'value1', b'field2': b'value2'})

    def test_query_table_no_client(self):
        """Test query_table raises error when client not initialized."""
        self.consumer.datasource.client = None
        
        with self.assertRaises(ConnectionError) as context:
            list(self.consumer.query_table('meta_interface_cache'))
        
        self.assertIn('Redis client not initialized', str(context.exception))

    def test_query_table_no_keys_found(self):
        """Test query_table handles no matching keys."""
        self.mock_client.scan_iter.return_value = []
        
        result = list(self.consumer.query_table('meta_interface_cache'))
        
        # Verify no results returned
        self.assertEqual(len(result), 0)
        
        # hgetall should not be called
        self.mock_client.hgetall.assert_not_called()

    def test_query_table_error(self):
        """Test query_table handles Redis errors."""
        self.mock_client.scan_iter.side_effect = Exception('Redis connection error')
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            with self.assertRaises(Exception):
                list(self.consumer.query_table('meta_interface_cache'))
            
            # Verify error was logged
            mock_error.assert_called_once()
            self.assertIn('Failed to query Redis hash', mock_error.call_args[0][0])

    def test_query_table_empty_hash_skipped(self):
        """Test that query_table skips empty hashes."""
        self.mock_client.scan_iter.return_value = [
            'meta_interface_cache:key1',
            'meta_interface_cache:key2'
        ]
        
        # First hash has data, second is empty
        self.mock_client.hgetall.side_effect = [
            {b'field': b'value'},
            {}  # Empty hash
        ]
        
        result = list(self.consumer.query_table('meta_interface_cache'))
        
        # Only one result should be returned
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['key'], 'meta_interface_cache:key1')

    def test_query_table_pattern_format(self):
        """Test that query_table builds correct pattern."""
        self.mock_client.scan_iter.return_value = []
        
        list(self.consumer.query_table('custom_table'))
        
        # Verify pattern matches table:*
        self.mock_client.scan_iter.assert_called_once_with(
            match='custom_table:*',
            _type='hash'
        )

    def test_query_table_returns_generator(self):
        """Test that query_table returns a generator/iterable."""
        self.mock_client.scan_iter.return_value = ['meta_cache:key1']
        self.mock_client.hgetall.return_value = {b'field': b'value'}
        
        result = self.consumer.query_table('meta_cache')
        
        # Verify it's a generator/iterator
        self.assertTrue(hasattr(result, '__iter__'))
        self.assertTrue(hasattr(result, '__next__'))

    def test_whitespace_handling_in_tables(self):
        """Test that table names are properly trimmed."""
        with patch.dict(os.environ, {
            'REDIS_CONSUMER_TABLES': ' table1 , table2 ,  table3  '
        }):
            with patch('metranova.consumers.redis.RedisConnector'):
                consumer = RedisHashConsumer(self.mock_pipeline)
                
                self.assertEqual(len(consumer.tables), 3)
                self.assertEqual(consumer.tables[0], 'table1')
                self.assertEqual(consumer.tables[1], 'table2')
                self.assertEqual(consumer.tables[2], 'table3')

    def test_inherits_from_base_redis_consumer(self):
        """Test that RedisHashConsumer inherits correctly."""
        self.assertIsInstance(self.consumer, RedisConsumer)

    def test_query_table_logs_debug_info(self):
        """Test that query_table logs debug information."""
        self.mock_client.scan_iter.return_value = ['meta_cache:key1']
        self.mock_client.hgetall.return_value = {b'field': b'value'}
        
        with patch.object(self.consumer.logger, 'debug') as mock_debug:
            list(self.consumer.query_table('meta_cache'))
            
            # Should have at least 2 debug calls (pattern and fetching)
            self.assertGreaterEqual(mock_debug.call_count, 2)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
