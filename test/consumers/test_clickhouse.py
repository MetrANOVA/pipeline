#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock, call

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.consumers.clickhouse import (
    BaseClickHouseConsumer,
    MetadataClickHouseConsumer,
    IPMetadataClickHouseConsumer
)


class TestBaseClickHouseConsumer(unittest.TestCase):
    """Unit tests for BaseClickHouseConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        with patch('metranova.consumers.clickhouse.ClickHouseConnector'):
            self.consumer = BaseClickHouseConsumer(self.mock_pipeline)

    def test_initialization(self):
        """Test that BaseClickHouseConsumer initializes correctly."""
        self.assertEqual(self.consumer.update_interval, -1)
        self.assertEqual(self.consumer.tables, [])
        self.assertIsNotNone(self.consumer.datasource)

    def test_consume_messages_no_tables(self):
        """Test consume_messages logs error when no tables specified."""
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer.consume_messages()
            
            mock_error.assert_called_once()
            self.assertIn('No tables specified', mock_error.call_args[0][0])
        
        self.mock_pipeline.process_message.assert_not_called()

    def test_consume_messages_with_tables(self):
        """Test consume_messages processes all tables."""
        self.consumer.tables = ['table1', 'table2', 'table3']
        self.consumer.query_table = Mock(side_effect=[
            {'table': 'table1', 'data': 'data1'},
            {'table': 'table2', 'data': 'data2'},
            {'table': 'table3', 'data': 'data3'}
        ])
        
        self.consumer.consume_messages()
        
        # Verify all tables were queried
        self.assertEqual(self.consumer.query_table.call_count, 3)
        self.consumer.query_table.assert_any_call('table1')
        self.consumer.query_table.assert_any_call('table2')
        self.consumer.query_table.assert_any_call('table3')
        
        # Verify all messages were processed
        self.assertEqual(self.mock_pipeline.process_message.call_count, 3)

    def test_consume_messages_handles_error(self):
        """Test consume_messages continues processing after error."""
        self.consumer.tables = ['table1', 'table2']
        self.consumer.query_table = Mock(side_effect=[
            Exception('Query error'),
            {'table': 'table2', 'data': 'data2'}
        ])
        
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


class TestMetadataClickHouseConsumer(unittest.TestCase):
    """Unit tests for MetadataClickHouseConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'CLICKHOUSE_CONSUMER_UPDATE_INTERVAL': '600',
            'CLICKHOUSE_CONSUMER_TABLES': 'meta_device,meta_interface:device_id:flow_index,meta_device:@loopback_ip'
        })
        self.env_patcher.start()
        
        with patch('metranova.consumers.clickhouse.ClickHouseConnector'):
            self.consumer = MetadataClickHouseConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that MetadataClickHouseConsumer initializes correctly."""
        self.assertEqual(self.consumer.update_interval, 600)
        self.assertEqual(len(self.consumer.tables), 3)
        self.assertIn('meta_device', self.consumer.tables)
        self.assertIn('meta_interface:device_id:flow_index', self.consumer.tables)
        self.assertIn('meta_device:@loopback_ip', self.consumer.tables)
        self.assertEqual(self.consumer.primary_columns, ['id', 'ref'])

    def test_initialization_empty_tables(self):
        """Test initialization with empty CLICKHOUSE_CONSUMER_TABLES."""
        with patch.dict(os.environ, {'CLICKHOUSE_CONSUMER_TABLES': ''}):
            with patch('metranova.consumers.clickhouse.ClickHouseConnector'):
                consumer = MetadataClickHouseConsumer(self.mock_pipeline)
                
                self.assertEqual(len(consumer.tables), 0)

    def test_build_query_simple_table(self):
        """Test build_query for simple table without fields."""
        query = self.consumer.build_query('meta_device')
        
        self.assertIn('SELECT', query)
        self.assertIn('argMax(id, insert_time) as latest_id', query)
        self.assertIn('argMax(ref, insert_time) as latest_ref', query)
        self.assertIn('FROM meta_device', query)
        self.assertIn('WHERE id IS NOT NULL AND ref IS NOT NULL', query)
        self.assertIn('GROUP BY id ORDER BY id', query)
        self.assertNotIn('ARRAY JOIN', query)

    def test_build_query_with_fields(self):
        """Test build_query for table with additional fields."""
        query = self.consumer.build_query('meta_interface:device_id:flow_index')
        
        self.assertIn('SELECT', query)
        self.assertIn('argMax(id, insert_time) as latest_id', query)
        self.assertIn('argMax(ref, insert_time) as latest_ref', query)
        self.assertIn('argMax(device_id, insert_time) as latest_device_id', query)
        self.assertIn('argMax(flow_index, insert_time) as latest_flow_index', query)
        self.assertIn('FROM meta_interface', query)
        self.assertIn('AND device_id IS NOT NULL', query)
        self.assertIn('AND flow_index IS NOT NULL', query)
        self.assertNotIn('ARRAY JOIN', query)

    def test_build_query_with_array_field(self):
        """Test build_query for table with array field (@prefix)."""
        query = self.consumer.build_query('meta_device:@loopback_ip')
        
        self.assertIn('SELECT', query)
        self.assertIn('argMax(loopback_ip, insert_time) as latest_loopback_ip', query)
        self.assertIn('FROM meta_device', query)
        self.assertIn('AND loopback_ip IS NOT NULL', query)
        self.assertIn('ARRAY JOIN latest_loopback_ip', query)

    def test_build_query_mixed_fields(self):
        """Test build_query with both regular and array fields."""
        query = self.consumer.build_query('meta_interface:device_id:@ip_addresses:flow_index')
        
        self.assertIn('argMax(device_id, insert_time) as latest_device_id', query)
        self.assertIn('argMax(ip_addresses, insert_time) as latest_ip_addresses', query)
        self.assertIn('argMax(flow_index, insert_time) as latest_flow_index', query)
        self.assertIn('ARRAY JOIN latest_ip_addresses', query)

    def test_query_table_success(self):
        """Test query_table successfully queries and returns data."""
        mock_result = Mock()
        mock_result.result_rows = [
            ('id1', 'ref1', 'value1'),
            ('id2', 'ref2', 'value2')
        ]
        mock_result.column_names = ['latest_id', 'latest_ref', 'latest_field']
        
        self.consumer.datasource.client.query.return_value = mock_result
        
        result = self.consumer.query_table('meta_device')
        
        # Verify query was executed
        self.consumer.datasource.client.query.assert_called_once()
        
        # Verify result structure
        self.assertEqual(result['table'], 'meta_device')
        self.assertEqual(result['column_names'], mock_result.column_names)
        self.assertEqual(result['rows'], mock_result.result_rows)
        self.assertEqual(len(result['rows']), 2)

    def test_query_table_no_data(self):
        """Test query_table handles empty result set."""
        mock_result = Mock()
        mock_result.result_rows = []
        mock_result.column_names = ['latest_id', 'latest_ref']
        
        self.consumer.datasource.client.query.return_value = mock_result
        
        with patch.object(self.consumer.logger, 'warning') as mock_warning:
            result = self.consumer.query_table('meta_device')
            
            # Verify warning was logged
            mock_warning.assert_called_once()
            self.assertIn('No data found', mock_warning.call_args[0][0])
        
        # Verify empty dict is returned
        self.assertEqual(result, {})

    def test_query_table_error(self):
        """Test query_table handles query errors."""
        self.consumer.datasource.client.query.side_effect = Exception('Query failed')
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            with self.assertRaises(Exception):
                self.consumer.query_table('meta_device')
            
            # Verify error was logged
            mock_error.assert_called_once()
            self.assertIn('Failed to load metadata', mock_error.call_args[0][0])

    def test_query_table_logs_query(self):
        """Test that query_table logs the query being executed."""
        mock_result = Mock()
        mock_result.result_rows = [('id1', 'ref1')]
        mock_result.column_names = ['latest_id', 'latest_ref']
        
        self.consumer.datasource.client.query.return_value = mock_result
        
        with patch.object(self.consumer.logger, 'debug') as mock_debug:
            self.consumer.query_table('meta_device')
            
            # Verify query was logged
            mock_debug.assert_called_once()
            self.assertIn('Executing query', mock_debug.call_args[0][0])


class TestIPMetadataClickHouseConsumer(unittest.TestCase):
    """Unit tests for IPMetadataClickHouseConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'CLICKHOUSE_CONSUMER_UPDATE_INTERVAL': '600',
            'CLICKHOUSE_CONSUMER_TABLES': 'meta_ip'
        })
        self.env_patcher.start()
        
        with patch('metranova.consumers.clickhouse.ClickHouseConnector'):
            self.consumer = IPMetadataClickHouseConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that IPMetadataClickHouseConsumer initializes correctly."""
        # Verify ip_subnet was added to primary_columns
        self.assertIn('id', self.consumer.primary_columns)
        self.assertIn('ref', self.consumer.primary_columns)
        self.assertIn('ip_subnet', self.consumer.primary_columns)

    def test_build_query_includes_ip_subnet(self):
        """Test that build_query includes ip_subnet in primary columns."""
        query = self.consumer.build_query('meta_ip')
        
        self.assertIn('argMax(id, insert_time) as latest_id', query)
        self.assertIn('argMax(ref, insert_time) as latest_ref', query)
        self.assertIn('argMax(ip_subnet, insert_time) as latest_ip_subnet', query)

    def test_inherits_from_metadata_consumer(self):
        """Test that IPMetadataClickHouseConsumer inherits correctly."""
        self.assertIsInstance(self.consumer, MetadataClickHouseConsumer)
        self.assertIsInstance(self.consumer, BaseClickHouseConsumer)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
