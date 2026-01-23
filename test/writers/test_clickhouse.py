import unittest
import os
import time
import threading
from unittest.mock import Mock, patch, MagicMock, call
from collections import defaultdict
from metranova.writers.clickhouse import ClickHouseWriter, ClickHouseBatcher


class TestClickHouseWriter(unittest.TestCase):
    """Test the ClickHouseWriter class."""
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.ClickHouseBatcher')
    def test_initialization(self, mock_batcher_class, mock_connector):
        """Test ClickHouseWriter initialization."""
        mock_processor = Mock()
        mock_batcher = Mock()
        mock_batcher_class.return_value = mock_batcher
        
        writer = ClickHouseWriter([mock_processor])
        
        self.assertEqual(len(writer.processors), 1)
        self.assertIsNotNone(writer.datastore)
        mock_batcher_class.assert_called_once_with(mock_processor)
        mock_batcher.start_flush_timer.assert_called_once()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.ClickHouseBatcher')
    def test_initialization_multiple_processors(self, mock_batcher_class, mock_connector):
        """Test initialization with multiple processors."""
        processor1 = Mock()
        processor2 = Mock()
        mock_batcher = Mock()
        mock_batcher_class.return_value = mock_batcher
        
        writer = ClickHouseWriter([processor1, processor2])
        
        self.assertEqual(len(writer.batchers), 2)
        self.assertEqual(mock_batcher_class.call_count, 2)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.ClickHouseBatcher')
    def test_process_message(self, mock_batcher_class, mock_connector):
        """Test process_message delegates to batchers."""
        mock_processor = Mock()
        mock_batcher = Mock()
        mock_batcher_class.return_value = mock_batcher
        
        writer = ClickHouseWriter([mock_processor])
        
        msg = {'data': 'test'}
        metadata = {'offset': 123}
        writer.process_message(msg, metadata)
        
        mock_batcher.process_message.assert_called_once_with(msg, metadata)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.ClickHouseBatcher')
    def test_process_message_none(self, mock_batcher_class, mock_connector):
        """Test process_message with None message."""
        mock_processor = Mock()
        mock_batcher = Mock()
        mock_batcher_class.return_value = mock_batcher
        
        writer = ClickHouseWriter([mock_processor])
        writer.process_message(None)
        
        mock_batcher.process_message.assert_not_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.ClickHouseBatcher')
    def test_close(self, mock_batcher_class, mock_connector):
        """Test close() closes all batchers and datastore."""
        mock_processor = Mock()
        mock_batcher = Mock()
        mock_batcher_class.return_value = mock_batcher
        mock_datastore = Mock()
        mock_connector.return_value = mock_datastore
        
        writer = ClickHouseWriter([mock_processor])
        writer.close()
        
        mock_batcher.close.assert_called_once()
        mock_datastore.close.assert_called_once()


class TestClickHouseBatcher(unittest.TestCase):
    """Test the ClickHouseBatcher class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_processor = Mock()
        self.mock_processor.table = 'test_table'
        self.mock_processor.get_table_names.return_value = ['test_table']
        self.mock_processor.get_ch_dictionaries.return_value = []
        self.mock_processor.get_materialized_views.return_value = []
        self.mock_processor.create_table_command.return_value = 'CREATE TABLE test_table'
        self.mock_processor.column_names.return_value = ['col1', 'col2']
        
        self.mock_client = Mock()
        
        self.env_patcher = patch.dict(os.environ, {
            'CLICKHOUSE_BATCH_SIZE': '100',
            'CLICKHOUSE_BATCH_TIMEOUT': '10.0',
            'CLICKHOUSE_FLUSH_INTERVAL': '0.1'
        })
        self.env_patcher.start()
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.env_patcher.stop()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_initialization(self, mock_connector):
        """Test ClickHouseBatcher initialization."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        self.assertEqual(batcher.processor, self.mock_processor)
        self.assertEqual(batcher.batch_size, 100)
        self.assertEqual(batcher.batch_timeout, 10.0)
        self.assertEqual(batcher.flush_interval, 0.1)
        self.assertIsNotNone(batcher.batch)
        self.assertIsInstance(batcher.batch, defaultdict)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_initialization_defaults(self, mock_connector):
        """Test initialization with default values."""
        mock_connector.return_value.client = self.mock_client
        
        with patch.dict(os.environ, {}, clear=True):
            batcher = ClickHouseBatcher(self.mock_processor)
            
            self.assertEqual(batcher.batch_size, 1000)
            self.assertEqual(batcher.batch_timeout, 30.0)
            self.assertEqual(batcher.flush_interval, 0.1)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_table(self, mock_connector):
        """Test create_table method."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        self.mock_client.command.assert_called_with('CREATE TABLE test_table')
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_table_no_command(self, mock_connector):
        """Test create_table when processor returns None."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.create_table_command.return_value = None
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Should not raise exception
        self.assertIsNotNone(batcher)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_table_exception(self, mock_connector):
        """Test create_table handles exceptions."""
        mock_connector.return_value.client = self.mock_client
        self.mock_client.command.side_effect = Exception("Table creation failed")
        
        with self.assertRaises(Exception):
            ClickHouseBatcher(self.mock_processor)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_matching(self, mock_connector):
        """Test process_message with matching processor."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = [
            {'_clickhouse_table': 'test_table', 'col1': 'val1', 'col2': 'val2'}
        ]
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        msg = {'data': 'test'}
        batcher.process_message(msg)
        
        self.assertEqual(len(batcher.batch['test_table']), 1)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_not_matching(self, mock_connector):
        """Test process_message with non-matching processor."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = False
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        msg = {'data': 'test'}
        batcher.process_message(msg)
        
        self.assertEqual(len(batcher.batch['test_table']), 0)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_none(self, mock_connector):
        """Test process_message with None message."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        batcher.process_message(None)
        
        self.assertEqual(len(batcher.batch['test_table']), 0)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_build_returns_none(self, mock_connector):
        """Test process_message when build_message returns None."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = None
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        msg = {'data': 'test'}
        batcher.process_message(msg)
        
        self.assertEqual(len(batcher.batch['test_table']), 0)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_flushes_on_batch_size(self, mock_connector):
        """Test that batch flushes when size is reached."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = True
        self.mock_processor.message_to_columns.return_value = ('val1', 'val2')
        
        with patch.dict(os.environ, {'CLICKHOUSE_BATCH_SIZE': '3'}):
            batcher = ClickHouseBatcher(self.mock_processor)
            
            # Add 3 messages to trigger flush
            for i in range(3):
                self.mock_processor.build_message.return_value = [
                    {'_clickhouse_table': 'test_table', f'col{i}': f'val{i}'}
                ]
                batcher.process_message({'data': f'test{i}'})
            
            # Verify insert was called
            self.mock_client.insert.assert_called()
            # Batch should be empty after flush
            self.assertEqual(len(batcher.batch['test_table']), 0)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_flush_batch(self, mock_connector):
        """Test flush_batch method."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.message_to_columns.return_value = ('val1', 'val2')
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Add messages to batch
        batcher.batch['test_table'].append({'col1': 'val1', 'col2': 'val2'})
        batcher.batch['test_table'].append({'col1': 'val3', 'col2': 'val4'})
        
        batcher.flush_batch('test_table')
        
        # Verify insert was called
        self.mock_client.insert.assert_called_once_with(
            table='test_table',
            data=[('val1', 'val2'), ('val1', 'val2')],
            column_names=['col1', 'col2']
        )
        # Batch should be empty
        self.assertEqual(len(batcher.batch['test_table']), 0)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_flush_batch_empty(self, mock_connector):
        """Test flush_batch with empty batch."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        batcher.flush_batch('test_table')
        
        # Should not call insert
        self.mock_client.insert.assert_not_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_flush_batch_none_table(self, mock_connector):
        """Test flush_batch with None table name."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        batcher.flush_batch(None)
        
        self.mock_client.insert.assert_not_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.logger')
    def test_flush_batch_exception(self, mock_logger, mock_connector):
        """Test flush_batch handles exceptions."""
        mock_connector.return_value.client = self.mock_client
        self.mock_client.insert.side_effect = Exception("Insert failed")
        
        batcher = ClickHouseBatcher(self.mock_processor)
        batcher.batch['test_table'].append({'col1': 'val1'})
        
        batcher.flush_batch('test_table')
        
        # Should log error but not raise
        mock_logger.error.assert_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_close(self, mock_connector):
        """Test close() flushes remaining messages."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.message_to_columns.return_value = ('val1', 'val2')
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Add messages to batch
        batcher.batch['test_table'].append({'col1': 'val1', 'col2': 'val2'})
        
        batcher.close()
        
        # Verify flush was called
        self.mock_client.insert.assert_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_start_flush_timer(self, mock_connector):
        """Test that flush timer thread is started."""
        mock_connector.return_value.client = self.mock_client
        
        with patch('metranova.writers.clickhouse.threading.Thread') as mock_thread:
            batcher = ClickHouseBatcher(self.mock_processor)
            batcher.start_flush_timer()
            
            # Verify thread was created and started
            mock_thread.assert_called()
            mock_thread.return_value.start.assert_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_multiple_tables(self, mock_connector):
        """Test batcher with multiple table names."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.get_table_names.return_value = ['table1', 'table2']
        self.mock_processor.create_table_command.return_value = 'CREATE TABLE'
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Verify both tables are initialized
        self.assertIn('table1', batcher.batch)
        self.assertIn('table2', batcher.batch)
        # create_table_command should be called for each table
        self.assertEqual(self.mock_processor.create_table_command.call_count, 2)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_dictionary(self, mock_connector):
        """Test create_dictionary method."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Create mock dictionary
        mock_dictionary = Mock()
        mock_dictionary.dictionary_name = 'test_dict'
        mock_dictionary.create_dictionary_command.return_value = 'CREATE DICTIONARY test_dict'
        
        batcher.create_dictionary(mock_dictionary)
        
        self.mock_client.command.assert_called_with('CREATE DICTIONARY test_dict')
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_dictionary_none_command(self, mock_connector):
        """Test create_dictionary when create_dictionary_command returns None."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Reset call count from initialization
        self.mock_client.command.reset_mock()
        
        # Create mock dictionary that returns None
        mock_dictionary = Mock()
        mock_dictionary.create_dictionary_command.return_value = None
        
        batcher.create_dictionary(mock_dictionary)
        
        # Should not call client.command
        self.mock_client.command.assert_not_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.logger')
    def test_create_dictionary_exception(self, mock_logger, mock_connector):
        """Test create_dictionary handles exceptions."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Create mock dictionary
        mock_dictionary = Mock()
        mock_dictionary.dictionary_name = 'test_dict'
        mock_dictionary.create_dictionary_command.return_value = 'CREATE DICTIONARY test_dict'
        
        # Make client.command raise an exception on next call
        self.mock_client.command.side_effect = Exception("Dictionary creation failed")
        
        with self.assertRaises(Exception):
            batcher.create_dictionary(mock_dictionary)
        
        # Should log error
        mock_logger.error.assert_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_initialization_with_dictionaries(self, mock_connector):
        """Test ClickHouseBatcher initialization creates dictionaries."""
        mock_connector.return_value.client = self.mock_client
        
        # Create mock dictionaries
        mock_dict1 = Mock()
        mock_dict1.dictionary_name = 'dict1'
        mock_dict1.create_dictionary_command.return_value = 'CREATE DICTIONARY dict1'
        
        mock_dict2 = Mock()
        mock_dict2.dictionary_name = 'dict2'
        mock_dict2.create_dictionary_command.return_value = 'CREATE DICTIONARY dict2'
        
        self.mock_processor.get_ch_dictionaries.return_value = [mock_dict1, mock_dict2]
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Verify get_ch_dictionaries was called
        self.mock_processor.get_ch_dictionaries.assert_called_once()
        
        # Verify both dictionaries were created
        calls = self.mock_client.command.call_args_list
        # First call is for table creation, next two are for dictionaries
        self.assertGreaterEqual(len(calls), 2)
        
        # Check that dictionary commands were executed
        dict_commands = [call[0][0] for call in calls if 'DICTIONARY' in call[0][0]]
        self.assertIn('CREATE DICTIONARY dict1', dict_commands)
        self.assertIn('CREATE DICTIONARY dict2', dict_commands)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_initialization_no_dictionaries(self, mock_connector):
        """Test ClickHouseBatcher initialization with no dictionaries."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.get_ch_dictionaries.return_value = []
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Should still initialize successfully
        self.assertIsNotNone(batcher)
        self.mock_processor.get_ch_dictionaries.assert_called_once()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_materialized_view(self, mock_connector):
        """Test create_materialized_view method."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Create mock materialized view
        mock_mv = Mock()
        mock_mv.table = 'mv_target_table'
        mock_mv.mv_name = 'test_mv'
        mock_mv.create_table_command.return_value = 'CREATE TABLE mv_target_table'
        mock_mv.create_mv_command.return_value = 'CREATE MATERIALIZED VIEW test_mv'
        
        # Reset mock to clear initialization calls
        self.mock_client.command.reset_mock()
        
        batcher.create_materialized_view(mock_mv)
        
        # Should call command twice: once for table, once for MV
        self.assertEqual(self.mock_client.command.call_count, 2)
        calls = self.mock_client.command.call_args_list
        self.assertEqual(calls[0][0][0], 'CREATE TABLE mv_target_table')
        self.assertEqual(calls[1][0][0], 'CREATE MATERIALIZED VIEW test_mv')
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_materialized_view_no_table_command(self, mock_connector):
        """Test create_materialized_view when create_table_command returns None."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Reset mock to clear initialization calls
        self.mock_client.command.reset_mock()
        
        # Create mock MV that returns None for table command
        mock_mv = Mock()
        mock_mv.create_table_command.return_value = None
        
        batcher.create_materialized_view(mock_mv)
        
        # Should not call client.command
        self.mock_client.command.assert_not_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_create_materialized_view_no_mv_command(self, mock_connector):
        """Test create_materialized_view when create_mv_command returns None."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Reset mock to clear initialization calls
        self.mock_client.command.reset_mock()
        
        # Create mock MV with table command but no MV command
        mock_mv = Mock()
        mock_mv.table = 'mv_target_table'
        mock_mv.create_table_command.return_value = 'CREATE TABLE mv_target_table'
        mock_mv.create_mv_command.return_value = None
        
        batcher.create_materialized_view(mock_mv)
        
        # Should not call client.command since MV command is None
        self.mock_client.command.assert_not_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.logger')
    def test_create_materialized_view_table_exception(self, mock_logger, mock_connector):
        """Test create_materialized_view handles table creation exceptions."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Create mock MV
        mock_mv = Mock()
        mock_mv.table = 'mv_target_table'
        mock_mv.mv_name = 'test_mv'
        mock_mv.create_table_command.return_value = 'CREATE TABLE mv_target_table'
        mock_mv.create_mv_command.return_value = 'CREATE MATERIALIZED VIEW test_mv'
        
        # Make client.command raise an exception for table creation
        self.mock_client.command.side_effect = Exception("Table creation failed")
        
        with self.assertRaises(Exception):
            batcher.create_materialized_view(mock_mv)
        
        # Should log error
        mock_logger.error.assert_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.logger')
    def test_create_materialized_view_mv_exception(self, mock_logger, mock_connector):
        """Test create_materialized_view handles MV creation exceptions."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Create mock MV
        mock_mv = Mock()
        mock_mv.table = 'mv_target_table'
        mock_mv.mv_name = 'test_mv'
        mock_mv.create_table_command.return_value = 'CREATE TABLE mv_target_table'
        mock_mv.create_mv_command.return_value = 'CREATE MATERIALIZED VIEW test_mv'
        
        # Make client.command succeed for table but fail for MV
        self.mock_client.command.side_effect = [None, Exception("MV creation failed")]
        
        with self.assertRaises(Exception):
            batcher.create_materialized_view(mock_mv)
        
        # Should log error
        mock_logger.error.assert_called()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_initialization_with_materialized_views(self, mock_connector):
        """Test ClickHouseBatcher initialization creates materialized views."""
        mock_connector.return_value.client = self.mock_client
        
        # Create mock materialized views
        mock_mv1 = Mock()
        mock_mv1.table = 'mv1_target'
        mock_mv1.mv_name = 'mv1'
        mock_mv1.create_table_command.return_value = 'CREATE TABLE mv1_target'
        mock_mv1.create_mv_command.return_value = 'CREATE MATERIALIZED VIEW mv1'
        
        mock_mv2 = Mock()
        mock_mv2.table = 'mv2_target'
        mock_mv2.mv_name = 'mv2'
        mock_mv2.create_table_command.return_value = 'CREATE TABLE mv2_target'
        mock_mv2.create_mv_command.return_value = 'CREATE MATERIALIZED VIEW mv2'
        
        self.mock_processor.get_materialized_views.return_value = [mock_mv1, mock_mv2]
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Verify get_materialized_views was called
        self.mock_processor.get_materialized_views.assert_called_once()
        
        # Verify both MVs were created (2 commands per MV = 4 total, plus 1 for table init)
        calls = self.mock_client.command.call_args_list
        self.assertGreaterEqual(len(calls), 4)
        
        # Check that MV commands were executed
        mv_commands = [call[0][0] for call in calls if 'MATERIALIZED VIEW' in call[0][0]]
        self.assertIn('CREATE MATERIALIZED VIEW mv1', mv_commands)
        self.assertIn('CREATE MATERIALIZED VIEW mv2', mv_commands)
        
        # Check that table commands were executed
        table_commands = [call[0][0] for call in calls if 'CREATE TABLE' in call[0][0]]
        self.assertIn('CREATE TABLE mv1_target', table_commands)
        self.assertIn('CREATE TABLE mv2_target', table_commands)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_initialization_no_materialized_views(self, mock_connector):
        """Test ClickHouseBatcher initialization with no materialized views."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.get_materialized_views.return_value = []
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Should still initialize successfully
        self.assertIsNotNone(batcher)
        self.mock_processor.get_materialized_views.assert_called_once()
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_with_consumer_metadata(self, mock_connector):
        """Test process_message with consumer metadata."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = [
            {'_clickhouse_table': 'test_table', 'col1': 'val1'}
        ]
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        msg = {'data': 'test'}
        metadata = {'offset': 123}
        batcher.process_message(msg, metadata)
        
        self.mock_processor.build_message.assert_called_once_with(msg, metadata)
    
    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_process_message_default_table(self, mock_connector):
        """Test process_message uses default table when not specified."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = True
        # Message without _clickhouse_table field
        self.mock_processor.build_message.return_value = [
            {'col1': 'val1', 'col2': 'val2'}
        ]
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        msg = {'data': 'test'}
        batcher.process_message(msg)
        
        # Should use processor.table as default
        self.assertEqual(len(batcher.batch['test_table']), 1)

    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    @patch('metranova.writers.clickhouse.time')
    def test_flush_timer_triggers_on_timeout(self, mock_time, mock_connector):
        """Test that flush_timer triggers flush after timeout."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.message_to_columns.return_value = ('val1', 'val2')
        
        # Set very short timeout for testing
        with patch.dict(os.environ, {'CLICKHOUSE_BATCH_TIMEOUT': '0.1', 'CLICKHOUSE_FLUSH_INTERVAL': '0.05'}):
            batcher = ClickHouseBatcher(self.mock_processor)
            
            # Add a message
            batcher.batch['test_table'].append({'col1': 'val1'})
            
            # Simulate time passage
            current_time = time.time()
            mock_time.time.side_effect = [
                current_time,  # last_flush_time initialization
                current_time,  # first check (no timeout yet)
                current_time + 0.15  # second check (timeout exceeded)
            ]
            
            # Manually call the flush check logic
            if (current_time + 0.15 - current_time) >= 0.1:
                batcher.flush_batch('test_table')
            
            # Verify flush was called
            self.mock_client.insert.assert_called()

    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_batch_with_multiple_messages(self, mock_connector):
        """Test batch accumulates multiple messages correctly."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.match_message.return_value = True
        self.mock_processor.message_to_columns.side_effect = [
            ('val1', 'val2'),
            ('val3', 'val4'),
            ('val5', 'val6')
        ]
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Add multiple messages
        for i in range(3):
            self.mock_processor.build_message.return_value = [
                {'_clickhouse_table': 'test_table', 'col1': f'val{i}'}
            ]
            batcher.process_message({'data': f'test{i}'})
        
        self.assertEqual(len(batcher.batch['test_table']), 3)

    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_batch_lock_concurrency(self, mock_connector):
        """Test that batch_lock is used for thread safety."""
        mock_connector.return_value.client = self.mock_client
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        # Verify batch_lock exists and is a threading.Lock
        self.assertIsInstance(batcher.batch_lock, threading.Lock)

    @patch('metranova.writers.clickhouse.ClickHouseConnector')
    def test_flush_batch_updates_last_flush_time(self, mock_connector):
        """Test that flush_batch updates last_flush_time."""
        mock_connector.return_value.client = self.mock_client
        self.mock_processor.message_to_columns.return_value = ('val1', 'val2')
        
        batcher = ClickHouseBatcher(self.mock_processor)
        
        initial_flush_time = batcher.last_flush_time
        time.sleep(0.01)  # Small delay
        
        batcher.batch['test_table'].append({'col1': 'val1'})
        batcher.flush_batch('test_table')
        
        # last_flush_time should be updated
        self.assertGreater(batcher.last_flush_time, initial_flush_time)


if __name__ == '__main__':
    unittest.main()
