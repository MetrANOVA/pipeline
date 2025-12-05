import unittest
from unittest.mock import Mock, MagicMock
from metranova.writers.base import BaseWriter


class TestBaseWriter(unittest.TestCase):
    """Test the BaseWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_processor = Mock()
        self.mock_processor.__class__.__name__ = 'MockProcessor'
        self.writer = BaseWriter([self.mock_processor])
    
    def test_initialization(self):
        """Test BaseWriter initialization."""
        self.assertEqual(len(self.writer.processors), 1)
        self.assertEqual(self.writer.processors[0], self.mock_processor)
        self.assertIsNone(self.writer.datastore)
    
    def test_initialization_multiple_processors(self):
        """Test BaseWriter with multiple processors."""
        processor1 = Mock()
        processor2 = Mock()
        writer = BaseWriter([processor1, processor2])
        self.assertEqual(len(writer.processors), 2)
    
    def test_process_message_matching(self):
        """Test process_message with matching processor."""
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = [{'data': 'test'}]
        
        msg = {'key': 'value'}
        self.writer.write_message = Mock()
        
        self.writer.process_message(msg)
        
        self.mock_processor.match_message.assert_called_once_with(msg)
        self.mock_processor.build_message.assert_called_once_with(msg, None)
        self.writer.write_message.assert_called_once_with({'data': 'test'}, None)
    
    def test_process_message_not_matching(self):
        """Test process_message with non-matching processor."""
        self.mock_processor.match_message.return_value = False
        
        msg = {'key': 'value'}
        self.writer.write_message = Mock()
        
        self.writer.process_message(msg)
        
        self.mock_processor.match_message.assert_called_once_with(msg)
        self.mock_processor.build_message.assert_not_called()
        self.writer.write_message.assert_not_called()
    
    def test_process_message_with_consumer_metadata(self):
        """Test process_message with consumer metadata."""
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = [{'data': 'test'}]
        
        msg = {'key': 'value'}
        metadata = {'offset': 123}
        self.writer.write_message = Mock()
        
        self.writer.process_message(msg, metadata)
        
        self.mock_processor.build_message.assert_called_once_with(msg, metadata)
        self.writer.write_message.assert_called_once_with({'data': 'test'}, metadata)
    
    def test_process_message_returns_none(self):
        """Test process_message when processor returns None."""
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = None
        
        msg = {'key': 'value'}
        self.writer.write_message = Mock()
        
        self.writer.process_message(msg)
        
        self.writer.write_message.assert_not_called()
    
    def test_process_message_returns_empty_list(self):
        """Test process_message when processor returns empty list."""
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = []
        
        msg = {'key': 'value'}
        self.writer.write_message = Mock()
        
        self.writer.process_message(msg)
        
        self.writer.write_message.assert_not_called()
    
    def test_process_message_multiple_results(self):
        """Test process_message when processor returns multiple messages."""
        self.mock_processor.match_message.return_value = True
        self.mock_processor.build_message.return_value = [
            {'data': 'test1'},
            {'data': 'test2'},
            {'data': 'test3'}
        ]
        
        msg = {'key': 'value'}
        self.writer.write_message = Mock()
        
        self.writer.process_message(msg)
        
        self.assertEqual(self.writer.write_message.call_count, 3)
    
    def test_process_message_multiple_processors(self):
        """Test process_message with multiple processors."""
        processor1 = Mock()
        processor1.match_message.return_value = True
        processor1.build_message.return_value = [{'data': 'proc1'}]
        
        processor2 = Mock()
        processor2.match_message.return_value = True
        processor2.build_message.return_value = [{'data': 'proc2'}]
        
        writer = BaseWriter([processor1, processor2])
        writer.write_message = Mock()
        
        msg = {'key': 'value'}
        writer.process_message(msg)
        
        self.assertEqual(writer.write_message.call_count, 2)
    
    def test_write_message_not_implemented(self):
        """Test that write_message raises NotImplementedError."""
        with self.assertRaises(NotImplementedError) as context:
            self.writer.write_message({'data': 'test'})
        self.assertIn("Subclasses should implement this method", str(context.exception))
    
    def test_close_with_datastore(self):
        """Test close() with datastore."""
        mock_datastore = Mock()
        self.writer.datastore = mock_datastore
        
        self.writer.close()
        
        mock_datastore.close.assert_called_once()
    
    def test_close_without_datastore(self):
        """Test close() without datastore."""
        self.writer.datastore = None
        self.writer.close()  # Should not raise exception


if __name__ == '__main__':
    unittest.main()
