import unittest
import os
import tempfile
import pickle
from unittest.mock import Mock, patch, MagicMock
from metranova.writers.file import PickleFileWriter


class TestPickleFileWriter(unittest.TestCase):
    """Test the PickleFileWriter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_processor = Mock()
        self.temp_dir = tempfile.mkdtemp()
        
        # Patch environment variable
        self.env_patcher = patch.dict(os.environ, {
            'PICKLE_FILE_DIRECTORY': self.temp_dir
        })
        self.env_patcher.start()
        
        self.writer = PickleFileWriter([self.mock_processor])
    
    def tearDown(self):
        """Clean up test fixtures."""
        self.env_patcher.stop()
        # Clean up temp directory
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_initialization(self):
        """Test PickleFileWriter initialization."""
        self.assertEqual(len(self.writer.processors), 1)
        self.assertIsNone(self.writer.datastore)
        self.assertEqual(self.writer.file_dir, self.temp_dir)
    
    def test_initialization_default_directory(self):
        """Test initialization with default directory."""
        with patch.dict(os.environ, {}, clear=True):
            writer = PickleFileWriter([self.mock_processor])
            self.assertEqual(writer.file_dir, 'caches')
    
    def test_write_message_valid(self):
        """Test writing a valid message."""
        msg = {
            'name': 'test_file.pkl',
            'data': {'key': 'value', 'number': 42}
        }
        
        self.writer.write_message(msg)
        
        # Verify file was created
        file_path = os.path.join(self.temp_dir, 'test_file.pkl')
        self.assertTrue(os.path.exists(file_path))
        
        # Verify file contents
        with open(file_path, 'rb') as f:
            loaded_data = pickle.load(f)
        self.assertEqual(loaded_data, msg['data'])
    
    def test_write_message_creates_directory(self):
        """Test that write_message creates directory if it doesn't exist."""
        # Use a new temp directory that doesn't exist yet
        new_temp_dir = os.path.join(self.temp_dir, 'subdir', 'nested')
        
        with patch.dict(os.environ, {'PICKLE_FILE_DIRECTORY': new_temp_dir}):
            writer = PickleFileWriter([self.mock_processor])
            msg = {
                'name': 'test_file.pkl',
                'data': {'test': 'data'}
            }
            
            writer.write_message(msg)
            
            # Verify directory and file were created
            self.assertTrue(os.path.exists(new_temp_dir))
            file_path = os.path.join(new_temp_dir, 'test_file.pkl')
            self.assertTrue(os.path.exists(file_path))
    
    def test_write_message_missing_data(self):
        """Test write_message with missing 'data' field."""
        msg = {'name': 'test_file.pkl'}
        
        self.writer.write_message(msg)
        
        # Verify file was not created
        file_path = os.path.join(self.temp_dir, 'test_file.pkl')
        self.assertFalse(os.path.exists(file_path))
    
    def test_write_message_missing_name(self):
        """Test write_message with missing 'name' field."""
        msg = {'data': {'key': 'value'}}
        
        self.writer.write_message(msg)
        
        # No file should be created
        files = os.listdir(self.temp_dir)
        self.assertEqual(len(files), 0)
    
    def test_write_message_none(self):
        """Test write_message with None message."""
        self.writer.write_message(None)
        
        # No file should be created
        files = os.listdir(self.temp_dir)
        self.assertEqual(len(files), 0)
    
    def test_write_message_empty_dict(self):
        """Test write_message with empty dict."""
        msg = {}
        
        self.writer.write_message(msg)
        
        # No file should be created
        files = os.listdir(self.temp_dir)
        self.assertEqual(len(files), 0)
    
    def test_write_message_complex_data(self):
        """Test writing complex nested data structures."""
        complex_data = {
            'list': [1, 2, 3],
            'nested': {'inner': {'deep': 'value'}},
            'tuple': (1, 2, 3),
            'mixed': [{'a': 1}, {'b': 2}]
        }
        
        msg = {
            'name': 'complex_test.pkl',
            'data': complex_data
        }
        
        self.writer.write_message(msg)
        
        # Verify file contents
        file_path = os.path.join(self.temp_dir, 'complex_test.pkl')
        with open(file_path, 'rb') as f:
            loaded_data = pickle.load(f)
        self.assertEqual(loaded_data, complex_data)
    
    def test_write_message_overwrites_existing(self):
        """Test that writing to same filename overwrites existing file."""
        msg1 = {
            'name': 'test.pkl',
            'data': {'version': 1}
        }
        msg2 = {
            'name': 'test.pkl',
            'data': {'version': 2}
        }
        
        self.writer.write_message(msg1)
        self.writer.write_message(msg2)
        
        # Verify only the second version exists
        file_path = os.path.join(self.temp_dir, 'test.pkl')
        with open(file_path, 'rb') as f:
            loaded_data = pickle.load(f)
        self.assertEqual(loaded_data, {'version': 2})
    
    def test_write_message_with_consumer_metadata(self):
        """Test write_message with consumer_metadata (should be ignored)."""
        msg = {
            'name': 'test.pkl',
            'data': {'key': 'value'}
        }
        metadata = {'offset': 123}
        
        self.writer.write_message(msg, metadata)
        
        # Verify file was created normally
        file_path = os.path.join(self.temp_dir, 'test.pkl')
        self.assertTrue(os.path.exists(file_path))
    
    def test_write_message_exception_handling(self):
        """Test that exceptions during write are caught and logged."""
        msg = {
            'name': 'test.pkl',
            'data': {'key': 'value'}
        }
        
        # Mock open to raise an exception
        with patch('builtins.open', side_effect=PermissionError("No permission")):
            # Should not raise exception
            self.writer.write_message(msg)
        
        # File should not exist since write failed
        file_path = os.path.join(self.temp_dir, 'test.pkl')
        self.assertFalse(os.path.exists(file_path))


if __name__ == '__main__':
    unittest.main()
