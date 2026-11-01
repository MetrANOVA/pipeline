import json
import os
import pickle
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock
from metranova.writers.file import JSONFileWriter, PickleFileWriter


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


class TestJSONFileWriter(unittest.TestCase):
    def setUp(self):
        self.mock_processor = Mock()
        self.temp_dir = tempfile.mkdtemp()
        self.env_patcher = patch.dict(os.environ, {
            'JSON_FILE_DIRECTORY': self.temp_dir,
            'JSON_FILE_INDENT': '2',
        })
        self.env_patcher.start()
        self.writer = JSONFileWriter([self.mock_processor])

    def tearDown(self):
        self.env_patcher.stop()
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_initialization_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            writer = JSONFileWriter([self.mock_processor])
        self.assertEqual(writer.file_dir, 'output')

    def test_initialization_reads_env(self):
        self.assertEqual(self.writer.file_dir, self.temp_dir)

    def test_write_valid_message(self):
        data = {'route-report': [{'device': 'rtr1'}]}
        msg = {'name': 'report.json', 'data': data}
        self.writer.write_message(msg)

        path = os.path.join(self.temp_dir, 'report.json')
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            loaded = json.load(f)
        self.assertEqual(loaded, data)

    def test_write_creates_directory(self):
        nested = os.path.join(self.temp_dir, 'sub', 'deep')
        with patch.dict(os.environ, {'JSON_FILE_DIRECTORY': nested, 'JSON_FILE_INDENT': '2'}):
            writer = JSONFileWriter([self.mock_processor])
        writer.write_message({'name': 'f.json', 'data': {'x': 1}})
        self.assertTrue(os.path.exists(os.path.join(nested, 'f.json')))

    def test_atomic_write_via_tmp_file(self):
        """The writer must use a .tmp file then os.replace()."""
        written_paths = []
        real_replace = os.replace

        def tracking_replace(src, dst):
            written_paths.append((src, dst))
            real_replace(src, dst)

        with patch('os.replace', side_effect=tracking_replace):
            self.writer.write_message({'name': 'a.json', 'data': {'k': 1}})

        self.assertEqual(len(written_paths), 1)
        src, dst = written_paths[0]
        self.assertTrue(src.endswith('.tmp'))
        self.assertEqual(dst, os.path.join(self.temp_dir, 'a.json'))

    def test_no_leftover_tmp_file_after_write(self):
        self.writer.write_message({'name': 'b.json', 'data': {'k': 2}})
        files = os.listdir(self.temp_dir)
        self.assertFalse(any(f.endswith('.tmp') for f in files))

    def test_pretty_output_when_indent_2(self):
        self.writer.write_message({'name': 'pretty.json', 'data': {'a': 1}})
        path = os.path.join(self.temp_dir, 'pretty.json')
        with open(path) as f:
            raw = f.read()
        self.assertIn('\n', raw)

    def test_compact_output_when_indent_0(self):
        with patch.dict(os.environ, {'JSON_FILE_DIRECTORY': self.temp_dir, 'JSON_FILE_INDENT': '0'}):
            writer = JSONFileWriter([self.mock_processor])
        writer.write_message({'name': 'compact.json', 'data': {'a': 1}})
        path = os.path.join(self.temp_dir, 'compact.json')
        with open(path) as f:
            raw = f.read()
        self.assertNotIn('\n', raw)

    def test_missing_name_skipped(self):
        self.writer.write_message({'data': {'x': 1}})
        self.assertEqual(os.listdir(self.temp_dir), [])

    def test_missing_data_skipped(self):
        self.writer.write_message({'name': 'f.json'})
        self.assertEqual(os.listdir(self.temp_dir), [])

    def test_none_message_skipped(self):
        self.writer.write_message(None)
        self.assertEqual(os.listdir(self.temp_dir), [])

    def test_empty_dict_skipped(self):
        self.writer.write_message({})
        self.assertEqual(os.listdir(self.temp_dir), [])

    def test_overwrite_existing_file(self):
        self.writer.write_message({'name': 'r.json', 'data': {'v': 1}})
        self.writer.write_message({'name': 'r.json', 'data': {'v': 2}})
        path = os.path.join(self.temp_dir, 'r.json')
        with open(path) as f:
            loaded = json.load(f)
        self.assertEqual(loaded['v'], 2)

    def test_exception_during_write_caught(self):
        with patch('builtins.open', side_effect=PermissionError("denied")):
            # Must not raise
            self.writer.write_message({'name': 'x.json', 'data': {'y': 3}})

    def test_consumer_metadata_ignored(self):
        self.writer.write_message({'name': 'z.json', 'data': {'ok': True}}, {'offset': 99})
        path = os.path.join(self.temp_dir, 'z.json')
        self.assertTrue(os.path.exists(path))

    def test_complex_nested_data(self):
        data = {'route-report': [{'re:output': {'neighbor-result': [
            {'address': '1.2.3.4', 'accepted': 100, 'rejected': 5}
        ]}}]}
        self.writer.write_message({'name': 'complex.json', 'data': data})
        path = os.path.join(self.temp_dir, 'complex.json')
        with open(path) as f:
            loaded = json.load(f)
        self.assertEqual(loaded, data)


if __name__ == '__main__':
    unittest.main()
