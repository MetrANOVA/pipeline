#!/usr/bin/env python3

import unittest
import os
import pickle
import tempfile
from unittest.mock import patch, MagicMock, Mock, mock_open

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from metranova.cachers.ip import IPCacher


class TestIPCacher(unittest.TestCase):
    """Unit tests for IPCacher class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Use a temporary directory for cache files
        self.temp_dir = tempfile.mkdtemp()
        
        with patch.dict(os.environ, {
            'IP_CACHER_DIR': self.temp_dir,
            'IP_CACHER_TABLES': 'meta_ip,meta_ip_scinet',
            'IP_CACHER_REFRESH_INTERVAL': '0'  # Disable refresh for tests
        }):
            self.cacher = IPCacher()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self.cacher, 'close'):
            self.cacher.close()
        
        # Clean up temp directory
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_inheritance_from_base_cacher(self):
        """Test that IPCacher inherits from BaseCacher."""
        from metranova.cachers.base import BaseCacher
        self.assertIsInstance(self.cacher, BaseCacher)

    def test_init_with_environment_variables(self):
        """Test initialization with environment variables."""
        self.assertEqual(self.cacher.cache_dir, self.temp_dir)
        self.assertEqual(self.cacher.cache_refresh_interval, 0)
        self.assertEqual(self.cacher.tables, ['meta_ip', 'meta_ip_scinet'])

    def test_init_default_values(self):
        """Test initialization with default values."""
        with patch.dict(os.environ, {}, clear=True):
            cacher = IPCacher()
            
            self.assertEqual(cacher.cache_dir, 'caches')
            self.assertEqual(cacher.cache_refresh_interval, 600)
            self.assertEqual(cacher.tables, [])
            self.assertIsNone(cacher.base_url)
            cacher.close()

    def test_init_with_base_url(self):
        """Test initialization with base URL."""
        with patch.dict(os.environ, {
            'IP_CACHER_BASE_URL': 'https://example.com/caches',
            'IP_CACHER_REFRESH_INTERVAL': '0'
        }):
            cacher = IPCacher()
            
            self.assertEqual(cacher.base_url, 'https://example.com/caches')
            cacher.close()

    def test_init_no_tables_warning(self):
        """Test that initialization logs warning when no tables specified."""
        with patch.dict(os.environ, {'IP_CACHER_TABLES': '', 'IP_CACHER_REFRESH_INTERVAL': '0'}):
            with patch('metranova.cachers.ip.logger') as mock_logger:
                cacher = IPCacher()
                
                # Should have logged a warning about no tables
                mock_logger.warning.assert_called()
                cacher.close()

    def test_prime_with_base_url(self):
        """Test prime calls load_from_url when base_url is set."""
        with patch.dict(os.environ, {
            'IP_CACHER_BASE_URL': 'https://example.com',
            'IP_CACHER_TABLES': 'meta_ip',
            'IP_CACHER_REFRESH_INTERVAL': '0'
        }):
            cacher = IPCacher()
            with patch.object(cacher, 'load_from_url') as mock_load_url:
                cacher.prime()
                mock_load_url.assert_called_once_with('meta_ip')
            cacher.close()

    def test_prime_with_cache_dir(self):
        """Test prime calls load_from_file when cache_dir is set but not base_url."""
        with patch.object(self.cacher, 'load_from_file') as mock_load_file:
            self.cacher.prime()
            # Should be called for each table
            self.assertEqual(mock_load_file.call_count, len(self.cacher.tables))

    def test_prime_no_base_url_or_cache_dir(self):
        """Test prime logs error when neither base_url nor cache_dir is set."""
        with patch.dict(os.environ, {
            'IP_CACHER_TABLES': 'meta_ip',
            'IP_CACHER_REFRESH_INTERVAL': '0'
        }):
            cacher = IPCacher()
            cacher.base_url = None
            cacher.cache_dir = None
            
            with patch.object(cacher.logger, 'error') as mock_error:
                cacher.prime()
                mock_error.assert_called()
            cacher.close()

    def test_prime_file_not_found(self):
        """Test prime when pickle file doesn't exist."""
        # Don't create any files, so they won't be found
        self.cacher.prime()
        
        # Should not have loaded anything
        self.assertEqual(len(self.cacher.local_cache), 0)

    def test_prime_file_exists_and_loads(self):
        """Test prime successfully loads pickle file."""
        # Create a mock trie/dict
        test_trie = {
            '192.168.1.1': {'id': 'ip1', 'ref': 'IP 1'},
            '10.0.0.1': {'id': 'ip2', 'ref': 'IP 2'}
        }
        
        # Create pickle file
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'wb') as f:
            pickle.dump(test_trie, f)
        
        self.cacher.prime()
        
        # Should have loaded the trie
        self.assertIn('meta_ip', self.cacher.local_cache)
        self.assertEqual(len(self.cacher.local_cache['meta_ip']), 2)
        self.assertEqual(self.cacher.local_cache['meta_ip']['192.168.1.1']['ref'], 'IP 1')

    def test_prime_multiple_tables(self):
        """Test prime loads multiple table pickle files."""
        # Create pickle files for both tables
        test_trie1 = {'192.168.1.1': {'id': 'ip1'}}
        test_trie2 = {'10.0.0.1': {'id': 'ip2'}}
        
        pickle_path1 = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        pickle_path2 = os.path.join(self.temp_dir, 'ip_trie_meta_ip_scinet.pickle')
        
        with open(pickle_path1, 'wb') as f:
            pickle.dump(test_trie1, f)
        with open(pickle_path2, 'wb') as f:
            pickle.dump(test_trie2, f)
        
        self.cacher.prime()
        
        # Should have loaded both tries
        self.assertIn('meta_ip', self.cacher.local_cache)
        self.assertIn('meta_ip_scinet', self.cacher.local_cache)

    def test_prime_corrupted_pickle_file(self):
        """Test prime handles corrupted pickle file."""
        # Create a corrupted file
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'w') as f:
            f.write('this is not a valid pickle file')
        
        # Should handle the error gracefully
        self.cacher.prime()
        
        # Should not have loaded anything for this table
        self.assertNotIn('meta_ip', self.cacher.local_cache)

    def test_prime_empty_pickle_file(self):
        """Test prime with empty pickle file."""
        # Create empty trie
        test_trie = {}
        
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'wb') as f:
            pickle.dump(test_trie, f)
        
        self.cacher.prime()
        
        # Should have loaded empty dict
        self.assertIn('meta_ip', self.cacher.local_cache)
        self.assertEqual(len(self.cacher.local_cache['meta_ip']), 0)

    def test_lookup_success(self):
        """Test successful lookup."""
        self.cacher.local_cache['meta_ip'] = {
            '192.168.1.1': {'id': 'ip1', 'ref': 'IP 1'}
        }
        
        result = self.cacher.lookup('meta_ip', '192.168.1.1')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['ref'], 'IP 1')

    def test_lookup_key_not_found(self):
        """Test lookup when key not found."""
        self.cacher.local_cache['meta_ip'] = {}
        
        result = self.cacher.lookup('meta_ip', '192.168.1.1')
        
        self.assertIsNone(result)

    def test_lookup_table_not_cached(self):
        """Test lookup when table not in cache."""
        result = self.cacher.lookup('nonexistent_table', 'key')
        
        self.assertIsNone(result)

    def test_lookup_with_none_key(self):
        """Test lookup with None key."""
        result = self.cacher.lookup('meta_ip', None)
        
        self.assertIsNone(result)

    def test_lookup_with_none_table(self):
        """Test lookup with None table."""
        result = self.cacher.lookup(None, 'key')
        
        self.assertIsNone(result)

    def test_lookup_with_empty_key(self):
        """Test lookup with empty string key."""
        result = self.cacher.lookup('meta_ip', '')
        
        self.assertIsNone(result)

    def test_lookup_with_empty_table(self):
        """Test lookup with empty string table."""
        result = self.cacher.lookup('', 'key')
        
        self.assertIsNone(result)

    def test_local_cache_structure(self):
        """Test that local_cache has correct structure."""
        self.assertIsInstance(self.cacher.local_cache, dict)

    def test_prime_logs_info_for_found_file(self):
        """Test that prime logs info message when file found."""
        # Create pickle file
        test_trie = {'key': 'value'}
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'wb') as f:
            pickle.dump(test_trie, f)
        
        with patch.object(self.cacher.logger, 'info') as mock_info:
            self.cacher.prime()
            
            # Should have logged info messages
            self.assertGreater(mock_info.call_count, 0)

    def test_prime_logs_warning_for_missing_file(self):
        """Test that prime logs warning when file not found."""
        with patch.object(self.cacher.logger, 'warning') as mock_warning:
            self.cacher.prime()
            
            # Should have logged warnings for missing files
            self.assertGreater(mock_warning.call_count, 0)

    def test_prime_with_pytricia_trie(self):
        """Test prime works with actual pytricia.PyTricia object."""
        try:
            import pytricia
            
            # Create a real PyTricia trie
            test_trie = pytricia.PyTricia()
            test_trie['192.168.1.0/24'] = {'id': 'subnet1', 'ref': 'Subnet 1'}
            test_trie['10.0.0.0/8'] = {'id': 'subnet2', 'ref': 'Subnet 2'}
            
            # PyTricia must be frozen before pickling
            test_trie.freeze()
            
            # Save to pickle
            pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
            with open(pickle_path, 'wb') as f:
                pickle.dump(test_trie, f)
            
            self.cacher.prime()
            
            # Should have loaded the trie
            self.assertIn('meta_ip', self.cacher.local_cache)
            self.assertIsInstance(self.cacher.local_cache['meta_ip'], pytricia.PyTricia)
            
        except ImportError:
            self.skipTest("pytricia not available")

    def test_load_from_url_success(self):
        """Test load_from_url successfully loads from remote URL."""
        test_trie = {'192.168.1.1': {'id': 'ip1', 'ref': 'IP 1'}}
        pickled_data = pickle.dumps(test_trie)
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = pickled_data
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response
            
            self.cacher.base_url = 'https://example.com'
            self.cacher.load_from_url('meta_ip')
            
            mock_get.assert_called_once_with('https://example.com/ip_trie_meta_ip.pickle')
            self.assertIn('meta_ip', self.cacher.local_cache)
            self.assertEqual(self.cacher.local_cache['meta_ip']['192.168.1.1']['ref'], 'IP 1')

    def test_load_from_url_http_error(self):
        """Test load_from_url handles HTTP errors."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.raise_for_status.side_effect = Exception("404 Not Found")
            mock_get.return_value = mock_response
            
            self.cacher.base_url = 'https://example.com'
            
            with patch.object(self.cacher.logger, 'error') as mock_error:
                self.cacher.load_from_url('meta_ip')
                mock_error.assert_called()
                
            # Should not have loaded anything
            self.assertNotIn('meta_ip', self.cacher.local_cache)

    def test_load_from_url_pickle_error(self):
        """Test load_from_url handles pickle deserialization errors."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = b'invalid pickle data'
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response
            
            self.cacher.base_url = 'https://example.com'
            
            with patch.object(self.cacher.logger, 'error') as mock_error:
                self.cacher.load_from_url('meta_ip')
                mock_error.assert_called()
                
            # Should not have loaded anything
            self.assertNotIn('meta_ip', self.cacher.local_cache)

    def test_load_from_url_network_error(self):
        """Test load_from_url handles network errors."""
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception("Connection error")
            
            self.cacher.base_url = 'https://example.com'
            
            with patch.object(self.cacher.logger, 'error') as mock_error:
                self.cacher.load_from_url('meta_ip')
                mock_error.assert_called()
                
            # Should not have loaded anything
            self.assertNotIn('meta_ip', self.cacher.local_cache)

    def test_load_from_file_success(self):
        """Test load_from_file successfully loads from local file."""
        test_trie = {'192.168.1.1': {'id': 'ip1', 'ref': 'IP 1'}}
        
        # Create pickle file
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'wb') as f:
            pickle.dump(test_trie, f)
        
        self.cacher.load_from_file('meta_ip')
        
        # Should have loaded the trie
        self.assertIn('meta_ip', self.cacher.local_cache)
        self.assertEqual(self.cacher.local_cache['meta_ip']['192.168.1.1']['ref'], 'IP 1')

    def test_load_from_file_not_found(self):
        """Test load_from_file handles missing file."""
        with patch.object(self.cacher.logger, 'warning') as mock_warning:
            self.cacher.load_from_file('nonexistent_table')
            mock_warning.assert_called()
            
        # Should not have loaded anything
        self.assertNotIn('nonexistent_table', self.cacher.local_cache)

    def test_load_from_file_pickle_error(self):
        """Test load_from_file handles pickle deserialization errors."""
        # Create a corrupted file
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'w') as f:
            f.write('this is not a valid pickle file')
        
        with patch.object(self.cacher.logger, 'error') as mock_error:
            self.cacher.load_from_file('meta_ip')
            mock_error.assert_called()
            
        # Should not have loaded anything for this table
        self.assertNotIn('meta_ip', self.cacher.local_cache)

    def test_load_from_file_logs_info(self):
        """Test load_from_file logs info messages."""
        test_trie = {'key': 'value'}
        pickle_path = os.path.join(self.temp_dir, 'ip_trie_meta_ip.pickle')
        with open(pickle_path, 'wb') as f:
            pickle.dump(test_trie, f)
        
        with patch.object(self.cacher.logger, 'info') as mock_info:
            self.cacher.load_from_file('meta_ip')
            # Should log at least twice: file found and loaded
            self.assertGreaterEqual(mock_info.call_count, 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
