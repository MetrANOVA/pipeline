#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, Mock
import redis

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from metranova.cachers.redis import RedisCacher


class TestRedisCacher(unittest.TestCase):
    """Unit tests for RedisCacher class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the RedisConnector to avoid actual connections
        with patch('metranova.cachers.redis.RedisConnector') as mock_connector:
            mock_instance = Mock()
            mock_instance.client = None
            mock_connector.return_value = mock_instance
            
            self.cacher = RedisCacher()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self.cacher, 'close'):
            self.cacher.close()

    def test_inheritance_from_base_cacher(self):
        """Test that RedisCacher inherits from BaseCacher."""
        from metranova.cachers.base import BaseCacher
        self.assertIsInstance(self.cacher, BaseCacher)

    def test_init_creates_redis_connector(self):
        """Test that initialization creates a RedisConnector."""
        self.assertIsNotNone(self.cacher.cache)

    def test_lookup_no_client(self):
        """Test lookup when Redis client is not available."""
        self.cacher.cache.client = None
        
        result = self.cacher.lookup('test_table', 'test_key')
        
        self.assertIsNone(result)

    def test_lookup_with_none_parameters(self):
        """Test lookup with None parameters."""
        result1 = self.cacher.lookup(None, 'key')
        result2 = self.cacher.lookup('table', None)
        
        self.assertIsNone(result1)
        self.assertIsNone(result2)

    def test_lookup_success(self):
        """Test successful lookup from Redis."""
        mock_client = Mock()
        mock_client.get.return_value = b'test_value'
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup('test_table', 'test_key')
        
        self.assertEqual(result, "b'test_value'")
        mock_client.get.assert_called_once_with('test_table:test_key')

    def test_lookup_key_not_found(self):
        """Test lookup when key not found in Redis."""
        mock_client = Mock()
        mock_client.get.return_value = None
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup('test_table', 'nonexistent_key')
        
        self.assertIsNone(result)

    def test_lookup_redis_error(self):
        """Test lookup handles RedisError gracefully."""
        mock_client = Mock()
        mock_client.get.side_effect = redis.RedisError("Connection error")
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup('test_table', 'test_key')
        
        self.assertIsNone(result)

    def test_lookup_unexpected_exception(self):
        """Test lookup handles unexpected exceptions gracefully."""
        mock_client = Mock()
        mock_client.get.side_effect = Exception("Unexpected error")
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup('test_table', 'test_key')
        
        self.assertIsNone(result)

    def test_lookup_builds_correct_redis_key(self):
        """Test that lookup builds Redis key correctly."""
        mock_client = Mock()
        mock_client.get.return_value = b'value'
        self.cacher.cache.client = mock_client
        
        self.cacher.lookup('meta_device', 'device123')
        
        mock_client.get.assert_called_once_with('meta_device:device123')

    def test_lookup_returns_string(self):
        """Test that lookup returns string type."""
        mock_client = Mock()
        mock_client.get.return_value = b'bytes_value'
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup('test_table', 'test_key')
        
        self.assertIsInstance(result, str)

    def test_lookup_list_empty_keys(self):
        """Test lookup_list with empty key list."""
        result = self.cacher.lookup_list('test_table', [])
        
        self.assertEqual(result, [])

    def test_lookup_list_success(self):
        """Test successful lookup_list."""
        mock_client = Mock()
        mock_client.get.side_effect = [b'value1', b'value2', b'value3']
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup_list('test_table', ['key1', 'key2', 'key3'])
        
        self.assertEqual(len(result), 3)
        self.assertEqual(mock_client.get.call_count, 3)

    def test_lookup_list_some_keys_not_found(self):
        """Test lookup_list when some keys not found."""
        mock_client = Mock()
        mock_client.get.side_effect = [b'value1', None, b'value3']
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup_list('test_table', ['key1', 'key2', 'key3'])
        
        # Should only return found values
        self.assertEqual(len(result), 2)
        self.assertIn("b'value1'", result)
        self.assertIn("b'value3'", result)

    def test_lookup_list_all_keys_not_found(self):
        """Test lookup_list when all keys not found."""
        mock_client = Mock()
        mock_client.get.return_value = None
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup_list('test_table', ['key1', 'key2', 'key3'])
        
        self.assertEqual(result, [])

    def test_lookup_list_with_redis_errors(self):
        """Test lookup_list handles Redis errors gracefully."""
        mock_client = Mock()
        mock_client.get.side_effect = [
            b'value1',
            redis.RedisError("Connection error"),
            b'value3'
        ]
        self.cacher.cache.client = mock_client
        
        result = self.cacher.lookup_list('test_table', ['key1', 'key2', 'key3'])
        
        # Should continue despite error and return found values
        self.assertEqual(len(result), 2)

    def test_lookup_list_no_client(self):
        """Test lookup_list when Redis client not available."""
        self.cacher.cache.client = None
        
        result = self.cacher.lookup_list('test_table', ['key1', 'key2'])
        
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
