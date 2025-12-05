#!/usr/bin/env python3

import unittest
import time
from unittest.mock import patch, MagicMock, Mock
import threading

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from metranova.cachers.base import BaseCacher, NoOpCacher


class TestBaseCacher(unittest.TestCase):
    """Unit tests for BaseCacher class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.cacher = BaseCacher()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self.cacher, 'close'):
            self.cacher.close()

    def test_init_default_values(self):
        """Test that BaseCacher initializes with correct default values."""
        self.assertIsNotNone(self.cacher.logger)
        self.assertIsNone(self.cacher.cache)
        self.assertEqual(self.cacher.cache_refresh_interval, 0)
        self.assertIsNone(self.cacher.refresh_thread)

    def test_prime_method_exists(self):
        """Test that prime method exists and can be called."""
        # Should not raise an error
        result = self.cacher.prime()
        self.assertIsNone(result)

    def test_start_refresh_thread_no_interval(self):
        """Test start_refresh_thread when interval is 0 (no refresh)."""
        self.cacher.cache_refresh_interval = 0
        self.cacher.start_refresh_thread()
        
        # Should not start a thread
        self.assertIsNone(self.cacher.refresh_thread)

    def test_start_refresh_thread_with_interval(self):
        """Test start_refresh_thread when interval is set."""
        self.cacher.cache_refresh_interval = 1
        
        with patch.object(self.cacher, 'prime') as mock_prime:
            self.cacher.start_refresh_thread()
            
            # Should have called prime initially
            mock_prime.assert_called_once()
            
            # Should have started a thread
            self.assertIsNotNone(self.cacher.refresh_thread)
            self.assertTrue(self.cacher.refresh_thread.daemon)
            self.assertIn('CacheRefresh', self.cacher.refresh_thread.name)
            
            # Clean up thread
            self.cacher.close()

    def test_lookup_not_implemented(self):
        """Test that lookup raises NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.cacher.lookup('table', 'key')

    def test_lookup_list_not_implemented(self):
        """Test that lookup_list raises NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.cacher.lookup_list('table', ['key1', 'key2'])

    def test_close_no_thread(self):
        """Test close method when no refresh thread exists."""
        self.cacher.refresh_thread = None
        self.cacher.cache = None
        
        # Should not raise an error
        self.cacher.close()

    def test_close_with_thread(self):
        """Test close method with active refresh thread."""
        # Create a mock thread
        mock_thread = Mock()
        mock_thread.is_alive.return_value = False
        self.cacher.refresh_thread = mock_thread
        
        self.cacher.close()
        
        # Should have called join on thread
        mock_thread.join.assert_called_once_with(timeout=2.0)
        self.assertIsNone(self.cacher.refresh_thread)

    def test_close_with_cache(self):
        """Test close method with cache connection."""
        mock_cache = Mock()
        self.cacher.cache = mock_cache
        
        self.cacher.close()
        
        # Should have called close on cache
        mock_cache.close.assert_called_once()

    def test_refresh_method_calls_prime(self):
        """Test that refresh method calls prime periodically."""
        self.cacher.cache_refresh_interval = 0.1
        
        with patch.object(self.cacher, 'prime') as mock_prime:
            # Start refresh in a thread that we can stop
            refresh_thread = threading.Thread(target=self.cacher.refresh, daemon=True)
            refresh_thread.start()
            
            # Wait a bit longer than interval
            time.sleep(0.3)
            
            # Should have called prime at least once (after initial sleep)
            self.assertGreater(mock_prime.call_count, 0)
            
            # Thread cleanup happens automatically since it's daemon


class TestNoOpCacher(unittest.TestCase):
    """Unit tests for NoOpCacher class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.cacher = NoOpCacher()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self.cacher, 'close'):
            self.cacher.close()

    def test_inheritance(self):
        """Test that NoOpCacher inherits from BaseCacher."""
        self.assertIsInstance(self.cacher, BaseCacher)

    def test_prime_does_nothing(self):
        """Test that prime method does nothing."""
        result = self.cacher.prime()
        self.assertIsNone(result)

    def test_lookup_returns_none(self):
        """Test that lookup always returns None."""
        result = self.cacher.lookup('any_table', 'any_key')
        self.assertIsNone(result)

    def test_lookup_list_returns_empty_list(self):
        """Test that lookup_list always returns empty list."""
        result = self.cacher.lookup_list('any_table', ['key1', 'key2'])
        self.assertEqual(result, [])

    def test_lookup_with_none_values(self):
        """Test lookup with None values."""
        result = self.cacher.lookup(None, None)
        self.assertIsNone(result)

    def test_lookup_list_with_empty_list(self):
        """Test lookup_list with empty key list."""
        result = self.cacher.lookup_list('table', [])
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
