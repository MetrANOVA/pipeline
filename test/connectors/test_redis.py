import unittest
import os
from unittest.mock import Mock, patch, MagicMock
import redis as redis_module
from metranova.connectors.redis import RedisConnector


class TestRedisConnector(unittest.TestCase):
    """Test the RedisConnector class."""
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_initialization_defaults(self, mock_redis):
        """Test RedisConnector initialization with default values."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        self.assertEqual(connector.redis_host, 'localhost')
        self.assertEqual(connector.redis_port, 6379)
        self.assertEqual(connector.redis_db, 0)
        self.assertIsNone(connector.redis_password)
        self.assertEqual(connector.client, mock_client)
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {
        'REDIS_HOST': 'redis-server',
        'REDIS_PORT': '6380',
        'REDIS_DB': '2',
        'REDIS_PASSWORD': 'secret123',
    })
    def test_initialization_custom_values(self, mock_redis):
        """Test RedisConnector initialization with custom values."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        self.assertEqual(connector.redis_host, 'redis-server')
        self.assertEqual(connector.redis_port, 6380)
        self.assertEqual(connector.redis_db, 2)
        self.assertEqual(connector.redis_password, 'secret123')
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_connect(self, mock_redis):
        """Test connect() method."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        # Verify Redis was instantiated with correct config
        mock_redis.assert_called_once()
        call_kwargs = mock_redis.call_args[1]
        self.assertEqual(call_kwargs['host'], 'localhost')
        self.assertEqual(call_kwargs['port'], 6379)
        self.assertEqual(call_kwargs['db'], 0)
        self.assertTrue(call_kwargs['decode_responses'])
        self.assertEqual(call_kwargs['socket_timeout'], 30)
        self.assertEqual(call_kwargs['socket_connect_timeout'], 10)
        self.assertTrue(call_kwargs['retry_on_timeout'])
        self.assertEqual(call_kwargs['health_check_interval'], 30)
        
        # Verify ping was called
        mock_client.ping.assert_called_once()
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {
        'REDIS_HOST': 'redis-server',
        'REDIS_PORT': '6380',
        'REDIS_DB': '3',
        'REDIS_PASSWORD': 'mypassword',
    })
    def test_connect_with_password(self, mock_redis):
        """Test connect() with password."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        call_kwargs = mock_redis.call_args[1]
        self.assertEqual(call_kwargs['host'], 'redis-server')
        self.assertEqual(call_kwargs['port'], 6380)
        self.assertEqual(call_kwargs['db'], 3)
        self.assertEqual(call_kwargs['password'], 'mypassword')
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_connect_no_password(self, mock_redis):
        """Test connect() without password."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        call_kwargs = mock_redis.call_args[1]
        # Password should not be in config if not set
        self.assertNotIn('password', call_kwargs)
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    @patch('metranova.connectors.redis.logger')
    def test_connect_failure(self, mock_logger, mock_redis):
        """Test connect() failure sets client to None."""
        mock_redis.return_value.ping.side_effect = Exception("Connection failed")
        
        connector = RedisConnector()
        
        self.assertIsNone(connector.client)
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    @patch('metranova.connectors.redis.logger')
    def test_connect_redis_exception(self, mock_logger, mock_redis):
        """Test connect() with Redis-specific exception."""
        mock_redis.return_value.ping.side_effect = redis_module.RedisError("Redis error")
        
        connector = RedisConnector()
        
        self.assertIsNone(connector.client)
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_close(self, mock_redis):
        """Test close() method."""
        mock_client = Mock()
        mock_connection_pool = Mock()
        mock_client.connection_pool = mock_connection_pool
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        connector.close()
        
        mock_connection_pool.disconnect.assert_called_once()
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_close_no_client(self, mock_redis):
        """Test close() with no client."""
        mock_redis.return_value.ping.side_effect = Exception("Connection failed")
        
        connector = RedisConnector()
        connector.close()  # Should not raise exception
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {
        'REDIS_PORT': 'invalid',
    })
    def test_initialization_invalid_port(self, mock_redis):
        """Test initialization with invalid port value."""
        with self.assertRaises(ValueError):
            RedisConnector()
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {
        'REDIS_DB': 'invalid',
    })
    def test_initialization_invalid_db(self, mock_redis):
        """Test initialization with invalid db value."""
        with self.assertRaises(ValueError):
            RedisConnector()
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {
        'REDIS_PASSWORD': '',
    })
    def test_empty_password(self, mock_redis):
        """Test with empty password string."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        # Empty string should be treated as None
        call_kwargs = mock_redis.call_args[1]
        self.assertNotIn('password', call_kwargs)
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_decode_responses_enabled(self, mock_redis):
        """Test that decode_responses is enabled."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        call_kwargs = mock_redis.call_args[1]
        self.assertTrue(call_kwargs['decode_responses'])
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_timeout_settings(self, mock_redis):
        """Test timeout configuration."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        call_kwargs = mock_redis.call_args[1]
        self.assertEqual(call_kwargs['socket_timeout'], 30)
        self.assertEqual(call_kwargs['socket_connect_timeout'], 10)
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_retry_on_timeout_enabled(self, mock_redis):
        """Test that retry_on_timeout is enabled."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        call_kwargs = mock_redis.call_args[1]
        self.assertTrue(call_kwargs['retry_on_timeout'])
    
    @patch('metranova.connectors.redis.redis.Redis')
    @patch.dict(os.environ, {})
    def test_health_check_interval(self, mock_redis):
        """Test health check interval configuration."""
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        connector = RedisConnector()
        
        call_kwargs = mock_redis.call_args[1]
        self.assertEqual(call_kwargs['health_check_interval'], 30)


if __name__ == '__main__':
    unittest.main()
