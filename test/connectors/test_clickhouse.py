import unittest
import os
from unittest.mock import Mock, patch, MagicMock
from metranova.connectors.clickhouse import ClickHouseConnector


class TestClickHouseConnector(unittest.TestCase):
    """Test the ClickHouseConnector class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock environment variables
        self.env_vars = {
            'CLICKHOUSE_HOST': 'test-host',
            'CLICKHOUSE_PORT': '8124',
            'CLICKHOUSE_DATABASE': 'test_db',
            'CLICKHOUSE_USERNAME': 'test_user',
            'CLICKHOUSE_PASSWORD': 'test_pass',
            'CLICKHOUSE_CLUSTER_NAME': 'test_cluster',
            'CLICKHOUSE_SKIP_DB_CREATE': 'true',
            'CLICKHOUSE_SECURE': 'false',
        }
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_HOST': 'test-host',
        'CLICKHOUSE_PORT': '8124',
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_USERNAME': 'test_user',
        'CLICKHOUSE_PASSWORD': 'test_pass',
        'CLICKHOUSE_SKIP_DB_CREATE': 'true',
    })
    def test_initialization(self, mock_clickhouse):
        """Test ClickHouseConnector initialization with environment variables."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        connector = ClickHouseConnector()
        
        self.assertEqual(connector.host, 'test-host')
        self.assertEqual(connector.port, 8124)
        self.assertEqual(connector.database, 'test_db')
        self.assertEqual(connector.username, 'test_user')
        self.assertEqual(connector.password, 'test_pass')
        self.assertIsNone(connector.cluster_name)
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {'CLICKHOUSE_SKIP_DB_CREATE': 'true'})
    def test_initialization_defaults(self, mock_clickhouse):
        """Test ClickHouseConnector initialization with default values."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        connector = ClickHouseConnector()
        
        self.assertEqual(connector.host, 'localhost')
        self.assertEqual(connector.port, 8123)
        self.assertEqual(connector.database, 'default')
        self.assertEqual(connector.username, 'default')
        self.assertEqual(connector.password, '')
        self.assertIsNone(connector.cluster_name)
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_HOST': 'test-host',
        'CLICKHOUSE_PORT': '8124',
        'CLICKHOUSE_SKIP_DB_CREATE': 'true',
    })
    def test_connect(self, mock_clickhouse):
        """Test connect() method."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        connector = ClickHouseConnector()
        
        mock_clickhouse.get_client.assert_called_with(
            host='test-host',
            port=8124,
            username='default',
            password='',
            database='default',
            secure=False,
            verify=False
        )
        mock_client.ping.assert_called_once()
        self.assertEqual(connector.client, mock_client)
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_SECURE': 'true',
        'CLICKHOUSE_SKIP_DB_CREATE': 'true',
    })
    def test_connect_secure(self, mock_clickhouse):
        """Test connect() with secure connection."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        connector = ClickHouseConnector()
        
        # Check that secure=True was passed
        call_kwargs = mock_clickhouse.get_client.call_args[1]
        self.assertTrue(call_kwargs['secure'])
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {'CLICKHOUSE_SKIP_DB_CREATE': 'true'})
    def test_connect_failure(self, mock_clickhouse):
        """Test connect() raises exception on failure."""
        mock_clickhouse.get_client.side_effect = Exception("Connection failed")
        
        with self.assertRaises(Exception) as context:
            ClickHouseConnector()
        self.assertIn("Connection failed", str(context.exception))
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_SKIP_DB_CREATE': 'false',
    })
    def test_create_database(self, mock_clickhouse):
        """Test create_database() method."""
        mock_db_client = Mock()
        mock_main_client = Mock()
        mock_clickhouse.get_client.side_effect = [mock_db_client, mock_main_client]
        
        connector = ClickHouseConnector()
        
        # First call is for database creation
        mock_db_client.command.assert_called_once_with(
            "CREATE DATABASE IF NOT EXISTS test_db"
        )
        mock_db_client.close.assert_called_once()
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_CLUSTER_NAME': 'test_cluster',
        'CLICKHOUSE_SKIP_DB_CREATE': 'false',
    })
    def test_create_database_with_cluster(self, mock_clickhouse):
        """Test create_database() with cluster name."""
        mock_db_client = Mock()
        mock_main_client = Mock()
        mock_clickhouse.get_client.side_effect = [mock_db_client, mock_main_client]
        
        connector = ClickHouseConnector()
        
        mock_db_client.command.assert_called_once_with(
            "CREATE DATABASE IF NOT EXISTS test_db ON CLUSTER 'test_cluster'"
        )
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_SKIP_DB_CREATE': 'false',
    }, clear=True)
    @patch('metranova.connectors.clickhouse.logger')
    def test_create_database_no_name(self, mock_logger, mock_clickhouse):
        """Test create_database() with empty environment (defaults to 'default' database)."""
        mock_db_client = Mock()
        mock_main_client = Mock()
        mock_clickhouse.get_client.side_effect = [mock_db_client, mock_main_client]
        
        connector = ClickHouseConnector()
        
        # Should create 'default' database since that's the default value
        # Two get_client calls: one for database creation, one for main connection
        self.assertEqual(mock_clickhouse.get_client.call_count, 2)
        mock_db_client.command.assert_called_once_with(
            "CREATE DATABASE IF NOT EXISTS default"
        )
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_SKIP_DB_CREATE': 'false',
    })
    def test_create_database_failure(self, mock_clickhouse):
        """Test create_database() raises exception on failure."""
        mock_db_client = Mock()
        mock_db_client.command.side_effect = Exception("DB creation failed")
        mock_clickhouse.get_client.side_effect = [mock_db_client]
        
        with self.assertRaises(Exception) as context:
            ClickHouseConnector()
        self.assertIn("DB creation failed", str(context.exception))
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {'CLICKHOUSE_SKIP_DB_CREATE': 'true'})
    def test_close(self, mock_clickhouse):
        """Test close() method."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        connector = ClickHouseConnector()
        connector.close()
        
        mock_client.close.assert_called_once()
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {'CLICKHOUSE_SKIP_DB_CREATE': 'true'})
    def test_close_no_client(self, mock_clickhouse):
        """Test close() with no client."""
        mock_clickhouse.get_client.return_value = Mock()
        
        connector = ClickHouseConnector()
        connector.client = None
        connector.close()  # Should not raise exception
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_SKIP_DB_CREATE': '1',
    })
    def test_skip_db_create_variations(self, mock_clickhouse):
        """Test various values for CLICKHOUSE_SKIP_DB_CREATE."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        # Test with '1' (already set in decorator)
        connector = ClickHouseConnector()
        # Should only call get_client once (for main connection)
        self.assertEqual(mock_clickhouse.get_client.call_count, 1)
    
    @patch('metranova.connectors.clickhouse.clickhouse_connect')
    @patch.dict(os.environ, {
        'CLICKHOUSE_SKIP_DB_CREATE': 'yes',
    })
    def test_skip_db_create_yes(self, mock_clickhouse):
        """Test CLICKHOUSE_SKIP_DB_CREATE with 'yes'."""
        mock_client = Mock()
        mock_clickhouse.get_client.return_value = mock_client
        
        connector = ClickHouseConnector()
        # Should only call get_client once (for main connection)
        self.assertEqual(mock_clickhouse.get_client.call_count, 1)


if __name__ == '__main__':
    unittest.main()
