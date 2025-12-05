import unittest
import os
from unittest.mock import Mock, patch, MagicMock
from metranova.connectors.http import HTTPConnector


class TestHTTPConnector(unittest.TestCase):
    """Test the HTTPConnector class."""
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {})
    def test_initialization_defaults(self, mock_requests):
        """Test HTTPConnector initialization with default values."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(connector.timeout, 30)
        self.assertFalse(connector.ssl_verify)
        self.assertEqual(connector.headers, {})
        self.assertEqual(connector.client, mock_session)
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_TIMEOUT': '60',
        'HTTP_SSL_VERIFY': 'true',
    })
    def test_initialization_custom_values(self, mock_requests):
        """Test HTTPConnector initialization with custom values."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(connector.timeout, 60)
        self.assertTrue(connector.ssl_verify)
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_SSL_VERIFY': '1',
    })
    def test_ssl_verify_true_with_one(self, mock_requests):
        """Test SSL verify with '1'."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertTrue(connector.ssl_verify)
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_SSL_VERIFY': 'yes',
    })
    def test_ssl_verify_true_with_yes(self, mock_requests):
        """Test SSL verify with 'yes'."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertTrue(connector.ssl_verify)
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_SSL_VERIFY': 'false',
    })
    def test_ssl_verify_false(self, mock_requests):
        """Test SSL verify with 'false'."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertFalse(connector.ssl_verify)
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_HEADERS': 'Authorization: Bearer token123, Content-Type: application/json',
    })
    def test_headers_parsing(self, mock_requests):
        """Test HTTP headers parsing from environment."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(connector.headers, {
            'Authorization': 'Bearer token123',
            'Content-Type': 'application/json'
        })
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_HEADERS': 'X-Custom-Header: value with spaces',
    })
    def test_headers_with_spaces(self, mock_requests):
        """Test headers with spaces in values."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(connector.headers, {
            'X-Custom-Header': 'value with spaces'
        })
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {})
    def test_connect(self, mock_requests):
        """Test connect() method."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        # Verify session was created and configured
        mock_requests.Session.assert_called_once()
        self.assertEqual(mock_session.verify, False)
        self.assertEqual(mock_session.timeout, 30)
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_TIMEOUT': '45',
        'HTTP_SSL_VERIFY': 'true',
        'HTTP_HEADERS': 'User-Agent: TestAgent',
    })
    def test_connect_with_custom_config(self, mock_requests):
        """Test connect() with custom configuration."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(mock_session.verify, True)
        self.assertEqual(mock_session.timeout, 45)
        mock_session.headers.update.assert_called_once_with({'User-Agent': 'TestAgent'})
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {})
    def test_connect_no_headers(self, mock_requests):
        """Test connect() without custom headers."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        # headers.update should not be called if no headers
        mock_session.headers.update.assert_not_called()
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {})
    def test_close(self, mock_requests):
        """Test close() method."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        connector.close()
        
        mock_session.close.assert_called_once()
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {})
    def test_close_no_client(self, mock_requests):
        """Test close() with no client."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        connector.client = None
        connector.close()  # Should not raise exception
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_HEADERS': 'Key:Value',
    })
    def test_headers_no_spaces(self, mock_requests):
        """Test headers without spaces around colon."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(connector.headers, {'Key': 'Value'})
    
    @patch('metranova.connectors.http.requests')
    @patch.dict(os.environ, {
        'HTTP_TIMEOUT': '0',
    })
    def test_timeout_zero(self, mock_requests):
        """Test timeout with zero value."""
        mock_session = Mock()
        mock_requests.Session.return_value = mock_session
        
        connector = HTTPConnector()
        
        self.assertEqual(connector.timeout, 0)


if __name__ == '__main__':
    unittest.main()
