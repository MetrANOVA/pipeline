import unittest
from metranova.connectors.base import BaseConnector, NoOpConnector


class TestBaseConnector(unittest.TestCase):
    """Test the BaseConnector abstract class."""
    
    def test_base_connector_initialization(self):
        """Test BaseConnector can be instantiated."""
        connector = BaseConnector()
        self.assertIsInstance(connector, BaseConnector)
    
    def test_connect_not_implemented(self):
        """Test that connect() raises NotImplementedError."""
        connector = BaseConnector()
        with self.assertRaises(NotImplementedError) as context:
            connector.connect()
        self.assertIn("Subclasses should implement this method", str(context.exception))
    
    def test_close_not_implemented(self):
        """Test that close() raises NotImplementedError."""
        connector = BaseConnector()
        with self.assertRaises(NotImplementedError) as context:
            connector.close()
        self.assertIn("Subclasses should implement this method", str(context.exception))


class TestNoOpConnector(unittest.TestCase):
    """Test the NoOpConnector class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.connector = NoOpConnector()
    
    def test_initialization(self):
        """Test NoOpConnector initialization."""
        self.assertIsInstance(self.connector, NoOpConnector)
        self.assertIsInstance(self.connector, BaseConnector)
        self.assertIsNone(self.connector.client)
    
    def test_connect(self):
        """Test connect() sets client to None."""
        self.connector.connect()
        self.assertIsNone(self.connector.client)
    
    def test_close(self):
        """Test close() does nothing (no errors)."""
        self.connector.close()  # Should not raise any exceptions


if __name__ == '__main__':
    unittest.main()
