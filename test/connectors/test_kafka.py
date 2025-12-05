import unittest
import os
from unittest.mock import Mock, patch, MagicMock, call
from confluent_kafka import KafkaError
from metranova.connectors.kafka import KafkaConnector


class TestKafkaConnector(unittest.TestCase):
    """Test the KafkaConnector class."""
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {})
    def test_initialization_defaults(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector initialization with default values."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        # Check consumer was created
        self.assertEqual(connector.client, mock_consumer_instance)
        
        # Verify consumer config
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['bootstrap.servers'], 'localhost:9092')
        self.assertEqual(call_args['group.id'], 'ch-writer-group')
        self.assertIn('ch-writer-', call_args['client.id'])
        self.assertEqual(call_args['auto.offset.reset'], 'latest')
        self.assertTrue(call_args['enable.auto.commit'])
        self.assertEqual(call_args['security.protocol'], 'PLAINTEXT')
        
        # Verify subscribe was called
        mock_consumer_instance.subscribe.assert_called_once_with(['metranova_flow'])
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {
        'KAFKA_BOOTSTRAP_SERVERS': 'kafka1:9092,kafka2:9092',
        'KAFKA_TOPIC': 'custom_topic',
        'KAFKA_CONSUMER_GROUP': 'custom-group',
        'KAFKA_AUTO_OFFSET_RESET': 'earliest',
        'KAFKA_ENABLE_AUTO_COMMIT': 'false',
    })
    def test_initialization_custom_values(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector initialization with custom values."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['bootstrap.servers'], 'kafka1:9092,kafka2:9092')
        self.assertEqual(call_args['group.id'], 'custom-group')
        self.assertEqual(call_args['auto.offset.reset'], 'earliest')
        self.assertFalse(call_args['enable.auto.commit'])
        
        mock_consumer_instance.subscribe.assert_called_once_with(['custom_topic'])
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {
        'KAFKA_AUTO_COMMIT_INTERVAL_MS': '10000',
        'KAFKA_SESSION_TIMEOUT_MS': '60000',
        'KAFKA_HEARTBEAT_INTERVAL_MS': '20000',
        'KAFKA_MAX_POLL_INTERVAL_MS': '600000',
        'KAFKA_FETCH_MIN_BYTES': '100',
        'KAFKA_FETCH_MAX_BYTES': '104857600',
    })
    def test_initialization_timing_config(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector initialization with timing configuration."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['auto.commit.interval.ms'], 10000)
        self.assertEqual(call_args['session.timeout.ms'], 60000)
        self.assertEqual(call_args['heartbeat.interval.ms'], 20000)
        self.assertEqual(call_args['max.poll.interval.ms'], 600000)
        self.assertEqual(call_args['fetch.min.bytes'], 100)
        self.assertEqual(call_args['fetch.max.bytes'], 104857600)
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {
        'KAFKA_SSL_CA_LOCATION': '/app/conf/certificates/ca-cert',
        'KAFKA_SSL_CERTIFICATE_LOCATION': '/app/conf/certificates/client-cert',
        'KAFKA_SSL_KEY_LOCATION': '/app/conf/certificates/client-key',
    })
    def test_initialization_ssl_config(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector initialization with SSL configuration."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = True  # SSL cert exists
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['security.protocol'], 'SSL')
        self.assertEqual(call_args['ssl.ca.location'], '/app/conf/certificates/ca-cert')
        self.assertEqual(call_args['ssl.certificate.location'], '/app/conf/certificates/client-cert')
        self.assertEqual(call_args['ssl.key.location'], '/app/conf/certificates/client-key')
        self.assertEqual(call_args['ssl.endpoint.identification.algorithm'], 'https')
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {
        'KAFKA_SSL_CA_LOCATION': '/app/conf/certificates/ca-cert',
        'KAFKA_SSL_CERTIFICATE_LOCATION': '/app/conf/certificates/client-cert',
        'KAFKA_SSL_KEY_LOCATION': '/app/conf/certificates/client-key',
        'KAFKA_SSL_KEY_PASSWORD': 'secret123',
    })
    def test_initialization_ssl_with_password(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector initialization with SSL key password."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = True
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['ssl.key.password'], 'secret123')
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {
        'KAFKA_SSL_CA_LOCATION': '/app/conf/certificates/ca-cert',
        'KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM': 'none',
    })
    def test_initialization_ssl_endpoint_algorithm(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector with custom SSL endpoint identification algorithm."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = True
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['ssl.endpoint.identification.algorithm'], 'none')
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {})
    def test_client_id_uniqueness(self, mock_urandom, mock_exists, mock_consumer):
        """Test that client ID is unique based on urandom."""
        mock_urandom.return_value = b'\xaa\xbb\xcc\xdd'
        mock_exists.return_value = False
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['client.id'], 'ch-writer-aabbccdd')
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {
        'KAFKA_SSL_CA_LOCATION': '/app/conf/certificates/ca-cert',
    })
    @patch('metranova.connectors.kafka.logger')
    def test_initialization_ssl_cert_not_found(self, mock_logger, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector when SSL cert doesn't exist."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False  # SSL cert doesn't exist
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        call_args = mock_consumer.call_args[0][0]
        self.assertEqual(call_args['security.protocol'], 'PLAINTEXT')
        # Should not have SSL config keys
        self.assertNotIn('ssl.ca.location', call_args)
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {})
    def test_initialization_failure(self, mock_urandom, mock_exists, mock_consumer):
        """Test KafkaConnector initialization failure."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False
        mock_consumer.side_effect = Exception("Kafka connection failed")
        
        with self.assertRaises(Exception) as context:
            KafkaConnector()
        self.assertIn("Kafka connection failed", str(context.exception))
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {})
    def test_kafka_connector_has_no_close_method(self, mock_urandom, mock_exists, mock_consumer):
        """Test that KafkaConnector doesn't implement close() method."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        # KafkaConnector doesn't override close(), so it should raise NotImplementedError
        with self.assertRaises(NotImplementedError):
            connector.close()
    
    @patch('metranova.connectors.kafka.Consumer')
    @patch('metranova.connectors.kafka.os.path.exists')
    @patch('metranova.connectors.kafka.os.urandom')
    @patch.dict(os.environ, {})
    def test_kafka_connector_has_no_connect_method(self, mock_urandom, mock_exists, mock_consumer):
        """Test that KafkaConnector doesn't implement connect() method."""
        mock_urandom.return_value = b'\x01\x02\x03\x04'
        mock_exists.return_value = False
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        connector = KafkaConnector()
        
        # KafkaConnector doesn't override connect(), so it should raise NotImplementedError
        with self.assertRaises(NotImplementedError):
            connector.connect()


if __name__ == '__main__':
    unittest.main()
