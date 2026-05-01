import logging
import os
from confluent_kafka import Consumer, KafkaError
from metranova.connectors.base import BaseConnector
    
logger = logging.getLogger(__name__)

class KafkaConnector(BaseConnector):
    def __init__(self):
        # setup logger
        self.logger = logger
        self.client = None

        """Initialize Kafka consumer with SSL or SASL/PLAIN configuration"""
        try:
            # Get Kafka configuration from environment variables
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            topic = os.getenv('KAFKA_TOPIC', 'metranova_flow')
            group_id = os.getenv('KAFKA_CONSUMER_GROUP', 'ch-writer-group')
            #generate a client id based on random uuid
            client_id = f"ch-writer-{os.urandom(4).hex()}"

            # SSL configuration
            ssl_ca_location = os.getenv('KAFKA_SSL_CA_LOCATION', '/app/conf/certificates/ca-cert')
            ssl_certificate_location = os.getenv('KAFKA_SSL_CERTIFICATE_LOCATION', '/app/conf/certificates/client-cert')
            ssl_key_location = os.getenv('KAFKA_SSL_KEY_LOCATION', '/app/conf/certificates/client-key')
            ssl_key_password = os.getenv('KAFKA_SSL_KEY_PASSWORD')

            # SASL/PLAIN configuration
            sasl_username = os.getenv('KAFKA_SASL_USERNAME')
            sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
            
            # Base consumer configuration
            consumer_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
                'client.id': client_id,
                'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest'),
                'enable.auto.commit': os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'true').lower() == 'true',
                'auto.commit.interval.ms': int(os.getenv('KAFKA_AUTO_COMMIT_INTERVAL_MS', '5000')),
                'session.timeout.ms': int(os.getenv('KAFKA_SESSION_TIMEOUT_MS', '30000')),
                'heartbeat.interval.ms': int(os.getenv('KAFKA_HEARTBEAT_INTERVAL_MS', '10000')),
                'max.poll.interval.ms': int(os.getenv('KAFKA_MAX_POLL_INTERVAL_MS', '300000')),
                'fetch.min.bytes': int(os.getenv('KAFKA_FETCH_MIN_BYTES', '1')),
                'fetch.max.bytes': int(os.getenv('KAFKA_FETCH_MAX_BYTES', '52428800')),  # 50MB - default
            }

            if (sasl_username and not sasl_password) or (sasl_password and not sasl_username):
                self.logger.warning("Both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be set to enable SASL/PLAIN")
            
            # Prefer SASL/PLAIN when credentials are provided.
            if sasl_username and sasl_password:
                self.logger.info("Configuring Kafka SASL/PLAIN authentication")
                consumer_config.update({
                    'sasl.mechanism': 'PLAIN',
                    'sasl.username': sasl_username,
                    'sasl.password': sasl_password,
                })

                # Use TLS for broker verification when CA cert is available.
                if ssl_ca_location and os.path.exists(ssl_ca_location):
                    consumer_config.update({
                        'security.protocol': 'SASL_SSL',
                        'ssl.ca.location': ssl_ca_location,
                        'ssl.endpoint.identification.algorithm': os.getenv('KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM', 'https'),
                    })
                else:
                    self.logger.warning("CA certificate not found, using SASL_PLAINTEXT protocol")
                    consumer_config['security.protocol'] = 'SASL_PLAINTEXT'

            # Add SSL client-certificate configuration when SASL/PLAIN is not set.
            elif ssl_ca_location and os.path.exists(ssl_ca_location):
                self.logger.info("Configuring Kafka SSL authentication")
                consumer_config.update({
                    'security.protocol': 'SSL',
                    'ssl.ca.location': ssl_ca_location,
                    'ssl.certificate.location': ssl_certificate_location,
                    'ssl.key.location': ssl_key_location,
                    'ssl.endpoint.identification.algorithm': os.getenv('KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM', 'https'),
                })
                
                # Add key password if provided
                if ssl_key_password:
                    consumer_config['ssl.key.password'] = ssl_key_password
                    
            else:
                self.logger.warning("SSL certificates not found, using PLAINTEXT protocol")
                consumer_config['security.protocol'] = 'PLAINTEXT'
            
            # Create consumer
            self.client = Consumer(consumer_config)
            self.client.subscribe([topic])

            self.logger.info(f"Kafka consumer initialized for topic: {topic}")
            self.logger.info(f"Bootstrap servers: {bootstrap_servers}")
            self.logger.info(f"Group ID: {group_id}")
            self.logger.info(f"Client ID: {client_id}")
        except Exception as e:
            self.logger.error(f"Failed to setup Kafka consumer: {e}")
            raise