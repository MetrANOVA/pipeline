"""
Base consumer classes for different message sources
"""

import logging
import orjson
import os
from metranova.pipelines.base import BasePipeline
from confluent_kafka import Consumer, KafkaError

# Configure logging
log_level = logging.INFO
if os.getenv('DEBUG', 'false').lower() == 'true' or os.getenv('DEBUG') == '1':
    log_level = logging.DEBUG
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseConsumer:
    
    def __init__(self, pipeline: BasePipeline):
        # Initialize pipeline
        self.pipeline = pipeline
    
    def consume_messages(self):
        """Consume messages from source"""
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        """Close operation if needed"""
        return
    
class KafkaConsumer(BaseConsumer):
    """Kafka consumer with SSL authentication using confluent-kafka"""
    
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.consumer = None
        """Initialize Kafka consumer with SSL configuration"""
        try:
            # Get Kafka configuration from environment variables
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            topic = os.getenv('KAFKA_TOPIC', 'metranova_flow')
            group_id = os.getenv('KAFKA_CONSUMER_GROUP', 'ch-writer-group')
            #generate a client id based on random uuid
            client_id = f"ch-writer-{os.urandom(4).hex()}"

            # SSL configuration
            ssl_ca_location = os.getenv('KAFKA_SSL_CA_LOCATION', '/app/certificates/ca-cert')
            ssl_certificate_location = os.getenv('KAFKA_SSL_CERTIFICATE_LOCATION', '/app/certificates/client-cert')
            ssl_key_location = os.getenv('KAFKA_SSL_KEY_LOCATION', '/app/certificates/client-key')
            ssl_key_password = os.getenv('KAFKA_SSL_KEY_PASSWORD')
            
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
            
            # Add SSL configuration if certificates are provided
            if ssl_ca_location and os.path.exists(ssl_ca_location):
                logger.info("Configuring Kafka SSL authentication")
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
                logger.warning("SSL certificates not found, using PLAINTEXT protocol")
                consumer_config['security.protocol'] = 'PLAINTEXT'
            
            # Create consumer
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe([topic])
            
            logger.info(f"Kafka consumer initialized for topic: {topic}")
            logger.info(f"Bootstrap servers: {bootstrap_servers}")
            logger.info(f"Group ID: {group_id}")
            logger.info(f"Client ID: {client_id}")
            
        except Exception as e:
            logger.error(f"Failed to setup Kafka consumer: {e}")
            raise
    
    def consume_messages(self):
        """Consume messages from Kafka"""
        if not self.consumer:
            logger.error("Kafka consumer not initialized")
            return

        try:
            logger.info("Starting message consumption...")
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                # check for errors
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached {msg.topic()}/{msg.partition()}")
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                
                # format the response as JSON
                msg_data = orjson.loads(msg.value()) if msg.value() else None
                msg_metadata = {
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
                    'key': msg.key().decode('utf-8') if msg.key() else None
                }

                # Process the message
                try:
                    self.pipeline.process_message(msg_data, consumer_metadata=msg_metadata)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Continue processing other messages
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping consumer...")
        finally:
            self.close()

    def close(self):
        """Close Kafka consumer"""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()