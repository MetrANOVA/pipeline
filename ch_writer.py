#!/usr/bin/env python3
"""
ClickHouse Writer Service with Kafka SSL Consumer
"""

import os
import json
import logging
import time
import threading
from typing import Dict, Any, Optional, List
from confluent_kafka import Consumer, KafkaError
import clickhouse_connect
import ssl

# Configure logging
log_level = logging.INFO
if os.getenv('DEBUG', 'false').lower() == 'true' or os.getenv('DEBUG') == '1':
    log_level = logging.DEBUG
    
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BaseOutput:
    def _process_message(self, msg):
        """Process individual Kafka message"""
        raise NotImplementedError("Subclasses should implement this method")
    
class JSONOutput(BaseOutput):
    def _process_message(self, msg):
        """Process individual Kafka message"""
        logger.info(f"Received message from topic {msg.topic()}, partition {msg.partition()}, offset {msg.offset()}")
        
        # Decode message
        try:
            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
            
            logger.debug(f"Message key: {key}")
            logger.debug(f"Message value: {value}")
            
            # Prepare metadata for output method
            msg_metadata = {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': key,
                'timestamp': msg.timestamp()[1] / 1000 if msg.timestamp()[1] > 0 else time.time()  # Convert to seconds
            }
            
            self._output_message(value, msg_metadata)     

            # For now, just log the message      
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message content: {e}")
    
    def _output_message(self, value: Optional[Dict[str, Any]], msg_metadata: Optional[Dict] = None):
        """Output message to console as formatted JSON"""
        if value:
            formatted_value = json.dumps(value, indent=2, sort_keys=True)
            logger.info(f"Processing message with data:\n{formatted_value}")

class ClickHouseOutput(JSONOutput):
    def __init__(self):
        # ClickHouse configuration from environment
        self.host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        self.username = os.getenv('CLICKHOUSE_USERNAME', 'default')
        self.password = os.getenv('CLICKHOUSE_PASSWORD', '')
        self.table = os.getenv('CLICKHOUSE_TABLE', 'kafka_messages')
        
        # Batching configuration
        self.batch_size = int(os.getenv('CLICKHOUSE_BATCH_SIZE', '1000'))
        self.batch_timeout = float(os.getenv('CLICKHOUSE_BATCH_TIMEOUT', '30.0'))  # seconds
        
        # Initialize ClickHouse connection
        self.client = None
        self.batch: List[Dict[str, Any]] = []
        self.batch_lock = threading.Lock()
        self.last_flush_time = time.time()
        
        self._connect_clickhouse()
        self._start_flush_timer()
    
    def _connect_clickhouse(self):
        """Initialize ClickHouse connection"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                secure=os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true',
                verify=False
            )
            
            # Test connection
            self.client.ping()
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}, database: {self.database}")
            
            # Create table if it doesn't exist
            self._create_table_if_not_exists()
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the target table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            timestamp DateTime64(3) DEFAULT now64(),
            topic String,
            partition UInt32,
            offset UInt64,
            key Nullable(String),
            value String,
            message_timestamp DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, topic, partition)
        """
        
        try:
            self.client.command(create_table_sql)
            logger.info(f"Table {self.table} is ready")
        except Exception as e:
            logger.error(f"Failed to create table {self.table}: {e}")
            raise
    
    def _start_flush_timer(self):
        """Start background timer to flush batches periodically"""
        def flush_timer():
            while True:
                time.sleep(1)  # Check every second
                current_time = time.time()
                
                with self.batch_lock:
                    if (self.batch and 
                        current_time - self.last_flush_time >= self.batch_timeout):
                        self._flush_batch()
        
        timer_thread = threading.Thread(target=flush_timer, daemon=True)
        timer_thread.start()
        logger.info(f"Started batch flush timer (timeout: {self.batch_timeout}s)")
    
    def _output_message(self, value: Optional[Dict[str, Any]], msg_metadata: Optional[Dict] = None):
        """Add message to batch and flush if needed"""
        if not value:
            return
        
        # Prepare message data for ClickHouse
        message_data = {
            'topic': msg_metadata.get('topic', '') if msg_metadata else '',
            'partition': msg_metadata.get('partition', 0) if msg_metadata else 0,
            'offset': msg_metadata.get('offset', 0) if msg_metadata else 0,
            'key': msg_metadata.get('key') if msg_metadata else None,
            'value': json.dumps(value, sort_keys=True),
            'message_timestamp': msg_metadata.get('timestamp') if msg_metadata else time.time()
        }
        
        with self.batch_lock:
            self.batch.append(message_data)
            
            # Check if we need to flush
            if len(self.batch) >= self.batch_size:
                self._flush_batch()
    
    def _flush_batch(self):
        """Flush current batch to ClickHouse"""
        if not self.batch:
            return
        
        try:
            # Prepare data for insertion
            data_to_insert = []
            for msg in self.batch:
                data_to_insert.append([
                    msg['topic'],
                    msg['partition'], 
                    msg['offset'],
                    msg['key'],
                    msg['value'],
                    msg['message_timestamp']
                ])
            
            # Insert batch into ClickHouse
            self.client.insert(
                table=self.table,
                data=data_to_insert,
                column_names=['topic', 'partition', 'offset', 'key', 'value', 'message_timestamp']
            )
            
            logger.info(f"Successfully inserted {len(self.batch)} messages into ClickHouse")
            
            # Clear batch and update flush time
            self.batch.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            logger.error(f"Failed to insert batch into ClickHouse: {e}")
            # Keep messages in batch for retry (in production, you might want to implement a DLQ)
    
    def close(self):
        """Close ClickHouse connection and flush remaining messages"""
        with self.batch_lock:
            if self.batch:
                logger.info("Flushing remaining messages before closing...")
                self._flush_batch()
        
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")

class KafkaSSLConsumer:
    """Kafka consumer with SSL authentication using confluent-kafka"""
    
    def __init__(self, output: BaseOutput):
        self.consumer: Optional[Consumer] = None
        self.output = output
        self._setup_consumer()

    def _setup_consumer(self):
        """Initialize Kafka consumer with SSL configuration"""
        try:
            # Get Kafka configuration from environment variables
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            topic = os.getenv('KAFKA_TOPIC', 'metranova_flow')
            group_id = os.getenv('KAFKA_CONSUMER_GROUP', 'ch-writer-group')
            
            # SSL configuration
            ssl_ca_location = os.getenv('KAFKA_SSL_CA_LOCATION', '/app/certificates/ca-cert')
            ssl_certificate_location = os.getenv('KAFKA_SSL_CERTIFICATE_LOCATION', '/app/certificates/client-cert')
            ssl_key_location = os.getenv('KAFKA_SSL_KEY_LOCATION', '/app/certificates/client-key')
            ssl_key_password = os.getenv('KAFKA_SSL_KEY_PASSWORD')
            
            # Base consumer configuration
            consumer_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
                'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest'),
                'enable.auto.commit': os.getenv('KAFKA_ENABLE_AUTO_COMMIT', 'true').lower() == 'true',
                'auto.commit.interval.ms': int(os.getenv('KAFKA_AUTO_COMMIT_INTERVAL_MS', '5000')),
                'session.timeout.ms': int(os.getenv('KAFKA_SESSION_TIMEOUT_MS', '30000')),
                'heartbeat.interval.ms': int(os.getenv('KAFKA_HEARTBEAT_INTERVAL_MS', '10000')),
                'max.poll.interval.ms': int(os.getenv('KAFKA_MAX_POLL_INTERVAL_MS', '300000')),
                'fetch.min.bytes': int(os.getenv('KAFKA_FETCH_MIN_BYTES', '1'))
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
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached {msg.topic()}/{msg.partition()}")
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        break
                
                # Process the message
                try:
                    self.output._process_message(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Continue processing other messages
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping consumer...")
        finally:
            self._close_consumer()

    def _close_consumer(self):
        """Close Kafka consumer"""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()


def main():
    """Main entry point"""
    logger.info("Starting ClickHouse Writer Service")
    
    output = None
    kafka_consumer = None
    
    try:
        #determine output method
        output_method = os.getenv('OUTPUT_METHOD', 'json').lower()
        if output_method == 'clickhouse':
            output = ClickHouseOutput()
        else:
            output = JSONOutput()

        # Initialize Kafka consumer
        kafka_consumer = KafkaSSLConsumer(output=output)

        # Start consuming messages
        kafka_consumer.consume_messages()
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # Clean shutdown
        if hasattr(output, 'close'):
            output.close()
        if kafka_consumer:
            kafka_consumer._close_consumer()


if __name__ == "__main__":
    main()