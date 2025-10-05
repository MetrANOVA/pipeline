"""
Pipeline classes for processing messages from consumers
"""

import os
from metranova.cachers.redis import RedisCacher
from metranova.connectors.clickhouse import ClickHouseConnector
from metranova.connectors.redis import RedisConnector
import orjson
import logging
import time
import threading
import importlib
from typing import Dict, Any, Optional, List, Iterator
import clickhouse_connect
import redis

# Configure logging
log_level = logging.INFO
if os.getenv('DEBUG', 'false').lower() == 'true' or os.getenv('DEBUG') == '1':
    log_level = logging.DEBUG
    
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BasePipeline:
    def process_message(self, msg):
        """Process individual Kafka message"""
        raise NotImplementedError("Subclasses should implement this method")
    
class JSONPipeline(BasePipeline):
    def process_message(self, msg):
        """Process individual Kafka message"""
        logger.debug(f"Received message from topic {msg.topic()}, partition {msg.partition()}, offset {msg.offset()}")
        
        # Decode message
        try:
            key = msg.key().decode('utf-8') if msg.key() else None
            value = orjson.loads(msg.value()) if msg.value() else None
            
            logger.debug(f"Message key: {key}")
            logger.debug(f"Message value: {value}")
            
            # Prepare metadata for output method
            msg_metadata = {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': key
            }
            
            self.output_message(value, msg_metadata)     

            # For now, just log the message      
        except Exception as e:
            logger.error(f"Error processing message content: {e}")
    
    def output_message(self, value: Optional[Dict[str, Any]], msg_metadata: Optional[Dict] = None):
        """Output message to console as formatted JSON"""
        if value:
            formatted_value = orjson.dumps(value, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS).decode('utf-8')
            logger.debug(f"Processing message with data:\n{formatted_value}")

class BaseClickHouseProcessor:
    def __init__(self, pipeline):
        # setup logger
        self.logger = logger
        
        #Set parent pipeline for metadata lookups
        self.pipeline = pipeline

        #Override values in child class
        self.create_table_cmd = None
        self.column_names = []
        self.required_fields = []

    def has_required_fields(self, value: dict) -> bool:
        """Check if the message contains all required fields"""
        # NOTE: Call this in build_message instead of match_message to allow more detailed logging
        # This means the message matched the processor, but there is a problem with the content
        for fields in self.required_fields:
            v = None
            for field in fields:
                if v is None:
                    v = value.get(field, None)
                else:
                    v = v.get(field, None)
                if v is None:
                    self.logger.error(f"Missing required field '{field}' in message value")
                    return False
        return True

    def match_message(self, value: dict) -> bool:
        """Determine if this processor should handle the given message"""
        return True  # Default to match all messages with required fields, override in subclass if needed

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        """Build message dictionary for ClickHouse insertion"""
        raise NotImplementedError("Subclasses should implement this method")
    
    def message_to_columns(self, message: dict) -> list:
        cols = []
        for col in self.column_names:
            if col not in message.keys():
                raise ValueError(f"Missing column '{col}' in message")
            self.logger.debug(f"Column '{col}': {message.get(col)}")
            cols.append(message.get(col))
        return cols
    
class ClickHousePipeline(JSONPipeline):
    def __init__(self):
        # setup logger
        self.logger = logger

        # Link processors
        self.processors = self._load_processors()
        if not self.processors:
            raise ValueError("At least one processor must be provided")

        # Initialize ClickHouse connection
        self.datastore = ClickHouseConnector()
        self.writers = []

        # Initialize Redis connection
        self.cacher = RedisCacher()

        # For each processor, build clickhouse tables
        for processor in self.processors:
            if processor.create_table_cmd is not None:
                self._create_table(processor.create_table_cmd, processor.table)
            writer = ClickHouseBatchWriter(self.datastore.client, processor)
            writer.start_flush_timer()
            self.writers.append(writer)

    def _load_processors(self) -> List[BaseClickHouseProcessor]:
        """Load and return list of ClickHouse processors"""
        processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        processors = []
        
        if processors_str:
            for processor_class in processors_str.split(','):
                processor_class = processor_class.strip()
                if not processor_class:
                    continue
                try:
                    module_name, class_name = processor_class.rsplit('.', 1)
                    module = importlib.import_module(module_name)
                    cls = getattr(module, class_name)
                    
                    if issubclass(cls, BaseClickHouseProcessor):
                        processors.append(cls(self))
                    else:
                        logger.warning(f"Class {class_name} is not a subclass of BaseClickHouseProcessor, skipping")
                        
                except (ImportError, AttributeError) as e:
                    logger.error(f"Failed to load processor class {class_name}: {e}")
        
            if processors:
                logger.info(f"Loaded {len(processors)} ClickHouse processors: {[type(p).__name__ for p in processors]}")
            else: 
                logger.warning("No valid ClickHouse processors loaded")
        else: 
            logger.warning("CLICKHOUSE_PROCESSORS environment variable is empty") 
        
        return processors

    def output_message(self, value: Optional[Dict[str, Any]], msg_metadata: Optional[Dict] = None):
        """Add message to batch and flush if needed"""
        if not value:
            return
        for writer in self.writers:
            writer.output_message(value, msg_metadata)
    
    def _create_table(self, create_table_cmd: str, table: str = None):
        """Create the target table if it doesn't exist"""
        if create_table_cmd is None:
            logger.info("No create_table_cmd defined, skipping table creation")
            return
        
        try:
            self.datastore.client.command(create_table_cmd)
            logger.info(f"Table {table} is ready")
        except Exception as e:
            logger.error(f"Failed to create table {table}: {e}")
            raise

    def close(self):
        """Close ClickHouse and Redis connections and flush remaining messages"""
        for writer in self.writers:
            writer.close()

        if self.datastore:
            self.datastore.close()
            logger.info("Datastore connection closed")

        if self.cacher:
            self.cacher.close()
            logger.info("Cache connection closed")

class ClickHouseBatchWriter:
    def __init__(self, ch_client, processor):
        # Logger
        self.logger = logger

        #Clickhouse Client and message processor
        self.client = ch_client
        self.processor = processor

        # Batching configuration
        self.batch_size = int(os.getenv('CLICKHOUSE_BATCH_SIZE', '1000'))
        self.batch_timeout = float(os.getenv('CLICKHOUSE_BATCH_TIMEOUT', '30.0'))  # seconds
        self.flush_interval = float(os.getenv('CLICKHOUSE_FLUSH_INTERVAL', '0.1'))  # seconds
       
        # Batch state
        self.batch: List[Dict[str, Any]] = []
        self.batch_lock = threading.Lock()
        self.last_flush_time = time.time()

    def start_flush_timer(self):
        """Start background timer to flush batches periodically"""
        def flush_timer():
            while True:
                # Check every flush_interval seconds. 
                # Note that max clickhouse throughput is self.flush_interval * self.batch_size
                time.sleep(self.flush_interval)  
                current_time = time.time()
                
                with self.batch_lock:
                    if (self.batch and 
                        current_time - self.last_flush_time >= self.batch_timeout):
                        self.flush_batch()
        
        timer_thread = threading.Thread(target=flush_timer, daemon=True)
        timer_thread.start()
        logger.info(f"Started batch flush timer (timeout: {self.batch_timeout}s)")

    def output_message(self, value: Optional[Dict[str, Any]], msg_metadata: Optional[Dict] = None):
        """Add message to batch and flush if needed"""
        if not value:
            return
        
        # Check if this processor should handle the message
        if not self.processor.match_message(value):
            self.logger.debug("Message did not match processor criteria, skipping")
            return

        # Prepare message data for ClickHouse
        message_data = self.processor.build_message(value, msg_metadata)
        if message_data is None:
            return
        
        #append to batch
        with self.batch_lock:
            #Note: message_data is a list of dicts so += adds all elements
            self.batch.extend(message_data)
            
            # Check if we need to flush
            if len(self.batch) >= self.batch_size:
                self.flush_batch()
    
    def flush_batch(self):
        """Flush current batch to ClickHouse"""
        if not self.batch:
            return
        
        try:
            # Prepare data for insertion
            data_to_insert = []
            for msg in self.batch:
                data_to_insert.append(self.processor.message_to_columns(msg))
            
            # Insert batch into ClickHouse
            self.client.insert(
                table=self.processor.table,
                data=data_to_insert,
                column_names=self.processor.column_names
            )
            
            self.logger.debug(f"Successfully inserted {len(self.batch)} messages into ClickHouse")
            
            # Clear batch and update flush time
            self.batch.clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            self.logger.error(f"Failed to insert batch into ClickHouse: {e}")
            self.logger.debug(f"table= {self.processor.table}")
            self.logger.debug(f"column_names= {self.processor.column_names}")
            self.logger.debug(f"data_to_insert= {data_to_insert}")
    
    def close(self):
        with self.batch_lock:
            if self.batch:
                self.logger.info("Flushing remaining messages before closing...")
                self.flush_batch()

