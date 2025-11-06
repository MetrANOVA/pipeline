from collections import defaultdict
import logging
import os
from platform import processor
import threading
import time
from typing import List, Dict, Any, Optional

from metranova.connectors.clickhouse import ClickHouseConnector
from metranova.processors.base import BaseProcessor
from metranova.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class ClickHouseWriter(BaseWriter):
    def __init__(self, processors: List[BaseProcessor]):
        super().__init__(processors)
        # setup logger
        self.logger = logger
        # Clickhouse connection for primary thread, batchers will use their own connections
        self.datastore = ClickHouseConnector()
        self.batchers = []

        # Setup batch writers for each processor
        for processor in self.processors:
            batcher = ClickHouseBatcher(processor)
            batcher.start_flush_timer()
            self.batchers.append(batcher)

    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        """Override process_message to use batchers"""
        if not msg:
            return
        for batcher in self.batchers:
            batcher.process_message(msg, consumer_metadata)

    def close(self):
        for batcher in self.batchers:
            batcher.close()
        
        if self.datastore:
            self.datastore.close()
        logger.info("Datastore connection closed")

class ClickHouseBatcher:
    def __init__(self, processor):
        # Logger
        self.logger = logger

        #Clickhouse Client and message processor
        # each batcher gets its own client to avoid threading issues
        self.client = ClickHouseConnector().client
        self.processor = processor

        # Batching configuration
        self.batch_size = int(os.getenv('CLICKHOUSE_BATCH_SIZE', '1000'))
        self.batch_timeout = float(os.getenv('CLICKHOUSE_BATCH_TIMEOUT', '30.0'))  # seconds
        self.flush_interval = float(os.getenv('CLICKHOUSE_FLUSH_INTERVAL', '0.1'))  # seconds
       
        # Batch state
        # self.batch is a dict where key is table name and value is list of messages for that table
        self.batch = defaultdict(list)
        self.batch_lock = threading.Lock()
        self.last_flush_time = time.time()

        # Prepare table and columns
        self.create_table()

    def create_table(self):
        """Create the target table if it doesn't exist"""
        create_table_cmd = self.processor.create_table_command() # store for reference
        if create_table_cmd is None:
            logger.info("No create_table_cmd defined, skipping table creation")
            return
        try:
            self.client.command(create_table_cmd)
            logger.info(f"Table {self.processor.table} is ready")
        except Exception as e:
            logger.error(f"Failed to create table {self.processor.table}: {e}")
            raise

    def start_flush_timer(self):
        """Start background timer to flush batches periodically"""
        def flush_timer():
            while True:
                # Check every flush_interval seconds. 
                # Note that max clickhouse throughput is self.flush_interval * self.batch_size
                time.sleep(self.flush_interval)  
                current_time = time.time()
                
                with self.batch_lock:
                    if (current_time - self.last_flush_time) >= self.batch_timeout:
                        for table_name in self.batch:
                            self.flush_batch(table_name)

        timer_thread = threading.Thread(target=flush_timer, daemon=True)
        timer_thread.start()
        logger.info(f"Started batch flush timer (timeout: {self.batch_timeout}s)")

    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        """Add message to batch and flush if needed"""
        if not msg:
            return
        
        # Check if this processor should handle the message
        if not self.processor.match_message(msg):
            self.logger.debug("Message did not match processor criteria, skipping")
            return

        # Prepare message data for ClickHouse
        message_data = self.processor.build_message(msg, consumer_metadata)
        if message_data is None:
            return
        
        #append to batch
        with self.batch_lock:
            #Note: message_data is a list of dicts so += adds all elements
            for formatted_msg in message_data:
                table_name = formatted_msg.get('_clickhouse_table', self.processor.table)
                self.batch[table_name].append(formatted_msg)
            
            # Check if we need to flush
            for table_name in self.batch:
                if len(self.batch[table_name]) >= self.batch_size:
                    self.flush_batch(table_name)

    def flush_batch(self, table_name):
        """Flush current batch to ClickHouse"""
        if not self.batch or not table_name or not self.batch.get(table_name, None):
            #nothing to flush
            return
        
        try:
            # Prepare data for insertion
            data_to_insert = []
            for msg in self.batch[table_name]:
                data_to_insert.append(self.processor.message_to_columns(msg, table_name))
            
            # Insert batch into ClickHouse
            self.client.insert(
                table=table_name,
                data=data_to_insert,
                column_names=self.processor.column_names()
            )

            self.logger.info(f"Successfully inserted {len(data_to_insert)} messages into ClickHouse table {table_name}")
            
            # Clear batch and update flush time
            self.batch[table_name].clear()
            self.last_flush_time = time.time()
            
        except Exception as e:
            self.logger.error(f"Failed to insert batch into ClickHouse: {e}")
            self.logger.debug(f"table= {self.processor.table}")
            self.logger.debug(f"column_names= {self.processor.column_names()}")
            self.logger.debug(f"data_to_insert= {data_to_insert}")
    
    def close(self):
        with self.batch_lock:
            for table_name in self.batch:
                self.logger.info("Flushing remaining messages before closing...")
                self.flush_batch(table_name)

