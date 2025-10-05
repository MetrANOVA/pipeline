import logging
import time
from typing import List, Optional, Dict
from metranova.connectors.redis import RedisConnector
from metranova.processors.base import BaseProcessor
from metranova.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class RedisWriter(BaseWriter):
    def __init__(self, processors: List[BaseProcessor]):
        super().__init__(processors)
        # setup logger
        self.logger = logger
        self.datastore = RedisConnector()
 
    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        #parse rows and tables
        rows = msg.get('rows', None)
        if not rows:
            self.logger.debug("No rows to process in message")
            return
        table = msg.get('table', None)
        if not table:
            self.logger.warning("Message missing 'table' field")
            return

        #load into redis
        self.logger.debug(f"Loading {len(rows)} records to Redis table: {table}")
        try:
            # Use pipeline for efficient batch operations
            pipe = self.datastore.client.pipeline()
            
            loaded_count = 0
            for row in rows:
                latest_id, latest_ref = row
                
                # Skip rows with null values
                if latest_id is None or latest_ref is None:
                    self.logger.debug(f"Skipping row with null values: id={latest_id}, ref={latest_ref}")
                    continue
                
                # Set the key-value pair in Redis
                # Key format: table:id, Value: ref
                redis_key = f"{table}:{latest_id}"
                pipe.set(redis_key, latest_ref)
                loaded_count += 1
                
                # Execute batch every 1000 records
                if loaded_count % 1000 == 0:
                    pipe.execute()
                    pipe = self.datastore.client.pipeline()
                    self.logger.debug(f"Loaded {loaded_count} records so far...")
            
            # Execute remaining records
            if loaded_count % 1000 != 0:
                pipe.execute()
            
            # Set metadata about the load
            metadata_key = f"{table}:_metadata"
            metadata_value = {
                'last_updated': int(time.time()),
                'record_count': loaded_count,
                'table_name': table
            }
            self.datastore.client.hset(metadata_key, mapping=metadata_value)
            
            return loaded_count
            
        except Exception as e:
            self.logger.error(f"Failed to load data to Redis for table {table}: {e}")
            raise
    
   