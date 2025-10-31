import logging
import time
from ipaddress import IPv6Address
from typing import List, Optional, Dict
from metranova.connectors.redis import RedisConnector
from metranova.processors.base import BaseProcessor
from metranova.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class BaseRedisWriter(BaseWriter):
    def __init__(self, processors: List[BaseProcessor]):
        super().__init__(processors)
        # setup logger
        self.logger = logger
        self.datastore = RedisConnector()

class RedisWriter(BaseRedisWriter):
    def write_message(self, msg, consumer_metadata=None):
        #parse table, key and value
        if not msg:
            return
        table = msg.get('table', None)
        if not table:
            self.logger.warning("Message missing 'table' field")
            return
        key = msg.get('key', None)
        if not key:
            self.logger.warning("Message missing 'key' field")
            return
        value = msg.get('value', None)
        if not value:
            self.logger.warning("Message missing 'value' field")
            return
        expires = msg.get('expires', None)
        if expires is not None:
            try:
                expires = int(expires)
            except ValueError:
                self.logger.warning(f"Invalid expires value: {expires}, must be an integer representing seconds")
                expires = None
        else:
            expires = 0  # default no expire

        # Load data into Redis
        self.logger.debug(f"Loading message to Redis: {msg}")
        try:
            redis_key = f"{table}:{key}"
            self.datastore.client.set(redis_key, value)
            if expires:
                self.datastore.client.expire(redis_key, expires)
        except Exception as e:
            self.logger.error(f"Failed to load data to Redis for key {redis_key}: {e}")
            raise

class RedisMetadataRefWriter(BaseRedisWriter):
    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        """Override process_message since don't use processors here"""
        #parse rows and tables
        if not msg:
            return
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
                if len(row) < 2:
                    self.logger.debug(f"Skipping row with insufficient columns: {row}")
                    continue
                latest_id, latest_ref = row[0], row[1]
                
                # Skip rows with null values
                if latest_id is None or latest_ref is None:
                    self.logger.debug(f"Skipping row with null values: id={latest_id}, ref={latest_ref}")
                    continue

                # If there are additional fields (e.g., flow_index), then index by those fields instead
                if len(row) > 2:
                    redis_key = f"{table}".replace(':', '__')
                    for latest_field in row[2:]:
                        if latest_field is None:
                            continue
                        #if latest field is an IPv6Address, make it is not a mapped IPv4 address (i.e. starts with ::ffff:)
                        if isinstance(latest_field, IPv6Address) and latest_field.ipv4_mapped:
                            latest_field = str(latest_field.ipv4_mapped)
                        redis_key += f":{latest_field}"
                    pipe.set(redis_key, latest_id)
                else:
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
        except Exception as e:
            self.logger.error(f"Failed to load data to Redis for table {table}: {e}")
            raise
    
class RedisHashWriter(BaseRedisWriter):

    def write_message(self, msg, consumer_metadata: Optional[Dict] = None):
        #grab table, key, data and expires object
        if not msg:
            return

        table = msg.get('table', None)
        if not table:
            self.logger.warning("Message missing 'table' field")
            return
        self.logger.debug(f"Processing message for Redis hash table: {table}")

        key = msg.get('key', None)
        if not key:
            self.logger.debug("No key to process in message")
            return
        
        data = msg.get('data', None)
        if not data:
            self.logger.debug("No data object to process in message")
            return
        
        # Filter out None values as Redis can't handle them
        filtered_data = {k: v for k, v in data.items() if v is not None}
        if not filtered_data:
            self.logger.debug("No valid data to process after filtering None values")
            return
        
        expires = msg.get('expires', None)
        if expires is not None:
            try:
                expires = int(expires)
            except ValueError:
                self.logger.warning(f"Invalid expires value: {expires}, must be an integer representing seconds")
                expires = None

        #load into redis setting field expires using HEXPIRE
        self.logger.debug(f"Loading data to Redis hash table: {table}, key: {key}, expires: {expires}, data: {filtered_data}") 
        try:
            # build redis key
            redis_key = f"{table}:{key}"

            # Use pipeline for efficient batch operations
            pipe = self.datastore.client.pipeline()
            
            # Set the hash fields
            pipe.hset(redis_key, mapping=filtered_data)
            
            # Use hexpire to set expire for each field in data
            if expires is not None and expires > 0:
                pipe.hexpire(redis_key, expires, *filtered_data.keys())

            # Execute the pipeline
            pipe.execute()
        except Exception as e:
            self.logger.error(f"Failed to load data to Redis for table {table}: {e}")
            raise
