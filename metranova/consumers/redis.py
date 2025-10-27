import logging
import os
from typing import Iterable
from metranova.connectors.redis import RedisConnector
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)

class RedisConsumer(TimedIntervalConsumer):
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.datasource = RedisConnector()
        self.tables = []

    def consume_messages(self):
        if not self.tables:
            self.logger.error("No tables specified for loading")
            return
        self.logger.info("Starting loading from Redis...")
        for table in self.tables:
            try:
                # Query all records from the table
                for msg in self.query_table(table):
                    self.pipeline.process_message(msg)
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
                # Continue processing other messages
                continue

    def query_table(self, table: str) -> dict:
        raise NotImplementedError("Subclasses must implement query_table method")

class RedisHashConsumer(RedisConsumer):
    """Redis consumer for loading hash data"""
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.update_interval = int(os.getenv('REDIS_CONSUMER_UPDATE_INTERVAL', -1))
        # Load tables from environment variable
        tables_str = os.getenv('REDIS_CONSUMER_TABLES', '')
        if tables_str:
            self.tables = [table.strip() for table in tables_str.split(',') if table.strip()]

    def query_table(self, table: str) -> Iterable[dict]:
        """Query all fields from a Redis hash"""
        if not self.datasource.client:
            raise ConnectionError("Redis client not initialized")
        
        try:
            #build table pattern 
            table_pattern = f"{table}:*"
            self.logger.debug(f"Querying Redis keys with pattern: {table_pattern}")
            #Scan for keys matching the pattern and of type hash with scan_iter
            for hash_key in self.datasource.client.scan_iter(match=table_pattern, _type='hash'):
                self.logger.debug(f"Fetching data for Redis hash: {hash_key}")
                data = self.datasource.client.hgetall(hash_key)
                if not data:
                    self.logger.debug(f"No data found in hash: {hash_key}")
                    continue
                msg = {
                    'table': table,
                    'key': hash_key,
                    'data': data
                }
                yield msg
        except Exception as e:
            self.logger.error(f"Failed to query Redis hash {table}: {e}")
            raise

        return []