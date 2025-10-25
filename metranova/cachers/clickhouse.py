import logging
import os
from typing import Optional
from metranova.cachers.base import BaseCacher
from metranova.connectors.clickhouse import ClickHouseConnector

logger = logging.getLogger(__name__)

class ClickHouseCacher(BaseCacher):

    def __init__(self):
        self.logger = logger
        self.cache = ClickHouseConnector()
        self.local_cache = {}
        tables_str = os.getenv('CLICKHOUSE_CACHER_TABLES', '')
        self.tables = [t.strip() for t in tables_str.split(',') if t.strip()]
        if not self.tables:
            self.logger.warning("No tables specified for ClickHouse cacher")

    def prime(self):
        for table in self.tables:
            self.prime_table(table)
    
    def prime_table(self, table):
        """Preload any necessary data into local cache"""
        # Example: preload some frequently accessed keys
        if self.cache.client is None:
            logger.debug("ClickHouse client not available, skipping priming")
            return
        
        # build query to get latest records by id
        query = f"""
        SELECT argMax(id, insert_time) as latest_id, 
               argMax(ref, insert_time) as latest_ref, 
               argMax(hash, insert_time) as latest_hash, 
               MAX(insert_time) as max_insert_time
        FROM {table} 
        GROUP BY id 
        ORDER BY id
        """

        # Pull data into local cache
        try:
            result = self.cache.client.query(query)
            tmp_cache = {}
            for row in result.result_rows:
                latest_id, latest_ref, latest_hash, max_insert_time = row
                tmp_cache[latest_id] = {
                    'ref': latest_ref,
                    'hash': latest_hash,
                    'max_insert_time': max_insert_time
                }
            self.local_cache[table] = tmp_cache
            logger.info(f"Loaded {len(tmp_cache)} existing records from {table}")
        except Exception as e:
            logger.info(f"Could not load existing data (table might be empty): {e}")

    def lookup(self, table, key: str) -> Optional[str]:
        #lookup in local cache
        if key is None or table is None:
            return None
        return self.local_cache.get(table, {}).get(key, None)