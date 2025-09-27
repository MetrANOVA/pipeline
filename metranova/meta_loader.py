#!/usr/bin/env python3
"""
Meta Loader Script - Loads metadata from ClickHouse to Redis
"""

import os
import logging
import time
import redis
import clickhouse_connect
from typing import List, Dict, Any

# Configure logging
log_level = logging.INFO
if os.getenv('DEBUG', 'false').lower() == 'true' or os.getenv('DEBUG') == '1':
    log_level = logging.DEBUG
    
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetaLoader:
    """Loads metadata from ClickHouse tables to Redis"""
    
    def __init__(self):
        # ClickHouse configuration (same as ch_writer.py)
        self.ch_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.ch_port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.ch_database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        self.ch_username = os.getenv('CLICKHOUSE_USERNAME', 'default')
        self.ch_password = os.getenv('CLICKHOUSE_PASSWORD', '')
        
        # Redis configuration
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
        self.redis_password = os.getenv('REDIS_PASSWORD')
        
        # Meta lookup tables
        self.meta_tables = self._parse_meta_tables()
        
        # Initialize connections
        self.ch_client = None
        self.redis_client = None
        
        self._connect_clickhouse()
        self._connect_redis()
    
    def _parse_meta_tables(self) -> List[str]:
        """Parse comma-separated list of meta lookup tables"""
        tables_str = os.getenv('META_LOOKUP_TABLES', '')
        if not tables_str:
            logger.warning("META_LOOKUP_TABLES environment variable is empty")
            return []
        
        tables = [table.strip() for table in tables_str.split(',') if table.strip()]
        logger.info(f"Found {len(tables)} meta lookup tables: {tables}")
        return tables
    
    def _connect_clickhouse(self):
        """Initialize ClickHouse connection"""
        try:
            self.ch_client = clickhouse_connect.get_client(
                host=self.ch_host,
                port=self.ch_port,
                database=self.ch_database,
                username=self.ch_username,
                password=self.ch_password,
                secure=os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true',
                verify=False
            )
            
            # Test connection
            self.ch_client.ping()
            logger.info(f"Connected to ClickHouse at {self.ch_host}:{self.ch_port}, database: {self.ch_database}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def _connect_redis(self):
        """Initialize Redis connection"""
        try:
            redis_config = {
                'host': self.redis_host,
                'port': self.redis_port,
                'db': self.redis_db,
                'decode_responses': True,  # Automatically decode responses to strings
                'socket_timeout': 30,
                'socket_connect_timeout': 10,
                'retry_on_timeout': True
            }
            
            if self.redis_password:
                redis_config['password'] = self.redis_password
            
            self.redis_client = redis.Redis(**redis_config)
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}, db: {self.redis_db}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def _build_query(self, table: str) -> str:
        """Build the metadata query for a specific table"""
        query = f"""
        SELECT argMax(id, insert_ts) as latest_id, 
               argMax(meta_ref, insert_ts) as latest_meta_ref
        FROM {table} 
        GROUP BY id
        ORDER BY id
        """
        return query
    
    def _load_table_metadata(self, table: str) -> int:
        """Load metadata from a single ClickHouse table to Redis"""
        logger.info(f"Loading metadata from table: {table}")
        
        try:
            # Build and execute query
            query = self._build_query(table)
            logger.debug(f"Executing query: {query}")
            
            start_time = time.time()
            result = self.ch_client.query(query)
            query_time = time.time() - start_time
            
            # Get the data
            rows = result.result_rows
            logger.info(f"Query completed in {query_time:.2f}s, found {len(rows)} records")
            
            if not rows:
                logger.warning(f"No data found in table {table}")
                return 0
            
            # Load data into Redis
            loaded_count = self._load_to_redis(table, rows)
            
            logger.info(f"Successfully loaded {loaded_count} records from {table} to Redis")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load metadata from table {table}: {e}")
            raise
    
    def _load_to_redis(self, table: str, rows: List[tuple]) -> int:
        """Load query results into Redis"""
        logger.debug(f"Loading {len(rows)} records to Redis table: {table}")
        
        try:
            # Use pipeline for efficient batch operations
            pipe = self.redis_client.pipeline()
            
            loaded_count = 0
            for row in rows:
                latest_id, latest_meta_ref = row
                
                # Skip rows with null values
                if latest_id is None or latest_meta_ref is None:
                    logger.debug(f"Skipping row with null values: id={latest_id}, meta_ref={latest_meta_ref}")
                    continue
                
                # Set the key-value pair in Redis
                # Key format: table:id, Value: meta_ref
                redis_key = f"{table}:{latest_id}"
                pipe.set(redis_key, latest_meta_ref)
                loaded_count += 1
                
                # Execute batch every 1000 records
                if loaded_count % 1000 == 0:
                    pipe.execute()
                    pipe = self.redis_client.pipeline()
                    logger.debug(f"Loaded {loaded_count} records so far...")
            
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
            self.redis_client.hset(metadata_key, mapping=metadata_value)
            
            return loaded_count
            
        except Exception as e:
            logger.error(f"Failed to load data to Redis for table {table}: {e}")
            raise
    
    def load_all_tables(self):
        """Load metadata from all configured tables"""
        if not self.meta_tables:
            logger.error("No meta lookup tables configured")
            return
        
        total_loaded = 0
        successful_tables = 0
        
        logger.info(f"Starting metadata load for {len(self.meta_tables)} tables")
        
        for table in self.meta_tables:
            try:
                loaded_count = self._load_table_metadata(table)
                total_loaded += loaded_count
                successful_tables += 1
                
            except Exception as e:
                logger.error(f"Failed to process table {table}: {e}")
                # Continue with other tables
                continue
        
        logger.info(f"Metadata load completed: {successful_tables}/{len(self.meta_tables)} tables processed, {total_loaded} total records loaded")
    
    def close(self):
        """Close connections"""
        if self.ch_client:
            self.ch_client.close()
            logger.info("ClickHouse connection closed")
        
        if self.redis_client:
            self.redis_client.connection_pool.disconnect()
            logger.info("Redis connection closed")