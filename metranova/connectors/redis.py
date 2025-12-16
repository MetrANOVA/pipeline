import logging
import os
import redis
import time
from metranova.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class RedisConnector(BaseConnector):
    def __init__(self):
        # setup logger
        self.logger = logger

        # Redis configuration (same as meta_loader.py)
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
        self.redis_password = os.getenv('REDIS_PASSWORD')
        self.redis_retries = int(os.getenv('REDIS_RETRIES', '5'))
        self.redis_retry_delay = float(os.getenv('REDIS_RETRY_DELAY', '2.0'))

        # Initialize Redis connection
        self.client = None
    
        # Connect to Redis - retry logic
        attempt = 0
        for attempt in range(self.redis_retries):
            try:
                self.connect()
                break  # Exit loop if connection is successful
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} to connect to Redis failed: {e}")
                if attempt < self.redis_retries - 1:
                    time.sleep(self.redis_retry_delay)
                else:
                    logger.error("All attempts to connect to Redis have failed.")
                    raise

    def connect(self):
        """Initialize Redis connection"""
        #TODO: Explore client-side caching options in redis-py: https://redis.io/blog/faster-redis-client-library-support-for-client-side-caching/
        try:
            redis_config = {
                'host': self.redis_host,
                'port': self.redis_port,
                'db': self.redis_db,
                'decode_responses': True,  # Automatically decode responses to strings
                'socket_timeout': 30,
                'socket_connect_timeout': 10,
                'retry_on_timeout': True,
                'health_check_interval': 30,  # Health check every 30 seconds
            }
            
            if self.redis_password:
                redis_config['password'] = self.redis_password
            
            # Create Redis connection with client-side caching enabled
            self.client = redis.Redis(**redis_config)
            
            # Test connection
            self.client.ping()
            logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}, db: {self.redis_db}")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            self.client = None
            raise

    def close(self):
        if self.client:
            self.client.connection_pool.disconnect()
            logger.info("Redis connection closed")
