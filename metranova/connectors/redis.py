import logging
import os
import redis
from redis.cluster import RedisCluster, ClusterNode
from metranova.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class RedisConnector(BaseConnector):
    def __init__(self):
        # setup logger
        self.logger = logger

        # Redis configuration
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
        self.redis_password = os.getenv('REDIS_PASSWORD')
        self.redis_cluster_enabled = os.getenv('REDIS_CLUSTER_ENABLED', 'false').lower() in ['true', '1', 'yes']
        self.redis_cluster_nodes = os.getenv('REDIS_CLUSTER_NODES', '')

        # Initialize Redis connection
        self.client = None
    
        # Connect to Redis
        self.connect()
    
    def connect(self):
        """Initialize Redis connection (cluster or standalone)"""
        try:
            if self.redis_cluster_enabled and self.redis_cluster_nodes:
                # Redis Cluster mode
                startup_nodes = []
                for node in self.redis_cluster_nodes.split(','):
                    if ':' in node:
                        host, port = node.strip().split(':')
                        startup_nodes.append(ClusterNode(host=host, port=int(port)))
                
                cluster_config = {
                    'startup_nodes': startup_nodes,
                    'decode_responses': True,
                    'socket_timeout': 30,
                    'socket_connect_timeout': 10,
                    'retry_on_timeout': True,
                    'skip_full_coverage_check': True,  # Allow partial cluster
                    'health_check_interval': 30,
                }
                
                if self.redis_password:
                    cluster_config['password'] = self.redis_password
                
                self.client = RedisCluster(**cluster_config)
                logger.info(f"Connected to Redis Cluster with nodes: {self.redis_cluster_nodes}")
                
            else:
                # Standalone Redis mode (existing code)
                redis_config = {
                    'host': self.redis_host,
                    'port': self.redis_port,
                    'db': self.redis_db,
                    'decode_responses': True,
                    'socket_timeout': 30,
                    'socket_connect_timeout': 10,
                    'retry_on_timeout': True,
                    'health_check_interval': 30,
                }
                
                if self.redis_password:
                    redis_config['password'] = self.redis_password
                
                self.client = redis.Redis(**redis_config)
                logger.info(f"Connected to Redis at {self.redis_host}:{self.redis_port}, db: {self.redis_db}")
            
            # Test connection
            self.client.ping()
            
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            self.client = None

    def close(self):
        if self.client:
            if hasattr(self.client, 'connection_pool'):
                self.client.connection_pool.disconnect()
            elif hasattr(self.client, 'close'):
                self.client.close()
            logger.info("Redis connection closed")
