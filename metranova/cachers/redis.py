
import logging
import redis
from typing import Optional, List
from metranova.connectors.redis import RedisConnector
from metranova.cachers.base import BaseCacher
logger = logging.getLogger(__name__)

class RedisCacher(BaseCacher):

    def __init__(self):
        self.logger = logger
        self.cache = RedisConnector()

    def lookup(self, table, key: str) -> Optional[str]:
        """Perform Redis GET lookup for the given key"""
        if key is None or table is None:
            return None
        key = f"{table}:{key}"

        if self.cache.client is None:
            logger.debug("Redis client not available, skipping lookup")
            return None
            
        try:
            # Perform Redis GET operation
            value = self.cache.client.get(key)
            if value is not None:
                logger.debug(f"Redis lookup successful: {key} -> {value}")
                return str(value)  # Ensure string return type
            else:
                logger.debug(f"Redis lookup found no value for key: {key}")
                return None
                
        except redis.RedisError as e:
            logger.warning(f"Redis lookup failed for key '{key}': {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error during Redis lookup for key '{key}': {e}")
            return None

    def lookup_list(self, table, keys: List[str]) -> List[str]:
        """Perform Redis lookups for a list of keys, returning only found values"""
        if not keys:
            return []
            
        vals = []
        for key in keys:
            v = self.lookup(table, key)
            if v is not None:
                vals.append(v)

        return vals

