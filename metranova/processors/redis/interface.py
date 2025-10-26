import logging
import os
from metranova.processors.redis.base import BaseRedisProcessor

logger = logging.getLogger(__name__)

class BaseInterfaceMetadataProcessor(BaseRedisProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('REDIS_IF_METADATA_TABLE', 'meta_interface_cache')
        self.expires = int(os.getenv('REDIS_IF_METADATA_EXPIRES', '86400'))  # default 1 day


