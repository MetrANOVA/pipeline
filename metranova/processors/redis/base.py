import logging
from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BaseRedisProcessor(BaseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.match_fields = []  # array of arrays where inner array is path to field in dict

