import logging
from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BaseRedisProcessor(BaseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.match_fields = []  # array of arrays where inner array is path to field in dict
    
    def has_match_field(self, value: dict) -> bool:
        for path in self.match_fields:
            current = value
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    break
                current = current[key]
            else: # only executed if inner loop did not break
                if current is not None:
                    return True
        return False

