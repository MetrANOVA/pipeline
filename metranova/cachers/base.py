import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

class BaseCacher:
    def __init__(self):
        self.logger = logger
        self.cache = None
    
    def prime(self):
        return
    
    def lookup(self, table, key: str) -> Optional[str]:
        raise NotImplementedError("Subclasses should implement this method")

    def lookup_list(self, table, keys: List[str]) -> List[str]:
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        if self.cache:
            self.cache.close()