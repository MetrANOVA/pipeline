
from typing import Optional, List

class BaseCacher:
    def __init__(self):
        pass

    def lookup(self, table, key: str) -> Optional[str]:
        raise NotImplementedError("Subclasses should implement this method")

    def lookup_list(self, table, keys: List[str]) -> List[str]:
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        raise NotImplementedError("Subclasses should implement this method")
