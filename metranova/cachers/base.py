import logging
import threading
import time
from typing import Optional, List

logger = logging.getLogger(__name__)

class BaseCacher:
    def __init__(self):
        self.logger = logger
        self.cache = None
        #Override in subclasses if needed - 0 means no refresh
        self.cache_refresh_interval = 0
        self.refresh_thread = None
    
    def prime(self):
        return
    
    def start_refresh_thread(self):
        #always prime cache initially
        self.prime()
        #keep refreshing if interval > 0
        if self.cache_refresh_interval > 0:
            self.logger.info(f"Setting up periodic cache refresh every {self.cache_refresh_interval} seconds")
            #create thread to periodically reload caches
            self.refresh_thread = threading.Thread(target=self.refresh, name=f"CacheRefresh-{type(self).__name__}")
            self.refresh_thread.daemon = True
            self.refresh_thread.start()

    def refresh(self):
        """Re-primes the cache periodically if cache_refresh_interval > 0"""
        #Initial wait before first refresh
        self.logger.info(f"Starting cache refresh thread with interval {self.cache_refresh_interval} seconds")
        time.sleep(self.cache_refresh_interval)
        while True:
            self.prime()
            # Sleep for 10 minutes before refreshing again
            self.logger.info(f"Next cache refresh in {self.cache_refresh_interval} seconds")
            time.sleep(self.cache_refresh_interval)
    
    def lookup(self, table, key: str) -> Optional[str]:
        raise NotImplementedError("Subclasses should implement this method")

    def lookup_list(self, table, keys: List[str]) -> List[str]:
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        #stop refresh thread if running
        if self.refresh_thread:
            self.logger.info("Waiting for refresh thread to finish...")
            self.refresh_thread.join(timeout=2.0)  # Wait up to 2 seconds per thread
            if self.refresh_thread.is_alive():
                self.logger.debug(f"Thread {self.refresh_thread.name} did not finish within timeout")
            self.refresh_thread = None
        
        #close cache connection if applicable
        if self.cache:
            self.cache.close()

class NoOpCacher(BaseCacher):
    def __init__(self):
        super().__init__()
    
    def prime(self):
        return
    
    def lookup(self, table, key: str) -> Optional[str]:
        return None

    def lookup_list(self, table, keys: List[str]) -> List[str]:
        return []