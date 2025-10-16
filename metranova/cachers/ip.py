import logging
import os
import pickle
import threading
import time
from typing import Optional, List
from metranova.cachers.base import BaseCacher

logger = logging.getLogger(__name__)

class IPCacher(BaseCacher):
    def __init__(self):
        super().__init__()
        self.logger = logger
        self.cache_dir = os.getenv('IP_CACHER_DIR', 'caches')
        self.cache_refresh_interval = int(os.getenv('IP_CACHER_REFRESH_INTERVAL', '600'))
        #tables to load
        tables_str = os.getenv('IP_CACHER_TABLES', '')
        self.tables = [t.strip() for t in tables_str.split(',') if t.strip()]
        if not self.tables:
            self.logger.warning("No tables specified for IP cacher")
        self.local_cache = {}
        #prime cache initially
        self.prime()
        if self.cache_refresh_interval > 0:
            self.logger.info(f"Setting up periodic cache refresh every {self.cache_refresh_interval} seconds")
            #create thread to periodically reload caches
            self.refresh_thread = threading.Thread(target=self.refresh, name=f"IPCacheRefresh-{type(self).__name__}")
            self.refresh_thread.daemon = True
            self.refresh_thread.start()

    def refresh(self):
        #Initial wait before first refresh
        self.logger.info(f"Starting cache refresh thread with interval {self.cache_refresh_interval} seconds")
        time.sleep(self.cache_refresh_interval)
        while True:
            self.prime()
            # Sleep for 10 minutes before refreshing again
            self.logger.info(f"Next cache refresh in {self.cache_refresh_interval} seconds")
            time.sleep(self.cache_refresh_interval)

    def prime(self):
        for table in self.tables:
            filename = os.path.join(self.cache_dir, f"ip_trie_{table}.pickle")
            if os.path.exists(filename):
                self.logger.info(f"Found IP trie cache file for table {table}: {filename}")
            else:
                self.logger.warning(f"No IP trie cache file found for table {table}: {filename}")
                continue
            # Load the trie from the pickle file
            try:
                with open(filename, 'rb') as f:
                    trie = pickle.load(f)
                self.local_cache[table] = trie
                self.logger.info(f"Loaded IP trie for table {table} with {len(trie)} entries")
            except Exception as e:
                self.logger.error(f"Error loading IP trie from {filename}: {e}")

    def lookup(self, table, key: str) -> Optional[str]:
        return self.local_cache.get(table, {}).get(key, None) if key and table else None
    
    def close(self):
        super().close()
        # Wait for all consumer threads to finish
        if self.refresh_thread:
            self.logger.info("Waiting for refresh thread to finish...")
            self.refresh_thread.join(timeout=2.0)  # Wait up to 2 seconds per thread
            if self.refresh_thread.is_alive():
                self.logger.warning(f"Thread {self.refresh_thread.name} did not finish within timeout")
            self.refresh_thread = None
