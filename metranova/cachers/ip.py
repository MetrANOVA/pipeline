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
        self.start_refresh_thread()

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

