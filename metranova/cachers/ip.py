import logging
import os
import pickle
import threading
import time
import requests
import gc
from typing import Optional, List
from metranova.cachers.base import BaseCacher

logger = logging.getLogger(__name__)

class IPCacher(BaseCacher):
    def __init__(self):
        super().__init__()
        self.logger = logger
        self.base_url = os.getenv('IP_CACHER_BASE_URL', None)
        self.cache_dir = os.getenv('IP_CACHER_DIR', 'caches')
        self.cache_refresh_interval = int(os.getenv('IP_CACHER_REFRESH_INTERVAL', '600'))
        #tables to load
        tables_str = os.getenv('IP_CACHER_TABLES', '')
        self.tables = [t.strip() for t in tables_str.split(',') if t.strip()]
        if not self.tables:
            self.logger.warning("No tables specified for IP cacher")
        self.local_cache = {}
        self.cache_lock = threading.RLock()  # Lock to protect cache during updates
        self.start_refresh_thread()

    def prime(self):
        for table in self.tables:
            if self.base_url:
                #load from remote URL
               self.load_from_url(table)
            elif self.cache_dir:
                #load from local file
                self.load_from_file(table)
            else:
                self.logger.error("No base URL or cache directory specified for IP cacher")
    
    def load_from_url(self, table: str):
        url = f"{self.base_url}/ip_trie_{table}.pickle"
        self.logger.info(f"Loading IP trie for table {table} from URL: {url}")
        try:
            # Use lock to prevent lookups during update
            # Delete old trie before assigning new one to avoid both in memory
            with self.cache_lock:
                if table in self.local_cache:
                    self.logger.debug(f"Deleting old trie for table {table} before loading new one")
                    old_trie = self.local_cache[table]
                    del self.local_cache[table]
                    del old_trie
                    gc.collect()
                response = requests.get(url)
                response.raise_for_status()
                self.local_cache[table] = pickle.loads(response.content)
            
            self.logger.info(f"Loaded IP trie for table {table} with {len(self.local_cache.get(table, []))} entries")
        except Exception as e:
            self.logger.error(f"Error loading IP trie from {url}: {e}") 

    def load_from_file(self, table: str):
        filename = os.path.join(self.cache_dir, f"ip_trie_{table}.pickle")
        if os.path.exists(filename):
            self.logger.info(f"Found IP trie cache file for table {table}: {filename}")
        else:
            self.logger.warning(f"No IP trie cache file found for table {table}: {filename}")
            return
        # Load the trie from the pickle file
        try:
            # Use lock to prevent lookups during update
            # Delete old trie before assigning new one to avoid both in memory
            with self.cache_lock:
                if table in self.local_cache:
                    self.logger.debug(f"Deleting old trie for table {table} before loading new one")
                    old_trie = self.local_cache[table]
                    del self.local_cache[table]
                    del old_trie
                    gc.collect()
                with open(filename, 'rb') as f:
                    self.local_cache[table] = pickle.load(f)
            
            self.logger.info(f"Loaded IP trie for table {table} with {len(self.local_cache.get(table, []))} entries")
        except Exception as e:
            self.logger.error(f"Error loading IP trie from {filename}: {e}")

    def lookup(self, table, key: str) -> Optional[str]:
        with self.cache_lock:
            if not key or not table:
                return None
            return self.local_cache.get(table, {}).get(key, None)

