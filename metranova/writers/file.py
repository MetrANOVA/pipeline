import logging
import os
import pickle
from typing import List
from metranova.processors.base import BaseProcessor
from metranova.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class PickleFileWriter(BaseWriter):
    def __init__(self, processors: List[BaseProcessor]):
        super().__init__(processors)
        self.logger = logger
        self.datastore = None # don't need a connector
        self.file_dir = os.getenv('PICKLE_FILE_DIRECTORY', 'caches')
    
    def write_message(self, msg, consumer_metadata=None):
        self.logger.info(f"Processing message for PickleFileWriter: {msg}")
        #Validate msg has 'data' and 'name'
        if not msg or 'data' not in msg or 'name' not in msg:
           self.logger.warning("Message missing 'data' or 'name' fields, skipping")
           return

        #write data to pickle file with given name
        filename = os.path.join(self.file_dir, msg['name'])
        try:
            os.makedirs(self.file_dir, exist_ok=True)
            with open(filename, 'wb') as f:
                pickle.dump(msg['data'], f)
            self.logger.info(f"Wrote pickle file: {filename}")
        except Exception as e:
            self.logger.error(f"Error writing pickle file {filename}: {e}")