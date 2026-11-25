import logging
import os
import pickle
from typing import List
import orjson
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


class JSONFileWriter(BaseWriter):
    """Write a JSON document to disk.

    Expects messages of the form::

        {"name": "some_file.json", "data": <json-serialisable object>}

    Environment variables:
        JSON_FILE_DIRECTORY  – output directory (default: ``output``)
        JSON_FILE_INDENT     – pretty-print indent level (default: 2, set 0 for compact)
    """

    def __init__(self, processors: List[BaseProcessor]):
        super().__init__(processors)
        self.logger = logger
        self.datastore = None
        self.file_dir = os.getenv('JSON_FILE_DIRECTORY', 'output')
        indent = int(os.getenv('JSON_FILE_INDENT', '2'))
        self._json_opts = orjson.OPT_INDENT_2 if indent >= 2 else 0

    def write_message(self, msg, consumer_metadata=None):
        if not msg or 'data' not in msg or 'name' not in msg:
            self.logger.warning("Message missing 'data' or 'name' fields, skipping")
            return

        filename = os.path.join(self.file_dir, msg['name'])
        try:
            os.makedirs(self.file_dir, exist_ok=True)
            raw = orjson.dumps(msg['data'], option=self._json_opts)
            # Write atomically: write to a temp file then rename
            tmp_filename = filename + '.tmp'
            with open(tmp_filename, 'wb') as f:
                f.write(raw)
            os.replace(tmp_filename, filename)
            self.logger.info(f"Wrote JSON file: {filename} ({len(raw)} bytes)")
        except Exception as e:
            self.logger.error(f"Error writing JSON file {filename}: {e}")