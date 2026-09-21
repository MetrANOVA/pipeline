import logging
import os
import pickle
import posixpath
import threading
import urllib.parse
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import List
from metranova.processors.base import BaseProcessor
from metranova.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class ThreadingPickleHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
    daemon_threads = True

class DirectoryHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, directory=None, **kwargs):
        self.directory = os.fspath(directory)
        super().__init__(*args, **kwargs)

    def translate_path(self, path):
        path = urllib.parse.urlparse(path).path
        path = posixpath.normpath(urllib.parse.unquote(path))
        words = [word for word in path.split('/') if word]
        translated_path = self.directory
        for word in words:
            if os.path.dirname(word) or word in (os.curdir, os.pardir):
                continue
            translated_path = os.path.join(translated_path, word)
        return translated_path

    def log_message(self, format, *args):
        logger.debug(format, *args)

class PickleFileWriter(BaseWriter):
    def __init__(self, processors: List[BaseProcessor]):
        super().__init__(processors)
        self.logger = logger
        self.datastore = None # don't need a connector
        self.file_dir = os.getenv('PICKLE_FILE_DIRECTORY', 'caches')
        self.http_enabled = os.getenv('PICKLE_FILE_HTTP_ENABLED', 'false').lower() in ('true', '1', 'yes')
        self.http_host = os.getenv('PICKLE_FILE_HTTP_HOST', '0.0.0.0')
        self.http_port = int(os.getenv('PICKLE_FILE_HTTP_PORT', '8080'))
        self.http_server = None
        self.http_thread = None
        if self.http_enabled:
            self.start_http_server()

    def start_http_server(self):
        os.makedirs(self.file_dir, exist_ok=True)
        handler = partial(DirectoryHTTPRequestHandler, directory=self.file_dir)
        self.http_server = ThreadingPickleHTTPServer((self.http_host, self.http_port), handler)
        self.http_thread = threading.Thread(
            target=self.http_server.serve_forever,
            name=f"PickleFileHTTPServer-{self.http_server.server_address[1]}",
            daemon=True,
        )
        self.http_thread.start()
        host, port = self.http_server.server_address
        self.logger.info(f"Serving pickle files from {self.file_dir} at http://{host}:{port}")
    
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

    def close(self):
        if self.http_server:
            self.http_server.shutdown()
            self.http_server.server_close()
            self.http_server = None
        if self.http_thread:
            self.http_thread.join(timeout=2.0)
            self.http_thread = None
        super().close()