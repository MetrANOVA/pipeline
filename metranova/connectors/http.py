import logging
import os
import requests
from metranova.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class HTTPConnector(BaseConnector):
    def __init__(self):
        super().__init__()
        #load timeout, ssl verify and from env vars
        self.timeout = int(os.getenv("HTTP_TIMEOUT", 30))  # default timeout
        self.ssl_verify = os.getenv("HTTP_SSL_VERIFY", "false").lower() in ['true', '1', 'yes']  # default SSL verification setting
        self.headers = {}
        header_str = os.getenv("HTTP_HEADERS", "")
        if header_str:
            for header in header_str.split(","):
                key, value = header.split(":", 1)
                self.headers[key.strip()] = value.strip()
        #start HTTP session
        self.connect()

    def connect(self):
        self.client = requests.Session()
        self.client.verify = self.ssl_verify
        self.client.timeout = self.timeout
        if self.headers:
            self.client.headers.update(self.headers)
        logger.info("HTTP session initialized")

    def close(self):
        #close requests session
        if self.client:
            self.client.close()
            logger.info("HTTP session closed")