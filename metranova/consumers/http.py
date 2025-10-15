import logging
import os
import time
from metranova.connectors.http import HTTPConnector
from metranova.consumers.base import BaseConsumer
from metranova.pipelines.base import BasePipeline
from requests.exceptions import ConnectionError, HTTPError, RequestException

logger = logging.getLogger(__name__)

class HTTPConsumer(BaseConsumer):
    def __init__(self, pipeline: BasePipeline, env_prefix: str = ''):
        super().__init__(pipeline)
        #Iniial values
        self.logger = logger
        self.datasource = HTTPConnector()
        self.update_interval = -1
        self.urls = []
        
        #append undescore if prefix is provided
        if env_prefix and not env_prefix.endswith('_'):
            env_prefix += '_'
        # grab update interval from env
        self.update_interval = int(os.getenv(f'{env_prefix}HTTP_CONSUMER_UPDATE_INTERVAL', -1))
        # Load URLs from environment variable
        url_str = os.getenv(f'{env_prefix}HTTP_CONSUMER_URLS', '')
        if url_str:
            self.urls = [url.strip() for url in url_str.split(',') if url.strip()]
            logger.info(f"Found {len(self.urls)} HTTP URLs: {self.urls}")
        else:
            logger.warning(f"{env_prefix}HTTP_CONSUMER_URLS environment variable is empty")


    def consume_messages(self):
         # check connection and tables
        if not self.datasource.client:
            self.logger.error("HTTP client not initialized")
            return
        
        # If update_interval is set, run periodically
        while True:
            # Load HTTP endpoints serially
            self.logger.info("Starting metadata loading from HTTP source")
            for url in self.urls:
                try:
                    result = self.datasource.client.get(url)
                    result.raise_for_status()
                    msg = {
                        'url': url,
                        'status_code': result.status_code,
                        'data': result.json()
                    }
                    self.pipeline.process_message(msg)
                except HTTPError as e:
                    self.logger.error("HTTP error {0}".format(e))
                except RequestException as e:
                    self.logger.error("Request error {0}".format(e))
                except ConnectionError as e:
                    self.logger.error("Connection error {0}".format(e))
                except ValueError as e:
                    self.logger.error("Invalid JSON returned. Make sure the URL is correct. {0}".format(e))
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")

            # Break if no update interval is set
            if self.update_interval <= 0:
                break  # Run once if no interval is set

            # Sleep before next update
            self.logger.info(f"Sleeping for {self.update_interval} seconds before next update")
            time.sleep(self.update_interval)