import logging
import os
from metranova.connectors.http import HTTPConnector
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline
from requests.exceptions import ConnectionError, HTTPError, RequestException

logger = logging.getLogger(__name__)

class StatusConsumer(TimedIntervalConsumer):
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        #Iniial values
        self.logger = logger
        self.datasource = HTTPConnector()
        self.urls = []
        # grab update interval from env
        self.update_interval = int(os.getenv(f'ALCF_STATUS_CONSUMER_UPDATE_INTERVAL', -1))
        # Load URLs from environment variable
        url_str = os.getenv(f'ALCF_STATUS_CONSUMER_URLS', '')
        if url_str:
            self.urls = [url.strip() for url in url_str.split(',') if url.strip()]
            logger.info(f"Found {len(self.urls)} HTTP URLs: {self.urls}")
        else:
            logger.warning(f"ALCF_STATUS_CONSUMER_URLS environment variable is empty")

    def consume_messages(self):
        for url in self.urls:
            try:
                #Fetch ALCF status APIs
                result = self.datasource.client.get(url)
                result.raise_for_status()
                result_json = result.json()
                #get compute_resource_id from url
                compute_resource_id = url.split('/')[-2]
                for job_type in ['queued', 'running']:
                    data_json = result_json.get(job_type, [])
                    #add compute_resource_id to each job record
                    for record in data_json:
                        record['compute_resource_id'] = compute_resource_id
                    msg = {
                        'url': url,
                        'type': 'job',
                        'status_code': result.status_code,
                        'data': data_json
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