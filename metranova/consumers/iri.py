import logging
import os
from metranova.connectors.http import HTTPConnector
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline
from requests.exceptions import ConnectionError, HTTPError, RequestException

logger = logging.getLogger(__name__)

class IncidentEventConsumer(TimedIntervalConsumer):
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        #Iniial values
        self.logger = logger
        self.datasource = HTTPConnector()
        self.urls = []
        # grab update interval from env
        self.update_interval = int(os.getenv(f'IRI_INCIDENT_EVENT_CONSUMER_UPDATE_INTERVAL', -1))
        # Load URLs from environment variable
        url_str = os.getenv(f'IRI_INCIDENT_EVENT_CONSUMER_URLS', '')
        if url_str:
            self.urls = [url.strip() for url in url_str.split(',') if url.strip()]
            logger.info(f"Found {len(self.urls)} HTTP URLs: {self.urls}")
        else:
            logger.warning(f"IRI_INCIDENT_EVENT_CONSUMER_URLS environment variable is empty")

    def consume_messages(self):
        for url in self.urls:
            try:
                #first fetch incidents
                result = self.datasource.client.get(url)
                result.raise_for_status()
                msg = {
                    'url': url,
                    'status_code': result.status_code,
                    'data': result.json()
                }
                self.pipeline.process_message(msg)
                #next fetch events for each incident
                for incident in msg.get('data', []):
                    incident_id = incident.get('id', None)
                    if not incident_id:
                        continue
                    event_url = f"{url}/{incident_id}/events"
                    event_result = self.datasource.client.get(event_url)
                    event_result.raise_for_status()
                    event_msg = {
                        'url': event_url,
                        'status_code': event_result.status_code,
                        'data': event_result.json()
                    }
                    self.pipeline.process_message(event_msg)
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