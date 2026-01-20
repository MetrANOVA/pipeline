import logging
import os
from metranova.connectors.http import HTTPConnector
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline
from requests.exceptions import ConnectionError, HTTPError, RequestException

logger = logging.getLogger(__name__)


class GraphQLConsumer(TimedIntervalConsumer):
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.datasource = HTTPConnector()
        
        env_prefix = os.getenv('GRAPHQL_CONSUMER_ENV_PREFIX', '')
        if env_prefix and not env_prefix.endswith('_'):
            env_prefix += '_'
        
        self.update_interval = int(os.getenv(f'{env_prefix}GRAPHQL_CONSUMER_UPDATE_INTERVAL', -1))
        self.endpoint = os.getenv(f'{env_prefix}GRAPHQL_ENDPOINT', '')
        self.token = os.getenv(f'{env_prefix}GRAPHQL_TOKEN', '')
        self.query = os.getenv(f'{env_prefix}GRAPHQL_QUERY', '')
        
        # Validate required fields
        if not self.endpoint:
            raise ValueError(f"Missing required {env_prefix}GRAPHQL_ENDPOINT")
        if not self.query:
            raise ValueError(f"Missing required {env_prefix}GRAPHQL_QUERY")

    def consume_messages(self):
        try:
            headers = {'Content-Type': 'application/json'}
            if self.token:
                headers['Authorization'] = f'Token {self.token}'
            
            payload = {'query': self.query}
            
            result = self.datasource.client.post(
                self.endpoint,
                json=payload,
                headers=headers
            )
            result.raise_for_status()
            
            msg = {
                'url': self.endpoint,
                'status_code': result.status_code,
                'data': result.json()
            }
            self.pipeline.process_message(msg)
            
        except ConnectionError as e:
            self.logger.error(f"GraphQL connection error: {e}")
        except HTTPError as e:
            self.logger.error(f"GraphQL HTTP error: {e}")
        except RequestException as e:
            self.logger.error(f"GraphQL request error: {e}")
        except Exception as e:
            self.logger.error(f"GraphQL unexpected error: {e}")