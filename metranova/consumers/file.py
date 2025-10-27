import logging
import orjson
import os
import yaml 
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)

class BaseFileConsumer(TimedIntervalConsumer):
    def __init__(self, pipeline: BasePipeline, env_prefix: str = ''):
        super().__init__(pipeline)
        # Initial values
        self.logger = logger
        self.datasource = None  # No external datasource needed for file reading
        self.file_paths = []
        
        # Append underscore if prefix is provided
        if env_prefix and not env_prefix.endswith('_'):
            env_prefix += '_'
        # Grab update interval from env
        self.update_interval = int(os.getenv(f'{env_prefix}FILE_CONSUMER_UPDATE_INTERVAL', -1))
        # Load file paths from environment variable
        file_str = os.getenv(f'{env_prefix}FILE_CONSUMER_PATHS', '')
        if file_str:
            self.file_paths = [path.strip() for path in file_str.split(',') if path.strip()]
            logger.info(f"Found {len(self.file_paths)} file paths: {self.file_paths}")
        else:
            logger.warning(f"{env_prefix}FILE_CONSUMER_FILE_PATHS environment variable is empty")
    
    def load_file_data(self, file: str):
        """Load data as text from file. Override in subclass for different formats."""
        return file.read()

    def consume_messages(self):
        for file_path in self.file_paths:
            #load data from file
            data = None
            try:
                with open(file_path, 'r') as file:
                    data = self.load_file_data(file)
            except FileNotFoundError as e:
                self.logger.error(f"File not found: {file_path}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")
            # process message if data loaded
            if data is None:
                continue
            self.handle_file_data(file_path, data)
    
    def handle_file_data(self, file_path: str, data):
        """Process loaded file data."""
        self.pipeline.process_message({'file_path': file_path,'data': data})

class YAMLFileConsumer(BaseFileConsumer):
    """YAML File Consumer"""
    def load_file_data(self, file: str):
        """Load data as YAML from file."""
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file: {e}")
            return None

class MetadataYAMLFileConsumer(YAMLFileConsumer):
    def handle_file_data(self, file_path, data):
        if data is None:
            return
        # Process metadata
        table = data.get('table', None)
        if table is None:
            self.logger.error(f"No table found in file: {file_path}")
            return
        metadata = data.get('data', [])
        for record in metadata:
            self.pipeline.process_message({'table': table, 'data': record})

class JSONFileConsumer(BaseFileConsumer):
    """JSON File Consumer"""
    def load_file_data(self, file: str):
        """Load data as JSON from file."""
        try:
            return orjson.loads(file.read())
        except orjson.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON file: {e}")
            return None