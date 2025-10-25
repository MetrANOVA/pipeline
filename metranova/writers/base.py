import logging
from typing import List, Dict, Any, Optional

from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BaseWriter:
    def __init__(self, processors: List[BaseProcessor]):
        # setup logger
        self.logger = logger
        self.processors = processors
        # Initialize datastore connection in child class
        self.datastore = None
    
    def process_message(self, msg, consumer_metadata=None):
        for processor in self.processors:
            if processor.match_message(msg):
                processed_msgs = processor.build_message(msg, consumer_metadata)
                if not processed_msgs:
                    self.logger.debug(f"Processor {processor.__class__.__name__} returned no messages to write")
                    continue
                for processed_msg in processed_msgs:
                    self.write_message(processed_msg, consumer_metadata)

    def write_message(self, msg, consumer_metadata=None):
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        if self.datastore:
            self.datastore.close()
        self.logger.info("Datastore connection closed")
