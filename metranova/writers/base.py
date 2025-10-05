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
    
    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        if self.datastore:
            self.datastore.close()
        self.logger.info("Datastore connection closed")
