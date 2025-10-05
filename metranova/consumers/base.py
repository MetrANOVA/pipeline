import logging

logger = logging.getLogger(__name__)

class BaseConsumer:
    
    def __init__(self, pipeline):
        # Initialize pipeline
        self.pipeline = pipeline
        self.logger = logger
    
    def consume_messages(self):
        """Consume messages from source"""
        raise NotImplementedError("Subclasses should implement this method")

    def close(self):
        """Close operation if needed"""
        return
