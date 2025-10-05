import logging
from typing import Dict, Any, Iterator

logger = logging.getLogger(__name__)

class BaseProcessor:
    def __init__(self, pipeline):
        # setup logger
        self.logger = logger

        # assign parent pipeline
        self.pipeline = pipeline

        # override in child class
        self.required_fields = []

    def has_required_fields(self, value: dict) -> bool:
        """Check if the message contains all required fields"""
        # NOTE: Call this in build_message instead of match_message to allow more detailed logging
        # This means the message matched the processor, but there is a problem with the content
        for fields in self.required_fields:
            v = None
            for field in fields:
                if v is None:
                    v = value.get(field, None)
                else:
                    v = v.get(field, None)
                if v is None:
                    self.logger.error(f"Missing required field '{field}' in message value")
                    return False
        return True

    def match_message(self, value: dict) -> bool:
        """Determine if this processor should handle the given message"""
        return True  # Default to match all messages with required fields, override in subclass if needed
    
    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        """Build message dictionary for ClickHouse insertion"""
        raise NotImplementedError("Subclasses should implement this method")
    
