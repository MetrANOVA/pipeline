import logging
from typing import Dict, Optional
import orjson
from metranova.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)

class JSONPipeline(BasePipeline):
    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        """Process individual Kafka message"""
        logger.debug(f"Received message from topic {consumer_metadata.get('topic', None)}, partition {consumer_metadata.get('partition')}, offset {consumer_metadata.get('offset')}")
        
        # Decode message
        try:
            logger.debug(f"Message topic: {consumer_metadata.get('topic', None)}")
            if msg:
                formatted_value = orjson.dumps(msg, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS).decode('utf-8') 
                logger.info(f"{formatted_value}")     
        except Exception as e:
            logger.error(f"Error processing message content: {e}")
