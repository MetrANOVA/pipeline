import importlib
import logging
from typing import Dict, Optional, List
from metranova.cachers.base import BaseCacher
from metranova.writers.base import BaseWriter
from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BasePipeline:
    def __init__(self):
        # setup logger
        self.logger = logger

        # Initialize values
        self.cacher: Optional[BaseCacher] = None
        self.writers: List[BaseWriter] = []
        self.processors: List[BaseProcessor] = []

    def process_message(self, msg, consumer_metadata: Optional[Dict] = None):
        if not msg:
            return
        for writer in self.writers:
            writer.process_message(msg, consumer_metadata)
    
    def load_processors(self, processors_str: str, required_class=BaseProcessor) -> List[BaseProcessor]:
        processors = []
        if processors_str:
            for processor_class in processors_str.split(','):
                processor_class = processor_class.strip()
                if not processor_class:
                    continue
                try:
                    module_name, class_name = processor_class.rsplit('.', 1)
                    module = importlib.import_module(module_name)
                    cls = getattr(module, class_name)
                    
                    if issubclass(cls, required_class):
                        processors.append(cls(self))
                    else:
                        self.logger.warning(f"Class {class_name} is not a subclass of {required_class.__name__}, skipping")
                        
                except (ImportError, AttributeError) as e:
                    self.logger.error(f"Failed to load processor class {class_name}: {e}")
        
            if processors:
                self.logger.info(f"Loaded {len(processors)} ClickHouse processors: {[type(p).__name__ for p in processors]}")
            else: 
                self.logger.warning("No valid ClickHouse processors loaded")
        else: 
            self.logger.warning("CLICKHOUSE_PROCESSORS environment variable is empty") 
        
        return processors

    def close(self):
        if self.writers:
            for writer in self.writers:
                writer.close()
            self.logger.info("Writers closed")
    
        if self.cacher:
            self.cacher.close()
            self.logger.info("Cacher closed")