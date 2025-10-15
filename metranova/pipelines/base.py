import importlib
import logging
import threading
from typing import Dict, Optional, List
from metranova.cachers.base import BaseCacher, NoOpCacher
from metranova.consumers.base import BaseConsumer
from metranova.writers.base import BaseWriter
from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BasePipeline:
    def __init__(self):
        # setup logger
        self.logger = logger

        # Initialize values
        self.consumers: List[BaseConsumer] = []
        self.processors: List[BaseProcessor] = []
        self.cachers: Dict[str, BaseCacher] = {}
        self.writers: List[BaseWriter] = []
        self.consumer_threads: List[threading.Thread] = []

    def start(self):
        if self.consumers:
            for consumer in self.consumers:
                thread = threading.Thread(target=consumer.consume_messages, name=f"Consumer-{type(consumer).__name__}")
                thread.daemon = True
                thread.start()
                self.consumer_threads.append(thread)
            self.logger.info(f"Started {len(self.consumer_threads)} consumer threads")
        else:
            self.logger.warning("No consumers to start")

        # block until threads are done (they won't be, unless there's an error)       
        for thread in self.consumer_threads:
            thread.join()

    def cacher(self, name: str) -> BaseCacher:
        # Return NoOpCacher if not found so don't have to check for None
        return self.cachers.get(name, NoOpCacher())

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
            self.logger.warning("Processors string is empty") 
        
        return processors

    def close(self):
        if self.consumers:
            for consumer in self.consumers:
                consumer.close()
            self.logger.info("Consumers closed")

        # Wait for all consumer threads to finish
        if self.consumer_threads:
            self.logger.info("Waiting for consumer threads to finish...")
            for thread in self.consumer_threads:
                thread.join(timeout=10.0)  # Wait up to 10 seconds per thread
                if thread.is_alive():
                    self.logger.warning(f"Thread {thread.name} did not finish within timeout")
            self.consumer_threads.clear()
            self.logger.info("Consumer threads cleanup completed")

        if self.writers:
            for writer in self.writers:
                writer.close()
            self.logger.info("Writers closed")

        if self.cachers:
            for cacher in self.cachers.values():
                cacher.close()
            self.logger.info("Cachers closed")