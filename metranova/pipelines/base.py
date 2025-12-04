import importlib
import logging
import threading
import yaml
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
                thread = threading.Thread(target=consumer.consume, name=f"Consumer-{type(consumer).__name__}")
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
    
    def load_classes(self, class_str: str, init_args={}, required_class=None) -> List:
        classes = []
        if class_str:
            for class_name in class_str.split(','):
                class_name = class_name.strip()
                if not class_name:
                    continue
                try:
                    module_name, class_name = class_name.rsplit('.', 1)
                    module = importlib.import_module(module_name)
                    cls = getattr(module, class_name)
                    
                    if required_class is None or issubclass(cls, required_class):
                        classes.append(cls(**init_args))
                    else:
                        self.logger.warning(f"Class {class_name} is not a subclass of {required_class.__name__}, skipping")
                        
                except (ImportError, AttributeError) as e:
                    self.logger.error(f"Failed to load class {class_name}: {e}")
        
            if classes:
                self.logger.info(f"Loaded {len(classes)} classes: {[type(p).__name__ for p in classes]}")
            else: 
                self.logger.warning("No valid ClickHouse processors loaded")
        else: 
            self.logger.warning("Processors string is empty") 
        
        return classes

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

    def __str__(self):
        #return a string that lists the consumers, cachers, and writer names and writer proccessor names
        consumer_names = [type(c).__name__ for c in self.consumers]
        cacher_names = [f"{name}:{type(c).__name__}" for name, c in self.cachers.items()]
        writer_names = []
        for w in self.writers:
            processor_names = [type(p).__name__ for p in w.processors] if hasattr(w, 'processors') else []
            writer_names.append(f"{type(w).__name__} (Processors: {processor_names})")
        return f"Pipeline(Consumers: {consumer_names}, Cachers: {cacher_names}, Writers: {writer_names})"

        
class YAMLPipeline(BasePipeline):
    def __init__(self, yaml_file: dict):
        super().__init__()
        self.yaml_file = yaml_file
        self.logger = logger
        
        #load yaml content
        yaml_config = None
        try:
            with open(self.yaml_file, 'r') as file:
                yaml_config = yaml.safe_load(file)
            self.logger.info(f"Loaded pipeline YAML configuration from {self.yaml_file}")
            self.logger.debug(f"YAML content: {yaml_config}")
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file: {e}")
            raise e
        if not yaml_config:
            raise ValueError("YAML configuration is empty or invalid")
 
        #load consumers
        for consumer_cfg in yaml_config.get('consumers', []):
            consumer_type = consumer_cfg.get('type')
            if consumer_type:
                self.consumers = self.load_classes(consumer_type, init_args={'pipeline': self}, required_class=BaseConsumer)
        
        #load cachers
        for cacher_cfg in yaml_config.get('cachers', []):
            cacher_type = cacher_cfg.get('type')
            cacher_name = cacher_cfg.get('name', None)
            if  cacher_type and cacher_name:
                cacher_instances = self.load_classes(cacher_type, required_class=BaseCacher)
                if cacher_instances:
                    self.cachers[cacher_name] = cacher_instances[0]
        
        #load writers
        for writer_cfg in yaml_config.get('writers', []):
            #get writer type
            writer_type = writer_cfg.get('type', None)
            if not writer_type:
                self.logger.warning("Writer type not specified in YAML, skipping")
                continue
            #load processors for writer if any
            processor_list = []
            for processor_type in writer_cfg.get('processors', []):
                processor_instances = self.load_classes(processor_type, init_args={'pipeline': self}, required_class=BaseProcessor)
                processor_list.extend(processor_instances)
            #load writer with processors if any
            writer_instances = self.load_classes(writer_type, init_args={'processors': processor_list}, required_class=BaseWriter)
            self.writers.extend(writer_instances)