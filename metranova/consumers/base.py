import logging
import time

logger = logging.getLogger(__name__)

class BaseConsumer:
    
    def __init__(self, pipeline):
        # Initialize pipeline
        self.pipeline = pipeline
        self.logger = logger
    
    def pre_consume_messages(self):
        """Hook to run before starting message consumption, e.g. to prime cachers"""
        return

    def post_consume_messages(self):
        """Hook to run after finishing message consumption"""
        return True

    def consume_messages(self):
        """Main method to consume messages, to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement consume_messages method")

    def consume(self):
        # check connection and tables
        if self.datasource and not self.datasource.client:
            self.logger.error("Datasource client not initialized")
            return
        # Run the consumer
        try:
            while True:
                self.pre_consume_messages()
                self.consume_messages()
                if not self.post_consume_messages():
                    break
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, stopping consumer...")
        finally:
            self.close()

    def close(self):
        """Close operation if needed"""
        return

class TimedIntervalConsumer(BaseConsumer):
    def __init__(self, pipeline: BaseConsumer):
        super().__init__(pipeline)
        self.logger = logger
        # Override below in subclass
        self.update_interval = -1  # in seconds, -1 means run once
        self.auto_prime_cachers = True

    def pre_consume_messages(self):
        """Hook to run before starting message consumption, e.g. to prime cachers"""
        # Prime cacher if exists
        if self.auto_prime_cachers and self.pipeline.cachers:
            for cacher in self.pipeline.cachers.values():
                cacher.prime()

    def post_consume_messages(self):
        # Break if no update interval is set
        if self.update_interval <= 0:
            return False  # Run once if no interval is set
        # Sleep before next update
        self.logger.info(f"Sleeping for {self.update_interval} seconds before next update")
        time.sleep(self.update_interval)
        return True  # Continue running

