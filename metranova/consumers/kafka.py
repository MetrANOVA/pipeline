import logging
from metranova.connectors.kafka import KafkaConnector
import orjson
from metranova.pipelines.base import BasePipeline
from metranova.consumers.base import BaseConsumer
from confluent_kafka import KafkaError

logger = logging.getLogger(__name__)

class KafkaConsumer(BaseConsumer):
    """Kafka consumer with SSL authentication using confluent-kafka"""
    
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.datasource = KafkaConnector()
    
    def consume_messages(self):
        """Consume messages from Kafka"""
        msg = self.datasource.client.poll(timeout=1.0)
        if msg is None:
            return
        
        # check for errors
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                self.logger.debug(f"End of partition reached {msg.topic()}/{msg.partition()}")
                return
            else:
                self.logger.error(f"Kafka error: {msg.error()}")
                return

        # format the response as JSON
        msg_data = orjson.loads(msg.value()) if msg.value() else None
        msg_metadata = {
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'timestamp': msg.timestamp()[1] if msg.timestamp() else None,
            'key': msg.key().decode('utf-8') if msg.key() else None
        }

        # Process the message
        try:
            self.pipeline.process_message(msg_data, consumer_metadata=msg_metadata)
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            # Continue processing other messages

    def close(self):
        """Close Kafka consumer"""
        if self.datasource.client:
            self.logger.info("Closing Kafka consumer...")
            try:
                self.datasource.client.close(timeout=5.0)  # 5 second timeout
            except Exception as e:
                self.logger.info(f"Kafka consumer closed with message: {e}")