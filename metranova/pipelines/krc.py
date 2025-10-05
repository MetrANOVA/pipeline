import logging
import os
from metranova.cachers.redis import RedisCacher
from metranova.consumers.kafka import KafkaConsumer
from metranova.pipelines.base import BasePipeline
from metranova.processors.clickhouse.base import BaseClickHouseProcessor
from metranova.writers.clickhouse import ClickHouseWriter

logger = logging.getLogger(__name__)

class KRCPipeline(BasePipeline):
    def __init__(self):
        super().__init__()

        # setup logger
        self.logger = logger

        # setup Kafka consumers
        self.consumers.append(KafkaConsumer(pipeline=self))

        # Initialize Redis connection
        self.cacher = RedisCacher()

        # Load clickhouse processors
        ch_processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        self.processors = self.load_processors(ch_processors_str, required_class=BaseClickHouseProcessor)
        if not self.processors:
            raise ValueError("At least one processor must be provided")

        # Load ClickHouse writers and for each clickhouse processor
        self.writers.append(ClickHouseWriter(self.processors))

        # Load redis processors
        #TODO: Write some redis processors

        #load redis writers for each redis processor
        #TODO: Write some redis processors
    
