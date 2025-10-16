import logging
import os
from metranova.cachers.ip import IPCacher
from metranova.cachers.redis import RedisCacher
from metranova.consumers.kafka import KafkaConsumer
from metranova.pipelines.base import BasePipeline
from metranova.processors.clickhouse.base import BaseClickHouseProcessor
from metranova.processors.redis.base import BaseRedisProcessor
from metranova.writers.clickhouse import ClickHouseWriter
from metranova.writers.redis import RedisHashWriter

logger = logging.getLogger(__name__)

class KRCPipeline(BasePipeline):
    def __init__(self):
        super().__init__()

        # setup logger
        self.logger = logger

        # setup Kafka consumers
        self.consumers.append(KafkaConsumer(pipeline=self))

        # Initialize Redis connection
        self.cachers['redis'] = RedisCacher()
        self.cachers['ip'] = IPCacher()

        # Load clickhouse processors
        ch_processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        self.processors = self.load_processors(ch_processors_str, required_class=BaseClickHouseProcessor)
        if not self.processors:
            raise ValueError("At least one processor must be provided")

        # Load ClickHouse writer
        self.writers.append(ClickHouseWriter(self.processors))

        # Load Redis processors and set writer if any
        redis_processors_str = os.getenv('REDIS_PROCESSORS', '')
        self.redis_processors = self.load_processors(redis_processors_str, required_class=BaseRedisProcessor)
        if self.redis_processors:
            self.writers.append(RedisHashWriter(self.redis_processors))
