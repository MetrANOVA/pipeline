import logging
import os

from metranova.cachers.clickhouse import ClickHouseCacher
from metranova.consumers.clickhouse import MetadataClickHouseConsumer
from metranova.consumers.redis import RedisHashConsumer
from metranova.pipelines.base import BasePipeline
from metranova.writers.clickhouse import ClickHouseWriter
from metranova.writers.redis import RedisMetadataRefWriter

logger = logging.getLogger(__name__)

class CRMetadataPipeline(BasePipeline):
    """Pipeline to load metadata from ClickHouse to Redis"""
    def __init__(self):
        super().__init__()
        # setup logger
        self.logger = logger

        #Add ClickHouse consumers
        self.consumers.append(MetadataClickHouseConsumer(pipeline=self))

        # Add Redis writer
        self.writers.append(RedisMetadataRefWriter(processors=[]))

        # NOTE: No processors or cacher needed for this pipeline

class RCMetadataPipeline(BasePipeline):
    """Pipeline to load metadata from Redis to Clickhouse"""
    def __init__(self):
        super().__init__()
        # setup logger
        self.logger = logger

        # add clickerhouse cacher
        self.cacher = ClickHouseCacher()

        # set processor to METADATA PROCESSORS
        meta_processors_str = os.getenv('METADATA_PROCESSORS', '')
        self.processors = self.load_processors(meta_processors_str)
        if not self.processors:
            raise ValueError("At least one processor must be provided for metadata pipeline")

        # Add Redis consumers
        self.consumers.append(RedisHashConsumer(pipeline=self))

        # Add ClickHouse writer
        self.writers.append(ClickHouseWriter(processors=self.processors))