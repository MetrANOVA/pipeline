import logging

from metranova.consumers.clickhouse import MetadataClickHouseConsumer
from metranova.pipelines.base import BasePipeline
from metranova.writers.redis import RedisWriter

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
        self.writers.append(RedisWriter(processors=[]))

        # NOTE: No processors or cacher needed for this pipeline
