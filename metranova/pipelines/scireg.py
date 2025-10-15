import logging
import os

from metranova.pipelines.base import BasePipeline
from metranova.writers.clickhouse import ClickHouseWriter
from metranova.consumers.http import HTTPConsumer

logger = logging.getLogger(__name__)


class ScienceRegistryPipeline(BasePipeline):
    """Pipeline to load metadata from Redis to Clickhouse"""
    def __init__(self):
        super().__init__()
        # setup logger
        self.logger = logger

        # set processor to METADATA PROCESSORS
        ch_processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        self.processors = self.load_processors(ch_processors_str)
        if not self.processors:
            raise ValueError("At least one processor must be provided for metadata pipeline")

        # Add HTTP consumer
        self.consumers.append(HTTPConsumer(pipeline=self, env_prefix='SCIREG'))

        # Add ClickHouse writer
        self.writers.append(ClickHouseWriter(processors=self.processors))