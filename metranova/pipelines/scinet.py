import logging
import os

from metranova.cachers.clickhouse import ClickHouseCacher
from metranova.pipelines.base import BasePipeline
from metranova.writers.clickhouse import ClickHouseWriter
from metranova.consumers.file import JSONFileConsumer

logger = logging.getLogger(__name__)


class SCinetMetadataPipeline(BasePipeline):
    """Pipeline to load metadata from JSON file to Clickhouse"""
    def __init__(self):
        super().__init__()
        # setup logger
        self.logger = logger

        # add clickehouse cacher to load existing metadata references
        self.cachers["clickhouse"] = ClickHouseCacher()

        # set processor
        ch_processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        self.processors = self.load_processors(ch_processors_str)
        if not self.processors:
            raise ValueError("At least one processor must be provided for pipeline")

        # Add File consumer
        self.consumers.append(JSONFileConsumer(pipeline=self, env_prefix='SCINET'))

        # Add ClickHouse writer
        self.writers.append(ClickHouseWriter(processors=self.processors))