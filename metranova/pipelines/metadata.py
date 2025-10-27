import logging
import os

from metranova.cachers.clickhouse import ClickHouseCacher
from metranova.cachers.ip import IPCacher
from metranova.cachers.redis import RedisCacher
from metranova.consumers.clickhouse import IPMetadataClickHouseConsumer, MetadataClickHouseConsumer
from metranova.consumers.file import MetadataYAMLFileConsumer
from metranova.consumers.redis import RedisHashConsumer
from metranova.pipelines.base import BasePipeline
from metranova.processors.file.base import BaseFileProcessor
from metranova.writers.clickhouse import ClickHouseWriter
from metranova.writers.file import PickleFileWriter
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
        self.cachers["clickhouse"] = ClickHouseCacher()
        self.cachers['redis'] = RedisCacher()
        self.cachers['ip'] = IPCacher()

        # set processor to METADATA PROCESSORS
        ch_processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        self.processors = self.load_processors(ch_processors_str)
        if not self.processors:
            raise ValueError("At least one processor must be provided for metadata pipeline")

        # Add Redis consumers
        self.consumers.append(RedisHashConsumer(pipeline=self))

        # Add ClickHouse writer
        self.writers.append(ClickHouseWriter(processors=self.processors))

class FCMetadataPipeline(BasePipeline):
    """Pipeline to load metadata from File to Clickhouse"""
    def __init__(self):
        super().__init__()
        # setup logger
        self.logger = logger

        # add clickerhouse cacher
        self.cachers["clickhouse"] = ClickHouseCacher()
        self.cachers['redis'] = RedisCacher()
        self.cachers['ip'] = IPCacher()

        # set processor to METADATA PROCESSORS
        ch_processors_str = os.getenv('CLICKHOUSE_PROCESSORS', '')
        self.processors = self.load_processors(ch_processors_str)
        if not self.processors:
            raise ValueError("At least one processor must be provided for metadata pipeline")

        # Add Redis consumers
        self.consumers.append(MetadataYAMLFileConsumer(pipeline=self))

        # Add ClickHouse writer
        self.writers.append(ClickHouseWriter(processors=self.processors))

class IPTrieMetadataPipeline(BasePipeline):
    """Pipeline to load IP metadata from ClickHouse to a IP Trie pickle file"""
    def __init__(self):
        super().__init__()
        self.logger = logger

        #Add ClickHouse consumers
        self.consumers.append(IPMetadataClickHouseConsumer(pipeline=self))

        # set processor to FILE PROCESSORS
        file_processors_str = os.getenv('FILE_PROCESSORS', '')
        self.processors = self.load_processors(file_processors_str, required_class=BaseFileProcessor)
        if not self.processors:
            raise ValueError("At least one processor must be provided for metadata pipeline")

        # Add IP Trie writer
        self.writers.append(PickleFileWriter(processors=self.processors))