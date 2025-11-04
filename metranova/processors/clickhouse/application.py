import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor


logger = logging.getLogger(__name__)

class ApplicationMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_APPLICATION_METADATA_TABLE', 'meta_application')
        self.versioned = False  # Application metadata is not versioned
        # No hash or ref since not versioned
        self.column_defs = [
            ['id', 'String', True],
            ['insert_time', 'DateTime DEFAULT now()', False],
            ['ext', None, True],
            ['tag', 'Array(LowCardinality(String))', True],
            ['protocol', 'LowCardinality(String)', True],
            ['port_range_min', 'UInt16', True],
            ['port_range_max', 'UInt16', True]
        ]
        self.table_engine = "ReplacingMergeTree"
        self.order_by = ['protocol', 'id', 'port_range_min', 'port_range_max']
        self.val_id_field = ['id']
        self.int_fields = ['port_range_min', 'port_range_max']
        self.required_fields = [['id'], ['protocol'], ['port_range_min'], ['port_range_max']]