import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor, BaseClickHouseDictionaryMixin


logger = logging.getLogger(__name__)

class ApplicationMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_APPLICATION_METADATA_TABLE', 'meta_application')
        self.versioned = False  # Application metadata is not versioned
        self.dictionary_enabled = os.getenv('CLICKHOUSE_APPLICATION_DICTIONARY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        if self.dictionary_enabled:
            self.ch_dictionaries.append(ApplicationDictionary(self.table))
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

class ApplicationDictionary(BaseClickHouseDictionaryMixin):
    def __init__(self, source_table_name: str):
        super().__init__(source_table_name)
        self.dictionary_name = os.getenv('CLICKHOUSE_APPLICATION_DICTIONARY_NAME', 'meta_application_dict')
        self.column_defs = [
            ['id', 'String'],
            ['protocol', 'String'],
            ['port_range_min', 'UInt16'],
            ['port_range_max', 'UInt16']
        ]
        self.primary_keys = ["protocol"]
        #miniumum and maximum lifetime in seconds
        self.lifetime_min = os.getenv('CLICKHOUSE_APPLICATION_DICTIONARY_LIFETIME_MIN', "600")
        self.lifetime_max = os.getenv('CLICKHOUSE_APPLICATION_DICTIONARY_LIFETIME_MAX', "3600")
        #set the layout, will be the full layout definition
        self.layout = "RANGE_HASHED(range_lookup_strategy 'min')"
        self.layout_range_min = "port_range_min"
        self.layout_range_max = "port_range_max"