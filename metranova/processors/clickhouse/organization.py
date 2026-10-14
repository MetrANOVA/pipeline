import logging
import os

from metranova.processors.clickhouse.base import (
    BaseMetadataProcessor,
    BaseClickHouseDictionaryMixin,
)

logger = logging.getLogger(__name__)

class OrganizationMetadataProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_ORGANIZATION_METADATA_TABLE', 'meta_organization')
        self.float_fields = ['latitude', 'longitude']
        self.array_fields = ['type']
        self.column_defs.extend([
            ['name', 'LowCardinality(String)', True],
            ['type', 'Array(LowCardinality(String))', True],
            ['city_name', 'LowCardinality(Nullable(String))', True],
            ['continent_name', 'LowCardinality(Nullable(String))', True],
            ['country_name', 'LowCardinality(Nullable(String))', True],
            ['country_code', 'LowCardinality(Nullable(String))', True],
            ['country_sub_name', 'LowCardinality(Nullable(String))', True],
            ['country_sub_code', 'LowCardinality(Nullable(String))', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['name']]

        # Build a ClickHouse dictionary for id -> name lookups, same idea as
        # ASDictionary in as.py. id is a String (not numeric) here, so this
        # needs COMPLEX_KEY_HASHED() rather than the plain HASHED() layout
        # ASDictionary uses -- otherwise it's the same simple exact-match
        # pattern (no array flattening needed, unlike scireg/community's
        # IP_TRIE dictionaries).
        self.dictionary_enabled = os.getenv('CLICKHOUSE_ORGANIZATION_DICTIONARY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        if self.dictionary_enabled:
            self.ch_dictionaries.append(OrganizationDictionary(self.table))


class OrganizationDictionary(BaseClickHouseDictionaryMixin):
    def __init__(self, source_table_name: str):
        super().__init__(source_table_name)
        self.dictionary_name = os.getenv('CLICKHOUSE_ORGANIZATION_DICTIONARY_NAME', 'meta_organization_dict')
        self.column_defs = [
            ['id', 'String'],
            ['name', 'String']
        ]
        self.primary_keys = ["id"]
        #miniumum and maximum lifetime in seconds
        self.lifetime_min = os.getenv('CLICKHOUSE_ORGANIZATION_DICTIONARY_LIFETIME_MIN', "600")
        self.lifetime_max = os.getenv('CLICKHOUSE_ORGANIZATION_DICTIONARY_LIFETIME_MAX', "3600")
        #set the layout, will be the full layout definition
        #note: id is a non-numeric String primary key, so this dictionary needs
        #a "complex key" layout rather than the plain HASHED() ASDictionary uses
        self.layout = "COMPLEX_KEY_HASHED()"