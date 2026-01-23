import logging
import os

from metranova.processors.clickhouse.base import BaseClickHouseDictionaryMixin, BaseMetadataProcessor

logger = logging.getLogger(__name__)

class ASMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_AS_METADATA_TABLE', 'meta_as')
        self.dictionary_enabled = os.getenv('CLICKHOUSE_AS_DICTIONARY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        if self.dictionary_enabled:
            self.ch_dictionaries.append(ASDictionary(self.table))
        #AS id is a UInt32
        self.int_fields = ['id']
        self.column_defs[0] = ['id', 'UInt32', True]
        self.column_defs.extend([
            ['name', 'LowCardinality(String)', True],
            ['organization_id', 'String', True],
            ['organization_ref', 'Nullable(String)', True],
        ])
        extension_options = {
            "peeringdb": [
                ['peeringdb_ipv6', 'Nullable(Boolean)', True],
                ['peeringdb_prefixes4', 'Nullable(UInt32)', True],
                ['peeringdb_prefixes6', 'Nullable(UInt32)', True],
                ['peeringdb_ratio', 'Nullable(String)', True],
                ['peeringdb_scope', 'Nullable(String)', True],
                ['peeringdb_traffic', 'Nullable(String)', True],
                ['peeringdb_type', 'Array(String)', True]
            ]
        }
        # determine columns to use from environment
        self.extension_defs['ext'] = self.get_extension_defs('CLICKHOUSE_AS_METADATA_EXTENSIONS', extension_options)
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['name'], ['organization_id']]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        # lookup ref fields from clickhouse cacher
        cached_org_info = self.pipeline.cacher("clickhouse").lookup("meta_organization", formatted_record.get('organization_id', None))
        if cached_org_info:
            formatted_record["organization_ref"] = cached_org_info.get(self.db_ref_field, None)
        else:
            formatted_record["organization_ref"] = None

        return formatted_record

class ASDictionary(BaseClickHouseDictionaryMixin):
    def __init__(self, source_table_name: str):
        super().__init__(source_table_name)
        self.dictionary_name = os.getenv('CLICKHOUSE_AS_DICTIONARY_NAME', 'meta_as_dict')
        self.column_defs = [
            ['id', 'UInt32'],
            ['name', 'String']
        ]
        self.primary_keys = ["id"]
        #miniumum and maximum lifetime in seconds
        self.lifetime_min = os.getenv('CLICKHOUSE_AS_DICTIONARY_LIFETIME_MIN', "600")
        self.lifetime_max = os.getenv('CLICKHOUSE_AS_DICTIONARY_LIFETIME_MAX', "3600")
        #set the layout, will be the full layout definition
        self.layout = "HASHED()"