import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class ASMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_AS_METADATA_TABLE', 'meta_as')
        self.float_fields = ['latitude', 'longitude']
        #AS id is a UInt32
        self.int_fields = ['id']
        self.column_defs[0] = ['id', 'UInt32', True]
        self.column_defs.extend([
            ['name', 'LowCardinality(String)', True],
            ['city_name', 'LowCardinality(Nullable(String))', True],
            ['continent_name', 'LowCardinality(Nullable(String))', True],
            ['country_name', 'LowCardinality(Nullable(String))', True],
            ['country_code', 'LowCardinality(Nullable(String))', True],
            ['country_sub_name', 'LowCardinality(Nullable(String))', True],
            ['country_sub_code', 'LowCardinality(Nullable(String))', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True],
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
        self.val_id_field = ['data', 'id']
        self.required_fields = [ ['data', 'id'], ['data', 'name'], ['data', 'organization_id'] ]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        # lookup ref fields from clickhouse cacher
        cached_org_info = self.pipeline.cacher("clickhouse").lookup("meta_organization", formatted_record.get('organization_id', None))
        if cached_org_info:
            formatted_record["organization_ref"] = cached_org_info.get(self.db_ref_field, None)

        return formatted_record