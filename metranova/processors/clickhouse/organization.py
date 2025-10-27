import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class OrganizationMetadataProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_ORGANIZATION_METADATA_TABLE', 'meta_organization')
        self.float_fields = ['latitude', 'longitude']
        self.array_fields = ['type', 'funding_agency']
        self.column_defs.extend([
            ['name', 'LowCardinality(String)', True],
            ['type', 'Array(LowCardinality(String))', True],
            ['funding_agency', 'Array(LowCardinality(String))', True],
            ['city_name', 'LowCardinality(Nullable(String))', True],
            ['continent_name', 'LowCardinality(Nullable(String))', True],
            ['country_name', 'LowCardinality(Nullable(String))', True],
            ['country_code', 'LowCardinality(Nullable(String))', True],
            ['country_sub_name', 'LowCardinality(Nullable(String))', True],
            ['country_sub_code', 'LowCardinality(Nullable(String))', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True]
        ])
        self.val_id_field = ['data', 'id']
        self.required_fields = [ ['data', 'id'], ['data', 'name'] ]