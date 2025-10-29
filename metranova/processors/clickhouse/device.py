import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class DeviceMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_DEVICE_METADATA_TABLE', 'meta_device')
        self.float_fields = ['latitude', 'longitude']
        self.array_fields = ['loopback_ip', 'management_ip', 'location_type']
        self.column_defs.extend([
            ['type', 'LowCardinality(String)', True],
            ['loopback_ip', 'Array(IPv6)', True],
            ['management_ip', 'Array(IPv6)', True],
            ['hostname', 'LowCardinality(Nullable(String))', True],
            ['location_name', 'LowCardinality(Nullable(String))', True],
            ['location_type', 'Array(LowCardinality(Nullable(String)))', True],
            ['city_name', 'LowCardinality(Nullable(String))', True],
            ['continent_name', 'LowCardinality(Nullable(String))', True],
            ['country_name', 'LowCardinality(Nullable(String))', True],
            ['country_code', 'LowCardinality(Nullable(String))', True],
            ['country_sub_name', 'LowCardinality(Nullable(String))', True],
            ['country_sub_code', 'LowCardinality(Nullable(String))', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True],
            ['manufacturer', 'LowCardinality(Nullable(String))', True],
            ['model', 'LowCardinality(Nullable(String))', True],
            ['network', 'LowCardinality(Nullable(String))', True],
            ['os', 'LowCardinality(Nullable(String))', True],
            ['role', 'LowCardinality(Nullable(String))', True],
            ['state', 'LowCardinality(Nullable(String))', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['type']]