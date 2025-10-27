import logging
import os

import orjson

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class DeviceMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_DEVICE_METADATA_TABLE', 'meta_device')
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
        self.val_id_field = ['data', 'id']
        self.required_fields = [ ['data', 'id'], ['data', 'type'] ]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #format potential JSON strings from redis
        json_array_fields = ['loopback_ip', 'management_ip', 'location_type']
        for field in json_array_fields:
            if formatted_record[field] is None:
                formatted_record[field] = []
            elif isinstance(formatted_record[field], str):
                try:
                    formatted_record[field] = orjson.loads(formatted_record[field])
                except orjson.JSONDecodeError:
                    formatted_record[field] = []

        # format float fields
        float_fields = ['latitude', 'longitude']
        for field in float_fields:
            if formatted_record.get(field, None) is not None:
                try:
                    formatted_record[field] = float(formatted_record[field])
                except (TypeError, ValueError):
                    formatted_record[field] = None

        return formatted_record