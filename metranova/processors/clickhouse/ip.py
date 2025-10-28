import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class IPMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_IP_METADATA_TABLE', 'meta_ip')
        self.float_fields = ['latitude', 'longitude']
        self.array_fields = ['ip_subnet']
        self.int_fields = ['as_id']
        self.column_defs.extend([
            ['ip_subnet', 'Array(Tuple(IPv6,UInt8))', True],
            ['city_name', 'LowCardinality(Nullable(String))', True],
            ['continent_name', 'LowCardinality(Nullable(String))', True],
            ['country_name', 'LowCardinality(Nullable(String))', True],
            ['country_code', 'LowCardinality(Nullable(String))', True],
            ['country_sub_name', 'LowCardinality(Nullable(String))', True],
            ['country_sub_code', 'LowCardinality(Nullable(String))', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True],
            ['as_id', 'Nullable(UInt32)', True],
            ['as_ref', 'Nullable(String)', True]
        ])
        self.val_id_field = ['data', 'id']
        self.required_fields = [ ['data', 'id'], ['data', 'ip_subnet'] ]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #make sure that each item in ip_subnet is a tuple of (ip, prefix length) where first element is string and second is int
        ip_subnets = []
        for item in formatted_record.get('ip_subnet', []):
            if isinstance(item, (list, tuple)) and len(item) == 2:
                try:
                    ip_subnets.append((str(item[0]), int(item[1])))
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid ip_subnet item: {item}")
        formatted_record['ip_subnet'] = ip_subnets

        # lookup ref fields from cacher
        cached_as_info = self.pipeline.cacher("clickhouse").lookup("meta_as", formatted_record.get('as_id', None))
        if cached_as_info:
            formatted_record["as_ref"] = cached_as_info.get(self.db_ref_field, None)
        else:
            formatted_record["as_ref"] = None

        return formatted_record