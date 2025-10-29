import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class CircuitMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_CIRCUIT_METADATA_TABLE', 'meta_circuit')
        self.array_fields = ['endpoint_type', 'endpoint_id']
        self.column_defs.extend([
            ['type', 'LowCardinality(Nullable(String))', True],
            ['description', 'Nullable(String)', True],
            ['state', 'LowCardinality(Nullable(String))', True],
            ['endpoint_type', 'Tuple(LowCardinality(String),LowCardinality(String))', True],
            ['endpoint_id', 'Tuple(String,String)', True],
            ['parent_circuit_id', 'LowCardinality(Nullable(String))', True],
            ['parent_circuit_ref', 'Nullable(String)', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['endpoint_id'], ['endpoint_type']]
        self.self_ref_fields = ['parent_circuit']

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        # lookup ref fields from cacher
        cached_parent_info = self.pipeline.cacher("clickhouse").lookup("meta_circuit", formatted_record.get('parent_circuit_id', None))
        if cached_parent_info:
            formatted_record["parent_circuit_ref"] = cached_parent_info.get(self.db_ref_field, None)
        else:
            formatted_record["parent_circuit_ref"] = None

        return formatted_record