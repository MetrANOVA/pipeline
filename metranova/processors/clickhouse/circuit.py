import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class CircuitMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_CIRCUIT_METADATA_TABLE', 'meta_circuit')
        self.array_fields = ['endpoint_type', 'endpoint_id', 'child_circuit_id', 'parent_circuit_id']
        self.column_defs.extend([
            ['type', 'LowCardinality(Nullable(String))', True],
            ['description', 'Nullable(String)', True],
            ['state', 'LowCardinality(Nullable(String))', True],
            ['endpoint_type', 'Tuple(LowCardinality(String),LowCardinality(String))', True],
            ['endpoint_id', 'Tuple(String,String)', True],
            ['child_circuit_id', 'Array(LowCardinality(String))', True],
            ['child_circuit_ref', 'Array(Nullable(String))', True],
            ['parent_circuit_id', 'Array(LowCardinality(String))', True],
            ['parent_circuit_ref', 'Array(Nullable(String))', True]
        ])
        self.val_id_field = ['data', 'id']
        self.required_fields = [ ['data', 'id'], ['data', 'endpoint_id'], ['data', 'endpoint_type'] ]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        # lookup ref fields from redis cacher
        formatted_record.update({
            "child_circuit_ref": [self.pipeline.cacher("redis").lookup("meta_circuit", cid) for cid in formatted_record["child_circuit_id"]],
            "parent_circuit_ref": [self.pipeline.cacher("redis").lookup("meta_circuit", pid) for pid in formatted_record["parent_circuit_id"]]
        })

        return formatted_record