from metranova.processors.clickhouse.base import BaseClickHouseProcessor
from typing import Iterator, Dict, Any
import os

class InterfaceBaseProcessor(BaseClickHouseProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IF_BASE_TABLE', 'if_base')
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            `start_ts` DateTime64(3, 'UTC') CODEC(Delta,ZSTD),
            `insert_ts` DateTime64(3, 'UTC') DEFAULT now64() CODEC(Delta,ZSTD),
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `id` LowCardinality(String),
            `device` LowCardinality(String),
            `name` LowCardinality(String),
            `collector_id` LowCardinality(Nullable(String)),
            `in_bits` Nullable(UInt64) CODEC(Delta,ZSTD),
            `in_pkts` Nullable(UInt64) CODEC(Delta,ZSTD),
            `in_errors` Nullable(UInt64) CODEC(Delta,ZSTD),
            `in_discards` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_bits` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_pkts` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_errors` Nullable(UInt64) CODEC(Delta,ZSTD),
            `out_discards` Nullable(UInt64) CODEC(Delta,ZSTD)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`id`, `start_ts`)
        TTL start_ts + INTERVAL 7 DAY
        SETTINGS index_granularity = 8192
        """

        self.column_names = [
            "start_ts",
            "policy_originator",
            "policy_level",
            "policy_scopes",
            "id",
            "device",
            "name",
            "collector_id",
            "in_bits",
            "in_pkts",
            "in_errors",
            "in_discards",
            "out_bits",
            "out_pkts",
            "out_errors",
            "out_discards"
        ]

        self.required_fields = [
            ['start'], 
            ['meta', 'id'], 
            ['meta', 'device'], 
            # name should be too but found some without, so handedle in build_message
        ]

    def match_message(self, value):
        #make sure we have at least one of values fields we care about
        values_fields = ['in_bits', 'in_pkts', 'in_errors', 'in_discards', 'out_bits', 'out_pkts', 'out_errors', 'out_discards']
        if not any(value.get('values', {}).get(f, {}).get("val", None) is not None for f in values_fields):
            self.logger.debug(f"Missing all values fields in message value")
            return False

        return True

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # handle special case for name. If missing parse from meta.id which is a string of form <device>::<name>
        if value.get('meta', {}).get('name', None) is None:
            meta_id = value.get('meta', {}).get('id', '')
            if '::' in meta_id:
                parts = meta_id.split('::', 1)
                if len(parts) == 2:
                    value.setdefault('meta', {})['name'] = parts[1]
        # double check name is now present
        if value.get('meta', {}).get('name', None) is None:
            self.logger.error(f"Missing required field 'name' in message value and has no parsable meta.id")
            return None

        # Build message dictionary
        return [{
            "start_ts": value.get("start"),
            "policy_originator": value.get("policy", {}).get("originator", None),
            "policy_level": value.get("policy", {}).get("level", None),
            "policy_scopes": value.get("policy", {}).get("scopes", []),
            "id": value.get("meta", {}).get("id", None),
            "device": value.get("meta", {}).get("device", None),
            "name": value.get("meta", {}).get("name", None),
            "collector_id": value.get("meta", {}).get("sensor_id", None)[0] if isinstance(value.get("meta", {}).get("sensor_id", None), list) else value.get("meta", {}).get("sensor_id", None),
            "in_bits": value.get("values", {}).get("in_bits", {}).get("val", None),
            "in_pkts": value.get("values", {}).get("in_pkts", {}).get("val", None),
            "in_errors": value.get("values", {}).get("in_errors", {}).get("val", None),
            "in_discards": value.get("values", {}).get("in_discards", {}).get("val", None),
            "out_bits": value.get("values", {}).get("out_bits", {}).get("val", None),
            "out_pkts": value.get("values", {}).get("out_pkts", {}).get("val", None),
            "out_errors": value.get("values", {}).get("out_errors", {}).get("val", None),
            "out_discards": value.get("values", {}).get("out_discards", {}).get("val", None)
        }]