from metranova.processors.clickhouse.base import BaseClickHouseProcessor
from typing import Iterator, Dict, Any
import orjson
import os

class RawKafkaProcessor(BaseClickHouseProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_RAW_KAFKA_TABLE', 'data_kafka_message')
        self.column_defs = [
            ['timestamp', 'DateTime64(3)', False],
            ['topic', 'String', True],
            ['partition', 'UInt32', True],
            ['offset', 'UInt64', True],
            ['key', 'Nullable(String)', True],
            ['value', 'String', True]
        ]
        self.order_by = ['timestamp', 'topic', 'partition']

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        return [{
            'topic': msg_metadata.get('topic', '') if msg_metadata else '',
            'partition': msg_metadata.get('partition', 0) if msg_metadata else 0,
            'offset': msg_metadata.get('offset', 0) if msg_metadata else 0,
            'key': msg_metadata.get('key') if msg_metadata else None,
            'value': orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        }]
