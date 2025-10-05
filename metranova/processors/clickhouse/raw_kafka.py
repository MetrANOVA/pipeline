from metranova.processors.clickhouse.base import BaseClickHouseProcessor
from typing import Iterator, Dict, Any
import orjson
import os

class RawKafkaProcessor(BaseClickHouseProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_RAW_KAFKA_TABLE', 'kafka_messages')
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            timestamp DateTime64(3) DEFAULT now64(),
            topic String,
            partition UInt32,
            offset UInt64,
            key Nullable(String),
            value String
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, topic, partition)
        """
        self.column_names = ['topic', 'partition', 'offset', 'key', 'value']

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        return [{
            'topic': msg_metadata.get('topic', '') if msg_metadata else '',
            'partition': msg_metadata.get('partition', 0) if msg_metadata else 0,
            'offset': msg_metadata.get('offset', 0) if msg_metadata else 0,
            'key': msg_metadata.get('key') if msg_metadata else None,
            'value': orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        }]

    def message_to_columns(self, message: dict) -> list:
        return [
            message.get('topic', ''),
            message.get('partition', 0),
            message.get('offset', 0),
            message.get('key', None),
            message.get('value', '')
        ]
