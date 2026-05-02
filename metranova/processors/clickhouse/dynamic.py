import logging
import os
import re
from typing import Iterator
from metranova.processors.clickhouse.base import (
    BaseClickHouseMaterializedViewMixin,
)

import orjson
import yaml

logger = logging.getLogger(__name__)


class DynamicProcessor(object):
    """Processor for handling user defined measurement types"""

    def __init__(self, pipeline):
        self.pipeline = pipeline
        logger.error(f"DynamicProcessor initialized with pipeline: {pipeline}")

    def match_message(self, value: dict) -> bool:
        logger.error(f"Matching message for value: {value.values()}")
        # load measurement types
        return True

    def scale_value(self, value, scale):
        return value

    def build_message(self, value: dict, src_metadata: dict) -> list[dict[str, any]]:
        logger.error(
            f"Building message for value: {value} with metadata: {src_metadata}"
        )
        return [
            {"node": "example", "intf": "0/0/0", "rx_bytes": 1000, "tx_bytes": 2000}
        ]

    def has_match_field(self, value: dict) -> bool:
        # load measurement types
        logger.error(f"Checking match fields for value: {value.values()}")
        return True

    def has_required_fields(self, value: dict) -> bool:
        # load measurement types.
        return True

    def get_ch_dictionaries(self) -> list:
        return []

    def get_materialized_views(self) -> list:
        return []

    def load_materialized_views(
        self, env_var_name: str, mv_class: type[BaseClickHouseMaterializedViewMixin]
    ) -> None:
        return None

    def get_ip_ref_extensions(self, env_var_name: str) -> list:
        return []

    def lookup_ip_ref_extensions(self, ip_address: str, direction: str) -> dict:
        return {}

    def column_names(self) -> list:
        return []

    def message_to_columns(self, message: dict, table_name: str) -> list:
        return []

    def get_table_names(self):
        return []

    def create_table_command(self, table_name=None) -> str:
        return "SELECT 1"

    def get_extension_defs(
        self, env_var_name: str, extension_options: dict, json_column_name: str = "ext"
    ) -> list:
        return []

    def extension_is_enabled(
        self, extension_name: str, json_column_name: str = "ext"
    ) -> bool:
        return False
