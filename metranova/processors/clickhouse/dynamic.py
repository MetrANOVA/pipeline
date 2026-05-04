import logging
import os
import re
from typing import Iterator
from metranova.processors.clickhouse.base import (
    BaseClickHouseMaterializedViewMixin,
)
from clickhouse_connect.driver.httpclient import HttpClient

import orjson
import yaml

logger = logging.getLogger(__name__)


class DynamicProcessor(object):
    """Processor for handling user defined measurement types"""

    def __init__(self, pipeline):
        self.pipeline = pipeline
        logger.error(f"DynamicProcessor initialized with pipeline: {pipeline}")

        self.datatypes = {}
        self.columns = {}

    @property
    def table(self):
        """Total hack to allow dynamic table names"""
        return "dynamic_table"

    # Abuse is / not logic to allow for both true/false and table name return values
    def match_message(self, value: dict) -> str:
        logger.error(f"Matching message for value: {value}")

        datatypes = self.api.get_data_types()
        logger.info(datatypes)

        for datatype in datatypes:
            matches = True
            for field in [
                f["field_name"]
                for f in datatype["data_fields"]
                if f["nullable"] is False
            ]:
                if field not in value["tags"]:
                    logger.info(
                        f"Field {field} not found in {datatype['name']}.data_fields"
                    )
                    matches = False
                    break
            if matches:
                logger.info(f"All fields matched for datatype {datatype['name']}.")
                return "data_" + datatype["name"]
        return None

    def scale_value(self, value, scale):
        return value

    def build_message(self, value: dict, src_metadata: dict) -> list[dict[str, any]]:
        logger.error(f"Building message: {value} with metadata: {src_metadata}")
        result = value["fields"]
        result["node"] = value["tags"].get("node", "unknown")
        result["insert_time"] = value["timestamp"]
        table_name = self.match_message(value)

        self.columns[table_name] = list(result.keys())
        result["_clickhouse_table"] = table_name
        return [result]

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
        """Used to declare table names to be created on ClickHouse side

        We return an empty list here because the dynamic processor is designed to handle
        dynamic table names that are created by users via API.
        """
        return []

    def load_materialized_views(
        self, env_var_name: str, mv_class: type[BaseClickHouseMaterializedViewMixin]
    ) -> None:
        return None

    def get_ip_ref_extensions(self, env_var_name: str) -> list:
        return []

    def lookup_ip_ref_extensions(self, ip_address: str, direction: str) -> dict:
        return {}

    def column_names(self, table_name: str = None) -> list:
        logger.info(f"Getting column names for table: {table_name}")
        return self.columns.get(table_name, [])

    def message_to_columns(self, message: dict, table_name: str) -> list:
        return list(message.values())

    def get_table_names(self) -> list:
        """Used to declare table names to be created on ClickHouse side

        We return an empty list here because the dynamic processor is designed to handle
        dynamic table names that are created by users via API.
        """
        return []

    def create_table_command(self, table_name=None) -> str:
        """Use to create the tables returned by get_table_names

        We ignore the table_name parameter here because the dynamic processor is designed
        to handle dynamic table names that are created by users via API. Instead, we
        return a command that will always succeed.
        """
        return "SELECT 1"

    def get_extension_defs(
        self, env_var_name: str, extension_options: dict, json_column_name: str = "ext"
    ) -> list:
        return []

    def extension_is_enabled(
        self, extension_name: str, json_column_name: str = "ext"
    ) -> bool:
        return False

    def set_clickhouse_client(self, client: HttpClient) -> None:
        """Set the ClickHouse client for this processor and any child classes that need it

        The primary purpose of this method is to grant the processor direct access to the ClickHouse client.
        While in most cases the processor will declare the database schema itself, the dynamic pipeline is
        required to access the type definitions directly as they are declared via API.
        """
        self.ch_client = client
        self.api = MetranovaSyncApi(client)


class MetranovaSyncApi:
    """Class for interacting with Metranova Sync API"""

    def __init__(self, client: HttpClient):
        self.client = client

    def get_data_types(self) -> list[dict]:
        """Get the data types defined in Metranova Sync"""
        try:
            result = self.client.query(f"""
                SELECT * FROM metranova.definition WHERE notEmpty(data_fields)
            """)
            return list(result.named_results())
        except Exception as e:
            logger.exception(f"Error retrieving all resource types: {e}")
            return None
