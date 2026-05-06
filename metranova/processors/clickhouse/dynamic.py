import datetime
import logging

from metranova.processors.clickhouse.base import (
    BaseClickHouseMaterializedViewMixin,
)
from clickhouse_connect.driver.httpclient import HttpClient

logger = logging.getLogger(__name__)


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


class ResourceDefinition:
    """Class representing a resource definition as defined by Metranova API"""

    _FIXED_COLUMNS = ["insert_time"]

    def __init__(self, definition: dict):
        self._name: str = definition["name"]
        self._data_fields: list[dict] = definition.get("data_fields", [])
        self._required_field_names: list[str] = [
            f["field_name"] for f in self._data_fields if f["nullable"] is False
        ]
        self._data_field_names: list[str] = [f["field_name"] for f in self._data_fields]
        logger.info(
            f"Initialized ResourceDefinition for {self._name} with required fields {self._required_field_names}."
        )

    @property
    def table_name(self) -> str:
        return "data_" + self._name

    def applies_to(self, message: dict) -> bool:
        fields = message.get("fields", {})
        tags = message.get("tags", {})
        return all(f in fields or f in tags for f in self._required_field_names)

    def columns(self) -> list[str]:
        return self._data_field_names + self._FIXED_COLUMNS

    def to_rows(self, message: dict, metadata: dict) -> list[dict]:
        fields = message.get("fields", {})
        tags = message.get("tags", {})
        row = {}
        for field_name in self._data_field_names:
            field_val = fields.get(field_name)
            if field_val is None:
                field_val = tags.get(field_name)
            row[field_name] = field_val
        row["insert_time"] = message.get("timestamp")
        row["_clickhouse_table"] = (
            self.table_name
        )  # Included to assist with batch processing
        return [row]


class DynamicProcessor(object):
    """Processor for handling user defined measurement types"""

    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.resource_definitions = {}
        self.resource_definitions_loaded_at = None
        logger.info(f"DynamicProcessor initialized with pipeline: {pipeline}")

    def set_clickhouse_client(self, client: HttpClient) -> None:
        """Set the ClickHouse client for this processor and any child classes that need it

        The primary purpose of this method is to grant the processor direct access to the ClickHouse client.
        While in most cases the processor will declare the database schema itself, the dynamic pipeline is
        required to access the type definitions directly as they are declared via API.
        """
        self.ch_client = client
        self.api = MetranovaSyncApi(client)

        rdefs = self.api.get_data_types()
        for rd in rdefs:
            self.resource_definitions["data_" + rd["name"]] = ResourceDefinition(rd)
        self.resource_definitions_loaded_at = datetime.datetime.now()

    def build_message(self, value: dict, src_metadata: dict) -> list[dict[str, any]]:
        rdef = self._find_resource_definition(value)
        if rdef is None:
            logger.warning(
                f"No matching resource definition found for message with fields: {value.get('fields', {})} and tags: {value.get('tags', {})}"
            )
            return None
        logger.info(f"Built message: {value} with metadata: {src_metadata}")
        return rdef.to_rows(value, src_metadata)

    def match_message(self, value: dict) -> str:
        rdef = self._find_resource_definition(value)
        if rdef is None:
            return None
        logger.info(f"All fields matched for datatype {rdef._name}.")
        return rdef.table_name

    def column_names(self, table_name: str = None) -> list:
        rdef = self.resource_definitions.get(table_name)
        if rdef is None:
            logger.warning(
                f"No resource definition found for table {table_name}. Writer is attempting to save to a table with no loaded/existing resource definitions."
            )
            return []

        cols = rdef.columns()
        logger.info(f"Got column names {cols} for table {table_name}.")
        return cols

    def scale_value(self, value, scale):
        # TODO
        return value

    # These are helper methods

    def _find_resource_definition(self, value: dict) -> ResourceDefinition | None:
        # NOTE: We reload resource definitions every minute. This is kinda hacky and
        # should be replaced in the future.
        if (
            datetime.datetime.now() - self.resource_definitions_loaded_at
            > datetime.timedelta(minutes=1)
        ):
            rdefs = self.api.get_data_types()
            for rd in rdefs:
                self.resource_definitions["data_" + rd["name"]] = ResourceDefinition(rd)
            self.resource_definitions_loaded_at = datetime.datetime.now()

        for rd in self.resource_definitions.values():
            if rd.applies_to(value):
                return rd
        return None

    # TODO Review these methods for implemtation

    def get_ch_dictionaries(self) -> list:
        return []

    def load_materialized_views(
        self, env_var_name: str, mv_class: type[BaseClickHouseMaterializedViewMixin]
    ) -> None:
        return None

    def get_ip_ref_extensions(self, env_var_name: str) -> list:
        return []

    def lookup_ip_ref_extensions(self, ip_address: str, direction: str) -> dict:
        return {}

    def message_to_columns(self, message: dict, table_name: str) -> list:
        return list(message.values())

    def get_extension_defs(
        self, env_var_name: str, extension_options: dict, json_column_name: str = "ext"
    ) -> list:
        return []

    def extension_is_enabled(
        self, extension_name: str, json_column_name: str = "ext"
    ) -> bool:
        return False

    # Everything below this not actually used. We just need it to satisfy the interface
    # requirements of the ClickHouse writer.

    @property
    def table(self) -> str:
        return "invalid_table"

    def has_match_field(self, value: dict) -> bool:
        # load measurement types
        return True

    def has_required_fields(self, value: dict) -> bool:
        # load measurement types
        return True

    def create_table_command(self, table_name=None) -> str:
        """Use to create the tables returned by get_table_names

        We ignore the table_name parameter here because the dynamic processor is designed
        to handle dynamic table names that are created by users via API. Instead, we
        return a command that will always succeed.
        """
        return "SELECT 1"

    def get_materialized_views(self) -> list:
        """Used to declare table names to be created on ClickHouse side

        We return an empty list here because the dynamic processor is designed to handle
        dynamic table names that are created by users via API.
        """
        return []

    def get_table_names(self) -> list:
        """Used to declare table names to be created on ClickHouse side

        We return an empty list here because the dynamic processor is designed to handle
        dynamic table names that are created by users via API.
        """
        return []
