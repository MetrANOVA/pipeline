#!/usr/bin/env python3

import datetime
import unittest
import os
import sys
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse import dynamic as dynamic_module
from metranova.processors.clickhouse.dynamic import (
    ResourceDefinition,
    MetranovaSyncApi,
    DynamicProcessorConfig,
    DynamicProcessor,
)
from metranova.transformer import Transformer, TransformerColumn


def _make_rdef(slug, data_fields):
    """Build a ResourceDefinition from a slug and list of (field_name, nullable) tuples."""
    return ResourceDefinition({
        "slug": slug,
        "data_fields": [
            {"field_name": n, "nullable": nullable} for n, nullable in data_fields
        ],
    })


def _mock_ch_client(definition_rows=None, transformer_rows=None, transformer_column_rows=None):
    """Build a mock HttpClient whose .query(sql).named_results() returns the requested rows."""
    client = MagicMock()

    def query_side_effect(sql):
        result = MagicMock()
        if "metranova.definition" in sql:
            result.named_results.return_value = definition_rows or []
        elif "metranova.transformer_column" in sql:
            result.named_results.return_value = transformer_column_rows or []
        elif "metranova.transformer" in sql:
            result.named_results.return_value = transformer_rows or []
        else:
            result.named_results.return_value = []
        return result

    client.query.side_effect = query_side_effect
    return client


class TestResourceDefinition(unittest.TestCase):
    def test_table_name_prefixes_with_data_(self):
        rdef = _make_rdef("interface", [("ifindex", False)])
        self.assertEqual(rdef.table_name, "data_interface")

    def test_columns_includes_data_fields_and_insert_time(self):
        rdef = _make_rdef("interface", [("ifindex", False), ("speed", True)])
        self.assertEqual(rdef.columns(), ["ifindex", "speed", "insert_time"])

    def test_required_field_names_only_includes_non_nullable(self):
        rdef = _make_rdef(
            "interface",
            [("ifindex", False), ("speed", True), ("name", False)],
        )
        self.assertEqual(rdef._required_field_names, ["ifindex", "name"])

    def test_applies_to_matches_default_name_field(self):
        rdef = _make_rdef("interface", [("ifindex", False)])
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(rdef.applies_to({"name": "interface"}))
            self.assertFalse(rdef.applies_to({"name": "cpu"}))

    def test_applies_to_uses_env_override_field(self):
        rdef = _make_rdef("interface", [("ifindex", False)])
        with patch.dict(os.environ, {"PROCESSOR_DYNAMIC_RESOURCE_TYPE_FIELD_NAME": "measurement"}):
            self.assertTrue(rdef.applies_to({"measurement": "interface"}))
            self.assertFalse(rdef.applies_to({"name": "interface"}))

    def test_to_rows_pulls_from_fields_then_tags(self):
        rdef = _make_rdef("interface", [("ifindex", False), ("device_id", True), ("missing", True)])
        message = {
            "fields": {"ifindex": 42},
            "tags": {"device_id": "router-01"},
            "timestamp": 1234567890,
        }
        rows = rdef.to_rows(message, {})
        row = rows[0]
        self.assertEqual(row["ifindex"], 42)
        self.assertEqual(row["device_id"], "router-01")
        self.assertIsNone(row["missing"])

    def test_to_rows_sets_insert_time_from_message_timestamp(self):
        rdef = _make_rdef("interface", [("ifindex", False)])
        rows = rdef.to_rows({"fields": {"ifindex": 1}, "timestamp": 999}, {})
        self.assertEqual(rows[0]["insert_time"], 999)

    def test_to_rows_includes_clickhouse_table_marker(self):
        rdef = _make_rdef("interface", [("ifindex", False)])
        rows = rdef.to_rows({"fields": {"ifindex": 1}, "timestamp": 0}, {})
        self.assertEqual(rows[0]["_clickhouse_table"], "data_interface")

    def test_to_rows_returns_list_with_single_row(self):
        rdef = _make_rdef("interface", [("ifindex", False)])
        rows = rdef.to_rows({"fields": {"ifindex": 1}, "timestamp": 0}, {})
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 1)

    def test_to_rows_handles_missing_fields_and_tags_keys(self):
        rdef = _make_rdef("interface", [("ifindex", False), ("speed", True)])
        rows = rdef.to_rows({"timestamp": 0}, {})
        row = rows[0]
        self.assertIsNone(row["ifindex"])
        self.assertIsNone(row["speed"])


class TestDynamicProcessorConfig(unittest.TestCase):
    def test_defaults_when_no_env_vars(self):
        with patch.dict(os.environ, {}, clear=True):
            config = DynamicProcessorConfig()
        self.assertEqual(config.resource_types, set())
        self.assertEqual(config.resource_type_field_name, "name")

    def test_resource_types_parses_comma_separated_list(self):
        with patch.dict(os.environ, {"PROCESSOR_DYNAMIC_RESOURCE_TYPES": "interface,cpu,memory"}):
            config = DynamicProcessorConfig()
        self.assertEqual(config.resource_types, {"interface", "cpu", "memory"})

    def test_resource_types_empty_string_yields_empty_set(self):
        with patch.dict(os.environ, {"PROCESSOR_DYNAMIC_RESOURCE_TYPES": ""}):
            config = DynamicProcessorConfig()
        self.assertEqual(config.resource_types, set())

    def test_resource_type_field_name_override(self):
        with patch.dict(os.environ, {"PROCESSOR_DYNAMIC_RESOURCE_TYPE_FIELD_NAME": "measurement"}):
            config = DynamicProcessorConfig()
        self.assertEqual(config.resource_type_field_name, "measurement")


class TestMetranovaSyncApi(unittest.TestCase):
    def test_get_data_types_returns_resource_definitions(self):
        rows = [
            {"slug": "interface", "data_fields": [{"field_name": "ifindex", "nullable": False}]},
            {"slug": "cpu", "data_fields": [{"field_name": "load", "nullable": False}]},
        ]
        client = _mock_ch_client(definition_rows=rows)
        api = MetranovaSyncApi(client)
        result = api.get_data_types()
        self.assertEqual(len(result), 2)
        self.assertEqual({r._slug for r in result}, {"interface", "cpu"})

    def test_get_data_types_returns_empty_on_query_exception(self):
        client = MagicMock()
        client.query.side_effect = Exception("boom")
        api = MetranovaSyncApi(client)
        with patch.object(dynamic_module.logger, "exception") as mock_log:
            result = api.get_data_types()
        self.assertEqual(result, [])
        mock_log.assert_called_once()

    def test_get_transformers_joins_columns_to_transformers_by_ref(self):
        transformer_rows = [{
            "id": "t1",
            "ref": "ref-1",
            "name": "vendor-transformer",
            "match_field": "vendor",
            "definition_ref": "def-1",
        }]
        column_rows = [
            {
                "id": "c2",
                "transformer_ref": "ref-1",
                "match_value": "vendor-a",
                "vendor_match_field": None,
                "vendor_match_value": None,
                "target_column": "label",
                "operation": "static",
                "config": '{"value": "B"}',
                "default_value": "",
                "order": 2,
            },
            {
                "id": "c1",
                "transformer_ref": "ref-1",
                "match_value": "vendor-a",
                "vendor_match_field": None,
                "vendor_match_value": None,
                "target_column": "label",
                "operation": "static",
                "config": '{"value": "A"}',
                "default_value": "",
                "order": 1,
            },
        ]
        client = _mock_ch_client(
            transformer_rows=transformer_rows,
            transformer_column_rows=column_rows,
        )
        api = MetranovaSyncApi(client)
        transformers = api.get_transformers()
        self.assertEqual(len(transformers), 1)
        ordered_ids = [c.id for c in transformers[0].columns]
        self.assertEqual(ordered_ids, ["c1", "c2"])

    def test_get_transformers_handles_null_default_value(self):
        transformer_rows = [{
            "id": "t1",
            "ref": "ref-1",
            "name": "x",
            "match_field": "vendor",
            "definition_ref": "def-1",
        }]
        column_rows = [{
            "id": "c1",
            "transformer_ref": "ref-1",
            "match_value": "v",
            "vendor_match_field": None,
            "vendor_match_value": None,
            "target_column": "col",
            "operation": "static",
            "config": "{}",
            "default_value": None,
            "order": 0,
        }]
        client = _mock_ch_client(
            transformer_rows=transformer_rows,
            transformer_column_rows=column_rows,
        )
        api = MetranovaSyncApi(client)
        transformers = api.get_transformers()
        self.assertEqual(transformers[0].columns[0].default_value, "")

    def test_get_transformers_with_no_columns_for_transformer(self):
        transformer_rows = [{
            "id": "t1",
            "ref": "ref-1",
            "name": "x",
            "match_field": "vendor",
            "definition_ref": "def-1",
        }]
        client = _mock_ch_client(transformer_rows=transformer_rows, transformer_column_rows=[])
        api = MetranovaSyncApi(client)
        transformers = api.get_transformers()
        self.assertEqual(transformers[0].columns, [])

    def test_get_transformers_returns_empty_on_query_exception(self):
        client = MagicMock()
        client.query.side_effect = Exception("boom")
        api = MetranovaSyncApi(client)
        with patch.object(dynamic_module.logger, "exception") as mock_log:
            result = api.get_transformers()
        self.assertEqual(result, [])
        mock_log.assert_called_once()

    def test_get_transformers_parses_config_json(self):
        transformer_rows = [{
            "id": "t1",
            "ref": "ref-1",
            "name": "x",
            "match_field": "vendor",
            "definition_ref": "def-1",
        }]
        column_rows = [{
            "id": "c1",
            "transformer_ref": "ref-1",
            "match_value": "v",
            "vendor_match_field": None,
            "vendor_match_value": None,
            "target_column": "col",
            "operation": "static",
            "config": '{"foo": "bar"}',
            "default_value": "",
            "order": 0,
        }]
        client = _mock_ch_client(
            transformer_rows=transformer_rows,
            transformer_column_rows=column_rows,
        )
        api = MetranovaSyncApi(client)
        transformers = api.get_transformers()
        self.assertEqual(transformers[0].columns[0].config, {"foo": "bar"})


class TestDynamicProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            self.processor = DynamicProcessor(self.mock_pipeline)

        self.rdef_interface = _make_rdef(
            "interface",
            [("ifindex", False), ("speed", True)],
        )
        self.processor.resource_definitions = {"data_interface": self.rdef_interface}
        self.processor.resource_definitions_loaded_at = datetime.datetime.now()
        self.processor.api = MagicMock()

    # Initialization

    def test_init_sets_empty_caches_and_config(self):
        with patch.dict(os.environ, {}, clear=True):
            proc = DynamicProcessor(self.mock_pipeline)
        self.assertEqual(proc.resource_definitions, {})
        self.assertIsNone(proc.resource_definitions_loaded_at)
        self.assertEqual(proc.transformers, {})
        self.assertIsInstance(proc.config, DynamicProcessorConfig)

    # set_clickhouse_client

    def test_set_clickhouse_client_loads_definitions_and_transformers(self):
        definition_rows = [{
            "slug": "interface",
            "data_fields": [{"field_name": "ifindex", "nullable": False}],
        }]
        transformer_rows = [{
            "id": "t1",
            "ref": "ref-1",
            "name": "x",
            "match_field": "vendor",
            "definition_ref": "def-1",
        }]
        column_rows = [{
            "id": "c1",
            "transformer_ref": "ref-1",
            "match_value": "v",
            "vendor_match_field": None,
            "vendor_match_value": None,
            "target_column": "col",
            "operation": "static",
            "config": "{}",
            "default_value": "",
            "order": 0,
        }]
        client = _mock_ch_client(
            definition_rows=definition_rows,
            transformer_rows=transformer_rows,
            transformer_column_rows=column_rows,
        )
        with patch.dict(os.environ, {}, clear=True):
            proc = DynamicProcessor(self.mock_pipeline)
        proc.set_clickhouse_client(client)
        self.assertIn("data_interface", proc.resource_definitions)
        self.assertIn("t1", proc.transformers)
        self.assertIsInstance(proc.resource_definitions_loaded_at, datetime.datetime)
        self.assertIsInstance(proc.api, MetranovaSyncApi)

    # match_message

    def test_match_message_returns_true_for_known_type(self):
        self.assertTrue(self.processor.match_message({"name": "interface"}))

    def test_match_message_returns_false_when_resource_type_field_missing(self):
        self.assertFalse(self.processor.match_message({"fields": {}}))

    def test_match_message_returns_false_when_type_not_in_configured_set(self):
        self.processor.config.resource_types = {"cpu"}
        self.assertFalse(self.processor.match_message({"name": "interface"}))

    def test_match_message_returns_true_when_configured_set_empty(self):
        self.processor.config.resource_types = set()
        self.assertTrue(self.processor.match_message({"name": "interface"}))

    def test_match_message_returns_false_when_no_resource_definition_found(self):
        self.assertFalse(self.processor.match_message({"name": "unknown"}))

    # build_message

    def test_build_message_returns_none_when_resource_type_missing(self):
        self.assertIsNone(self.processor.build_message({"fields": {}}, {}))

    def test_build_message_returns_none_when_type_not_in_configured_set(self):
        self.processor.config.resource_types = {"cpu"}
        self.assertIsNone(self.processor.build_message({"name": "interface"}, {}))

    def test_build_message_returns_none_when_no_resource_definition(self):
        self.assertIsNone(self.processor.build_message({"name": "unknown"}, {}))

    def test_build_message_produces_row_with_clickhouse_table_marker(self):
        msg = {
            "name": "interface",
            "fields": {"ifindex": 7, "speed": 1000},
            "timestamp": 1234,
        }
        rows = self.processor.build_message(msg, {})
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["ifindex"], 7)
        self.assertEqual(row["speed"], 1000)
        self.assertEqual(row["insert_time"], 1234)
        self.assertEqual(row["_clickhouse_table"], "data_interface")

    def test_build_message_applies_matching_transformer_columns(self):
        rdef = _make_rdef(
            "interface",
            [("ifindex", False), ("vendor_label", True)],
        )
        self.processor.resource_definitions = {"data_interface": rdef}
        column = TransformerColumn(
            id="c1",
            match_value="vendor-a",
            target_column="vendor_label",
            operation="static",
            config={"value": "X"},
        )
        transformer = Transformer(
            id="t1",
            name="vendor-transformer",
            match_field="vendor",
            definition_ref="def-1",
            columns=[column],
        )
        self.processor.transformers = {transformer.id: transformer}

        msg = {"name": "interface", "fields": {"ifindex": 1}, "timestamp": 0}
        rows = self.processor.build_message(msg, {"vendor": "vendor-a"})
        self.assertEqual(rows[0]["vendor_label"], "X")

    def test_build_message_skips_transformer_columns_with_non_matching_value(self):
        rdef = _make_rdef(
            "interface",
            [("ifindex", False), ("vendor_label", True)],
        )
        self.processor.resource_definitions = {"data_interface": rdef}
        column = TransformerColumn(
            id="c1",
            match_value="vendor-a",
            target_column="vendor_label",
            operation="static",
            config={"value": "X"},
        )
        transformer = Transformer(
            id="t1",
            name="vendor-transformer",
            match_field="vendor",
            definition_ref="def-1",
            columns=[column],
        )
        self.processor.transformers = {transformer.id: transformer}

        msg = {"name": "interface", "fields": {"ifindex": 1}, "timestamp": 0}
        rows = self.processor.build_message(msg, {"vendor": "vendor-b"})
        self.assertIsNone(rows[0]["vendor_label"])

    def test_build_message_skips_transformer_columns_when_match_field_absent_from_metadata(self):
        rdef = _make_rdef(
            "interface",
            [("ifindex", False), ("vendor_label", True)],
        )
        self.processor.resource_definitions = {"data_interface": rdef}
        column = TransformerColumn(
            id="c1",
            match_value="vendor-a",
            target_column="vendor_label",
            operation="static",
            config={"value": "X"},
        )
        transformer = Transformer(
            id="t1",
            name="vendor-transformer",
            match_field="vendor",
            definition_ref="def-1",
            columns=[column],
        )
        self.processor.transformers = {transformer.id: transformer}

        msg = {"name": "interface", "fields": {"ifindex": 1}, "timestamp": 0}
        rows = self.processor.build_message(msg, {})
        self.assertIsNone(rows[0]["vendor_label"])

    # column_names

    def test_column_names_returns_definition_columns(self):
        self.assertEqual(
            self.processor.column_names("data_interface"),
            ["ifindex", "speed", "insert_time"],
        )

    def test_column_names_returns_empty_list_for_unknown_table(self):
        with patch.object(dynamic_module.logger, "warning") as mock_warning:
            result = self.processor.column_names("data_unknown")
        self.assertEqual(result, [])
        mock_warning.assert_called_once()

    # Interface stubs

    def test_get_ch_dictionaries_returns_empty_list(self):
        self.assertEqual(self.processor.get_ch_dictionaries(), [])

    def test_get_ip_ref_extensions_returns_empty_list(self):
        self.assertEqual(self.processor.get_ip_ref_extensions("ANY_ENV_VAR"), [])

    def test_lookup_ip_ref_extensions_returns_empty_dict(self):
        self.assertEqual(self.processor.lookup_ip_ref_extensions("1.2.3.4", "src"), {})

    def test_get_extension_defs_returns_empty_list(self):
        self.assertEqual(self.processor.get_extension_defs("ENV", {}), [])

    def test_extension_is_enabled_returns_false(self):
        self.assertFalse(self.processor.extension_is_enabled("any_ext"))

    def test_get_materialized_views_returns_empty_list(self):
        self.assertEqual(self.processor.get_materialized_views(), [])

    def test_get_table_names_returns_empty_list(self):
        self.assertEqual(self.processor.get_table_names(), [])

    def test_create_table_command_returns_select_1(self):
        self.assertEqual(self.processor.create_table_command(), "SELECT 1")
        self.assertEqual(self.processor.create_table_command("any_name"), "SELECT 1")

    def test_table_property_returns_invalid_table_sentinel(self):
        self.assertEqual(self.processor.table, "invalid_table")

    def test_has_match_field_returns_true(self):
        self.assertTrue(self.processor.has_match_field({}))

    def test_has_required_fields_returns_true(self):
        self.assertTrue(self.processor.has_required_fields({}))

    def test_load_materialized_views_returns_none(self):
        self.assertIsNone(self.processor.load_materialized_views("ANY_ENV", object))

    def test_message_to_columns_returns_dict_values_as_list(self):
        result = self.processor.message_to_columns({"a": 1, "b": 2}, "data_interface")
        self.assertEqual(result, [1, 2])

    def test_scale_value_returns_value_unchanged(self):
        self.assertEqual(self.processor.scale_value(42, 1000), 42)


if __name__ == '__main__':
    unittest.main(verbosity=2)
