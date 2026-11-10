#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, Mock
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.telegraf import DataGenericMetricProcessor



class TestDataGenericMetricProcessor(unittest.TestCase):
    """Unit tests for DataGenericMetricProcessor."""

    def _make_pipeline(self, device_details=None):
        """Return a mock pipeline whose clickhouse cacher returns device_details."""
        mock_pipeline = MagicMock()
        mock_ch_cacher = Mock()
        mock_ch_cacher.lookup.return_value = device_details

        def cacher_side_effect(name):
            return mock_ch_cacher

        mock_pipeline.cacher.side_effect = cacher_side_effect
        return mock_pipeline, mock_ch_cacher

    def setUp(self):
        self.mock_pipeline, self.mock_ch_cacher = self._make_pipeline(
            device_details={"id": "dev1", "ref": "ref-abc", "hash": "h1"}
        )
        self.processor = DataGenericMetricProcessor(self.mock_pipeline)

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def test_init_default_resource_type(self):
        """TELEGRAF_RESOURCE_TYPE defaults to 'telegraf'."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TELEGRAF_RESOURCE_TYPE", None)
            p = DataGenericMetricProcessor(self.mock_pipeline)
        self.assertEqual(p.resource_type, "telegraf")

    def test_init_custom_resource_type(self):
        """TELEGRAF_RESOURCE_TYPE is read from environment."""
        with patch.dict(os.environ, {"TELEGRAF_RESOURCE_TYPE": "my_type"}):
            p = DataGenericMetricProcessor(self.mock_pipeline)
        self.assertEqual(p.resource_type, "my_type")

    def test_init_no_device_id_regex_by_default(self):
        """device_id_regex is None when TELEGRAF_DEVICE_ID_REGEX is unset."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TELEGRAF_DEVICE_ID_REGEX", None)
            p = DataGenericMetricProcessor(self.mock_pipeline)
        self.assertIsNone(p.device_id_regex)

    def test_init_device_id_regex_compiled(self):
        """TELEGRAF_DEVICE_ID_REGEX is compiled into a regex object."""
        import re
        with patch.dict(os.environ, {"TELEGRAF_DEVICE_ID_REGEX": r"^([^.]+)\."}):
            p = DataGenericMetricProcessor(self.mock_pipeline)
        self.assertIsNotNone(p.device_id_regex)
        self.assertIsInstance(p.device_id_regex, type(re.compile("")))

    # ------------------------------------------------------------------
    # build_message – missing/None device id
    # ------------------------------------------------------------------

    def test_build_message_missing_device_returns_empty(self):
        """Returns [] when 'device' tag is absent."""
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 50},
            "tags": {},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(result, [])

    def test_build_message_missing_required_fields_returns_empty(self):
        """Returns [] when required top-level keys are missing."""
        result = self.processor.build_message({"timestamp": 1609459200}, {})
        self.assertEqual(result, [])

    # ------------------------------------------------------------------
    # build_message – device_details None guard
    # ------------------------------------------------------------------

    def test_build_message_device_not_in_cache_ref_is_none(self):
        """ref is None when cache lookup returns None (no AttributeError)."""
        pipeline, ch = self._make_pipeline(device_details=None)
        p = DataGenericMetricProcessor(pipeline)
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 50},
            "tags": {"device": "unknown-device"},
        }
        result = p.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["ref"])

    def test_build_message_device_in_cache_ref_populated(self):
        """ref is taken from cache when lookup returns a dict."""
        pipeline, _ = self._make_pipeline(
            device_details={"id": "dev1", "ref": "ref-xyz", "hash": "h"}
        )
        p = DataGenericMetricProcessor(pipeline)
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 50},
            "tags": {"device": "router1"},
        }
        result = p.build_message(value, {})
        self.assertEqual(result[0]["ref"], "ref-xyz")

    # ------------------------------------------------------------------
    # build_message – device_id_regex
    # ------------------------------------------------------------------

    def test_device_id_regex_strips_domain(self):
        """Regex with one capture group rewrites device_id before lookup."""
        with patch.dict(os.environ, {"TELEGRAF_DEVICE_ID_REGEX": r"^([^.]+)\."}):
            p = DataGenericMetricProcessor(self.mock_pipeline)
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 50},
            "tags": {"device": "router1.example.com"},
        }
        result = p.build_message(value, {})
        self.assertEqual(result[0]["id"], "router1")

    def test_device_id_regex_no_match_keeps_original(self):
        """When regex does not match, original device_id is kept."""
        with patch.dict(os.environ, {"TELEGRAF_DEVICE_ID_REGEX": r"^([^.]+)\."}):
            p = DataGenericMetricProcessor(self.mock_pipeline)
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 50},
            "tags": {"device": "router1"},  # no dot → no match
        }
        result = p.build_message(value, {})
        self.assertEqual(result[0]["id"], "router1")

    def test_device_id_regex_no_capture_group_keeps_original(self):
        """Regex without a capture group leaves device_id unchanged."""
        with patch.dict(os.environ, {"TELEGRAF_DEVICE_ID_REGEX": r"router\d+"}):
            p = DataGenericMetricProcessor(self.mock_pipeline)
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 50},
            "tags": {"device": "router1.example.com"},
        }
        result = p.build_message(value, {})
        self.assertEqual(result[0]["id"], "router1.example.com")

    # ------------------------------------------------------------------
    # build_message – field type handling
    # ------------------------------------------------------------------

    def test_build_message_integer_field_is_counter(self):
        """Integer field → counter table, value unchanged."""
        value = {
            "timestamp": 1609459200,
            "fields": {"packets": 100},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertIn("counter", result[0]["_clickhouse_table"])
        self.assertEqual(result[0]["metric_value"], 100)

    def test_build_message_float_field_is_gauge(self):
        """Float field → gauge table, value unchanged."""
        value = {
            "timestamp": 1609459200,
            "fields": {"temp": 45.5},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertIn("gauge", result[0]["_clickhouse_table"])
        self.assertEqual(result[0]["metric_value"], 45.5)

    def test_build_message_string_field_is_counter_with_value_in_ext(self):
        """String field → counter table, metric_value=1, original in ext."""
        import orjson
        value = {
            "timestamp": 1609459200,
            "fields": {"status": "up"},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertIn("counter", record["_clickhouse_table"])
        self.assertEqual(record["metric_value"], 1)
        ext = orjson.loads(record["ext"])
        self.assertEqual(ext["metric_value"], "up")

    def test_build_message_null_field_skipped(self):
        """Fields with None value are skipped."""
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": None, "mem": 80},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["metric_name"], "mem")

    def test_build_message_multiple_fields_produce_multiple_records(self):
        """Each non-null field produces one record."""
        value = {
            "timestamp": 1609459200,
            "fields": {"a": 1, "b": 2, "c": 3.0},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(len(result), 3)

    # ------------------------------------------------------------------
    # build_message – tags and ext
    # ------------------------------------------------------------------

    def test_build_message_default_skip_tags_excluded_from_ext(self):
        """Default skip tags (device, collector, etc.) are not in ext."""
        import orjson
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 1},
            "tags": {
                "device": "r1",
                "collector": "col1",
                "date": "2021-01-01",
                "routing_tag": "rt",
                "custom": "keep_me",
            },
        }
        result = self.processor.build_message(value, {})
        ext = orjson.loads(result[0]["ext"])
        for skip in ("device", "collector", "date", "routing_tag"):
            self.assertNotIn(skip, ext)
        self.assertEqual(ext["custom"], "keep_me")

    def test_build_message_collector_id_from_tags(self):
        """collector_id is taken from the 'collector' tag."""
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 1},
            "tags": {"device": "r1", "collector": "my-collector"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(result[0]["collector_id"], "my-collector")

    def test_build_message_collector_id_defaults_to_unknown(self):
        """collector_id falls back to 'unknown' when tag is absent."""
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 1},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(result[0]["collector_id"], "unknown")

    # ------------------------------------------------------------------
    # build_message – timestamp
    # ------------------------------------------------------------------

    def test_build_message_timestamp_converted_to_ms(self):
        """observation_time = timestamp * 1000."""
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 1},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(result[0]["observation_time"], 1609459200000)

    def test_build_message_invalid_timestamp_returns_empty(self):
        """Non-numeric timestamp returns []."""
        value = {
            "timestamp": "bad",
            "fields": {"cpu": 1},
            "tags": {"device": "r1"},
        }
        result = self.processor.build_message(value, {})
        self.assertEqual(result, [])

    # ------------------------------------------------------------------
    # Cache lookup call verification
    # ------------------------------------------------------------------

    def test_build_message_lookup_uses_device_id(self):
        """Clickhouse cacher lookup is called with 'meta_device' and device_id."""
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 1},
            "tags": {"device": "router1"},
        }
        self.processor.build_message(value, {})
        self.mock_ch_cacher.lookup.assert_called_once_with("meta_device", "router1")

    def test_build_message_lookup_uses_transformed_device_id(self):
        """Cache lookup uses the regex-transformed device_id, not the original."""
        pipeline, ch = self._make_pipeline(device_details=None)
        with patch.dict(os.environ, {"TELEGRAF_DEVICE_ID_REGEX": r"^([^.]+)\."}):
            p = DataGenericMetricProcessor(pipeline)
        value = {
            "timestamp": 1609459200,
            "fields": {"cpu": 1},
            "tags": {"device": "router1.example.com"},
        }
        p.build_message(value, {})
        ch.lookup.assert_called_once_with("meta_device", "router1")


if __name__ == '__main__':
    unittest.main(verbosity=2)
