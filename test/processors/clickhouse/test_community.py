#!/usr/bin/env python3

import unittest
import os
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.community import (
    CommunityRegistryProcessor,
    CommunityPrefixMaterializedView,
    CommunityDictionary,
)


class TestCommunityRegistryProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()

        # Mock clickhouse cacher
        self.mock_clickhouse_cacher = MagicMock()
        self.mock_clickhouse_cacher.lookup.return_value = None  # No cached record by default

        def mock_cacher(cache_type):
            if cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            return MagicMock()

        self.mock_pipeline.cacher.side_effect = mock_cacher

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_COMMUNITY_METADATA_TABLE': 'test_community_table'}):
            processor = CommunityRegistryProcessor(self.mock_pipeline)
            result = processor.create_table_command()

            self.assertIn('CREATE TABLE IF NOT EXISTS test_community_table', result)
            self.assertIn('`ip_subnet` Array(Tuple(IPv6,UInt8))', result)
            self.assertIn('`organization_name` Nullable(String)', result)
            self.assertIn('`organization_id` LowCardinality(Nullable(String))', result)
            self.assertIn('`organization_ref` Nullable(String)', result)
            self.assertIn('`community` LowCardinality(Nullable(String))', result)
            self.assertIn('`as_id` Nullable(UInt32)', result)
            self.assertIn('`as_ref` Nullable(String)', result)
            self.assertIn('`notes` Nullable(String)', result)

    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            processor = CommunityRegistryProcessor(self.mock_pipeline)
            result = processor.create_table_command()
            self.assertIn('CREATE TABLE IF NOT EXISTS meta_ip_community', result)

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "org_name": "Santa Rosa Community College",
                    "community": "CENIC",
                    "asn": "2152",
                    "notes": "test note"
                }
            ]
        }

        result = processor.build_message(input_data, {})

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        record = result[0]

        self.assertEqual(record["organization_name"], "Santa Rosa Community College")
        self.assertEqual(record["community"], "CENIC")
        self.assertEqual(record["as_id"], 2152)
        self.assertEqual(record["notes"], "test note")

    def test_build_message_missing_prefix_length(self):
        """Addresses without a '/' prefix are skipped (unlike scireg, which defaults one)."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        with patch.object(processor.logger, 'warning') as mock_warning:
            input_data = {
                "data": [
                    {
                        "community_id": "test123",
                        "addresses": ["198.189.140.1", "198.189.140.0/24"]
                    }
                ]
            }
            result = processor.build_message(input_data, {})
            record = result[0]

            mock_warning.assert_called_with("Missing prefix length (no '/'): 198.189.140.1")
            # ip_subnet stores IPv4 as ::ffff:a.b.c.d (a 96-bit offset), so a
            # /24 IPv4 CIDR becomes /120 once embedded there.
            self.assertEqual(record["ip_subnet"], [("198.189.140.0", 120)])

    def test_build_message_deidentified_address_added_ipv4(self):
        """A /24-or-narrower IPv4 subnet should also add the .1 de-identified host address."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/25"]
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        # /25 -> /121 (96-bit offset); the de-identified host is always a
        # full-length /128 once embedded in the IPv6 storage column.
        self.assertEqual(record["ip_subnet"], [
            ("198.189.140.0", 121),
            ("198.189.140.1", 128),
        ])

    def test_build_message_deidentified_address_added_ipv6(self):
        """A /64-or-narrower IPv6 subnet should also add the ::1 de-identified host address."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["2001:db8::/65"]
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["ip_subnet"], [
            ("2001:db8::", 65),
            ("2001:db8::1", 128),
        ])

    def test_build_message_no_deidentified_address_for_wide_subnet(self):
        """A /24-or-wider IPv4 subnet (e.g. /16) should NOT get an extra de-identified address."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["130.157.0.0/16"]
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        # /16 -> /112 (96-bit offset for IPv4-mapped storage)
        self.assertEqual(record["ip_subnet"], [("130.157.0.0", 112)])

    def test_build_message_dedup_shared_deidentified_address(self):
        """Multiple narrow subnets sharing an enclosing /24 shouldn't duplicate the de-identified address."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/25", "198.189.140.128/25"]
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["ip_subnet"], [
            ("198.189.140.0", 121),
            ("198.189.140.1", 128),
            ("198.189.140.128", 121),
        ])

    def test_build_message_invalid_asn(self):
        """Invalid asn values should be set to None."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "asn": "not-a-number"
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertIsNone(record["as_id"])

    def test_organization_id_mapping_no_cache_match(self):
        """organization_id falls back to org_name when there's no cache match."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "org_name": "Test College"
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["organization_name"], "Test College")
        self.assertEqual(record["organization_id"], "Test College")
        self.assertIsNone(record["organization_ref"])

    def test_organization_id_mapping_cache_match(self):
        """organization_id/organization_ref resolve from the clickhouse cacher when there's a match."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)
        self.mock_clickhouse_cacher.lookup.side_effect = lambda table, key: (
            {"id": "caida:2152-ARIN", "ref": "caida:2152-ARIN__v2", "hash": "abc"}
            if table == "meta_organization:name" and key == "Test College"
            else None
        )

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "org_name": "Test College"
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["organization_id"], "caida:2152-ARIN")
        self.assertEqual(record["organization_ref"], "caida:2152-ARIN__v2")

    def test_clickhouse_cacher_lookup_for_organization(self):
        """Organization lookup goes through the clickhouse cacher, keyed by name."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "org_name": "Test College"
                }
            ]
        }
        processor.build_message(input_data, {})

        self.mock_clickhouse_cacher.lookup.assert_any_call("meta_organization:name", "Test College")

    def test_as_ref_no_cache_match(self):
        """as_ref is None when there's no cache match for the AS number."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "asn": "2152"
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["as_id"], 2152)
        self.assertIsNone(record["as_ref"])

    def test_as_ref_cache_match(self):
        """as_ref resolves from the clickhouse cacher when there's a match, keyed by as_id."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)
        self.mock_clickhouse_cacher.lookup.side_effect = lambda table, key: (
            {"id": 2152, "ref": "meta_as:2152__v3", "hash": "abc"}
            if table == "meta_as" and key == 2152
            else None
        )

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "asn": "2152"
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["as_id"], 2152)
        self.assertEqual(record["as_ref"], "meta_as:2152__v3")

    def test_clickhouse_cacher_lookup_for_as(self):
        """AS lookup goes through the clickhouse cacher, keyed directly by the (int) AS number."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"],
                    "asn": "2152"
                }
            ]
        }
        processor.build_message(input_data, {})

        self.mock_clickhouse_cacher.lookup.assert_any_call("meta_as", 2152)

    def test_as_ref_none_when_as_id_missing(self):
        """No asn in the input -> as_id is None, and the cacher is still queried with None
        (same pattern as organization_id/organization_ref with a missing org_name)."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "community_id": "test123",
                    "addresses": ["198.189.140.0/24"]
                }
            ]
        }
        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertIsNone(record["as_id"])
        self.assertIsNone(record["as_ref"])

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        input_data = {"data": [{"addresses": ["198.189.140.0/24"]}]}  # missing community_id
        self.assertEqual(processor.build_message(input_data, {}), [])

        input_data = {"data": [{"community_id": "test123"}]}  # missing addresses
        self.assertEqual(processor.build_message(input_data, {}), [])

    def test_dictionary_enabled_by_default(self):
        """Test that the community dictionary (and its backing prefix MV) is enabled by default."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)
        self.assertIsInstance(processor.ch_dictionaries[0], CommunityDictionary)
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertIsInstance(processor.materialized_views[0], CommunityPrefixMaterializedView)

    @patch.dict(os.environ, {'CLICKHOUSE_COMMUNITY_DICTIONARY_ENABLED': 'false'})
    def test_dictionary_disabled_by_env(self):
        """Test that the dictionary/MV can be disabled via environment variable."""
        processor = CommunityRegistryProcessor(self.mock_pipeline)

        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)
        self.assertEqual(len(processor.materialized_views), 0)

    def test_dictionary_sourced_from_prefix_mv_table(self):
        """Test that CommunityDictionary is sourced from the flattened prefix table, not the raw community table."""
        with patch.dict(os.environ, {'CLICKHOUSE_COMMUNITY_METADATA_TABLE': 'custom_community'}):
            processor = CommunityRegistryProcessor(self.mock_pipeline)

            mv = processor.materialized_views[0]
            self.assertEqual(mv.source_table_name, 'custom_community')
            self.assertEqual(mv.table, 'meta_ip_community_prefix')

            dictionary = processor.ch_dictionaries[0]
            self.assertEqual(dictionary.source_table_name, 'meta_ip_community_prefix')


class TestCommunityPrefixMaterializedView(unittest.TestCase):
    """Unit tests for CommunityPrefixMaterializedView class."""

    def test_init_default_values(self):
        mv = CommunityPrefixMaterializedView(source_table_name='meta_ip_community')

        self.assertEqual(mv.source_table_name, 'meta_ip_community')
        self.assertEqual(mv.table, 'meta_ip_community_prefix')
        self.assertEqual(mv.mv_name, 'meta_ip_community_prefix_mv')
        self.assertEqual(mv.primary_keys, ['prefix'])
        self.assertEqual(mv.order_by, ['prefix', 'id'])
        self.assertEqual(mv.table_engine, 'ReplacingMergeTree')

    @patch.dict(os.environ, {'CLICKHOUSE_COMMUNITY_PREFIX_TABLE': 'custom_prefix_table'})
    def test_init_custom_table_name(self):
        mv = CommunityPrefixMaterializedView(source_table_name='meta_ip_community')
        self.assertEqual(mv.table, 'custom_prefix_table')
        self.assertEqual(mv.mv_name, 'custom_prefix_table_mv')

    def test_mv_select_query_flattens_ip_subnet(self):
        mv = CommunityPrefixMaterializedView(source_table_name='meta_ip_community')

        self.assertIn('ARRAY JOIN ip_subnet AS ip_subnet_entry', mv.mv_select_query)
        self.assertIn('FROM meta_ip_community', mv.mv_select_query)
        self.assertIn('AS prefix', mv.mv_select_query)

    def test_create_table_command(self):
        mv = CommunityPrefixMaterializedView(source_table_name='meta_ip_community')
        result = mv.create_table_command()

        self.assertIn('CREATE TABLE IF NOT EXISTS meta_ip_community_prefix', result)
        self.assertIn('`prefix` String', result)
        self.assertIn('`organization_name` String', result)
        self.assertIn('`organization_id` String', result)
        self.assertIn('`community` String', result)
        self.assertIn('ENGINE = ReplacingMergeTree(insert_time)', result)

    def test_mv_select_query_coalesces_nulls(self):
        """IP_TRIE dictionaries don't support Nullable attributes, so NULLs
        from the raw community table must be coalesced to '' before they
        reach the flattened prefix table."""
        mv = CommunityPrefixMaterializedView(source_table_name='meta_ip_community')

        self.assertIn("coalesce(organization_name, '') AS organization_name", mv.mv_select_query)
        self.assertIn("coalesce(organization_id, '') AS organization_id", mv.mv_select_query)
        self.assertIn("coalesce(community, '') AS community", mv.mv_select_query)

    def test_create_mv_command(self):
        mv = CommunityPrefixMaterializedView(source_table_name='meta_ip_community')
        result = mv.create_mv_command()

        self.assertIn('CREATE MATERIALIZED VIEW IF NOT EXISTS meta_ip_community_prefix_mv', result)
        self.assertIn('TO meta_ip_community_prefix', result)


class TestCommunityDictionary(unittest.TestCase):
    """Unit tests for CommunityDictionary class."""

    def test_init_default_values(self):
        dictionary = CommunityDictionary('meta_ip_community_prefix')

        self.assertEqual(dictionary.source_table_name, 'meta_ip_community_prefix')
        self.assertEqual(dictionary.dictionary_name, 'meta_ip_community_dict')
        expected_columns = [
            ['prefix', 'String'],
            ['organization_name', "String DEFAULT ''"],
            ['organization_id', "String DEFAULT ''"],
            ['community', "String DEFAULT ''"],
        ]
        self.assertEqual(dictionary.column_defs, expected_columns)
        self.assertEqual(dictionary.primary_keys, ['prefix'])
        self.assertEqual(dictionary.lifetime_min, '600')
        self.assertEqual(dictionary.lifetime_max, '3600')
        self.assertEqual(dictionary.layout, "IP_TRIE()")

    @patch.dict(os.environ, {'CLICKHOUSE_COMMUNITY_DICTIONARY_NAME': 'custom_community_dict'})
    def test_init_with_custom_dictionary_name(self):
        dictionary = CommunityDictionary('meta_ip_community_prefix')
        self.assertEqual(dictionary.dictionary_name, 'custom_community_dict')

    def test_create_dictionary_command(self):
        dictionary = CommunityDictionary('meta_ip_community_prefix')
        command = dictionary.create_dictionary_command()

        self.assertIn("CREATE DICTIONARY IF NOT EXISTS meta_ip_community_dict", command)
        self.assertIn("`prefix` String", command)
        self.assertIn("PRIMARY KEY (`prefix`)", command)
        self.assertIn("SOURCE(CLICKHOUSE(TABLE 'meta_ip_community_prefix'", command)
        self.assertIn("LAYOUT(IP_TRIE())", command)


if __name__ == '__main__':
    unittest.main(verbosity=2)
