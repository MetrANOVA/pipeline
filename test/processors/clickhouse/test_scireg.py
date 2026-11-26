#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, date

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.scireg import (
    ScienceRegistryProcessor,
    SciregPrefixMaterializedView,
    SciregDictionary,
)


class TestScienceRegistryProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
        # Mock redis cacher
        self.mock_redis_cacher = MagicMock()
        self.mock_redis_cacher.lookup.return_value = "mock_org_ref"
        
        # Mock clickhouse cacher
        self.mock_clickhouse_cacher = MagicMock()
        self.mock_clickhouse_cacher.lookup.return_value = None  # No cached record by default
        
        # Set up cacher method to return the appropriate mock based on the type
        def mock_cacher(cache_type):
            if cache_type == "redis":
                return self.mock_redis_cacher
            elif cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            else:
                return MagicMock()
        
        self.mock_pipeline.cacher.side_effect = mock_cacher
        
    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_SCIREG_METADATA_TABLE': 'test_scireg_table'}):
            processor = ScienceRegistryProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (ScienceRegistryProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Verify basic structure
            self.assertIn('CREATE TABLE IF NOT EXISTS test_scireg_table', result)
            self.assertIn('ENGINE = MergeTree()', result)
            
            # Check for key columns
            self.assertIn('`scireg_update_time` Date', result)
            self.assertIn('`ip_subnet` Array(Tuple(IPv6,UInt8))', result)
            self.assertIn('`organization_name` Nullable(String)', result)
            self.assertIn('`organization_id` LowCardinality(Nullable(String))', result)
            self.assertIn('`organization_ref` Nullable(String)', result)
            self.assertIn('`discipline` Nullable(String)', result)
            self.assertIn('`latitude` Nullable(Float64)', result)
            self.assertIn('`longitude` Nullable(Float64)', result)
            self.assertIn('`resource_name` Nullable(String)', result)
            self.assertIn('`project_name` Nullable(String)', result)
            self.assertIn('`contact_email` Nullable(String)', result)
    
    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            processor = ScienceRegistryProcessor(self.mock_pipeline)
            
            result = processor.create_table_command()
            
            # Should use default table name
            self.assertIn('CREATE TABLE IF NOT EXISTS meta_ip_scireg', result)
    
    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.0/24", "10.0.0.1", "2001:db8::1/64"],
                    "last_updated": "2023-10-24",
                    "org_name": "Test Organization",
                    "discipline": "Computer Science",
                    "latitude": "40.7128",
                    "longitude": "-74.0060",
                    "resource_name": "Test Resource",
                    "project_name": "Test Project",
                    "contact_email": "test@example.com"
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        
        # Should return a list with one record
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        
        # Verify field mappings
        self.assertEqual(record["scireg_update_time"], date(2023, 10, 24))
        self.assertEqual(record["organization_name"], "Test Organization")
        self.assertEqual(record["organization_id"], "Test Organization")
        # setUp's mock clickhouse cacher has no match configured, so this falls back to None
        self.assertIsNone(record["organization_ref"])
        self.assertEqual(record["discipline"], "Computer Science")
        self.assertEqual(record["latitude"], 40.7128)
        self.assertEqual(record["longitude"], -74.0060)
        self.assertEqual(record["resource_name"], "Test Resource")
        self.assertEqual(record["project_name"], "Test Project")
        self.assertEqual(record["contact_email"], "test@example.com")
        
        # Verify IP subnet processing. ip_subnet is stored as Array(Tuple(IPv6,
        # UInt8)): IPv4 addresses get mapped into that 128-bit space as
        # ::ffff:a.b.c.d (a fixed 96-bit prefix), so IPv4 CIDR lengths are
        # offset by 96 to stay correct once embedded there (24 -> 120); a bare
        # IPv4 host address becomes a full-length /128, same as IPv6. Native
        # IPv6 CIDR lengths (e.g. the /64 below) are untouched.
        expected_subnets = [
            ("192.168.1.0", 120),
            ("10.0.0.1", 128),  # IPv4 default
            ("2001:db8::1", 64)
        ]
        self.assertEqual(record["ip_subnet"], expected_subnets)
    
    def test_build_message_multiple_records(self):
        """Test build_message with multiple records."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.0/24"],
                    "org_name": "Org 1"
                },
                {
                    "scireg_id": "test456",
                    "addresses": ["10.0.0.0/8"],
                    "org_name": "Org 2"
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        
        # Should return a list with two records
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        
        # Verify both records are processed
        org_names = [record["organization_name"] for record in result]
        self.assertIn("Org 1", org_names)
        self.assertIn("Org 2", org_names)
    
    def test_build_message_ipv4_default_prefix(self):
        """Test that IPv4 addresses without CIDR get a full-length /128 prefix
        (since ip_subnet stores them as IPv4-mapped IPv6, ::ffff:a.b.c.d)."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"]  # IPv4 without prefix
                }
            ]
        }

        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["ip_subnet"], [("192.168.1.1", 128)])
    
    def test_build_message_ipv6_default_prefix(self):
        """Test that IPv6 addresses without CIDR get /128 prefix."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["2001:db8::1"]  # IPv6 without prefix
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        record = result[0]
        
        self.assertEqual(record["ip_subnet"], [("2001:db8::1", 128)])
    
    def test_build_message_invalid_ip_address(self):
        """Test handling of invalid IP addresses."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        with patch.object(processor.logger, 'warning') as mock_warning:
            input_data = {
                "data": [
                    {
                        "scireg_id": "test123",
                        "addresses": ["invalid.ip.address", "192.168.1.1"]
                    }
                ]
            }
            
            result = processor.build_message(input_data, {})
            record = result[0]
            
            # Should log warning for invalid IP
            mock_warning.assert_called_with("Invalid IP address format: invalid.ip.address")
            
            # Should only include valid IP
            self.assertEqual(record["ip_subnet"], [("192.168.1.1", 128)])
    
    def test_build_message_unknown_last_updated(self):
        """Test handling of unknown last_updated date."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "last_updated": "unknown"
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        record = result[0]
        
        # Should default to 1970-01-01
        self.assertEqual(record["scireg_update_time"], date(1970, 1, 1))
    
    def test_build_message_invalid_date_format(self):
        """Test handling of invalid date format."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "last_updated": "invalid-date-format"
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        record = result[0]
        
        # Should default to 1970-01-01 for invalid dates
        self.assertEqual(record["scireg_update_time"], date(1970, 1, 1))
    
    def test_build_message_invalid_coordinates(self):
        """Test handling of invalid latitude/longitude values."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "latitude": "invalid_lat",
                    "longitude": "invalid_lon"
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        record = result[0]
        
        # Should set invalid coordinates to None
        self.assertIsNone(record["latitude"])
        self.assertIsNone(record["longitude"])
    
    def test_build_message_valid_coordinates(self):
        """Test handling of valid latitude/longitude values."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "latitude": 40.7128,
                    "longitude": -74.0060
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        record = result[0]
        
        # Should convert to float
        self.assertEqual(record["latitude"], 40.7128)
        self.assertEqual(record["longitude"], -74.0060)
    
    def test_build_message_empty_data(self):
        """Test build_message with empty data."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        # Test with None value
        result = processor.build_message(None, {})
        self.assertEqual(result, [])
        
        # Test with empty dict
        result = processor.build_message({}, {})
        self.assertEqual(result, [])
        
        # Test with None data field
        result = processor.build_message({"data": None}, {})
        self.assertEqual(result, [])
    
    def test_build_message_non_list_data(self):
        """Test build_message coerces a single dict in data to a one-item list."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": {
                "scireg_id": "test123",
                "addresses": ["192.168.1.1"],
                "org_name": "Test Organization"
            }
        }

        result = processor.build_message(input_data, {})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["organization_name"], "Test Organization")
        # setUp's mock clickhouse cacher has no match configured, so this falls back to None
        self.assertIsNone(result[0]["organization_ref"])
    
    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        # Missing scireg_id
        input_data = {
            "data": [
                {
                    "addresses": ["192.168.1.1"]
                    # Missing scireg_id
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        
        # Should return empty list due to missing required field
        self.assertEqual(result, [])
        
        # Missing addresses
        input_data = {
            "data": [
                {
                    "scireg_id": "test123"
                    # Missing addresses
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        
        # Should return empty list due to missing required field
        self.assertEqual(result, [])
    
    def test_build_message_mixed_ip_formats(self):
        """Test build_message with mixed IPv4 and IPv6 addresses in various formats."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": [
                        "192.168.1.0/24",      # IPv4 with CIDR
                        "10.0.0.1",            # IPv4 without CIDR
                        "2001:db8::/32",       # IPv6 with CIDR
                        "fe80::1",             # IPv6 without CIDR
                        "::1"                  # IPv6 loopback
                    ]
                }
            ]
        }
        
        result = processor.build_message(input_data, {})
        record = result[0]
        
        expected_subnets = [
            ("192.168.1.0", 120),  # IPv4 /24 -> /120 (96-bit ::ffff: offset)
            ("10.0.0.1", 128),
            ("2001:db8::", 32),    # native IPv6, no offset
            ("fe80::1", 128),
            ("::1", 128)
        ]

        self.assertEqual(record["ip_subnet"], expected_subnets)
    
    def test_organization_id_mapping_no_cache_match(self):
        """Test that organization_id falls back to org_name when there's no cache match."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        # default mock_clickhouse_cacher.lookup returns None (set up in setUp)

        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "org_name": "Test University"
                }
            ]
        }

        result = processor.build_message(input_data, {})
        record = result[0]

        # organization_name always comes from org_name; organization_id falls back
        # to org_name when the clickhouse cacher has no match for it
        self.assertEqual(record["organization_name"], "Test University")
        self.assertEqual(record["organization_id"], "Test University")
        self.assertIsNone(record["organization_ref"])

    def test_organization_id_mapping_cache_match(self):
        """Test that organization_id/organization_ref resolve from the clickhouse cacher when there's a match."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        self.mock_clickhouse_cacher.lookup.side_effect = lambda table, key: (
            {"id": "caida:1800CO-2-ARIN", "ref": "caida:1800CO-2-ARIN__v3", "hash": "abc"}
            if table == "meta_organization:name" and key == "Test University"
            else None
        )

        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "org_name": "Test University"
                }
            ]
        }

        result = processor.build_message(input_data, {})
        record = result[0]

        self.assertEqual(record["organization_name"], "Test University")
        self.assertEqual(record["organization_id"], "caida:1800CO-2-ARIN")
        self.assertEqual(record["organization_ref"], "caida:1800CO-2-ARIN__v3")

    def test_clickhouse_cacher_lookup_for_organization(self):
        """Test that the clickhouse cacher (not redis) is used for organization lookup, keyed by name."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)

        input_data = {
            "data": [
                {
                    "scireg_id": "test123",
                    "addresses": ["192.168.1.1"],
                    "org_name": "Test University"
                }
            ]
        }

        result = processor.build_message(input_data, {})

        # organization lookup now goes through the clickhouse cacher, keyed by
        # "meta_organization:name" (composite-key form), not redis
        self.mock_clickhouse_cacher.lookup.assert_any_call("meta_organization:name", "Test University")
        self.mock_redis_cacher.lookup.assert_not_called()

        # No cache match configured in setUp, so organization_ref stays None
        record = result[0]
        self.assertIsNone(record["organization_ref"])

    def test_dictionary_enabled_by_default(self):
        """Test that the scireg dictionary (and its backing prefix MV) is enabled by default."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)

        self.assertTrue(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 1)
        self.assertIsInstance(processor.ch_dictionaries[0], SciregDictionary)
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertIsInstance(processor.materialized_views[0], SciregPrefixMaterializedView)

    @patch.dict(os.environ, {'CLICKHOUSE_SCIREG_DICTIONARY_ENABLED': 'false'})
    def test_dictionary_disabled_by_env(self):
        """Test that the dictionary/MV can be disabled via environment variable."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)

        self.assertFalse(processor.dictionary_enabled)
        self.assertEqual(len(processor.ch_dictionaries), 0)
        self.assertEqual(len(processor.materialized_views), 0)

    def test_dictionary_sourced_from_prefix_mv_table(self):
        """Test that SciregDictionary is sourced from the flattened prefix table, not the raw scireg table."""
        with patch.dict(os.environ, {'CLICKHOUSE_SCIREG_METADATA_TABLE': 'custom_scireg'}):
            processor = ScienceRegistryProcessor(self.mock_pipeline)

            mv = processor.materialized_views[0]
            self.assertEqual(mv.source_table_name, 'custom_scireg')
            self.assertEqual(mv.table, 'meta_ip_scireg_prefix')

            dictionary = processor.ch_dictionaries[0]
            self.assertEqual(dictionary.source_table_name, 'meta_ip_scireg_prefix')


class TestSciregPrefixMaterializedView(unittest.TestCase):
    """Unit tests for SciregPrefixMaterializedView class."""

    def test_init_default_values(self):
        mv = SciregPrefixMaterializedView(source_table_name='meta_ip_scireg')

        self.assertEqual(mv.source_table_name, 'meta_ip_scireg')
        self.assertEqual(mv.table, 'meta_ip_scireg_prefix')
        self.assertEqual(mv.mv_name, 'meta_ip_scireg_prefix_mv')
        self.assertEqual(mv.primary_keys, ['prefix'])
        self.assertEqual(mv.order_by, ['prefix', 'id'])
        self.assertEqual(mv.table_engine, 'ReplacingMergeTree')

    @patch.dict(os.environ, {'CLICKHOUSE_SCIREG_PREFIX_TABLE': 'custom_prefix_table'})
    def test_init_custom_table_name(self):
        mv = SciregPrefixMaterializedView(source_table_name='meta_ip_scireg')
        self.assertEqual(mv.table, 'custom_prefix_table')
        self.assertEqual(mv.mv_name, 'custom_prefix_table_mv')

    def test_mv_select_query_flattens_ip_subnet(self):
        mv = SciregPrefixMaterializedView(source_table_name='meta_ip_scireg')

        self.assertIn('ARRAY JOIN ip_subnet AS ip_subnet_entry', mv.mv_select_query)
        self.assertIn('FROM meta_ip_scireg', mv.mv_select_query)
        self.assertIn('AS prefix', mv.mv_select_query)

    def test_create_table_command(self):
        mv = SciregPrefixMaterializedView(source_table_name='meta_ip_scireg')
        result = mv.create_table_command()

        self.assertIn('CREATE TABLE IF NOT EXISTS meta_ip_scireg_prefix', result)
        self.assertIn('`prefix` String', result)
        self.assertIn('`organization_name` String', result)
        self.assertIn('`organization_id` String', result)
        self.assertIn('`resource_name` String', result)
        self.assertIn('ENGINE = ReplacingMergeTree(insert_time)', result)

    def test_mv_select_query_coalesces_nulls(self):
        """IP_TRIE dictionaries don't support Nullable attributes, so NULLs
        from the raw scireg table must be coalesced to '' before they reach
        the flattened prefix table."""
        mv = SciregPrefixMaterializedView(source_table_name='meta_ip_scireg')

        self.assertIn("coalesce(organization_name, '') AS organization_name", mv.mv_select_query)
        self.assertIn("coalesce(organization_id, '') AS organization_id", mv.mv_select_query)
        self.assertIn("coalesce(resource_name, '') AS resource_name", mv.mv_select_query)

    def test_create_mv_command(self):
        mv = SciregPrefixMaterializedView(source_table_name='meta_ip_scireg')
        result = mv.create_mv_command()

        self.assertIn('CREATE MATERIALIZED VIEW IF NOT EXISTS meta_ip_scireg_prefix_mv', result)
        self.assertIn('TO meta_ip_scireg_prefix', result)


class TestSciregDictionary(unittest.TestCase):
    """Unit tests for SciregDictionary class."""

    def test_init_default_values(self):
        dictionary = SciregDictionary('meta_ip_scireg_prefix')

        self.assertEqual(dictionary.source_table_name, 'meta_ip_scireg_prefix')
        self.assertEqual(dictionary.dictionary_name, 'meta_ip_scireg_dict')
        expected_columns = [
            ['prefix', 'String'],
            ['organization_name', "String DEFAULT ''"],
            ['organization_id', "String DEFAULT ''"],
            ['resource_name', "String DEFAULT ''"],
        ]
        self.assertEqual(dictionary.column_defs, expected_columns)
        self.assertEqual(dictionary.primary_keys, ['prefix'])
        self.assertEqual(dictionary.lifetime_min, '600')
        self.assertEqual(dictionary.lifetime_max, '3600')
        self.assertEqual(dictionary.layout, "IP_TRIE()")

    @patch.dict(os.environ, {'CLICKHOUSE_SCIREG_DICTIONARY_NAME': 'custom_scireg_dict'})
    def test_init_with_custom_dictionary_name(self):
        dictionary = SciregDictionary('meta_ip_scireg_prefix')
        self.assertEqual(dictionary.dictionary_name, 'custom_scireg_dict')

    def test_create_dictionary_command(self):
        dictionary = SciregDictionary('meta_ip_scireg_prefix')
        command = dictionary.create_dictionary_command()

        self.assertIn("CREATE DICTIONARY IF NOT EXISTS meta_ip_scireg_dict", command)
        self.assertIn("`prefix` String", command)
        self.assertIn("PRIMARY KEY (`prefix`)", command)
        self.assertIn("SOURCE(CLICKHOUSE(TABLE 'meta_ip_scireg_prefix'", command)
        self.assertIn("LAYOUT(IP_TRIE())", command)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
