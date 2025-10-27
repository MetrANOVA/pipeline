#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, date

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.scireg import ScienceRegistryProcessor


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
        self.assertEqual(record["organization_ref"], "mock_org_ref")
        self.assertEqual(record["discipline"], "Computer Science")
        self.assertEqual(record["latitude"], 40.7128)
        self.assertEqual(record["longitude"], -74.0060)
        self.assertEqual(record["resource_name"], "Test Resource")
        self.assertEqual(record["project_name"], "Test Project")
        self.assertEqual(record["contact_email"], "test@example.com")
        
        # Verify IP subnet processing
        expected_subnets = [
            ("192.168.1.0", 24),
            ("10.0.0.1", 32),  # IPv4 default
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
        """Test that IPv4 addresses without CIDR get /32 prefix."""
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
        
        self.assertEqual(record["ip_subnet"], [("192.168.1.1", 32)])
    
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
            self.assertEqual(record["ip_subnet"], [("192.168.1.1", 32)])
    
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
        """Test build_message with non-list data field."""
        processor = ScienceRegistryProcessor(self.mock_pipeline)
        
        with patch.object(processor.logger, 'warning') as mock_warning:
            input_data = {
                "data": "not a list"
            }
            
            result = processor.build_message(input_data, {})
            
            # Should log warning and return empty list
            mock_warning.assert_called_with("Expected 'data' to be a list, got %s", str)
            self.assertEqual(result, [])
    
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
            ("192.168.1.0", 24),
            ("10.0.0.1", 32),
            ("2001:db8::", 32),
            ("fe80::1", 128),
            ("::1", 128)
        ]
        
        self.assertEqual(record["ip_subnet"], expected_subnets)
    
    def test_organization_id_mapping(self):
        """Test that organization_id is correctly mapped from org_name."""
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
        record = result[0]
        
        # Both organization_name and organization_id should be set to org_name
        self.assertEqual(record["organization_name"], "Test University")
        self.assertEqual(record["organization_id"], "Test University")
    
    def test_redis_cacher_lookup(self):
        """Test that redis cacher lookup is called for organization reference."""
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
        
        # Verify both cachers were called - clickhouse for base record lookup and redis for organization lookup
        expected_calls = [
            (("clickhouse",), {}),
            (("redis",), {})
        ]
        self.mock_pipeline.cacher.assert_has_calls(expected_calls, any_order=True)
        self.mock_redis_cacher.lookup.assert_called_with("meta_organization", "Test University")
        
        # Verify the result includes the mocked reference
        record = result[0]
        self.assertEqual(record["organization_ref"], "mock_org_ref")


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
