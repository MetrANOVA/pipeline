#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.scinet import SCinetMetadataProcessor


class TestSCinetMetadataProcessor(unittest.TestCase):
    """Unit tests for SCinetMetadataProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_ip_scinet')
        
        # Test val_id_field
        self.assertEqual(processor.val_id_field, ['resource_name'])
        
        # Test required_fields
        expected_required = [['addresses'], ['org_name'], ['resource_name']]
        self.assertEqual(processor.required_fields, expected_required)

    @patch.dict(os.environ, {'CLICKHOUSE_SCINET_METADATA_TABLE': 'custom_scinet_table'})
    def test_init_with_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        self.assertEqual(processor.table, 'custom_scinet_table')

    def test_inheritance_from_base_metadata_processor(self):
        """Test that SCinetMetadataProcessor inherits from BaseMetadataProcessor."""
        from metranova.processors.clickhouse.base import BaseMetadataProcessor
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        self.assertIsInstance(processor, BaseMetadataProcessor)

    def test_match_message_with_required_fields(self):
        """Test that match_message returns True when all required fields present."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['192.168.1.0/24'],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        self.assertTrue(processor.match_message(value))

    def test_match_message_missing_fields(self):
        """Test that match_message returns True even when some required fields missing (only checks has_match_field)."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        # Missing org_name but has match field (resource_name)
        value = {
            'addresses': ['192.168.1.0/24'],
            'resource_name': 'test-resource'
        }
        
        # match_message only checks has_match_field, not all required fields
        self.assertTrue(processor.match_message(value))

    def test_build_metadata_fields_with_cidr_notation(self):
        """Test build_metadata_fields with CIDR notation addresses."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['192.168.1.0/24', '10.0.0.0/8'],
            'org_name': 'Test Organization',
            'resource_name': 'test-resource',
            'latitude': 37.7749,
            'longitude': -122.4194
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24), ('10.0.0.0', 8)])
        self.assertEqual(result['organization_name'], 'Test Organization')
        self.assertEqual(result['coordinate_x'], 37.7749)
        self.assertEqual(result['coordinate_y'], -122.4194)
        self.assertEqual(result['ext'], '{}')
        self.assertEqual(result['tag'], [])

    def test_build_metadata_fields_with_plain_ipv4_addresses(self):
        """Test build_metadata_fields with plain IPv4 addresses (no CIDR)."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['192.168.1.1', '10.0.0.1'],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNotNone(result)
        # Plain IPv4 should default to /32
        self.assertEqual(result['ip_subnet'], [('192.168.1.1', 32), ('10.0.0.1', 32)])

    def test_build_metadata_fields_with_plain_ipv6_addresses(self):
        """Test build_metadata_fields with plain IPv6 addresses (no CIDR)."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['2001:db8::1', 'fe80::1'],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNotNone(result)
        # Plain IPv6 should default to /128
        self.assertEqual(result['ip_subnet'], [('2001:db8::1', 128), ('fe80::1', 128)])

    def test_build_metadata_fields_with_mixed_addresses(self):
        """Test build_metadata_fields with mixed CIDR and plain addresses."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['192.168.1.0/24', '10.0.0.1', '2001:db8::/32'],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNotNone(result)
        expected_subnets = [
            ('192.168.1.0', 24),
            ('10.0.0.1', 32),
            ('2001:db8::', 32)
        ]
        self.assertEqual(result['ip_subnet'], expected_subnets)

    def test_build_metadata_fields_with_invalid_address(self):
        """Test build_metadata_fields skips invalid IP addresses."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['192.168.1.0/24', 'invalid-ip', '10.0.0.0/8'],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        with patch.object(processor.logger, 'warning') as mock_warning:
            result = processor.build_metadata_fields(value)
        
        # Should skip invalid address and continue with valid ones
        self.assertEqual(len(result['ip_subnet']), 2)
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24), ('10.0.0.0', 8)])
        mock_warning.assert_called_once()

    def test_build_metadata_fields_missing_optional_coordinates(self):
        """Test build_metadata_fields with missing latitude/longitude."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': ['192.168.1.0/24'],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNotNone(result)
        self.assertIsNone(result['coordinate_x'])
        self.assertIsNone(result['coordinate_y'])

    def test_build_metadata_fields_empty_addresses_list(self):
        """Test build_metadata_fields with empty addresses list."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        value = {
            'addresses': [],
            'org_name': 'Test Org',
            'resource_name': 'test-resource'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['ip_subnet'], [])

    def test_column_definitions_include_scinet_fields(self):
        """Test that column definitions include SCinet-specific fields."""
        processor = SCinetMetadataProcessor(self.mock_pipeline)
        
        # Get all column names
        column_names = [col[0] for col in processor.column_defs]
        
        # Verify SCinet-specific columns exist
        self.assertIn('ip_subnet', column_names)
        self.assertIn('organization_name', column_names)
        self.assertIn('coordinate_x', column_names)
        self.assertIn('coordinate_y', column_names)


if __name__ == '__main__':
    unittest.main(verbosity=2)
