#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
import hashlib
import orjson
# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.device import DeviceMetadataProcessor


class TestDeviceMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
        # Mock clickhouse cacher
        self.mock_clickhouse_cacher = MagicMock()
        self.mock_clickhouse_cacher.lookup.return_value = None  # No cached record by default
        
        # Set up cacher method to return the appropriate mock based on the type
        def mock_cacher(cache_type):
            if cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            else:
                return MagicMock()
        
        self.mock_pipeline.cacher.side_effect = mock_cacher

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_device')
        
        # Test that val_id_field is correctly set
        self.assertEqual(processor.val_id_field, ['id'])
        
        # Test that required_fields is correctly set
        self.assertEqual(processor.required_fields, [['id'], ['type']])

        # Test that additional column definitions were added
        column_names = [col[0] for col in processor.column_defs]
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',  # from base class
            'type', 'loopback_ip', 'management_ip', 'hostname', 'location_name',
            'location_type', 'city_name', 'continent_name', 'country_name',
            'country_code', 'country_sub_name', 'country_sub_code', 'latitude',
            'longitude', 'manufacturer', 'model', 'network', 'os', 'role', 'state'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names, f"Column '{expected_col}' not found in column definitions")

    def test_init_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        with patch.dict(os.environ, {'CLICKHOUSE_DEVICE_METADATA_TABLE': 'custom_device_table'}):
            processor = DeviceMetadataProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_device_table')

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_DEVICE_METADATA_TABLE': 'test_device_table'}):
            processor = DeviceMetadataProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (DeviceMetadataProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Basic assertions
            self.assertIsInstance(result, str)
            self.assertIn("CREATE TABLE IF NOT EXISTS test_device_table", result)
            self.assertIn("ENGINE = MergeTree()", result)
            self.assertIn("ORDER BY", result)
            
            # Check that device-specific columns are included
            device_columns = [
                'type', 'loopback_ip', 'management_ip', 'hostname', 'location_name',
                'latitude', 'longitude', 'manufacturer', 'model', 'network', 'os', 'role', 'state'
            ]
            for column in device_columns:
                self.assertIn(f"`{column}`", result, f"Column {column} not found in CREATE TABLE command")

    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        # Ensure the environment variable is not set
        with patch.dict(os.environ, {}, clear=True):
            processor = DeviceMetadataProcessor(self.mock_pipeline)
            result = processor.create_table_command()
            
            self.assertIn("CREATE TABLE IF NOT EXISTS meta_device", result)

    def test_column_definitions_structure(self):
        """Test that column definitions are properly structured."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        # Check each column definition has correct structure [name, type, insert_flag]
        for col_def in processor.column_defs:
            self.assertIsInstance(col_def, list, "Column definition should be a list")
            self.assertEqual(len(col_def), 3, "Column definition should have 3 elements")
            self.assertIsInstance(col_def[0], str, "Column name should be a string")
            # col_def[1] can be string or None (for ext field)
            self.assertIsInstance(col_def[2], bool, "Insert flag should be a boolean")

    def test_build_metadata_fields_basic(self):
        """Test that build_metadata_fields returns correctly formatted data."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        value = {
            'type': 'router',
            'hostname': 'test-device',
            'latitude': '34.0522',
            'longitude': '-118.2437',
            'loopback_ip': '["10.1.1.1", "10.1.1.2"]',  # JSON string
            'management_ip': ['192.168.1.1'],  # Already array
            'location_type': '["datacenter", "core"]',  # JSON string
            'manufacturer': 'Cisco',
            'model': 'ASR9000',
            'network': 'backbone',
            'os': 'IOS-XR',
            'role': 'core',
            'state': 'active'
        }
    
        
        result = processor.build_metadata_fields(value)
        
        # Check basic field mapping
        self.assertEqual(result['type'], 'router')
        self.assertEqual(result['hostname'], 'test-device')
        self.assertEqual(result['manufacturer'], 'Cisco')
        self.assertEqual(result['model'], 'ASR9000')
        self.assertEqual(result['network'], 'backbone')
        self.assertEqual(result['os'], 'IOS-XR')
        self.assertEqual(result['role'], 'core')
        self.assertEqual(result['state'], 'active')
        
        # Check float conversion
        self.assertEqual(result['latitude'], 34.0522)
        self.assertEqual(result['longitude'], -118.2437)
        
        # Check JSON array parsing
        self.assertEqual(result['loopback_ip'], ['10.1.1.1', '10.1.1.2'])
        self.assertEqual(result['management_ip'], ['192.168.1.1'])
        self.assertEqual(result['location_type'], ['datacenter', 'core'])

    def test_build_metadata_fields_json_array_handling(self):
        """Test that JSON array fields are properly handled."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        # Test cases for JSON array fields
        test_cases = [
            # None values should become empty arrays
            {'loopback_ip': None, 'management_ip': None, 'location_type': None},
            # Valid JSON strings should be parsed
            {'loopback_ip': '["10.1.1.1"]', 'management_ip': '["192.168.1.1", "192.168.1.2"]', 'location_type': '["edge"]'},
            # Invalid JSON strings should become empty arrays
            {'loopback_ip': 'invalid-json', 'management_ip': '[invalid}', 'location_type': 'not-json'},
            # Already arrays should remain arrays
            {'loopback_ip': ['10.1.1.1'], 'management_ip': ['192.168.1.1'], 'location_type': ['edge']}
        ]
        
        for i, value in enumerate(test_cases):
            with self.subTest(case=i):
                result = processor.build_metadata_fields(value)
                
                if i == 0:  # None values
                    self.assertEqual(result['loopback_ip'], [])
                    self.assertEqual(result['management_ip'], [])
                    self.assertEqual(result['location_type'], [])
                elif i == 1:  # Valid JSON
                    self.assertEqual(result['loopback_ip'], ['10.1.1.1'])
                    self.assertEqual(result['management_ip'], ['192.168.1.1', '192.168.1.2'])
                    self.assertEqual(result['location_type'], ['edge'])
                elif i == 2:  # Invalid JSON
                    self.assertEqual(result['loopback_ip'], [])
                    self.assertEqual(result['management_ip'], [])
                    self.assertEqual(result['location_type'], [])
                elif i == 3:  # Already arrays
                    self.assertEqual(result['loopback_ip'], ['10.1.1.1'])
                    self.assertEqual(result['management_ip'], ['192.168.1.1'])
                    self.assertEqual(result['location_type'], ['edge'])

    def test_build_metadata_fields_float_conversion(self):
        """Test that latitude and longitude are properly converted to floats."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        test_cases = [
            # Valid string numbers
            {'latitude': '34.0522', 'longitude': '-118.2437'},
            # Valid float numbers
            {'latitude': 40.7128, 'longitude': -74.0060},
            # Valid integer numbers
            {'latitude': 51, 'longitude': 0},
            # Invalid values should become None
            {'latitude': 'invalid', 'longitude': 'not-a-number'},
            {'latitude': None, 'longitude': None},
            {'latitude': '', 'longitude': ''}
        ]
        
        expected_results = [
            {'latitude': 34.0522, 'longitude': -118.2437},
            {'latitude': 40.7128, 'longitude': -74.0060},
            {'latitude': 51.0, 'longitude': 0.0},
            {'latitude': None, 'longitude': None},
            {'latitude': None, 'longitude': None},
            {'latitude': None, 'longitude': None},
        ]
        
        for i, (value, expected) in enumerate(zip(test_cases, expected_results)):
            with self.subTest(case=i):
                result = processor.build_metadata_fields(value)
                self.assertEqual(result['latitude'], expected['latitude'])
                self.assertEqual(result['longitude'], expected['longitude'])

    def test_build_metadata_fields_comprehensive_scenario(self):
        """Test build_metadata_fields with a comprehensive real-world scenario."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        value = {
            'type': 'router',
            'loopback_ip': '["10.1.1.1", "10.1.1.2", "2001:db8::1"]',
            'management_ip': '["192.168.100.1", "2001:db8:mgmt::1"]',
            'hostname': 'losa-cr6.example.com',
            'location_name': 'Los Angeles Core 6',
            'location_type': '["datacenter", "core"]',
            'city_name': 'Los Angeles',
            'continent_name': 'North America',
            'country_name': 'United States',
            'country_code': 'US',
            'country_sub_name': 'California',
            'country_sub_code': 'CA',
            'latitude': '34.0522',
            'longitude': '-118.2437',
            'manufacturer': 'Cisco',
            'model': 'ASR 9922',
            'network': 'backbone',
            'os': 'IOS XR 7.10.2',
            'role': 'core',
            'state': 'active',
            'ext': '{"snmp_community": "public", "bgp_asn": 65001}',
            'tag': ['production', 'core', 'ipv6-enabled']
        }
        
        result = processor.build_metadata_fields(value)
        
        # Verify all fields are properly processed
        self.assertEqual(result['type'], 'router')
        self.assertEqual(result['loopback_ip'], ['10.1.1.1', '10.1.1.2', '2001:db8::1'])
        self.assertEqual(result['management_ip'], ['192.168.100.1', '2001:db8:mgmt::1'])
        self.assertEqual(result['hostname'], 'losa-cr6.example.com')
        self.assertEqual(result['location_name'], 'Los Angeles Core 6')
        self.assertEqual(result['location_type'], ['datacenter', 'core'])
        self.assertEqual(result['city_name'], 'Los Angeles')
        self.assertEqual(result['continent_name'], 'North America')
        self.assertEqual(result['country_name'], 'United States')
        self.assertEqual(result['country_code'], 'US')
        self.assertEqual(result['country_sub_name'], 'California')
        self.assertEqual(result['country_sub_code'], 'CA')
        self.assertEqual(result['latitude'], 34.0522)
        self.assertEqual(result['longitude'], -118.2437)
        self.assertEqual(result['manufacturer'], 'Cisco')
        self.assertEqual(result['model'], 'ASR 9922')
        self.assertEqual(result['network'], 'backbone')
        self.assertEqual(result['os'], 'IOS XR 7.10.2')
        self.assertEqual(result['role'], 'core')
        self.assertEqual(result['state'], 'active')
        
        # Verify ext and tag from parent class
        self.assertEqual(result['ext'], '{"snmp_community": "public", "bgp_asn": 65001}')
        self.assertEqual(result['tag'], ['production', 'core', 'ipv6-enabled'])

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        # Missing 'data' field entirely
        input_data_1 = {'other': 'value'}
        result_1 = processor.build_message(input_data_1, {})
        self.assertEqual(result_1, [])
        
        # Missing 'id' field in data
        input_data_2 = {'data': [{'type': 'router', 'hostname': 'test'}]}
        result_2 = processor.build_message(input_data_2, {})
        self.assertEqual(result_2, [])
        
        # Missing 'type' field in data
        input_data_3 = {'data': [{'id': 'test-device', 'hostname': 'test'}]}
        result_3 = processor.build_message(input_data_3, {})
        self.assertEqual(result_3, [])

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'losa-cr6',
                'type': 'router',
                'hostname': 'losa-cr6.example.com',
                'latitude': '34.0522',
                'longitude': '-118.2437',
                'loopback_ip': '["10.1.1.1"]',
                'manufacturer': 'Cisco',
                'model': 'ASR 9922'
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'losa-cr6')
        self.assertEqual(record['type'], 'router')
        self.assertEqual(record['hostname'], 'losa-cr6.example.com')
        self.assertEqual(record['latitude'], 34.0522)
        self.assertEqual(record['longitude'], -118.2437)
        self.assertEqual(record['loopback_ip'], ['10.1.1.1'])
        self.assertEqual(record['manufacturer'], 'Cisco')
        self.assertEqual(record['model'], 'ASR 9922')
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('losa-cr6__v'))

    def test_build_message_existing_record_no_change(self):
        """Test build_message skips unchanged records."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'id': 'test-device',
            'type': 'router',
            'hostname': 'test.example.com'
        }
        
        # Calculate the expected hash based on how the processor builds the record
        formatted_record = {
            "id": "test-device",
            "type": "router",
            "hostname": "test.example.com",
            "ext": "{}",
            "tag": [],
            "loopback_ip": [],
            "management_ip": [],
            "location_name": None,
            "location_type": [],
            "city_name": None,
            "continent_name": None,
            "country_name": None,
            "country_code": None,
            "country_sub_name": None,
            "country_sub_code": None,
            "latitude": None,
            "longitude": None,
            "manufacturer": None,
            "model": None,
            "network": None,
            "os": None,
            "role": None,
            "state": None
        }
        record_json = orjson.dumps(formatted_record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_hash = hashlib.md5(record_json.encode('utf-8')).hexdigest()
        
        # Mock existing record with same hash
        mock_existing = {'hash': record_hash, 'ref': 'test-device__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertEqual(result, [])

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        processor = DeviceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-device',
                'type': 'router',
                'hostname': 'test-updated.example.com'  # Changed value
            }]
        }
        
        # Mock existing record with different hash
        mock_existing = {'hash': 'different_hash', 'ref': 'test-device__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test-device')
        self.assertEqual(record['hostname'], 'test-updated.example.com')
        self.assertEqual(record['ref'], 'test-device__v2')  # Should increment version


if __name__ == '__main__':
    unittest.main()
