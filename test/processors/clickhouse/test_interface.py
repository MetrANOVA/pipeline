#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.interface import InterfaceMetadataProcessor, BaseInterfaceTrafficProcessor


class TestInterfaceMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        # Mock ClickHouse cacher returns dict with ref, hash, and max_insert_time
        self.mock_pipeline.cacher.return_value.lookup.return_value = {
            'ref': 'mock_lookup_result__v1',
            'hash': 'mock_hash',
            'max_insert_time': '2023-01-01 00:00:00'
        }
        
    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        
        # Create an instance of InterfaceMetadataProcessor
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (InterfaceMetadataProcessor - Basic):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure
        self.assertIn("CREATE TABLE IF NOT EXISTS meta_interface", result)
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertIn("ORDER BY (`ref`,`id`,`insert_time`)", result)
        self.assertIn("SETTINGS index_granularity = 8192", result)
        
        # Check for specific core columns
        self.assertIn("`id` String", result)
        self.assertIn("`insert_time` DateTime DEFAULT now()", result)
        self.assertIn("`ext` JSON", result)
        
        # Check for interface-specific columns
        self.assertIn("`type` LowCardinality(String)", result)
        self.assertIn("`description` Nullable(String)", result)
        self.assertIn("`device_id` String", result)
        self.assertIn("`device_ref` Nullable(String)", result)
        self.assertIn("`edge` Bool", result)
        self.assertIn("`flow_index` Nullable(UInt32)", result)
        self.assertIn("`ipv4` Nullable(IPv4)", result)
        self.assertIn("`ipv6` Nullable(IPv6)", result)
        self.assertIn("`name` String", result)
        self.assertIn("`speed` Nullable(UInt64)", result)
        self.assertIn("`circuit_id` Array(String)", result)
        self.assertIn("`circuit_ref` Array(Nullable(String))", result)
        self.assertIn("`peer_as_id` Nullable(UInt32)", result)
        self.assertIn("`peer_as_ref` Nullable(String)", result)
        self.assertIn("`peer_interface_ipv4` Nullable(IPv4)", result)
        self.assertIn("`peer_interface_ipv6` Nullable(IPv6)", result)
        self.assertIn("`lag_member_interface_id` Array(LowCardinality(String))", result)
        self.assertIn("`lag_member_interface_ref` Array(Nullable(String))", result)
        self.assertIn("`port_interface_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`port_interface_ref` Nullable(String)", result)
        self.assertIn("`remote_interface_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`remote_interface_ref` Nullable(String)", result)
        self.assertIn("`remote_organization_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`remote_organization_ref` Nullable(String)", result)

    @patch.dict(os.environ, {'CLICKHOUSE_IF_METADATA_TABLE': 'custom_interface_metadata'})
    def test_create_table_command_with_custom_table_name(self):
        """Test the create_table_command method with custom table name."""
        
        # Create an instance of InterfaceMetadataProcessor
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (InterfaceMetadataProcessor - Custom Table Name):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Should use custom table name
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_interface_metadata", result)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS meta_interface", result)

    def test_build_metadata_fields_basic(self):
        """Test that build_metadata_fields returns correctly formatted data."""
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        # Sample input data
        input_data = {
            "id": "interface1",
            "device_id": "device1", 
            "name": "eth0",
            "type": "ethernet",
            "description": "Main interface",
            "edge": "true",
            "flow_index": "100",
            "ipv4": "192.168.1.1",
            "ipv6": "2001:db8::1",
            "speed": "1000000000",
            "circuit_id": '["circuit1", "circuit2"]',
            "peer_as_id": "65001",
            "peer_interface_ipv4": "192.168.1.2",
            "peer_interface_ipv6": "2001:db8::2",
            "lag_member_interface_id": '["member1", "member2"]',
            "port_interface_id": "port1",
            "remote_interface_id": "remote1",
            "remote_organization_id": "org1",
            "tag": '["tag1", "tag2"]',
            "ext": '{"custom": "data"}'
        }
        
        result = processor.build_metadata_fields(input_data)
        
        # Should return a dict with all required fields
        self.assertIsInstance(result, dict)
        
        # Verify field mappings
        self.assertEqual(result["type"], "ethernet")
        self.assertEqual(result["device_id"], "device1")
        self.assertEqual(result["device_ref"], "mock_lookup_result__v1")
        self.assertEqual(result["description"], "Main interface")
        self.assertEqual(result["edge"], True)  # Should be converted to boolean
        self.assertEqual(result["flow_index"], 100)  # Should be converted to int
        self.assertEqual(result["ipv4"], "192.168.1.1")
        self.assertEqual(result["ipv6"], "2001:db8::1")
        self.assertEqual(result["name"], "eth0")
        self.assertEqual(result["speed"], 1000000000)  # Should be converted to int
        self.assertEqual(result["circuit_id"], ["circuit1", "circuit2"])  # Should be parsed JSON
        self.assertEqual(result["circuit_ref"], ["mock_lookup_result__v1", "mock_lookup_result__v1"])
        self.assertEqual(result["peer_as_id"], 65001)  # Should be converted to int
        self.assertEqual(result["peer_as_ref"], "mock_lookup_result__v1")
        self.assertEqual(result["peer_interface_ipv4"], "192.168.1.2")
        self.assertEqual(result["peer_interface_ipv6"], "2001:db8::2")
        self.assertEqual(result["lag_member_interface_id"], ["member1", "member2"])
        self.assertEqual(result["lag_member_interface_ref"], ["mock_lookup_result__v1", "mock_lookup_result__v1"])
        self.assertEqual(result["port_interface_id"], "port1")
        self.assertEqual(result["port_interface_ref"], "mock_lookup_result__v1")
        self.assertEqual(result["remote_interface_id"], "remote1")
        self.assertEqual(result["remote_interface_ref"], "mock_lookup_result__v1")
        self.assertEqual(result["remote_organization_id"], "org1")
        self.assertEqual(result["remote_organization_ref"], "mock_lookup_result__v1")
        self.assertEqual(result["tag"], ["tag1", "tag2"])
        self.assertEqual(result["ext"], '{"custom": "data"}')

    def test_build_metadata_fields_boolean_conversion(self):
        """Test that edge field is properly converted to boolean."""
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        test_cases = [
            (True, True),
            ('true', True),
            ('True', True),
            (1, True),
            ('1', True),
            (False, False),
            ('false', False),
            (0, False),
            ('0', False),
            (None, False)
        ]
        
        for input_value, expected in test_cases:
            input_data = {
                "id": "interface1",
                "device_id": "device1", 
                "name": "eth0",
                "type": "ethernet",
                "edge": input_value
            }
            
            result = processor.build_metadata_fields(input_data)
            self.assertEqual(result["edge"], expected, f"Failed for input: {input_value}")

    def test_build_metadata_fields_integer_conversion(self):
        """Test that integer fields are properly converted and handle invalid values."""
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            "id": "interface1",
            "device_id": "device1", 
            "name": "eth0",
            "type": "ethernet",
            "flow_index": "invalid",  # Should become None
            "speed": "1000",  # Should become 1000
            "peer_as_id": "not_a_number"  # Should become None
        }
        
        result = processor.build_metadata_fields(input_data)
        
        self.assertIsNone(result["flow_index"])
        self.assertEqual(result["speed"], 1000)
        self.assertIsNone(result["peer_as_id"])

    def test_build_metadata_fields_empty_json_arrays(self):
        """Test that empty JSON arrays are handled correctly."""
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            "data": {
                "id": "interface1",
                "device_id": "device1", 
                "name": "eth0",
                "type": "ethernet"
                # Missing circuit_id, lag_member_interface_id, tag - should use defaults
            }
        }
        
        result = processor.build_metadata_fields(input_data)
        
        self.assertEqual(result["circuit_id"], [])
        self.assertEqual(result["circuit_ref"], [])
        self.assertEqual(result["lag_member_interface_id"], [])
        self.assertEqual(result["lag_member_interface_ref"], [])
        self.assertEqual(result["tag"], [])

    def test_build_metadata_fields_json_parsing_error(self):
        """Test that invalid JSON strings are handled gracefully."""
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            "data": {
                "id": "interface1",
                "device_id": "device1", 
                "name": "eth0",
                "type": "ethernet",
                "circuit_id": "invalid_json_string",
                "lag_member_interface_id": "[malformed json",
                "tag": "not_json_at_all"
            }
        }
        
        result = processor.build_metadata_fields(input_data)
        
        # Should default to empty arrays for invalid JSON
        self.assertEqual(result["circuit_id"], [])
        self.assertEqual(result["circuit_ref"], [])
        self.assertEqual(result["lag_member_interface_id"], [])
        self.assertEqual(result["lag_member_interface_ref"], [])
        self.assertEqual(result["tag"], [])

    def test_build_metadata_fields_array_ref_lookup(self):
        """Test that array reference lookups work correctly."""
        
        # Configure mock to return different values for different lookups
        def mock_lookup(table, key):
            if key:
                return {
                    'ref': f"ref_for_{key}",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                }
            return None
        
        self.mock_pipeline.cacher.return_value.lookup.side_effect = mock_lookup
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            "id": "interface1",
            "device_id": "device1", 
            "name": "eth0",
            "type": "ethernet",
            "circuit_id": '["circuit1", "circuit2"]',
            "lag_member_interface_id": '["lag1", "lag2", "lag3"]'
        }
        
        result = processor.build_metadata_fields(input_data)
        
        # Check that circuit_ref is populated correctly
        self.assertEqual(result["circuit_id"], ["circuit1", "circuit2"])
        self.assertEqual(result["circuit_ref"], ["ref_for_circuit1", "ref_for_circuit2"])
        
        # Check that lag_member_interface_ref is populated correctly
        self.assertEqual(result["lag_member_interface_id"], ["lag1", "lag2", "lag3"])
        self.assertEqual(result["lag_member_interface_ref"], ["ref_for_lag1", "ref_for_lag2", "ref_for_lag3"])

    def test_build_metadata_fields_ext_dict_conversion(self):
        """Test that ext field dict is converted to JSON string."""
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            "id": "interface1",
            "device_id": "device1", 
            "name": "eth0",
            "type": "ethernet",
            "ext": {"custom_field": "value", "number": 42}
        }
        
        result = processor.build_metadata_fields(input_data)
        
        # Should be converted to JSON string
        self.assertIsInstance(result["ext"], str)
        # Parse it back to verify content
        import orjson
        parsed_ext = orjson.loads(result["ext"])
        self.assertEqual(parsed_ext["custom_field"], "value")
        self.assertEqual(parsed_ext["number"], 42)

    def test_build_metadata_fields_comprehensive_scenario(self):
        """Test build_metadata_fields with a comprehensive real-world scenario including all new functionality."""
        
        # Configure mock lookup to simulate different ref resolutions
        def mock_lookup(table, key):
            lookup_map = {
                ("meta_device", "device1"): {
                    'ref': "Device Reference 1",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_as", "65001"): {
                    'ref': "AS Reference 65001",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_as", 65001): {
                    'ref': "AS Reference 65001",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_interface", "port1"): {
                    'ref': "Port Interface Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_interface", "remote1"): {
                    'ref': "Remote Interface Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_circuit", "circuit1"): {
                    'ref': "Circuit 1 Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_circuit", "circuit2"): {
                    'ref': "Circuit 2 Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_interface", "lag1"): {
                    'ref': "LAG Member 1 Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_interface", "lag2"): {
                    'ref': "LAG Member 2 Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                },
                ("meta_organization", "org1"): {
                    'ref': "Organization Ref",
                    'hash': 'mock_hash',
                    'max_insert_time': '2023-01-01 00:00:00'
                }
            }
            return lookup_map.get((table, key), None)
        
        self.mock_pipeline.cacher.return_value.lookup.side_effect = mock_lookup
        
        processor = InterfaceMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            "id": "interface1",
            "device_id": "device1", 
            "name": "eth0",
            "type": "ethernet",
            "description": "Main interface",
            "edge": "true",
            "flow_index": "100",
            "ipv4": "192.168.1.1",
            "ipv6": "2001:db8::1",
            "speed": "1000000000",
            "circuit_id": '["circuit1", "circuit2"]',
            "peer_as_id": "65001",
            "peer_interface_ipv4": "192.168.1.2",
            "peer_interface_ipv6": "2001:db8::2",
            "lag_member_interface_id": '["lag1", "lag2"]',
            "port_interface_id": "port1",
            "remote_interface_id": "remote1",
            "remote_organization_id": "org1",
            "tag": '["production", "critical"]',
            "ext": {"vlan": 100, "mtu": 1500}
        }
        
        result = processor.build_metadata_fields(input_data)
        
        # Verify all the new JSON array processing and ref lookups
        self.assertEqual(result["circuit_id"], ["circuit1", "circuit2"])
        self.assertEqual(result["circuit_ref"], ["Circuit 1 Ref", "Circuit 2 Ref"])
        self.assertEqual(result["lag_member_interface_id"], ["lag1", "lag2"])
        self.assertEqual(result["lag_member_interface_ref"], ["LAG Member 1 Ref", "LAG Member 2 Ref"])
        self.assertEqual(result["tag"], ["production", "critical"])

        # Verify ext conversion
        self.assertIsInstance(result["ext"], str)
        import orjson
        parsed_ext = orjson.loads(result["ext"])
        self.assertEqual(parsed_ext["vlan"], 100)
        self.assertEqual(parsed_ext["mtu"], 1500)
        
        # Verify other standard lookups still work
        self.assertEqual(result["device_ref"], "Device Reference 1")
        self.assertEqual(result["peer_as_ref"], "AS Reference 65001")
        self.assertEqual(result["port_interface_ref"], "Port Interface Ref")
        self.assertEqual(result["remote_interface_ref"], "Remote Interface Ref")
        self.assertEqual(result["remote_organization_ref"], "Organization Ref")


class TestBaseInterfaceTrafficProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        
        # Create an instance of BaseInterfaceTrafficProcessor
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseInterfaceTrafficProcessor - Basic):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure
        self.assertIn("CREATE TABLE IF NOT EXISTS data_interface_traffic", result)
        self.assertIn("ENGINE = CoalescingMergeTree", result)
        self.assertIn("PARTITION BY toYYYYMMDD(start_time)", result)
        self.assertIn("ORDER BY (`interface_id`,`policy_level`,`policy_scope`,`policy_originator`,`collector_id`,`start_time`)", result)
        self.assertIn("SETTINGS index_granularity = 8192", result)
        
        # Check for specific core columns with proper codecs
        self.assertIn("`start_time` DateTime('UTC') CODEC(Delta,ZSTD)", result)
        self.assertIn("`end_time` DateTime('UTC') CODEC(Delta,ZSTD)", result)
        self.assertIn("`insert_time` DateTime64(3, 'UTC') DEFAULT now64()", result)
        self.assertIn("`collector_id` LowCardinality(String)", result)
        self.assertIn("`policy_originator` LowCardinality(String)", result)
        self.assertIn("`policy_level` LowCardinality(String)", result)
        self.assertIn("`policy_scope` Array(LowCardinality(String))", result)
        self.assertIn("`ext` JSON", result)
        
        # Check for interface traffic-specific columns
        self.assertIn("`interface_id` String", result)
        self.assertIn("`interface_ref` Nullable(String)", result)
        self.assertIn("`admin_status` LowCardinality(Nullable(String))", result)
        self.assertIn("`oper_status` LowCardinality(Nullable(String))", result)
        
        # Check for traffic counter columns with codecs
        self.assertIn("`in_bit_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`in_discard_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`in_error_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`in_bcast_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`in_ucast_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`in_mcast_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`out_bit_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`out_discard_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`out_error_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`out_bcast_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`out_ucast_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)
        self.assertIn("`out_mcast_packet_count` Nullable(UInt64) CODEC(Delta,ZSTD)", result)

    @patch.dict(os.environ, {'CLICKHOUSE_IF_TRAFFIC_TABLE': 'custom_interface_traffic'})
    def test_create_table_command_with_custom_table_name(self):
        """Test the create_table_command method with custom table name."""
        
        # Create an instance of BaseInterfaceTrafficProcessor
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseInterfaceTrafficProcessor - Custom Table Name):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Should use custom table name
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_interface_traffic", result)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS data_interface_traffic", result)

    def test_table_engine_configuration(self):
        """Test that the table engine is correctly set to CoalescingMergeTree."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Check that the table engine is set correctly
        self.assertEqual(processor.table_engine, "CoalescingMergeTree")
        
        # Check partition and order by settings
        self.assertEqual(processor.partition_by, 'toYYYYMMDD(start_time)')
        self.assertEqual(processor.order_by, ['interface_id', 'policy_level', 'policy_scope', 'policy_originator', 'collector_id', 'start_time'])

    def test_column_definitions_structure(self):
        """Test that column definitions are properly structured."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Check that start_time and end_time are at the beginning
        self.assertEqual(processor.column_defs[0][0], "start_time")
        self.assertEqual(processor.column_defs[1][0], "end_time")
        
        # Check that they have the correct codec
        self.assertIn("CODEC(Delta,ZSTD)", processor.column_defs[0][1])
        self.assertIn("CODEC(Delta,ZSTD)", processor.column_defs[1][1])
        
        # Find traffic counter columns and verify they have codecs
        traffic_columns = [col for col in processor.column_defs if col[0].endswith('_count') and 'CODEC(Delta,ZSTD)' in col[1]]
        
        # Should have many traffic counter columns with codecs
        self.assertGreater(len(traffic_columns), 10)
        
        # Check specific columns exist
        column_names = [col[0] for col in processor.column_defs]
        expected_columns = [
            'start_time', 'end_time', 'interface_id', 'interface_ref',
            'admin_status', 'oper_status', 'in_bit_count',
            'out_bit_count'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names)

    def test_required_fields_configuration(self):
        """Test that BaseInterfaceTrafficProcessor has the basic required fields from BaseDataProcessor."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Should inherit required fields from BaseDataProcessor
        # The exact required fields depend on the base class implementation
        self.assertIsInstance(processor.required_fields, list)

    @patch.dict(os.environ, {'CLICKHOUSE_IF_TRAFFIC_PARTITION_BY': 'toYYYYMM(start_time)'})
    def test_create_table_command_with_custom_partition(self):
        """Test create_table_command with custom partition configuration."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseInterfaceTrafficProcessor - Custom Partition):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for custom partition
        self.assertIn("PARTITION BY toYYYYMM(start_time)", result)

    @patch.dict(os.environ, {'CLICKHOUSE_IF_TRAFFIC_TTL': '60 DAY', 'CLICKHOUSE_IF_TRAFFIC_TTL_COLUMN': 'end_time'})
    def test_create_table_command_with_ttl(self):
        """Test create_table_command with TTL configuration."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseInterfaceTrafficProcessor - TTL):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for TTL clause and settings
        self.assertIn("TTL end_time + INTERVAL 60 DAY", result)
        self.assertIn("ttl_only_drop_parts = 1", result)

    @patch.dict(os.environ, {
        'CLICKHOUSE_IF_TRAFFIC_TABLE': 'custom_interface_metrics',
        'CLICKHOUSE_IF_TRAFFIC_PARTITION_BY': 'toYYYYMMDD(end_time)',
        'CLICKHOUSE_IF_TRAFFIC_TTL': '365 DAY',
        'CLICKHOUSE_IF_TRAFFIC_TTL_COLUMN': 'start_time'
    })
    def test_create_table_command_comprehensive_configuration(self):
        """Test create_table_command with comprehensive custom configuration."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseInterfaceTrafficProcessor - Comprehensive):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for all custom configurations
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_interface_metrics", result)
        self.assertIn("PARTITION BY toYYYYMMDD(end_time)", result)
        self.assertIn("TTL start_time + INTERVAL 365 DAY", result)
        self.assertIn("ttl_only_drop_parts = 1", result)
        
        # Verify CoalescingMergeTree engine is still used
        self.assertIn("ENGINE = CoalescingMergeTree", result)

    def test_default_configuration_values(self):
        """Test that the processor has correct default values."""
        
        processor = BaseInterfaceTrafficProcessor(self.mock_pipeline)
        
        # Check default values
        self.assertEqual(processor.table, 'data_interface_traffic')
        self.assertEqual(processor.partition_by, 'toYYYYMMDD(start_time)')
        self.assertEqual(processor.table_ttl, '180 DAY')
        self.assertEqual(processor.table_ttl_column, 'start_time')
        self.assertEqual(processor.table_engine, 'CoalescingMergeTree')
        
        # Check that order_by is properly set
        expected_order_by = ['interface_id', 'policy_level', 'policy_scope', 'policy_originator', 'collector_id', 'start_time']
        self.assertEqual(processor.order_by, expected_order_by)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
