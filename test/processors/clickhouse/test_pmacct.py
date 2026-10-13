#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, Mock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.pmacct import PMAcctFlowProcessor


class TestPMAcctFlowProcessor(unittest.TestCase):
    """Unit tests for PMAcctFlowProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Setup mock cachers
        mock_clickhouse_cacher = Mock()
        mock_clickhouse_cacher.lookup.return_value = None
        mock_clickhouse_cacher.lookup_dict_key.return_value = None
        
        mock_ip_cacher = Mock()
        mock_ip_cacher.lookup.return_value = None

        mock_clickhouse_ranged_cacher = Mock()
        mock_clickhouse_ranged_cacher.lookup_dict_key.return_value = None
        
        def cacher_side_effect(name):
            if name == "clickhouse":
                return mock_clickhouse_cacher
            elif name == "ip":
                return mock_ip_cacher
            elif name == "clickhouse_ranged":
                return mock_clickhouse_ranged_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        
        self.processor = PMAcctFlowProcessor(self.mock_pipeline)

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        self.assertIsNotNone(self.processor.flow_type_map)
        self.assertEqual(self.processor.flow_type_map['nfacctd'], 'netflow')
        self.assertEqual(self.processor.flow_type_map['sfacctd'], 'sflow')
        
        # Test required_fields
        expected_required = [['ip_src'], ['ip_dst'], ['port_src'], ['port_dst'], ['ip_proto']]
        self.assertEqual(self.processor.required_fields, expected_required)

    def test_inheritance_from_base_flow_processor(self):
        """Test that PMAcctFlowProcessor inherits from BaseFlowProcessor."""
        from metranova.processors.clickhouse.flow import BaseFlowProcessor
        self.assertIsInstance(self.processor, BaseFlowProcessor)

    def test_add_bgp_extensions_with_as_path(self):
        """Test add_bgp_extensions with AS path data."""
        value = {
            'as_path': '65001_65001_65002_65003_65003_65003'
        }
        ext = {}
        
        self.processor.add_bgp_extensions(value, ext)
        
        # Padding logic: appends padding_count when NEW ASN encountered
        # 65001(count=1), 65001(count++), 65002(append prev count=2, count=1), 65003(append 1, count=1), 65003(count++), 65003(count++)
        # Final count not appended, so padding = [1, 2, 1]
        self.assertEqual(ext['bgp_as_path_id'], [65001, 65002, 65003])
        self.assertEqual(ext['bgp_as_path_padding'], [1, 2, 1])

    def test_add_bgp_extensions_with_communities(self):
        """Test add_bgp_extensions with community data."""
        value = {
            'as_path': '65001',  # Need as_path to trigger BGP processing
            'comms': '64512:100_64512:200',
            'ecomms': 'RT:64512:100',
            'lcomms': '65001:100:200'
        }
        ext = {}
        
        self.processor.add_bgp_extensions(value, ext)
        
        self.assertEqual(ext['bgp_community'], ['64512:100', '64512:200'])
        self.assertEqual(ext['bgp_ext_community'], ['RT:64512:100'])
        self.assertEqual(ext['bgp_large_community'], ['65001:100:200'])

    def test_add_bgp_extensions_with_local_pref_and_med(self):
        """Test add_bgp_extensions with local preference and MED."""
        value = {
            'as_path': '65001',  # Need as_path to trigger bgp extension processing
            'local_pref': '100',
            'med': '50'
        }
        ext = {}
        
        self.processor.add_bgp_extensions(value, ext)
        
        self.assertEqual(ext['bgp_local_pref'], 100)
        self.assertEqual(ext['bgp_med'], 50)

    def test_add_ipv4_extensions(self):
        """Test add_ipv4_extensions with TOS value."""
        value = {'tos': '64'}
        ext = {}
        
        self.processor.add_ipv4_extensions(value, ext)
        
        self.assertEqual(ext['ipv4_tos'], 64)
        self.assertEqual(ext['ipv4_dscp'], 16)  # 64 >> 2 = 16

    def test_add_ipv6_extensions(self):
        """Test add_ipv6_extensions with flow label."""
        value = {'ipv6_flow_label': '12345'}
        ext = {}
        
        self.processor.add_ipv6_extensions(value, ext)
        
        self.assertEqual(ext['ipv6_flow_label'], 12345)

    def test_add_mpls_extensions_with_single_string_field(self):
        """Test add_mpls_extensions with mpls_label as single string."""
        value = {
            'mpls_label': '12345-0_67890-0'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        self.assertIn('mpls_label', ext)
        self.assertIn('mpls_exp', ext)
        self.assertGreater(len(ext['mpls_label']), 0)

    def test_add_mpls_extensions_with_vpn_rd(self):
        """Test add_mpls_extensions with VPN RD."""
        value = {'mpls_vpn_rd': '65001:100'}
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        self.assertEqual(ext['mpls_vpn_rd'], '65001:100')

    def test_add_vlan_extensions_all_types(self):
        """Test add_vlan_extensions with all VLAN types."""
        value = {
            'vlan': '100',
            'vlan_in': '200',
            'vlan_out': '300',
            'cvlan_in': '400',
            'cvlan_out': '500'
        }
        ext = {}
        
        self.processor.add_vlan_extensions(value, ext)
        
        self.assertEqual(ext['vlan_id'], 100)
        self.assertEqual(ext['vlan_in_id'], 200)
        self.assertEqual(ext['vlan_out_id'], 300)
        self.assertEqual(ext['vlan_in_inner_id'], 400)
        self.assertEqual(ext['vlan_out_inner_id'], 500)

    def test_lookup_ip_as_fields_with_ip_cache_hit(self):
        """Test lookup_ip_as_fields when IP is found in cache."""
        mock_ip_result = ('ip_ref_123', 65001)
        
        def cacher_side_effect(name):
            if name == "ip":
                mock_cacher = Mock()
                mock_cacher.lookup.return_value = mock_ip_result
                return mock_cacher
            elif name == "clickhouse":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = 'as_ref_123'
                return mock_cacher
            elif name == "clickhouse_ranged":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = None
                return mock_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        value = {'ip_src': '192.168.1.1'}
        formatted_record = {}
        
        processor.lookup_ip_as_fields('ip_src', 'src_ip', 'as_src', 'src_as', value, formatted_record)
        
        self.assertEqual(formatted_record['src_ip'], '192.168.1.1')
        self.assertEqual(formatted_record['src_ip_ref'], 'ip_ref_123')
        self.assertEqual(formatted_record['src_as_id'], 65001)
        self.assertEqual(formatted_record['src_as_ref'], 'as_ref_123')

    def test_lookup_ip_as_fields_with_empty_ip(self):
        """Test lookup_ip_as_fields with empty IP address."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        value = {'ip_src': ''}
        formatted_record = {}
        
        processor.lookup_ip_as_fields('ip_src', 'src_ip', 'as_src', 'src_as', value, formatted_record)
        
        self.assertIsNone(formatted_record['src_ip'])
        self.assertIsNone(formatted_record['src_ip_ref'])
        self.assertIsNone(formatted_record['src_as_id'])
        self.assertIsNone(formatted_record['src_as_ref'])

    def test_lookup_interface_found_edge_true(self):
        """Test lookup_interface when interface is found with edge=True."""
        mock_interface = {'id': 'if123', 'ref': 'if_ref_123'}
        
        def cacher_side_effect(name):
            if name == "clickhouse":
                mock_cacher = Mock()
                mock_cacher.lookup.side_effect = lambda table, key: mock_interface if 'True' in key else None
                return mock_cacher
            elif name == "clickhouse_ranged":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = None
                return mock_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_interface('device1', '10', formatted_record, 'in')
        
        self.assertEqual(formatted_record['in_interface_id'], 'if123')
        self.assertEqual(formatted_record['in_interface_ref'], 'if_ref_123')
        self.assertTrue(formatted_record['in_interface_edge'])

    def test_lookup_interface_not_found(self):
        """Test lookup_interface when interface is not found."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_interface('device1', '10', formatted_record, 'out')
        
        self.assertIsNone(formatted_record['out_interface_id'])
        self.assertIsNone(formatted_record['out_interface_ref'])
        self.assertFalse(formatted_record['out_interface_edge'])

    def test_lookup_device_found(self):
        """Test lookup_device when device is found in cache."""
        mock_device = {'id': 'device123', 'ref': 'device_ref_123'}
        
        def cacher_side_effect(name):
            if name == "clickhouse":
                mock_cacher = Mock()
                mock_cacher.lookup.return_value = mock_device
                return mock_cacher
            elif name == "clickhouse_ranged":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = None
                return mock_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_device('10.0.0.1', formatted_record)
        
        self.assertEqual(formatted_record['device_id'], 'device123')
        self.assertEqual(formatted_record['device_ref'], 'device_ref_123')

    def test_lookup_device_not_found(self):
        """Test lookup_device when device is not found."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_device('10.0.0.1', formatted_record)
        
        # Should fall back to IP address
        self.assertEqual(formatted_record['device_id'], '10.0.0.1')
        self.assertIsNone(formatted_record['device_ref'])

    def test_build_message_missing_required_fields(self):
        """Test build_message returns None when required fields missing."""
        value = {'ip_src': '192.168.1.1'}  # Missing other required fields
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNone(result)

    def test_build_message_netflow_with_timestamp_start_end(self):
        """Test build_message with netflow data (timestamp_start and timestamp_end)."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',  # 2021-01-01 00:00:00
            'timestamp_end': '1609459260.0',    # 2021-01-01 00:01:00
            'bytes': 1024,
            'packets': 10,
            'writer_id': 'nfacctd',
            'label': 'collector1',
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        record = result[0]
        
        self.assertEqual(record['start_time'], 1609459200000)
        self.assertEqual(record['end_time'], 1609459260000)
        self.assertEqual(record['flow_type'], 'netflow')
        self.assertEqual(record['src_port'], 12345)
        self.assertEqual(record['dst_port'], 80)
        self.assertEqual(record['protocol'], 6)
        self.assertEqual(record['bit_count'], 1024 * 8)  # Scaled to bits
        self.assertEqual(record['packet_count'], 10)

    def test_build_message_sflow_with_timestamp_arrival(self):
        """Test build_message with sflow data (timestamp_arrival only)."""
        value = {
            'ip_src': '10.0.0.1',
            'ip_dst': '10.0.0.2',
            'port_src': 443,
            'port_dst': 54321,
            'ip_proto': 6,
            'timestamp_arrival': '1609459200.0',
            'bytes': 2048,
            'packets': 5,
            'writer_id': 'sfacctd',
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        record = result[0]
        
        # Should use arrival time for both start and end
        self.assertEqual(record['start_time'], 1609459200000)
        self.assertEqual(record['end_time'], 1609459200000)
        self.assertEqual(record['flow_type'], 'sflow')

    def test_build_message_missing_timestamp(self):
        """Test build_message returns None when timestamp fields missing."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'bytes': 1024,
            'packets': 10
            # No timestamp fields
        }
        
        with patch.object(self.processor.logger, 'debug') as mock_debug:
            result = self.processor.build_message(value, {})
        
        self.assertIsNone(result)

    def test_build_message_ipv6_detection(self):
        """Test build_message correctly detects IPv6 addresses."""
        value = {
            'ip_src': '2001:db8::1',
            'ip_dst': '2001:db8::2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        self.assertEqual(record['ip_version'], 6)

    def test_build_message_application_port_dst(self):
        """Test build_message uses dst_port for application_port when available."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 54321,
            'port_dst': 443,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        self.assertEqual(record['application_port'], 443)

    def test_build_message_application_port_when_dst_port_zero(self):
        """Test build_message when dst_port is zero (valid port)."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 443,
            'port_dst': 0,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # port 0 is valid, so application_port should be 0, not fallback to src
        self.assertEqual(record['dst_port'], 0)
        self.assertEqual(record['application_port'], 0)

    def test_add_bgp_extensions_with_invalid_asn(self):
        """Test add_bgp_extensions skips invalid ASN values."""
        value = {
            'as_path': '65001_invalid_65003'
        }
        ext = {}
        
        self.processor.add_bgp_extensions(value, ext)
        
        # Should skip 'invalid' and only include valid ASNs
        self.assertEqual(ext['bgp_as_path_id'], [65001, 65003])

    def test_add_bgp_extensions_with_invalid_local_pref(self):
        """Test add_bgp_extensions with invalid local_pref value."""
        value = {
            'as_path': '65001',
            'local_pref': 'invalid'
        }
        ext = {}
        
        self.processor.add_bgp_extensions(value, ext)
        
        # Should not include local_pref if invalid
        self.assertNotIn('bgp_local_pref', ext)

    def test_add_bgp_extensions_with_invalid_med(self):
        """Test add_bgp_extensions with invalid MED value."""
        value = {
            'as_path': '65001',
            'med': 'invalid'
        }
        ext = {}
        
        self.processor.add_bgp_extensions(value, ext)
        
        # Should not include med if invalid
        self.assertNotIn('bgp_med', ext)

    def test_add_ipv4_extensions_with_invalid_tos(self):
        """Test add_ipv4_extensions with invalid TOS value."""
        value = {'tos': 'invalid'}
        ext = {}
        
        self.processor.add_ipv4_extensions(value, ext)
        
        # Should not add fields if invalid
        self.assertNotIn('ipv4_tos', ext)
        self.assertNotIn('ipv4_dscp', ext)

    def test_add_ipv6_extensions_with_invalid_flow_label(self):
        """Test add_ipv6_extensions with invalid flow_label."""
        value = {'ipv6_flow_label': 'invalid'}
        ext = {}
        
        self.processor.add_ipv6_extensions(value, ext)
        
        # Should not add field if invalid
        self.assertNotIn('ipv6_flow_label', ext)

    def test_add_mpls_extensions_with_separate_label_fields(self):
        """Test add_mpls_extensions with separate mpls_label1, mpls_label2, etc."""
        value = {
            'mpls_label1': '12345-0',
            'mpls_label2': '67890-2',
            'mpls_label3': '11111-4'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        self.assertIn('mpls_label', ext)
        self.assertIn('mpls_exp', ext)
        self.assertGreater(len(ext['mpls_label']), 0)

    def test_add_mpls_extensions_with_invalid_label_hex(self):
        """Test add_mpls_extensions skips invalid hex values."""
        value = {
            'mpls_label': '12345-0_invalid-0'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        # Should process valid label and skip invalid
        self.assertIn('mpls_label', ext)

    def test_add_mpls_extensions_with_pw_id(self):
        """Test add_mpls_extensions with pseudowire ID."""
        value = {
            'mpls_label': '12345-0',
            'mpls_pw_id': '100'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        self.assertEqual(ext['mpls_pw'], 100)

    def test_add_mpls_extensions_with_invalid_pw_id(self):
        """Test add_mpls_extensions with invalid pseudowire ID."""
        value = {
            'mpls_label': '12345-0',
            'mpls_pw_id': 'invalid'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        # Should not include pw if invalid
        self.assertNotIn('mpls_pw', ext)

    def test_add_mpls_extensions_with_top_label_ip(self):
        """Test add_mpls_extensions with top label IPv4."""
        value = {
            'mpls_label': '12345-0',
            'mpls_top_label_ipv4': '192.168.1.1'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        self.assertEqual(ext['mpls_top_label_ip'], '192.168.1.1')

    def test_add_mpls_extensions_with_top_label_type(self):
        """Test add_mpls_extensions with top label type."""
        value = {
            'mpls_label': '12345-0',
            'mpls_top_label_type': '1'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        self.assertEqual(ext['mpls_top_label_type'], 1)

    def test_add_mpls_extensions_with_invalid_top_label_type(self):
        """Test add_mpls_extensions with invalid top label type."""
        value = {
            'mpls_label': '12345-0',
            'mpls_top_label_type': 'invalid'
        }
        ext = {}
        
        self.processor.add_mpls_extensions(value, ext)
        
        # Should not include type if invalid
        self.assertNotIn('mpls_top_label_type', ext)

    def test_add_vlan_extensions_with_invalid_values(self):
        """Test add_vlan_extensions skips invalid integer values."""
        value = {
            'vlan': 'invalid',
            'vlan_in': 'bad',
            'vlan_out': 'wrong',
            'cvlan_in': 'nope',
            'cvlan_out': 'no'
        }
        ext = {}
        
        self.processor.add_vlan_extensions(value, ext)
        
        # Should not add any fields if all are invalid
        self.assertNotIn('vlan_id', ext)
        self.assertNotIn('vlan_in_id', ext)
        self.assertNotIn('vlan_out_id', ext)
        self.assertNotIn('vlan_in_inner_id', ext)
        self.assertNotIn('vlan_out_inner_id', ext)

    def test_lookup_interface_with_none_device_id(self):
        """Test lookup_interface when device_id is None."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_interface(None, '10', formatted_record, 'in')
        
        self.assertIsNone(formatted_record['in_interface_id'])
        self.assertIsNone(formatted_record['in_interface_ref'])
        self.assertFalse(formatted_record['in_interface_edge'])

    def test_lookup_interface_with_none_flow_index(self):
        """Test lookup_interface when flow_index is None."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_interface('device1', None, formatted_record, 'out')
        
        self.assertIsNone(formatted_record['out_interface_id'])
        self.assertIsNone(formatted_record['out_interface_ref'])
        self.assertFalse(formatted_record['out_interface_edge'])

    def test_lookup_device_with_none_ip(self):
        """Test lookup_device when device_ip is None."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_device(None, formatted_record)
        
        self.assertEqual(formatted_record['device_id'], 'unknown')
        self.assertIsNone(formatted_record['device_ref'])

    def test_lookup_device_with_empty_ip(self):
        """Test lookup_device when device_ip is empty string."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        formatted_record = {}
        processor.lookup_device('', formatted_record)
        
        self.assertEqual(formatted_record['device_id'], 'unknown')
        self.assertIsNone(formatted_record['device_ref'])

    def test_lookup_ip_as_fields_with_zero_as_id(self):
        """Test lookup_ip_as_fields when AS ID is 0 (should try to get from IP cache)."""
        mock_ip_result = ('ip_ref_123', 65001)
        
        def cacher_side_effect(name):
            if name == "ip":
                mock_cacher = Mock()
                mock_cacher.lookup.return_value = mock_ip_result
                return mock_cacher
            elif name == "clickhouse":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = 'as_ref_123'
                return mock_cacher
            elif name == "clickhouse_ranged":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = None
                return mock_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        value = {'ip_src': '192.168.1.1', 'as_src': 0}
        formatted_record = {}
        
        processor.lookup_ip_as_fields('ip_src', 'src_ip', 'as_src', 'src_as', value, formatted_record)
        
        # Should use AS from IP cache since provided AS is 0
        self.assertEqual(formatted_record['src_as_id'], 65001)

    def test_lookup_ip_as_fields_with_invalid_as_id(self):
        """Test lookup_ip_as_fields when AS ID can't be converted to int."""
        mock_ip_result = ('ip_ref_123', 65001)
        
        def cacher_side_effect(name):
            if name == "ip":
                mock_cacher = Mock()
                mock_cacher.lookup.return_value = mock_ip_result
                return mock_cacher
            elif name == "clickhouse":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = None
                return mock_cacher
            elif name == "clickhouse_ranged":
                mock_cacher = Mock()
                mock_cacher.lookup_dict_key.return_value = None
                return mock_cacher
            return Mock()
        
        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        
        value = {'ip_src': '192.168.1.1', 'as_src': 'invalid'}
        formatted_record = {}
        
        processor.lookup_ip_as_fields('ip_src', 'src_ip', 'as_src', 'src_as', value, formatted_record)
        
        # Should handle ValueError gracefully
        self.assertEqual(formatted_record['src_ip'], '192.168.1.1')

    def test_build_message_invalid_timestamp_format(self):
        """Test build_message with invalid timestamp that raises ValueError."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': 'invalid',
            'timestamp_end': 'invalid',
            'bytes': 1024,
            'packets': 10
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNone(result)

    def test_build_message_missing_only_timestamp_end(self):
        """Test build_message when only timestamp_end is missing (uses start for end)."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            # timestamp_end is missing
            'bytes': 1024,
            'packets': 10,
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # Both should be same when end is missing
        self.assertEqual(record['start_time'], record['end_time'])

    def test_build_message_with_policy_auto_scopes_rd_mapping(self):
        """Test build_message with policy auto scopes and VPN RD mapping."""
        # Set up processor with auto scopes and community mapping
        self.processor.policy_auto_scopes = True
        self.processor.policy_community_scope_map = {'100': 'community1'}
        
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'as_src': 7922,
            'as_dst': 15169,
            'mpls_vpn_rd': '65000:100'
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # Should include AS scopes and community scope
        self.assertIn('as:7922', record['policy_scope'])
        self.assertIn('as:15169', record['policy_scope'])
        self.assertIn('comm:community1', record['policy_scope'])

    def test_build_message_application_port_with_none_port_dst_in_value(self):
        """Test build_message application_port logic when port_dst value is None after conversion."""
        # Note: port_dst is required, but we can test the application_port logic
        # when port_dst is 0 which is valid but falsy
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 443,
            'port_dst': 0,  # 0 is valid but triggers None check in code
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # port_dst is 0, so application_port should be 0 (not fallback)
        self.assertEqual(record['application_port'], 0)

    def test_build_message_with_peer_ip_and_as(self):
        """Test build_message processes peer IP and AS fields."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'peer_ip_dst': '10.0.0.1',
            'peer_as_dst': 65000,
            'peer_ip_src': '10.0.0.2',
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        self.assertEqual(record['peer_ip'], '10.0.0.1')

    def test_build_message_integer_field_conversion_errors(self):
        """Test build_message handles ValueError in integer field conversion."""
        value = {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 'invalid',  # Will cause ValueError
            'port_dst': 'bad',
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 'wrong',
            'packets': 'nope',
            'as_src': 7922,
            'as_dst': 15169
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # Should default to 0 for fields that can't be converted
        self.assertEqual(record['src_port'], 0)
        self.assertEqual(record['dst_port'], 0)
        self.assertEqual(record['bit_count'], 0)
        self.assertEqual(record['packet_count'], 0)


class TestPMAcctFlowProcessorReservedAsnFilter(unittest.TestCase):
    """Tests for the reserved/private/unknown ASN drop filter -- the pipeline
    equivalent of the old Logstash conditional that dropped a flow when
    [meta][src_asn] or [meta][dst_asn] was unset or one of a handful of
    IANA-reserved, non-routable values."""

    def setUp(self):
        self.mock_pipeline = MagicMock()

        mock_clickhouse_cacher = Mock()
        mock_clickhouse_cacher.lookup.return_value = None
        mock_clickhouse_cacher.lookup_dict_key.return_value = None

        mock_ip_cacher = Mock()
        mock_ip_cacher.lookup.return_value = None

        mock_clickhouse_ranged_cacher = Mock()
        mock_clickhouse_ranged_cacher.lookup_dict_key.return_value = None

        def cacher_side_effect(name):
            if name == "clickhouse":
                return mock_clickhouse_cacher
            elif name == "ip":
                return mock_ip_cacher
            elif name == "clickhouse_ranged":
                return mock_clickhouse_ranged_cacher
            return Mock()

        self.mock_pipeline.cacher.side_effect = cacher_side_effect
        self.processor = PMAcctFlowProcessor(self.mock_pipeline)

    def _base_value(self, as_src, as_dst):
        return {
            'ip_src': '192.168.1.1',
            'ip_dst': '192.168.1.2',
            'port_src': 12345,
            'port_dst': 80,
            'ip_proto': 6,
            'timestamp_start': '1609459200.0',
            'timestamp_end': '1609459260.0',
            'bytes': 1024,
            'packets': 10,
            'as_src': as_src,
            'as_dst': as_dst,
        }

    def _enabled_processor(self):
        """A processor with the filter explicitly turned on -- it's off by
        default, so tests exercising drop behavior need this instead of
        self.processor."""
        with patch.dict(os.environ, {'CLICKHOUSE_FLOW_DROP_RESERVED_ASN': 'true'}):
            return PMAcctFlowProcessor(self.mock_pipeline)

    def test_is_reserved_asn_true_cases(self):
        reserved = [
            None, 0, 4294967295, 23456,
            64512, 65000, 65534,  # 16-bit private use range
            4200000000, 4294967000, 4294967294,  # 32-bit private use range
            'not-a-number',
        ]
        for asn in reserved:
            with self.subTest(asn=asn):
                self.assertTrue(self.processor.is_reserved_asn(asn))

    def test_is_reserved_asn_false_cases(self):
        valid = [7922, 15169, 1, 64511, 65535, 4199999999]
        for asn in valid:
            with self.subTest(asn=asn):
                self.assertFalse(self.processor.is_reserved_asn(asn))

    def test_drop_reserved_asn_disabled_by_default(self):
        """Filter is opt-in -- matches the rest of this pipeline's behavior
        unless CLICKHOUSE_FLOW_DROP_RESERVED_ASN is explicitly turned on."""
        self.assertFalse(self.processor.drop_reserved_asn)

    def test_build_message_not_dropped_when_filter_disabled_by_default(self):
        """With the filter at its default (off), even a reserved dst ASN
        passes through untouched."""
        result = self.processor.build_message(self._base_value(7922, 65001), {})
        self.assertIsNotNone(result)

    def test_build_message_valid_both_asns_not_dropped(self):
        result = self._enabled_processor().build_message(self._base_value(7922, 15169), {})
        self.assertIsNotNone(result)

    def test_build_message_dropped_when_dst_asn_zero(self):
        """Equivalent of the Logstash filter's [meta][dst_asn] == 0 check."""
        result = self._enabled_processor().build_message(self._base_value(7922, 0), {})
        self.assertIsNone(result)

    def test_build_message_dropped_when_dst_asn_reserved(self):
        """Equivalent of the Logstash filter's [meta][dst_asn] == 4294967295 check."""
        result = self._enabled_processor().build_message(self._base_value(7922, 4294967295), {})
        self.assertIsNone(result)

    def test_build_message_dropped_when_dst_asn_as_trans(self):
        """Equivalent of the Logstash filter's [meta][dst_asn] == 23456 check."""
        result = self._enabled_processor().build_message(self._base_value(7922, 23456), {})
        self.assertIsNone(result)

    def test_build_message_dropped_when_dst_asn_16bit_private_use(self):
        """Equivalent of the Logstash filter's 64512 <= dst_asn <= 65534 check."""
        result = self._enabled_processor().build_message(self._base_value(7922, 65001), {})
        self.assertIsNone(result)

    def test_build_message_dropped_when_dst_asn_32bit_private_use(self):
        """Equivalent of the Logstash filter's 4200000000 <= dst_asn <= 4294967294 check."""
        result = self._enabled_processor().build_message(self._base_value(7922, 4200000001), {})
        self.assertIsNone(result)

    def test_build_message_dropped_when_src_asn_reserved(self):
        """Same checks apply to src_asn, not just dst_asn."""
        result = self._enabled_processor().build_message(self._base_value(65001, 15169), {})
        self.assertIsNone(result)

    def test_build_message_dropped_when_src_asn_missing(self):
        value = self._base_value(7922, 15169)
        del value['as_src']
        result = self._enabled_processor().build_message(value, {})
        self.assertIsNone(result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_DROP_RESERVED_ASN': 'false'})
    def test_build_message_not_dropped_when_filter_explicitly_disabled(self):
        """CLICKHOUSE_FLOW_DROP_RESERVED_ASN=false turns the filter off explicitly,
        same as leaving it unset (the default)."""
        processor = PMAcctFlowProcessor(self.mock_pipeline)
        self.assertFalse(processor.drop_reserved_asn)
        result = processor.build_message(self._base_value(7922, 65001), {})
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main(verbosity=2)
