#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, Mock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.pmacct import (
    PMAcctBMPPeerDownProcessor,
    PMAcctBMPPeerUpProcessor,
    PMAcctBMPStatProcessor,
    PMAcctFlowProcessor,
)


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
            'label': 'collector1'
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
            'writer_id': 'sfacctd'
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
            'packets': 10
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
            'packets': 10
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
            'packets': 10
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
            'packets': 10
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
            'as_src': 65001,
            'as_dst': 65002,
            'mpls_vpn_rd': '65000:100'
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # Should include AS scopes and community scope
        self.assertIn('as:65001', record['policy_scope'])
        self.assertIn('as:65002', record['policy_scope'])
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
            'packets': 10
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
            'peer_ip_src': '10.0.0.2'
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
            'as_src': 'bad_as'
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertIsNotNone(result)
        record = result[0]
        # Should default to 0 for fields that can't be converted
        self.assertEqual(record['src_port'], 0)
        self.assertEqual(record['dst_port'], 0)
        self.assertEqual(record['bit_count'], 0)
        self.assertEqual(record['packet_count'], 0)


class TestPMAcctBMPStatProcessor(unittest.TestCase):
    """Unit tests for PMAcctBMPStatProcessor."""

    # Minimal valid stats message from pbmpd
    BASE_MSG = {
        "event_type": "log",
        "seq": 1,
        "timestamp": "2024-01-15T12:00:00.000000+00:00",
        "timestamp_arrival": "2024-01-15T12:00:00.100000+00:00",
        "bmp_msg_type": "stats",
        "bmp_router": "192.0.2.1",
        "bmp_router_port": 45000,
        "peer_ip": "203.0.113.1",
        "peer_asn": 64501,
        "peer_type": 0,
        "bgp_id": "192.0.2.1",
        "rd": "0:64499:1",
        "rd_origin": "bmp",
        "is_loc": 0,
        "is_filtered": 0,
        "counter_type": 0,
        "counter_type_str": "Number of prefixes rejected by inbound policy",
        "counter_value": 42,
        "label": "collector-1",
    }

    def _make_pipeline(self, device_lookup=None, ip_lookup=None, as_lookup=None):
        pipeline = MagicMock()
        ch_cacher = Mock()
        ch_cacher.lookup.side_effect = lambda table, key: device_lookup if device_lookup and table.startswith("meta_device") else None
        ch_cacher.lookup_dict_key.return_value = as_lookup
        ip_cacher = Mock()
        ip_cacher.lookup.return_value = [ip_lookup] if ip_lookup else None
        def cacher_side_effect(name):
            if name == "clickhouse":
                return ch_cacher
            elif name == "ip":
                return ip_cacher
            return Mock()
        pipeline.cacher.side_effect = cacher_side_effect
        return pipeline

    def _make(self, **kwargs):
        return PMAcctBMPStatProcessor(self._make_pipeline(**kwargs))

    def _msg(self, **overrides):
        return {**self.BASE_MSG, **overrides}

    # ------------------------------------------------------------------
    # match_message
    # ------------------------------------------------------------------

    def test_match_message_stats(self):
        self.assertTrue(self._make().match_message(self._msg()))

    def test_match_message_wrong_type(self):
        self.assertFalse(self._make().match_message(self._msg(bmp_msg_type="route_monitor")))

    def test_match_message_missing_type(self):
        msg = self._msg()
        del msg["bmp_msg_type"]
        self.assertFalse(self._make().match_message(msg))

    # ------------------------------------------------------------------
    # Required fields / early exit
    # ------------------------------------------------------------------

    def test_build_message_missing_bmp_router_returns_none(self):
        msg = self._msg()
        del msg["bmp_router"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_missing_peer_ip_returns_none(self):
        msg = self._msg()
        del msg["peer_ip"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_missing_counter_type_returns_none(self):
        msg = self._msg()
        del msg["counter_type"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_invalid_counter_type_returns_none(self):
        self.assertIsNone(self._make().build_message(self._msg(counter_type="bad"), {}))

    # ------------------------------------------------------------------
    # observation_time
    # ------------------------------------------------------------------

    def test_observation_time_parsed_from_timestamp(self):
        result = self._make().build_message(self._msg(timestamp="2024-01-15T12:00:01.500000+00:00"), {})
        self.assertIsNotNone(result)
        # 1705320001.5 * 1000 = 1705320001500
        self.assertEqual(result[0]["observation_time"], 1705320001500)

    def test_build_message_missing_timestamp_returns_none(self):
        msg = self._msg()
        del msg["timestamp"]
        self.assertIsNone(self._make().build_message(msg, {}))

    # ------------------------------------------------------------------
    # Device lookup
    # ------------------------------------------------------------------

    def test_device_id_defaults_to_bmp_router_ip(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["device_id"], "192.0.2.1")
        self.assertIsNone(result[0]["device_ref"])

    def test_device_id_from_cache(self):
        result = self._make(
            device_lookup={"id": "router-a", "ref": "ref:router-a"}
        ).build_message(self._msg(), {})
        self.assertEqual(result[0]["device_id"], "router-a")
        self.assertEqual(result[0]["device_ref"], "ref:router-a")

    # ------------------------------------------------------------------
    # Peer IP lookup
    # ------------------------------------------------------------------

    def test_peer_ip_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_ip"], "203.0.113.1")
        self.assertIsNone(result[0]["peer_ip_ref"])

    def test_peer_ip_ref_from_cache(self):
        result = self._make(ip_lookup="ref:peer-ip").build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_ip_ref"], "ref:peer-ip")

    # ------------------------------------------------------------------
    # Peer AS
    # ------------------------------------------------------------------

    def test_peer_as_id_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_as_id"], 64501)

    def test_peer_as_id_none_on_missing(self):
        msg = self._msg()
        del msg["peer_asn"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["peer_as_id"])

    def test_peer_as_id_none_on_invalid(self):
        result = self._make().build_message(self._msg(peer_asn="bad"), {})
        self.assertIsNone(result[0]["peer_as_id"])

    def test_peer_as_ref_from_cache(self):
        result = self._make(as_lookup="ref:as64501").build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_as_ref"], "ref:as64501")

    def test_peer_as_ref_none_when_no_cache(self):
        result = self._make().build_message(self._msg(), {})
        self.assertIsNone(result[0]["peer_as_ref"])

    # ------------------------------------------------------------------
    # Other peer fields
    # ------------------------------------------------------------------

    def test_peer_type_parsed(self):
        result = self._make().build_message(self._msg(peer_type=2), {})
        self.assertEqual(result[0]["peer_type"], 2)

    def test_peer_type_none_on_invalid(self):
        result = self._make().build_message(self._msg(peer_type="bad"), {})
        self.assertIsNone(result[0]["peer_type"])

    def test_bgp_id_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["bgp_id"], "192.0.2.1")

    def test_rd_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["rd"], "0:64499:1")

    def test_rd_none_when_absent(self):
        msg = self._msg()
        del msg["rd"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["rd"])

    # ------------------------------------------------------------------
    # Scope flags
    # ------------------------------------------------------------------

    def test_is_loc_false_by_default(self):
        result = self._make().build_message(self._msg(), {})
        self.assertFalse(result[0]["is_loc"])

    def test_is_loc_true(self):
        result = self._make().build_message(self._msg(is_loc=1), {})
        self.assertTrue(result[0]["is_loc"])

    def test_is_filtered_false_by_default(self):
        result = self._make().build_message(self._msg(), {})
        self.assertFalse(result[0]["is_filtered"])

    def test_is_filtered_true(self):
        result = self._make().build_message(self._msg(is_filtered=1), {})
        self.assertTrue(result[0]["is_filtered"])

    # ------------------------------------------------------------------
    # Metric fields (counter_type -> metric_type mapping)
    # ------------------------------------------------------------------

    def test_metric_type_parsed(self):
        result = self._make().build_message(self._msg(counter_type=7), {})
        self.assertEqual(result[0]["metric_type"], 7)

    def test_metric_type_str_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["metric_type_str"], "Number of prefixes rejected by inbound policy")

    def test_metric_type_str_empty_when_absent(self):
        msg = self._msg()
        del msg["counter_type_str"]
        result = self._make().build_message(msg, {})
        self.assertEqual(result[0]["metric_type_str"], "")

    def test_metric_value_parsed(self):
        result = self._make().build_message(self._msg(counter_value=12345), {})
        self.assertEqual(result[0]["metric_value"], 12345)

    def test_metric_value_zero_on_missing(self):
        msg = self._msg()
        del msg["counter_value"]
        result = self._make().build_message(msg, {})
        self.assertEqual(result[0]["metric_value"], 0)

    def test_metric_value_zero_on_invalid(self):
        result = self._make().build_message(self._msg(counter_value="bad"), {})
        self.assertEqual(result[0]["metric_value"], 0)

    # ------------------------------------------------------------------
    # Collector / policy fields
    # ------------------------------------------------------------------

    def test_collector_id_from_label(self):
        result = self._make().build_message(self._msg(label="my-collector"), {})
        self.assertEqual(result[0]["collector_id"], "my-collector")

    def test_returns_list_with_one_record(self):
        result = self._make().build_message(self._msg(), {})
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)


class TestPMAcctBMPPeerUpProcessor(unittest.TestCase):
    """Unit tests for PMAcctBMPPeerUpProcessor."""

    # Minimal valid peer_up message from pbmpd
    BASE_MSG = {
        "event_type": "log",
        "seq": 10,
        "timestamp": "2024-01-15T12:00:00.000000+00:00",
        "timestamp_arrival": "2024-01-15T12:00:00.100000+00:00",
        "bmp_msg_type": "peer_up",
        "bmp_router": "192.0.2.2",
        "bmp_router_port": 45047,
        "peer_ip": "203.0.113.2",
        "peer_asn": 64496,
        "peer_type": 0,
        "rd": "0:64499:2",
        "rd_origin": "bmp",
        "bgp_id": "192.0.2.2",
        "local_as": 64500,
        "local_port": 26354,
        "remote_port": 179,
        "local_ip": "203.0.113.1",
        "label": "collector-1",
    }

    def _make_pipeline(self, device_lookup=None, ip_lookup=None, as_lookup=None):
        pipeline = MagicMock()
        ch_cacher = Mock()
        ch_cacher.lookup.side_effect = lambda table, key: device_lookup if device_lookup and table.startswith("meta_device") else None
        ch_cacher.lookup_dict_key.return_value = as_lookup
        ip_cacher = Mock()
        ip_cacher.lookup.return_value = [ip_lookup] if ip_lookup else None
        def cacher_side_effect(name):
            if name == "clickhouse":
                return ch_cacher
            elif name == "ip":
                return ip_cacher
            return Mock()
        pipeline.cacher.side_effect = cacher_side_effect
        return pipeline

    def _make(self, **kwargs):
        return PMAcctBMPPeerUpProcessor(self._make_pipeline(**kwargs))

    def _msg(self, **overrides):
        return {**self.BASE_MSG, **overrides}

    # ------------------------------------------------------------------
    # match_message
    # ------------------------------------------------------------------

    def test_match_message_peer_up(self):
        self.assertTrue(self._make().match_message(self._msg()))

    def test_match_message_wrong_type(self):
        self.assertFalse(self._make().match_message(self._msg(bmp_msg_type="peer_down")))

    def test_match_message_missing_type(self):
        msg = self._msg()
        del msg["bmp_msg_type"]
        self.assertFalse(self._make().match_message(msg))

    # ------------------------------------------------------------------
    # Required fields / early exit
    # ------------------------------------------------------------------

    def test_build_message_missing_bmp_router_returns_none(self):
        msg = self._msg()
        del msg["bmp_router"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_missing_peer_ip_returns_none(self):
        msg = self._msg()
        del msg["peer_ip"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_missing_timestamp_returns_none(self):
        msg = self._msg()
        del msg["timestamp"]
        self.assertIsNone(self._make().build_message(msg, {}))

    # ------------------------------------------------------------------
    # observation_time
    # ------------------------------------------------------------------

    def test_observation_time_parsed_from_iso8601(self):
        result = self._make().build_message(self._msg(timestamp="2024-01-15T12:00:01.500000+00:00"), {})
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["observation_time"], 1705320001500)

    def test_observation_time_fallback_to_unix_float(self):
        result = self._make().build_message(self._msg(timestamp="1705320000.0"), {})
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["observation_time"], 1705320000000)

    def test_observation_time_zero_on_invalid(self):
        result = self._make().build_message(self._msg(timestamp="not-a-date"), {})
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["observation_time"], 0)

    # ------------------------------------------------------------------
    # Device lookup
    # ------------------------------------------------------------------

    def test_device_id_defaults_to_bmp_router(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["device_id"], "192.0.2.2")
        self.assertIsNone(result[0]["device_ref"])

    def test_device_id_from_cache(self):
        result = self._make(
            device_lookup={"id": "router-x", "ref": "ref:router-x"}
        ).build_message(self._msg(), {})
        self.assertEqual(result[0]["device_id"], "router-x")
        self.assertEqual(result[0]["device_ref"], "ref:router-x")

    # ------------------------------------------------------------------
    # Peer IP
    # ------------------------------------------------------------------

    def test_peer_ip_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_ip"], "203.0.113.2")
        self.assertIsNone(result[0]["peer_ip_ref"])

    def test_peer_ip_ref_from_cache(self):
        result = self._make(ip_lookup="ref:peer").build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_ip_ref"], "ref:peer")

    # ------------------------------------------------------------------
    # Peer AS
    # ------------------------------------------------------------------

    def test_peer_as_id_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_as_id"], 64496)

    def test_peer_as_id_none_on_missing(self):
        msg = self._msg()
        del msg["peer_asn"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["peer_as_id"])

    def test_peer_as_id_none_on_invalid(self):
        result = self._make().build_message(self._msg(peer_asn="bad"), {})
        self.assertIsNone(result[0]["peer_as_id"])

    def test_peer_as_ref_from_cache(self):
        result = self._make(as_lookup="ref:as64496").build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_as_ref"], "ref:as64496")

    # ------------------------------------------------------------------
    # BGP session fields
    # ------------------------------------------------------------------

    def test_bgp_id_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["bgp_id"], "192.0.2.2")

    def test_bgp_id_none_when_absent(self):
        msg = self._msg()
        del msg["bgp_id"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["bgp_id"])

    def test_local_as_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["local_as"], 64500)

    def test_local_as_none_on_missing(self):
        msg = self._msg()
        del msg["local_as"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["local_as"])

    def test_local_as_none_on_invalid(self):
        result = self._make().build_message(self._msg(local_as="bad"), {})
        self.assertIsNone(result[0]["local_as"])

    def test_local_port_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["local_port"], 26354)

    def test_local_port_none_on_missing(self):
        msg = self._msg()
        del msg["local_port"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["local_port"])

    def test_remote_port_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["remote_port"], 179)

    def test_remote_port_none_on_missing(self):
        msg = self._msg()
        del msg["remote_port"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["remote_port"])

    def test_local_ip_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["local_ip"], "203.0.113.1")

    def test_local_ip_none_when_absent(self):
        msg = self._msg()
        del msg["local_ip"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["local_ip"])

    # ------------------------------------------------------------------
    # Peer type / RD / collector
    # ------------------------------------------------------------------

    def test_peer_type_parsed(self):
        result = self._make().build_message(self._msg(peer_type=1), {})
        self.assertEqual(result[0]["peer_type"], 1)

    def test_peer_type_none_on_invalid(self):
        result = self._make().build_message(self._msg(peer_type="bad"), {})
        self.assertIsNone(result[0]["peer_type"])

    def test_rd_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["rd"], "0:64499:2")

    def test_rd_none_when_absent(self):
        msg = self._msg()
        del msg["rd"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["rd"])

    def test_collector_id_from_label(self):
        result = self._make().build_message(self._msg(label="my-collector"), {})
        self.assertEqual(result[0]["collector_id"], "my-collector")

    def test_returns_list_with_one_record(self):
        result = self._make().build_message(self._msg(), {})
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

    # ------------------------------------------------------------------
    # No peer_down columns
    # ------------------------------------------------------------------

    def test_no_reason_type_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("reason_type", result[0])

    def test_no_reason_str_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("reason_str", result[0])


class TestPMAcctBMPPeerDownProcessor(unittest.TestCase):
    """Unit tests for PMAcctBMPPeerDownProcessor."""

    # Minimal valid peer_down message from pbmpd
    BASE_MSG = {
        "event_type": "log",
        "seq": 18696,
        "timestamp": "2024-01-15T12:00:00.000000+00:00",
        "timestamp_arrival": "2024-01-15T12:00:00.100000+00:00",
        "bmp_msg_type": "peer_down",
        "bmp_router": "192.0.2.2",
        "bmp_router_port": 45047,
        "peer_ip": "203.0.113.3",
        "peer_asn": 64496,
        "peer_type": 0,
        "rd": "0:64499:2",
        "rd_origin": "bmp",
        "reason_type": 1,
        "reason_str": "The local system closed the session",
        "label": "collector-1",
    }

    def _make_pipeline(self, device_lookup=None, ip_lookup=None, as_lookup=None):
        pipeline = MagicMock()
        ch_cacher = Mock()
        ch_cacher.lookup.side_effect = lambda table, key: device_lookup if device_lookup and table.startswith("meta_device") else None
        ch_cacher.lookup_dict_key.return_value = as_lookup
        ip_cacher = Mock()
        ip_cacher.lookup.return_value = [ip_lookup] if ip_lookup else None
        def cacher_side_effect(name):
            if name == "clickhouse":
                return ch_cacher
            elif name == "ip":
                return ip_cacher
            return Mock()
        pipeline.cacher.side_effect = cacher_side_effect
        return pipeline

    def _make(self, **kwargs):
        return PMAcctBMPPeerDownProcessor(self._make_pipeline(**kwargs))

    def _msg(self, **overrides):
        return {**self.BASE_MSG, **overrides}

    # ------------------------------------------------------------------
    # match_message
    # ------------------------------------------------------------------

    def test_match_message_peer_down(self):
        self.assertTrue(self._make().match_message(self._msg()))

    def test_match_message_wrong_type(self):
        self.assertFalse(self._make().match_message(self._msg(bmp_msg_type="peer_up")))

    def test_match_message_missing_type(self):
        msg = self._msg()
        del msg["bmp_msg_type"]
        self.assertFalse(self._make().match_message(msg))

    # ------------------------------------------------------------------
    # Required fields / early exit
    # ------------------------------------------------------------------

    def test_build_message_missing_bmp_router_returns_none(self):
        msg = self._msg()
        del msg["bmp_router"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_missing_peer_ip_returns_none(self):
        msg = self._msg()
        del msg["peer_ip"]
        self.assertIsNone(self._make().build_message(msg, {}))

    def test_build_message_missing_timestamp_returns_none(self):
        msg = self._msg()
        del msg["timestamp"]
        self.assertIsNone(self._make().build_message(msg, {}))

    # ------------------------------------------------------------------
    # observation_time
    # ------------------------------------------------------------------

    def test_observation_time_parsed_from_iso8601(self):
        result = self._make().build_message(self._msg(timestamp="2024-01-15T12:00:01.500000+00:00"), {})
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["observation_time"], 1705320001500)

    def test_observation_time_fallback_to_unix_float(self):
        result = self._make().build_message(self._msg(timestamp="1705320000.0"), {})
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["observation_time"], 1705320000000)

    def test_observation_time_zero_on_invalid(self):
        result = self._make().build_message(self._msg(timestamp="not-a-date"), {})
        self.assertIsNotNone(result)
        self.assertEqual(result[0]["observation_time"], 0)

    # ------------------------------------------------------------------
    # Device lookup
    # ------------------------------------------------------------------

    def test_device_id_defaults_to_bmp_router(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["device_id"], "192.0.2.2")
        self.assertIsNone(result[0]["device_ref"])

    def test_device_id_from_cache(self):
        result = self._make(
            device_lookup={"id": "router-y", "ref": "ref:router-y"}
        ).build_message(self._msg(), {})
        self.assertEqual(result[0]["device_id"], "router-y")
        self.assertEqual(result[0]["device_ref"], "ref:router-y")

    # ------------------------------------------------------------------
    # Peer IP
    # ------------------------------------------------------------------

    def test_peer_ip_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_ip"], "203.0.113.3")
        self.assertIsNone(result[0]["peer_ip_ref"])

    def test_peer_ip_ref_from_cache(self):
        result = self._make(ip_lookup="ref:peer").build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_ip_ref"], "ref:peer")

    # ------------------------------------------------------------------
    # Peer AS
    # ------------------------------------------------------------------

    def test_peer_as_id_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_as_id"], 64496)

    def test_peer_as_id_none_on_missing(self):
        msg = self._msg()
        del msg["peer_asn"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["peer_as_id"])

    def test_peer_as_ref_from_cache(self):
        result = self._make(as_lookup="ref:as64496").build_message(self._msg(), {})
        self.assertEqual(result[0]["peer_as_ref"], "ref:as64496")

    # ------------------------------------------------------------------
    # Reason fields
    # ------------------------------------------------------------------

    def test_reason_type_parsed(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["reason_type"], 1)

    def test_reason_type_none_on_missing(self):
        msg = self._msg()
        del msg["reason_type"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["reason_type"])

    def test_reason_type_none_on_invalid(self):
        result = self._make().build_message(self._msg(reason_type="bad"), {})
        self.assertIsNone(result[0]["reason_type"])

    def test_reason_str_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["reason_str"], "The local system closed the session")

    def test_reason_str_none_when_absent(self):
        msg = self._msg()
        del msg["reason_str"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["reason_str"])

    # ------------------------------------------------------------------
    # Peer type / RD / collector
    # ------------------------------------------------------------------

    def test_peer_type_parsed(self):
        result = self._make().build_message(self._msg(peer_type=2), {})
        self.assertEqual(result[0]["peer_type"], 2)

    def test_rd_set(self):
        result = self._make().build_message(self._msg(), {})
        self.assertEqual(result[0]["rd"], "0:64499:2")

    def test_rd_none_when_absent(self):
        msg = self._msg()
        del msg["rd"]
        result = self._make().build_message(msg, {})
        self.assertIsNone(result[0]["rd"])

    def test_collector_id_from_label(self):
        result = self._make().build_message(self._msg(label="my-collector"), {})
        self.assertEqual(result[0]["collector_id"], "my-collector")

    def test_returns_list_with_one_record(self):
        result = self._make().build_message(self._msg(), {})
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

    # ------------------------------------------------------------------
    # No peer_up columns
    # ------------------------------------------------------------------

    def test_no_bgp_id_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("bgp_id", result[0])

    def test_no_local_as_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("local_as", result[0])

    def test_no_local_port_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("local_port", result[0])

    def test_no_remote_port_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("remote_port", result[0])

    def test_no_local_ip_column(self):
        result = self._make().build_message(self._msg(), {})
        self.assertNotIn("local_ip", result[0])


if __name__ == '__main__':
    unittest.main(verbosity=2)
