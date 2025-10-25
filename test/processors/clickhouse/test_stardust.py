#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.stardust import FlowProcessor


class TestFlowProcessor(unittest.TestCase):
    def setUp(self):
        self.mock_pipeline = MagicMock()
        self.mock_pipeline.cacher.return_value.lookup.return_value = "mock_lookup_result"
        self.mock_pipeline.cacher.return_value.lookup_list.return_value = ["mock_list_result"]
        
    def test_build_message_basic_structure(self):
        """Test that build_message returns a list with a single dict containing all required fields"""
        # Mock environment variables to disable all extensions
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            # Sample input data with all required fields
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "sensor_id": "sensor1",
                    "flow_type": "flow",
                    "router": {"name": "router1"},
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "protocol": 6,
                    "iface_in": {"id": 1},
                    "iface_out": {"id": 2},
                    "bgp": {
                        "peer_as_dst": 11111,
                        "next_hop": "192.168.1.3"
                    },
                    "ip_version": 4,
                    "app_port": 80
                },
                "policy": {
                    "originator": "test_originator",
                    "level": 1,
                    "scopes": ["scope1", "scope2"]
                },
                "values": {
                    "num_bits": 1024,
                    "num_packets": 10
                }
            }
            
            result = processor.build_message(input_data, {})
            
            # Should return a list with one dict
            self.assertIsInstance(result, list)
            self.assertEqual(len(result), 1)
            
            message = result[0]
            self.assertIsInstance(message, dict)
            
            # Check all required fields are present
            required_fields = [
                "start_time", "end_time", "collector_id", "policy_originator",
                "policy_level", "policy_scope", "ext", "flow_type", "device_id",
                "device_ref", "src_as_id", "src_as_ref", "src_ip", "src_ip_ref",
                "src_port", "dst_as_id", "dst_as_ref", "dst_ip", "dst_ip_ref",
                "dst_port", "protocol", "in_interface_id", "in_interface_ref",
                "out_interface_id", "out_interface_ref", "peer_as_id", "peer_as_ref",
                "peer_ip", "peer_ip_ref", "ip_version", "application_port",
                "bit_count", "packet_count"
            ]
            
            for field in required_fields:
                self.assertIn(field, message, f"Required field '{field}' missing from message")
    
    def test_build_message_field_values(self):
        """Test that build_message correctly maps input values to output fields"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "sensor_id": "sensor1",
                    "flow_type": "flow",
                    "router": {"name": "router1"},
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "protocol": 6,
                    "iface_in": {"id": 1},
                    "iface_out": {"id": 2},
                    "bgp": {
                        "peer_as_dst": 11111,
                        "next_hop": "192.168.1.3"
                    },
                    "ip_version": 4,
                    "app_port": 80
                },
                "policy": {
                    "originator": "test_originator",
                    "level": 1,
                    "scopes": ["scope1", "scope2"]
                },
                "values": {
                    "num_bits": 1024,
                    "num_packets": 10
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify field mappings
            self.assertEqual(message["start_time"], "2023-01-01T00:00:00Z")
            self.assertEqual(message["end_time"], "2023-01-01T00:01:00Z")
            self.assertEqual(message["collector_id"], "sensor1")
            self.assertEqual(message["policy_originator"], "test_originator")
            self.assertEqual(message["policy_level"], 1)
            self.assertEqual(message["policy_scope"], ["scope1", "scope2"])
            self.assertEqual(message["flow_type"], "flow")
            self.assertEqual(message["device_id"], "router1")
            self.assertEqual(message["device_ref"], "mock_lookup_result")
            self.assertEqual(message["src_as_id"], 12345)
            self.assertEqual(message["src_as_ref"], "mock_lookup_result")
            self.assertEqual(message["src_ip"], "192.168.1.1")
            self.assertEqual(message["src_ip_ref"], "mock_lookup_result")
            self.assertEqual(message["src_port"], 80)
            self.assertEqual(message["dst_as_id"], 67890)
            self.assertEqual(message["dst_as_ref"], "mock_lookup_result")
            self.assertEqual(message["dst_ip"], "192.168.1.2")
            self.assertEqual(message["dst_ip_ref"], "mock_lookup_result")
            self.assertEqual(message["dst_port"], 443)
            self.assertEqual(message["protocol"], 6)
            self.assertEqual(message["in_interface_id"], 1)
            self.assertEqual(message["in_interface_ref"], "mock_lookup_result")
            self.assertEqual(message["out_interface_id"], 2)
            self.assertEqual(message["out_interface_ref"], "mock_lookup_result")
            self.assertEqual(message["peer_as_id"], 11111)
            self.assertEqual(message["peer_as_ref"], "mock_lookup_result")
            self.assertEqual(message["peer_ip"], "192.168.1.3")
            self.assertEqual(message["peer_ip_ref"], "mock_lookup_result")
            self.assertEqual(message["ip_version"], 4)
            self.assertEqual(message["application_port"], 80)
            self.assertEqual(message["bit_count"], 1024)
            self.assertEqual(message["packet_count"], 10)
    
    def test_build_message_with_bgp_extension(self):
        """Test that build_message includes BGP extension fields when enabled"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "bgp",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "bgp": {
                        "as_path": [1, 2, 3],
                        "as_hop0_padding": "10",
                        "as_hop1_padding": "20",
                        "comms": [100, 200],
                        "ecomms": [300, 400],
                        "lcomms": [500, 600],
                        "local_pref": 100,
                        "med": 50
                    }
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify ext field contains BGP data
            import orjson
            ext_data = orjson.loads(message["ext"])
            
            self.assertIn("bgp_as_path_id", ext_data)
            self.assertIn("bgp_as_path_padding", ext_data)
            self.assertIn("bgp_community", ext_data)
            self.assertIn("bgp_ext_community", ext_data)
            self.assertIn("bgp_large_community", ext_data)
            self.assertIn("bgp_local_pref", ext_data)
            self.assertIn("bgp_med", ext_data)
            
            self.assertEqual(ext_data["bgp_as_path_id"], [1, 2, 3])
            self.assertEqual(ext_data["bgp_as_path_padding"], [10, 20])
            self.assertEqual(ext_data["bgp_community"], [100, 200])
            self.assertEqual(ext_data["bgp_ext_community"], [300, 400])
            self.assertEqual(ext_data["bgp_large_community"], [500, 600])
            self.assertEqual(ext_data["bgp_local_pref"], 100)
            self.assertEqual(ext_data["bgp_med"], 50)
    
    def test_build_message_with_ipv4_extension(self):
        """Test that build_message includes IPv4 extension fields when enabled"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "ipv4",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "dscp": 10,
                    "ip_tos": 4
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify ext field contains IPv4 data
            import orjson
            ext_data = orjson.loads(message["ext"])
            
            self.assertIn("ipv4_dscp", ext_data)
            self.assertIn("ipv4_tos", ext_data)
            
            self.assertEqual(ext_data["ipv4_dscp"], 10)
            self.assertEqual(ext_data["ipv4_tos"], 4)
    
    def test_build_message_with_mpls_extension(self):
        """Test that build_message includes MPLS extension fields when enabled"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "mpls",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "mpls": {
                        "bottom_label": 100,
                        "exp0": "1",
                        "exp1": "2",
                        "labels": [10, 20, 30],
                        "pw_id": 123,
                        "top_label_ip": "10.0.0.1",
                        "top_label_type": "IGP",
                        "vpn_rd": "65000:100"
                    }
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify ext field contains MPLS data
            import orjson
            ext_data = orjson.loads(message["ext"])
            
            self.assertIn("mpls_bottom_label", ext_data)
            self.assertIn("mpls_exp", ext_data)
            self.assertIn("mpls_labels", ext_data)
            self.assertIn("mpls_pw", ext_data)
            self.assertIn("mpls_top_label_ip", ext_data)
            self.assertIn("mpls_top_label_type", ext_data)
            self.assertIn("mpls_vpn_rd", ext_data)
            
            self.assertEqual(ext_data["mpls_bottom_label"], 100)
            self.assertEqual(ext_data["mpls_exp"], [1, 2])
            self.assertEqual(ext_data["mpls_labels"], [10, 20, 30])
            self.assertEqual(ext_data["mpls_pw"], 123)
            self.assertEqual(ext_data["mpls_top_label_ip"], "10.0.0.1")
            self.assertEqual(ext_data["mpls_top_label_type"], "IGP")
            self.assertEqual(ext_data["mpls_vpn_rd"], "65000:100")
    
    def test_build_message_with_esdb_extension(self):
        """Test that build_message includes ESDB extension fields when enabled"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "esdb",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "esdb": {
                        "src": {
                            "service": {
                                "prefix_group_name": ["service1", "service2"]
                            }
                        },
                        "dst": {
                            "service": {
                                "prefix_group_name": ["service3", "service4"]
                            }
                        }
                    }
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify ext field contains ESDB data
            import orjson
            ext_data = orjson.loads(message["ext"])
            
            self.assertIn("src_ip_esdb_ref", ext_data)
            self.assertIn("dst_ip_esdb_ref", ext_data)
            
            self.assertEqual(ext_data["src_ip_esdb_ref"], ["mock_list_result"])
            self.assertEqual(ext_data["dst_ip_esdb_ref"], ["mock_list_result"])
    
    def test_build_message_with_scireg_extension(self):
        """Test that build_message includes SciReg extension fields when enabled"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "scireg",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify ext field contains SciReg data
            import orjson
            ext_data = orjson.loads(message["ext"])
            
            self.assertIn("dst_scireg_ref", ext_data)
            self.assertIn("src_scireg_ref", ext_data)
            
            self.assertEqual(ext_data["dst_scireg_ref"], "mock_lookup_result")
            self.assertEqual(ext_data["src_scireg_ref"], "mock_lookup_result")
    
    def test_build_message_multiple_extensions(self):
        """Test that build_message works with multiple extensions enabled"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "bgp,ipv4,mpls",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "bgp": {
                        "as_path": [1, 2, 3],
                        "comms": [100, 200]
                    },
                    "dscp": 10,
                    "mpls": {
                        "labels": [10, 20],
                        "bottom_label": 100
                    }
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Verify ext field contains data from all extensions
            import orjson
            ext_data = orjson.loads(message["ext"])
            
            # BGP fields
            self.assertIn("bgp_as_path_id", ext_data)
            self.assertIn("bgp_community", ext_data)
            
            # IPv4 fields
            self.assertIn("ipv4_dscp", ext_data)
            
            # MPLS fields
            self.assertIn("mpls_labels", ext_data)
            self.assertIn("mpls_bottom_label", ext_data)
    
    def test_build_message_missing_required_fields(self):
        """Test that build_message returns None when required fields are missing"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            # Missing required fields
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1"
                    # Missing dst_asn, dst_ip, dst_port, src_port
                }
            }
            
            result = processor.build_message(input_data, {})
            self.assertIsNone(result)
    
    def test_build_message_invalid_padding_values(self):
        """Test that build_message handles invalid BGP padding values gracefully"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "bgp",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "bgp": {
                        "as_hop0_padding": "10",
                        "as_hop1_padding": "invalid"  # Invalid value
                    }
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Should only include valid padding values
            import orjson
            ext_data = orjson.loads(message["ext"])
            self.assertEqual(ext_data["bgp_as_path_padding"], [10])
    
    def test_build_message_invalid_mpls_exp_values(self):
        """Test that build_message handles invalid MPLS exp values gracefully"""
        with patch.dict(os.environ, {
            "CLICKHOUSE_FLOW_EXTENSIONS": "mpls",
            "CLICKHOUSE_TABLE_NAME": "test_table"
        }):
            processor = FlowProcessor(self.mock_pipeline)
            
            input_data = {
                "start": "2023-01-01T00:00:00Z",
                "end": "2023-01-01T00:01:00Z",
                "meta": {
                    "src_asn": 12345,
                    "src_ip": "192.168.1.1",
                    "src_port": 80,
                    "dst_asn": 67890,
                    "dst_ip": "192.168.1.2",
                    "dst_port": 443,
                    "mpls": {
                        "exp0": "1",
                        "exp1": "invalid"  # Invalid value
                    }
                }
            }
            
            result = processor.build_message(input_data, {})
            message = result[0]
            
            # Should only include valid exp values
            import orjson
            ext_data = orjson.loads(message["ext"])
            self.assertEqual(ext_data["mpls_exp"], [1])


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
