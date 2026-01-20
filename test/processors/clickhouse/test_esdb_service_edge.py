#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
import sys

# Add the project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.service_edge import MetaServiceEdgeProcessor


class TestMetaServiceEdgeProcessor(unittest.TestCase):
    """Test suite for MetaServiceEdgeProcessor"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Mock clickhouse cacher
        self.mock_clickhouse_cacher = MagicMock()
        self.mock_clickhouse_cacher.lookup.return_value = None
        
        def mock_cacher(cache_type):
            if cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            return MagicMock()
        
        self.mock_pipeline.cacher.side_effect = mock_cacher

    # ==================== Initialization tests ====================

    def test_init_default_values(self):
        """Test processor initializes with correct default values."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.table, 'meta_esdb_service_edge')
        self.assertEqual(processor.val_id_field, ['id'])
        self.assertEqual(processor.required_fields, [['id']])
        self.assertEqual(processor.int_fields, ['peer_as_id'])
        self.assertEqual(processor.bool_fields, ['org_hide'])
        self.assertIn('org_tag', processor.array_fields)
        self.assertIn('org_type', processor.array_fields)
        self.assertEqual(processor.service_edge_url_pattern, 'esdb')

    def test_init_custom_table_name(self):
        """Test initialization with custom table name."""
        with patch.dict(os.environ, {'CLICKHOUSE_ESDB_SERVICE_EDGE_TABLE': 'custom_service_edge_table'}):
            processor = MetaServiceEdgeProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_service_edge_table')

    def test_init_custom_url_pattern(self):
        """Test initialization with custom URL match pattern."""
        with patch.dict(os.environ, {'SERVICE_EDGE_URL_MATCH_PATTERN': 'test-esdb.example.com'}):
            processor = MetaServiceEdgeProcessor(self.mock_pipeline)
            self.assertEqual(processor.service_edge_url_pattern, 'test-esdb.example.com')

    def test_column_definitions(self):
        """Test that all required columns are defined."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        column_names = [col[0] for col in processor.column_defs]
        
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',
            'connection_name', 'svc_edge_id', 'org_short_name',
            'org_full_name', 'org_hide', 'org_tag', 'org_tag_str',
            'org_type', 'org_funding_agency', 'peer_as_id',
            'peer_ipv4', 'peer_ipv6'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names,
                         f"Column '{expected_col}' not found in column definitions")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        self.assertEqual(column_types['connection_name'], 'Nullable(String)')
        self.assertEqual(column_types['svc_edge_id'], 'Nullable(String)')
        self.assertEqual(column_types['org_short_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['org_full_name'], 'Nullable(String)')
        self.assertEqual(column_types['org_hide'], 'Bool')
        self.assertEqual(column_types['org_tag'], 'Array(String)')
        self.assertEqual(column_types['org_tag_str'], 'Nullable(String)')
        self.assertEqual(column_types['org_type'], 'Array(String)')
        self.assertEqual(column_types['org_funding_agency'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['peer_as_id'], 'Nullable(UInt32)')
        self.assertEqual(column_types['peer_ipv4'], 'Nullable(String)')
        self.assertEqual(column_types['peer_ipv6'], 'Nullable(String)')

    def test_create_table_command(self):
        """Test CREATE TABLE command generation."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        self.assertIn("CREATE TABLE IF NOT EXISTS meta_esdb_service_edge", result)
        self.assertIn("ENGINE = MergeTree", result)
        self.assertIn("ORDER BY", result)

    # ==================== match_message tests ====================

    def test_match_message_with_table_field(self):
        """Test match_message with standard table field (backward compatibility)."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        msg = {'table': 'meta_esdb_service_edge', 'data': []}
        result = processor.match_message(msg)
        
        self.assertTrue(result)

    def test_match_message_with_wrong_table(self):
        """Test match_message rejects messages with wrong table."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        msg = {'table': 'other_table', 'data': []}
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_with_esdb_url_and_service_edge_data(self):
        """Test match_message matches GraphQLConsumer messages with ESDB URL and serviceEdgeList."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceEdgeList': {
                        'list': []
                    }
                }
            }
        }
        result = processor.match_message(msg)
        
        self.assertTrue(result)

    def test_match_message_with_esdb_url_but_wrong_data_structure(self):
        """Test match_message rejects ESDB URL without serviceEdgeList."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceList': {  # Wrong structure (IP service data)
                        'list': []
                    }
                }
            }
        }
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_with_non_esdb_url(self):
        """Test match_message rejects messages with non-ESDB URL."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://other-api.example.com/data',
            'status_code': 200,
            'data': {}
        }
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_empty_message(self):
        """Test match_message handles empty message."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor.match_message({})
        
        self.assertFalse(result)

    def test_match_message_with_custom_url_pattern(self):
        """Test match_message uses custom URL pattern from env."""
        with patch.dict(os.environ, {'SERVICE_EDGE_URL_MATCH_PATTERN': 'custom-esdb.example.com'}):
            processor = MetaServiceEdgeProcessor(self.mock_pipeline)
            
            # Should match custom pattern
            msg_match = {
                'url': 'https://custom-esdb.example.com/graphql',
                'status_code': 200,
                'data': {
                    'data': {
                        'serviceEdgeList': {'list': []}
                    }
                }
            }
            self.assertTrue(processor.match_message(msg_match))
            
            # Should NOT match default pattern anymore
            msg_no_match = {
                'url': 'https://esdb.example.com/graphql',
                'status_code': 200,
                'data': {
                    'data': {
                        'serviceEdgeList': {'list': []}
                    }
                }
            }
            self.assertFalse(processor.match_message(msg_no_match))

    # ==================== _build_service_edge_key tests ====================

    def test_build_service_edge_key_basic(self):
        """Test building service edge key with basic inputs."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor._build_service_edge_key('router-test1', 'se_test_100')
        
        self.assertEqual(result, 'router-test1::se_test_100')

    def test_build_service_edge_key_with_special_chars(self):
        """Test building service edge key with special characters."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor._build_service_edge_key('test-router-cr5', 'test_se-1234')
        
        self.assertEqual(result, 'test-router-cr5::test_se-1234')

    # ==================== _build_service_edge_record tests ====================

    def test_build_service_edge_record_basic(self):
        """Test building service edge record with basic data."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        org_info = {
            'short_name': 'TESTORG',
            'full_name': 'Test Organization',
            'types': ['Research Site'],
            'funding_agency': 'TESTFUND',
            'hide': False,
            'tags': ['primary'],
            'tags_str': 'primary'
        }
        peer_info = {
            'asn': 64496,  # RFC 5398 documentation ASN
            'ipv4': '192.0.2.1',  # RFC 5737 TEST-NET-1
            'ipv6': '2001:db8::1'  # RFC 3849 documentation
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name='test-connection-1',
            svc_edge_id='100',
            org_info=org_info,
            peer_info=peer_info
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'router-test1::se_test_100')
        self.assertEqual(result['connection_name'], 'test-connection-1')
        self.assertEqual(result['svc_edge_id'], '100')
        self.assertEqual(result['org_short_name'], 'TESTORG')
        self.assertEqual(result['org_full_name'], 'Test Organization')
        self.assertEqual(result['org_type'], ['Research Site'])
        self.assertEqual(result['org_funding_agency'], 'TESTFUND')
        self.assertFalse(result['org_hide'])
        self.assertEqual(result['org_tag'], ['primary'])
        self.assertEqual(result['org_tag_str'], 'primary')
        self.assertEqual(result['peer_as_id'], 64496)
        self.assertEqual(result['peer_ipv4'], '192.0.2.1')
        self.assertEqual(result['peer_ipv6'], '2001:db8::1')

    def test_build_service_edge_record_no_org_info(self):
        """Test building service edge record without organization info."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=None,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertIsNone(result['org_short_name'])
        self.assertIsNone(result['org_full_name'])
        self.assertEqual(result['org_type'], [])
        self.assertEqual(result['org_tag'], [])
        self.assertIsNone(result['org_tag_str'])
        self.assertFalse(result['org_hide'])

    def test_build_service_edge_record_no_peer_info(self):
        """Test building service edge record without peer info."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        org_info = {
            'short_name': 'TESTORG',
            'full_name': 'Test Organization',
            'types': [],
            'funding_agency': None,
            'hide': False,
            'tags': [],
            'tags_str': None
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=org_info,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertIsNone(result['peer_as_id'])
        self.assertIsNone(result['peer_ipv4'])
        self.assertIsNone(result['peer_ipv6'])

    def test_build_service_edge_record_hidden_org(self):
        """Test building service edge record with hidden organization."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        org_info = {
            'short_name': 'HIDDENORG',
            'full_name': 'Hidden Organization',
            'types': [],
            'funding_agency': None,
            'hide': True,
            'tags': ['hide'],
            'tags_str': 'hide'
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=org_info,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertTrue(result['org_hide'])
        self.assertEqual(result['org_tag'], ['hide'])

    def test_build_service_edge_record_multiple_org_types(self):
        """Test building service edge record with multiple organization types."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        org_info = {
            'short_name': 'TESTORG',
            'full_name': 'Test Organization',
            'types': ['Research Site', 'Commercial Peer', 'Education'],
            'funding_agency': 'TESTFUND',
            'hide': False,
            'tags': [],
            'tags_str': None
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=org_info,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result['org_type']), 3)
        self.assertIn('Research Site', result['org_type'])
        self.assertIn('Commercial Peer', result['org_type'])
        self.assertIn('Education', result['org_type'])

    def test_build_service_edge_record_multiple_tags(self):
        """Test building service edge record with multiple tags."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        org_info = {
            'short_name': 'TESTORG',
            'full_name': 'Test Organization',
            'types': [],
            'funding_agency': None,
            'hide': False,
            'tags': ['primary', 'transit', 'backup'],
            'tags_str': 'primary,transit,backup'
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=org_info,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result['org_tag']), 3)
        self.assertEqual(result['org_tag_str'], 'primary,transit,backup')

    def test_build_service_edge_record_ipv4_only_peer(self):
        """Test building service edge record with IPv4-only peer."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        peer_info = {
            'asn': 64496,
            'ipv4': '192.0.2.1'
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=None,
            peer_info=peer_info
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['peer_as_id'], 64496)
        self.assertEqual(result['peer_ipv4'], '192.0.2.1')
        self.assertIsNone(result['peer_ipv6'])

    def test_build_service_edge_record_ipv6_only_peer(self):
        """Test building service edge record with IPv6-only peer."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        peer_info = {
            'asn': 64496,
            'ipv6': '2001:db8::1'
        }
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            svc_edge_id='100',
            org_info=None,
            peer_info=peer_info
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['peer_as_id'], 64496)
        self.assertIsNone(result['peer_ipv4'])
        self.assertEqual(result['peer_ipv6'], '2001:db8::1')

    # ==================== _extract_service_edge_records tests ====================

    def test_extract_service_edge_records_complete(self):
        """Test extracting service edge records from complete ESDB GraphQL data."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': {
                                'id': '50',
                                'connectionName': 'test-connection-1'
                            },
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '2',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [
                                    {
                                        'id': '1',
                                        'name': 'Research Site'
                                    }
                                ],
                                'fundingAgency': {
                                    'id': '3',
                                    'shortName': 'TESTFUND'
                                },
                                'visibility': 'Public',
                                'tags': ['primary']
                            },
                            'bgpNeighbors': [
                                {
                                    'id': '30',
                                    'remoteIp': '192.0.2.1',
                                    'peerDetail': {
                                        'id': '40',
                                        'asn': 64496
                                    }
                                }
                            ],
                            'equipment': {
                                'id': '60',
                                'model': {
                                    'id': '70',
                                    'manufacturer': {
                                        'id': '80',
                                        'shortName': 'TestVendor'
                                    }
                                },
                                'platform': {
                                    'id': '90',
                                    'manufacturer': {
                                        'id': '100',
                                        'shortName': 'TestVendor'
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        
        self.assertEqual(record['id'], 'router-test1::se_test_100')
        self.assertEqual(record['connection_name'], 'test-connection-1')
        self.assertEqual(record['svc_edge_id'], '100')
        self.assertEqual(record['org_short_name'], 'TESTORG')
        self.assertEqual(record['org_full_name'], 'Test Organization')
        self.assertEqual(record['org_type'], ['Research Site'])
        self.assertEqual(record['org_funding_agency'], 'TESTFUND')
        self.assertFalse(record['org_hide'])
        self.assertEqual(record['org_tag'], ['primary'])
        self.assertEqual(record['peer_as_id'], 64496)
        self.assertEqual(record['peer_ipv4'], '192.0.2.1')

    def test_extract_service_edge_records_dual_stack_bgp(self):
        """Test extraction with dual-stack BGP neighbors (IPv4 + IPv6)."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [
                                {
                                    'id': '30',
                                    'remoteIp': '192.0.2.1',
                                    'peerDetail': {
                                        'id': '40',
                                        'asn': 64496
                                    }
                                },
                                {
                                    'id': '31',
                                    'remoteIp': '2001:db8::1',
                                    'peerDetail': {
                                        'id': '40',
                                        'asn': 64496
                                    }
                                }
                            ],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        
        self.assertEqual(record['peer_as_id'], 64496)
        self.assertEqual(record['peer_ipv4'], '192.0.2.1')
        self.assertEqual(record['peer_ipv6'], '2001:db8::1')

    def test_extract_service_edge_records_exchange_point_skipped(self):
        """Test that service edges with 3+ BGP neighbors don't get peer info (exchange points)."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_ixp',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'IXP',
                                'fullName': 'Internet Exchange Point',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [
                                {'id': '30', 'remoteIp': '192.0.2.1', 'peerDetail': {'id': '40', 'asn': 64496}},
                                {'id': '31', 'remoteIp': '192.0.2.2', 'peerDetail': {'id': '41', 'asn': 64497}},
                                {'id': '32', 'remoteIp': '192.0.2.3', 'peerDetail': {'id': '42', 'asn': 64498}}
                            ],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        # Record is created (service edge exists) but without peer info (3+ neighbors = exchange point)
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record['id'], 'router-test1::se_test_ixp')
        # Peer info should be None for exchange points
        self.assertIsNone(record['peer_as_id'])
        self.assertIsNone(record['peer_ipv4'])
        self.assertIsNone(record['peer_ipv6'])

    def test_extract_service_edge_records_different_asns_skipped(self):
        """Test that service edges with different ASNs are skipped."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [
                                {
                                    'id': '30',
                                    'remoteIp': '192.0.2.1',
                                    'peerDetail': {
                                        'id': '40',
                                        'asn': 64496
                                    }
                                },
                                {
                                    'id': '31',
                                    'remoteIp': '192.0.2.2',
                                    'peerDetail': {
                                        'id': '41',
                                        'asn': 64497  # Different ASN!
                                    }
                                }
                            ],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        # Should create record but without peer info (different ASNs)
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertIsNone(record['peer_as_id'])

    def test_extract_service_edge_records_hidden_visibility(self):
        """Test extraction with Hidden visibility."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'HIDDENORG',
                                'fullName': 'Hidden Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'HIDDEN',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]['org_hide'])

    def test_extract_service_edge_records_hide_tag(self):
        """Test that 'hide' tag sets org_hide to True."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': ['primary', 'hide', 'backup']
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]['org_hide'])
        self.assertIn('hide', result[0]['org_tag'])

    def test_extract_service_edge_records_multiple_org_types(self):
        """Test extraction with multiple organization types."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [
                                    {'id': '1', 'name': 'Research Site'},
                                    {'id': '2', 'name': 'Commercial Peer'},
                                    {'id': '3', 'name': 'Education'}
                                ],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]['org_type']), 3)
        self.assertIn('Research Site', result[0]['org_type'])
        self.assertIn('Commercial Peer', result[0]['org_type'])
        self.assertIn('Education', result[0]['org_type'])

    def test_extract_service_edge_records_string_org_type(self):
        """Test extraction when org type is a string instead of dict."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': ['Research Site'],  # String instead of dict
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['org_type'], ['Research Site'])

    def test_extract_service_edge_records_string_funding_agency(self):
        """Test extraction when funding agency is a string."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': 'TESTFUND',  # String instead of object
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['org_funding_agency'], 'TESTFUND')

    def test_extract_service_edge_records_no_service_edge_name(self):
        """Test that service edges without name are skipped."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': None,  # Missing name
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_service_edge_records_no_device_name(self):
        """Test that service edges without device name are skipped."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': None  # Missing device name
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_service_edge_records_empty_data(self):
        """Test extracting service edge records from empty data."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor._extract_service_edge_records({})
        
        self.assertEqual(len(result), 0)

    def test_extract_service_edge_records_none_data(self):
        """Test extracting service edge records from None data."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor._extract_service_edge_records(None)
        
        self.assertEqual(len(result), 0)

    def test_extract_service_edge_records_empty_service_edge_list(self):
        """Test extracting service edge records when serviceEdgeList is empty."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': []
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_service_edge_records_null_organization(self):
        """Test extraction when organization is null."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': None,
                            'bgpNeighbors': [],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertIsNone(record['org_short_name'])
        self.assertIsNone(record['org_full_name'])
        self.assertEqual(record['org_type'], [])
        self.assertFalse(record['org_hide'])

    def test_extract_service_edge_records_null_bgp_neighbors(self):
        """Test extraction when bgpNeighbors is null."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': None,
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertIsNone(record['peer_as_id'])
        self.assertIsNone(record['peer_ipv4'])
        self.assertIsNone(record['peer_ipv6'])

    def test_extract_service_edge_records_invalid_asn(self):
        """Test extraction with invalid ASN value."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {
                                    'id': '5',
                                    'name': 'router-test1'
                                },
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {
                                'id': '1',
                                'name': 'Layer 3'
                            },
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'TESTORG',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [
                                {
                                    'id': '30',
                                    'remoteIp': '192.0.2.1',
                                    'peerDetail': {
                                        'id': '40',
                                        'asn': 'invalid'  # Invalid ASN
                                    }
                                }
                            ],
                            'equipment': {
                                'id': '60',
                                'model': None,
                                'platform': None
                            }
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        # Should have IP but no ASN (invalid ASN was skipped)
        self.assertEqual(record['peer_ipv4'], '192.0.2.1')
        self.assertIsNone(record['peer_as_id'])

    def test_extract_service_edge_records_multiple_service_edges(self):
        """Test extraction with multiple service edges."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        service_edge_data = {
            'data': {
                'serviceEdgeList': {
                    'list': [
                        {
                            'id': '100',
                            'name': 'se_test_100',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '10',
                                'device': {'id': '5', 'name': 'router-test1'},
                                'interface': '1/1/c1/1'
                            },
                            'serviceEdgeType': {'id': '1', 'name': 'Layer 3'},
                            'vlan': None,
                            'organization': {
                                'id': '20',
                                'shortName': 'ORG1',
                                'fullName': 'Organization One',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {'id': '60', 'model': None, 'platform': None}
                        },
                        {
                            'id': '101',
                            'name': 'se_test_101',
                            'physicalConnection': None,
                            'equipmentInterface': {
                                'id': '11',
                                'device': {'id': '6', 'name': 'router-test2'},
                                'interface': '1/1/c1/2'
                            },
                            'serviceEdgeType': {'id': '1', 'name': 'Layer 3'},
                            'vlan': None,
                            'organization': {
                                'id': '21',
                                'shortName': 'ORG2',
                                'fullName': 'Organization Two',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public',
                                'tags': []
                            },
                            'bgpNeighbors': [],
                            'equipment': {'id': '61', 'model': None, 'platform': None}
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_service_edge_records(service_edge_data)
        
        self.assertEqual(len(result), 2)
        
        ids = [r['id'] for r in result]
        self.assertIn('router-test1::se_test_100', ids)
        self.assertIn('router-test2::se_test_101', ids)

    # ==================== build_metadata_fields tests ====================

    def test_build_metadata_fields_basic(self):
        """Test building metadata fields with basic data."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        value = {
            'connection_name': 'test-connection-1',
            'svc_edge_id': '100',
            'org_short_name': 'TESTORG',
            'org_full_name': 'Test Organization',
            'org_hide': False,
            'org_tag': ['primary'],
            'org_tag_str': 'primary',
            'org_type': ['Research Site'],
            'org_funding_agency': 'TESTFUND',
            'peer_as_id': 64496,
            'peer_ipv4': '192.0.2.1',
            'peer_ipv6': '2001:db8::1'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['connection_name'], 'test-connection-1')
        self.assertEqual(result['svc_edge_id'], '100')
        self.assertEqual(result['org_short_name'], 'TESTORG')
        self.assertEqual(result['org_full_name'], 'Test Organization')
        self.assertFalse(result['org_hide'])
        self.assertEqual(result['org_tag'], ['primary'])
        self.assertEqual(result['org_tag_str'], 'primary')
        self.assertEqual(result['org_type'], ['Research Site'])
        self.assertEqual(result['org_funding_agency'], 'TESTFUND')
        self.assertEqual(result['peer_as_id'], 64496)
        self.assertEqual(result['peer_ipv4'], '192.0.2.1')
        self.assertEqual(result['peer_ipv6'], '2001:db8::1')

    def test_build_metadata_fields_ensures_arrays(self):
        """Test that array fields are converted to lists."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        value = {
            'org_tag': None,
            'org_type': 'not_a_list',
            'org_hide': False
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['org_tag'], [])
        self.assertEqual(result['org_type'], [])

    def test_build_metadata_fields_ensures_bool(self):
        """Test that org_hide is converted to bool."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        test_cases = [
            (True, True),
            (False, False),
            (1, True),
            (0, False),
            ('true', True),
            ('', False),
            (None, False)
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                value = {
                    'org_tag': [],
                    'org_type': [],
                    'org_hide': input_val
                }
                result = processor.build_metadata_fields(value)
                self.assertEqual(result['org_hide'], expected)

    def test_build_metadata_fields_validates_asn(self):
        """Test that peer_as_id is validated and converted to int."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        test_cases = [
            (64496, 64496),
            ('64496', 64496),
            ('invalid', None),
            (None, None),
            ('', None)
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                value = {
                    'org_tag': [],
                    'org_type': [],
                    'org_hide': False,
                    'peer_as_id': input_val
                }
                result = processor.build_metadata_fields(value)
                self.assertEqual(result['peer_as_id'], expected)

    # ==================== build_message tests ====================

    def test_build_message_from_graphql_consumer(self):
        """Test building message from GraphQLConsumer data format."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceEdgeList': {
                        'list': [
                            {
                                'id': '100',
                                'name': 'se_test_100',
                                'physicalConnection': {
                                    'id': '50',
                                    'connectionName': 'test-connection-1'
                                },
                                'equipmentInterface': {
                                    'id': '10',
                                    'device': {
                                        'id': '5',
                                        'name': 'router-test1'
                                    },
                                    'interface': '1/1/c1/1'
                                },
                                'serviceEdgeType': {
                                    'id': '1',
                                    'name': 'Layer 3'
                                },
                                'vlan': None,
                                'organization': {
                                    'id': '20',
                                    'shortName': 'TESTORG',
                                    'fullName': 'Test Organization',
                                    'types': [{'id': '1', 'name': 'Research Site'}],
                                    'fundingAgency': {'id': '3', 'shortName': 'TESTFUND'},
                                    'visibility': 'Public',
                                    'tags': ['primary']
                                },
                                'bgpNeighbors': [
                                    {
                                        'id': '30',
                                        'remoteIp': '192.0.2.1',
                                        'peerDetail': {
                                            'id': '40',
                                            'asn': 64496
                                        }
                                    }
                                ],
                                'equipment': {
                                    'id': '60',
                                    'model': None,
                                    'platform': None
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'router-test1::se_test_100')
        self.assertEqual(record['org_short_name'], 'TESTORG')
        self.assertEqual(record['connection_name'], 'test-connection-1')
        self.assertIn('ref', record)
        self.assertIn('hash', record)

    def test_build_message_empty_data(self):
        """Test build_message with empty data."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {}
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(result, [])

    def test_build_message_no_data_field(self):
        """Test build_message when data field is missing."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(result, [])

    def test_build_message_multiple_service_edges(self):
        """Test building message with multiple service edges."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceEdgeList': {
                        'list': [
                            {
                                'id': '100',
                                'name': 'se_test_100',
                                'physicalConnection': None,
                                'equipmentInterface': {
                                    'id': '10',
                                    'device': {'id': '5', 'name': 'router-test1'},
                                    'interface': '1/1/c1/1'
                                },
                                'serviceEdgeType': {'id': '1', 'name': 'Layer 3'},
                                'vlan': None,
                                'organization': {
                                    'id': '20',
                                    'shortName': 'ORG1',
                                    'fullName': 'Organization One',
                                    'types': [],
                                    'fundingAgency': None,
                                    'visibility': 'Public',
                                    'tags': []
                                },
                                'bgpNeighbors': [],
                                'equipment': {'id': '60', 'model': None, 'platform': None}
                            },
                            {
                                'id': '101',
                                'name': 'se_test_101',
                                'physicalConnection': None,
                                'equipmentInterface': {
                                    'id': '11',
                                    'device': {'id': '6', 'name': 'router-test2'},
                                    'interface': '1/1/c1/2'
                                },
                                'serviceEdgeType': {'id': '1', 'name': 'Layer 3'},
                                'vlan': None,
                                'organization': {
                                    'id': '21',
                                    'shortName': 'ORG2',
                                    'fullName': 'Organization Two',
                                    'types': [],
                                    'fundingAgency': None,
                                    'visibility': 'Public',
                                    'tags': []
                                },
                                'bgpNeighbors': [],
                                'equipment': {'id': '61', 'model': None, 'platform': None}
                            }
                        ]
                    }
                }
            }
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(len(result), 2)
        ids = [r['id'] for r in result]
        self.assertIn('router-test1::se_test_100', ids)
        self.assertIn('router-test2::se_test_101', ids)

    def test_build_message_skips_unchanged_records(self):
        """Test that unchanged records are skipped when cached."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceEdgeList': {
                        'list': [
                            {
                                'id': '100',
                                'name': 'se_test_100',
                                'physicalConnection': None,
                                'equipmentInterface': {
                                    'id': '10',
                                    'device': {'id': '5', 'name': 'router-test1'},
                                    'interface': '1/1/c1/1'
                                },
                                'serviceEdgeType': {'id': '1', 'name': 'Layer 3'},
                                'vlan': None,
                                'organization': {
                                    'id': '20',
                                    'shortName': 'TESTORG',
                                    'fullName': 'Test Organization',
                                    'types': [],
                                    'fundingAgency': None,
                                    'visibility': 'Public',
                                    'tags': []
                                },
                                'bgpNeighbors': [],
                                'equipment': {'id': '60', 'model': None, 'platform': None}
                            }
                        ]
                    }
                }
            }
        }
        
        # First call with no cache
        self.mock_clickhouse_cacher.lookup.return_value = None
        result1 = processor.build_message(graphql_msg, {})
        self.assertEqual(len(result1), 1)
        
        # Second call - mock cache returns record with same hash
        self.mock_clickhouse_cacher.lookup.return_value = {
            'id': 'router-test1::se_test_100',
            'hash': result1[0]['hash'],
            'ref': result1[0]['ref']
        }
        result2 = processor.build_message(graphql_msg, {})
        self.assertEqual(len(result2), 0)  # Should skip unchanged record


if __name__ == '__main__':
    unittest.main()