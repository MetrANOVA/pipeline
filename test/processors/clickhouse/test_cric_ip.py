#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
import sys

# Add the project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.cric import MetaIPCRICProcessor


class TestMetaIPCRICProcessor(unittest.TestCase):
    """Test suite for MetaIPCRICProcessor"""
    
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

    def test_init_default_values(self):
        """Test processor initializes with correct default values."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.table, 'meta_ip_cric')
        self.assertEqual(processor.val_id_field, ['id'])
        self.assertEqual(processor.required_fields, [['id'], ['ip_subnet']])
        self.assertEqual(processor.float_fields, ['latitude', 'longitude'])
        self.assertEqual(processor.int_fields, ['tier'])
        self.assertIn('ip_subnet', processor.array_fields)
        self.assertEqual(processor.cric_url_pattern, 'cric.cern.ch')

    def test_init_custom_table_name(self):
        """Test initialization with custom table name."""
        with patch.dict(os.environ, {'CLICKHOUSE_CRIC_IP_METADATA_TABLE': 'custom_cric_table'}):
            processor = MetaIPCRICProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_cric_table')

    def test_init_custom_url_pattern(self):
        """Test initialization with custom URL match pattern."""
        with patch.dict(os.environ, {'CRIC_URL_MATCH_PATTERN': 'test-cric.example.com'}):
            processor = MetaIPCRICProcessor(self.mock_pipeline)
            self.assertEqual(processor.cric_url_pattern, 'test-cric.example.com')

    def test_column_definitions(self):
        """Test that all required columns are defined."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        column_names = [col[0] for col in processor.column_defs]
        
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',
            'ip_subnet', 'rcsite_name', 'latitude', 'longitude',
            'country_name', 'netsite_name', 'tier'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names,
                         f"Column '{expected_col}' not found in column definitions")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        self.assertEqual(column_types['ip_subnet'], 'Array(Tuple(IPv6, UInt8))')
        self.assertEqual(column_types['rcsite_name'], 'LowCardinality(String)')
        self.assertEqual(column_types['latitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['longitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['country_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['netsite_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['tier'], 'Nullable(UInt8)')

    def test_create_table_command(self):
        """Test CREATE TABLE command generation."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        self.assertIn("CREATE TABLE IF NOT EXISTS meta_ip_cric", result)
        self.assertIn("ENGINE = MergeTree", result)
        self.assertIn("ORDER BY", result)
        self.assertIn("rcsite_name", result)
        self.assertIn("id", result)

    # ==================== match_message tests ====================

    def test_match_message_with_table_field(self):
        """Test match_message with standard table field (backward compatibility)."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        msg = {'table': 'meta_ip_cric', 'data': []}
        result = processor.match_message(msg)
        
        self.assertTrue(result)

    def test_match_message_with_wrong_table(self):
        """Test match_message rejects messages with wrong table."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        msg = {'table': 'other_table', 'data': []}
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_with_cric_url(self):
        """Test match_message matches HTTPConsumer messages with CRIC URL."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200,
            'data': {}
        }
        result = processor.match_message(msg)
        
        self.assertTrue(result)

    def test_match_message_with_non_cric_url(self):
        """Test match_message rejects HTTPConsumer messages with non-CRIC URL."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://other-api.example.com/data',
            'status_code': 200,
            'data': {}
        }
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_empty_message(self):
        """Test match_message handles empty message."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        result = processor.match_message({})
        
        self.assertFalse(result)

    def test_match_message_with_custom_url_pattern(self):
        """Test match_message uses custom URL pattern from env."""
        with patch.dict(os.environ, {'CRIC_URL_MATCH_PATTERN': 'custom-cric.example.com'}):
            processor = MetaIPCRICProcessor(self.mock_pipeline)
            
            # Should match custom pattern
            msg_match = {
                'url': 'https://custom-cric.example.com/api/data',
                'status_code': 200,
                'data': {}
            }
            self.assertTrue(processor.match_message(msg_match))
            
            # Should NOT match default pattern anymore
            msg_no_match = {
                'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
                'status_code': 200,
                'data': {}
            }
            self.assertFalse(processor.match_message(msg_no_match))

    # ==================== _parse_ip_subnet tests ====================

    def test_parse_ip_subnet_valid_ipv4(self):
        """Test IP subnet parsing with valid IPv4 input."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        test_cases = [
            ('192.168.1.0/24', ('192.168.1.0', 24)),
            ('10.0.0.0/8', ('10.0.0.0', 8)),
            ('172.16.0.0/12', ('172.16.0.0', 12))
        ]
        
        for ip_str, expected in test_cases:
            with self.subTest(ip_str=ip_str):
                result = processor._parse_ip_subnet(ip_str)
                self.assertEqual(result, expected)

    def test_parse_ip_subnet_valid_ipv6(self):
        """Test IP subnet parsing with valid IPv6 input."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        result = processor._parse_ip_subnet('2001:db8::/32')
        self.assertEqual(result, ('2001:db8::', 32))

    def test_parse_ip_subnet_invalid(self):
        """Test IP subnet parsing with invalid input."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        invalid_inputs = [
            '',
            None,
            'invalid',
            '192.168.1.0',  # Missing prefix
            '192.168.1.0/24/extra'  # Too many parts
        ]
        
        for invalid_input in invalid_inputs:
            with self.subTest(invalid_input=invalid_input):
                result = processor._parse_ip_subnet(invalid_input)
                self.assertIsNone(result)

    # ==================== _build_ip_record tests ====================

    def test_build_ip_record_basic(self):
        """Test building IP record with basic data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        rcsite_info = {
            'rcsite_name': 'CERN',
            'tier': 0,
            'country_name': 'Switzerland',
            'latitude': 46.2044,
            'longitude': 6.1432
        }
        netsite_name = 'CERN-PRIMARY'
        netroute_id = 'CERN_LHCONE'
        ip_subnets = [('192.168.1.0', 24), ('10.0.0.0', 8)]
        
        result = processor._build_ip_record(netroute_id, ip_subnets, rcsite_info, netsite_name)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'CERN_LHCONE')
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24), ('10.0.0.0', 8)])
        self.assertEqual(result['rcsite_name'], 'CERN')
        self.assertEqual(result['latitude'], 46.2044)
        self.assertEqual(result['longitude'], 6.1432)
        self.assertEqual(result['country_name'], 'Switzerland')
        self.assertEqual(result['netsite_name'], 'CERN-PRIMARY')
        self.assertEqual(result['tier'], 0)

    def test_build_ip_record_empty_subnets(self):
        """Test building IP record with empty subnets."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        rcsite_info = {
            'rcsite_name': 'CERN',
            'tier': 0,
            'country_name': 'Switzerland',
            'latitude': 46.2044,
            'longitude': 6.1432
        }
        netsite_name = 'CERN-PRIMARY'
        netroute_id = 'CERN_LHCONE'
        ip_subnets = []
        
        result = processor._build_ip_record(netroute_id, ip_subnets, rcsite_info, netsite_name)
        
        self.assertIsNone(result)

    def test_build_ip_record_none_subnets(self):
        """Test building IP record with None subnets."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        rcsite_info = {
            'rcsite_name': 'CERN',
            'tier': 0,
            'country_name': 'Switzerland',
            'latitude': 46.2044,
            'longitude': 6.1432
        }
        netsite_name = 'CERN-PRIMARY'
        netroute_id = 'CERN_LHCONE'
        ip_subnets = None
        
        result = processor._build_ip_record(netroute_id, ip_subnets, rcsite_info, netsite_name)
        
        self.assertIsNone(result)

    # ==================== _extract_ip_records tests ====================

    def test_extract_ip_records_complete(self):
        """Test extracting IP records from complete CRIC data - one record per netroute."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        cric_data = {
            'CERN': {
                'country': 'Switzerland',
                'latitude': 46.2044,
                'longitude': 6.1432,
                'rc_tier_level': 0,
                'netroutes': {
                    'CERN_LHCONE': {
                        'netsite': 'CERN-PRIMARY',
                        'networks': {
                            'ipv4': ['192.168.1.0/24', '10.0.0.0/8'],
                            'ipv6': ['2001:db8::/32']
                        }
                    }
                }
            }
        }
        
        result = processor._extract_ip_records(cric_data)
        
        # Should be 1 record (one netroute with all IPs combined)
        self.assertEqual(len(result), 1)
        
        # Check the record
        record = result[0]
        self.assertEqual(record['id'], 'CERN_LHCONE')
        self.assertEqual(record['rcsite_name'], 'CERN')
        self.assertEqual(record['netsite_name'], 'CERN-PRIMARY')
        self.assertEqual(record['tier'], 0)
        
        # Should have 3 IP subnets combined
        self.assertEqual(len(record['ip_subnet']), 3)
        self.assertIn(('192.168.1.0', 24), record['ip_subnet'])
        self.assertIn(('10.0.0.0', 8), record['ip_subnet'])
        self.assertIn(('2001:db8::', 32), record['ip_subnet'])

    def test_extract_ip_records_multiple_netroutes(self):
        """Test extracting IP records with multiple netroutes per site."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        cric_data = {
            'AGLT2': {
                'country': 'United States',
                'latitude': 42.291637,
                'longitude': -83.71831,
                'rc_tier_level': 2,
                'netroutes': {
                    'AGLT2_UM': {
                        'netsite': 'US-AGLT2 University of Michigan',
                        'networks': {
                            'ipv4': ['192.41.230.0/23', '192.41.238.0/28']
                        }
                    },
                    'AGLT2_MSU': {
                        'netsite': 'US-AGLT2 Michigan State University',
                        'networks': {
                            'ipv4': ['192.41.236.0/23']
                        }
                    }
                }
            }
        }
        
        result = processor._extract_ip_records(cric_data)
        
        # Should be 2 records (one per netroute)
        self.assertEqual(len(result), 2)
        
        # Find records by id
        records_by_id = {r['id']: r for r in result}
        
        # Check AGLT2_UM record
        self.assertIn('AGLT2_UM', records_by_id)
        um_record = records_by_id['AGLT2_UM']
        self.assertEqual(um_record['rcsite_name'], 'AGLT2')
        self.assertEqual(um_record['netsite_name'], 'US-AGLT2 University of Michigan')
        self.assertEqual(len(um_record['ip_subnet']), 2)
        
        # Check AGLT2_MSU record
        self.assertIn('AGLT2_MSU', records_by_id)
        msu_record = records_by_id['AGLT2_MSU']
        self.assertEqual(msu_record['rcsite_name'], 'AGLT2')
        self.assertEqual(msu_record['netsite_name'], 'US-AGLT2 Michigan State University')
        self.assertEqual(len(msu_record['ip_subnet']), 1)

    def test_extract_ip_records_no_netroutes(self):
        """Test extracting IP records when site has no netroutes."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        cric_data = {
            'SITE_NO_ROUTES': {
                'country': 'US',
                'latitude': 34.0,
                'longitude': -118.2,
                'rc_tier_level': 2
                # No netroutes
            }
        }
        
        result = processor._extract_ip_records(cric_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_empty_networks(self):
        """Test extracting IP records when netroute has no networks."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        cric_data = {
            'SITE1': {
                'country': 'US',
                'latitude': 34.0,
                'longitude': -118.2,
                'rc_tier_level': 2,
                'netroutes': {
                    'route1': {
                        'netsite': 'SITE1-NET',
                        'networks': {}  # Empty networks
                    }
                }
            }
        }
        
        result = processor._extract_ip_records(cric_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_empty_data(self):
        """Test extracting IP records from empty CRIC data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        result = processor._extract_ip_records({})
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_none_data(self):
        """Test extracting IP records from None CRIC data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        result = processor._extract_ip_records(None)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_null_netsite(self):
        """Test extracting IP records when netsite is null."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        cric_data = {
            'SITE1': {
                'country': 'US',
                'latitude': 34.0,
                'longitude': -118.2,
                'rc_tier_level': 2,
                'netroutes': {
                    'route1': {
                        'netsite': None,  # Null netsite
                        'networks': {
                            'ipv4': ['10.0.0.0/8']
                        }
                    }
                }
            }
        }
        
        result = processor._extract_ip_records(cric_data)
        
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]['netsite_name'])

    # ==================== build_metadata_fields tests ====================

    def test_build_metadata_fields_basic(self):
        """Test building metadata fields with basic data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [('192.168.1.0', 24), ('10.0.0.0', 8)],
            'rcsite_name': 'CERN',
            'latitude': 46.2044,
            'longitude': 6.1432,
            'country_name': 'Switzerland',
            'netsite_name': 'CERN-PRIMARY',
            'tier': 0
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24), ('10.0.0.0', 8)])
        self.assertEqual(result['rcsite_name'], 'CERN')
        self.assertEqual(result['latitude'], 46.2044)
        self.assertEqual(result['longitude'], 6.1432)
        self.assertEqual(result['country_name'], 'Switzerland')
        self.assertEqual(result['netsite_name'], 'CERN-PRIMARY')
        self.assertEqual(result['tier'], 0)

    def test_build_metadata_fields_ip_subnet_validation(self):
        """Test IP subnet validation in metadata fields."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [
                ('192.168.1.0', 24),  # Valid
                ('invalid', 'bad'),    # Invalid
                ('10.0.0.0', 8),       # Valid
            ],
            'rcsite_name': 'Test Site'
        }
        
        with patch.object(processor.logger, 'warning'):
            result = processor.build_metadata_fields(value)
            
            # Should only contain valid subnets
            self.assertEqual(len(result['ip_subnet']), 2)
            self.assertIn(('192.168.1.0', 24), result['ip_subnet'])
            self.assertIn(('10.0.0.0', 8), result['ip_subnet'])

    def test_build_metadata_fields_rcsite_name_default(self):
        """Test that rcsite_name has default value if missing."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {'ip_subnet': [('192.168.1.0', 24)]}
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['rcsite_name'], 'Unknown')

    def test_build_metadata_fields_null_values(self):
        """Test handling of null/None values in fields."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [('192.168.1.0', 24)],
            'rcsite_name': 'Test Site',
            'latitude': None,
            'longitude': None,
            'country_name': None,
            'netsite_name': None,
            'tier': None
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNone(result['latitude'])
        self.assertIsNone(result['longitude'])
        self.assertIsNone(result['country_name'])
        self.assertIsNone(result['netsite_name'])
        self.assertIsNone(result['tier'])

    # ==================== build_message tests ====================

    def test_build_message_from_http_consumer(self):
        """Test building message from HTTPConsumer data format."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        # HTTPConsumer sends: {'url': ..., 'status_code': ..., 'data': {...}}
        http_msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200,
            'data': {
                'CERN': {
                    'country': 'Switzerland',
                    'latitude': 46.2044,
                    'longitude': 6.1432,
                    'rc_tier_level': 0,
                    'netroutes': {
                        'CERN_LHCONE': {
                            'netsite': 'CERN-PRIMARY',
                            'networks': {
                                'ipv4': ['192.168.1.0/24', '10.0.0.0/8'],
                                'ipv6': []
                            }
                        }
                    }
                }
            }
        }
        
        result = processor.build_message(http_msg, {})
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record['id'], 'CERN_LHCONE')
        self.assertEqual(record['rcsite_name'], 'CERN')
        self.assertEqual(record['netsite_name'], 'CERN-PRIMARY')
        self.assertEqual(record['country_name'], 'Switzerland')
        self.assertEqual(len(record['ip_subnet']), 2)
        self.assertIn('ref', record)
        self.assertIn('hash', record)

    def test_build_message_empty_data(self):
        """Test build_message with empty data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        http_msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200,
            'data': {}
        }
        
        result = processor.build_message(http_msg, {})
        
        self.assertEqual(result, [])

    def test_build_message_no_data_field(self):
        """Test build_message when data field is missing."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        http_msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200
        }
        
        result = processor.build_message(http_msg, {})
        
        self.assertEqual(result, [])

    def test_build_message_multiple_sites(self):
        """Test building message with multiple sites."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        http_msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200,
            'data': {
                'SITE1': {
                    'country': 'Switzerland',
                    'latitude': 46.0,
                    'longitude': 6.0,
                    'rc_tier_level': 0,
                    'netroutes': {
                        'SITE1_ROUTE': {
                            'netsite': 'SITE1-NET',
                            'networks': {'ipv4': ['10.0.0.0/8'], 'ipv6': []}
                        }
                    }
                },
                'SITE2': {
                    'country': 'US',
                    'latitude': 34.0,
                    'longitude': -118.0,
                    'rc_tier_level': 1,
                    'netroutes': {
                        'SITE2_ROUTE': {
                            'netsite': 'SITE2-NET',
                            'networks': {'ipv4': ['172.16.0.0/12'], 'ipv6': []}
                        }
                    }
                }
            }
        }
        
        result = processor.build_message(http_msg, {})
        
        self.assertEqual(len(result), 2)
        ids = [r['id'] for r in result]
        self.assertIn('SITE1_ROUTE', ids)
        self.assertIn('SITE2_ROUTE', ids)

    def test_build_message_multiple_netroutes_per_site(self):
        """Test building message with multiple netroutes per site (like AGLT2)."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        http_msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200,
            'data': {
                'AGLT2': {
                    'country': 'United States',
                    'latitude': 42.291637,
                    'longitude': -83.71831,
                    'rc_tier_level': 2,
                    'netroutes': {
                        'AGLT2_LHCONE_RT': {
                            'netsite': 'US-AGLT2 Michigan State University',
                            'networks': {
                                'ipv6': ['2001:48a8:68f7:4000::/50', '2001:48a8:68f7:c000::/50']
                            }
                        },
                        'AGLT2_UM': {
                            'netsite': 'US-AGLT2 University of Michigan',
                            'networks': {
                                'ipv4': ['192.41.230.0/23', '192.41.238.0/28', '35.199.60.100/32']
                            }
                        },
                        'AGLT2_MSU': {
                            'netsite': 'US-AGLT2 Michigan State University',
                            'networks': {
                                'ipv4': ['192.41.236.0/23', '192.41.238.0/28']
                            }
                        }
                    }
                }
            }
        }
        
        result = processor.build_message(http_msg, {})
        
        # Should have 3 records (one per netroute)
        self.assertEqual(len(result), 3)
        
        # Check IDs
        ids = [r['id'] for r in result]
        self.assertIn('AGLT2_LHCONE_RT', ids)
        self.assertIn('AGLT2_UM', ids)
        self.assertIn('AGLT2_MSU', ids)
        
        # Find and verify AGLT2_UM record
        um_record = next(r for r in result if r['id'] == 'AGLT2_UM')
        self.assertEqual(um_record['rcsite_name'], 'AGLT2')
        self.assertEqual(um_record['netsite_name'], 'US-AGLT2 University of Michigan')
        self.assertEqual(len(um_record['ip_subnet']), 3)  # 3 IPv4 ranges combined

    def test_build_message_skips_unchanged_records(self):
        """
        Test that unchanged records are skipped when cached.

        When the same data comes in twice, the processor should skip inserting it again if nothing changed. 
        This prevents unnecessary duplicate rows in ClickHouse
        This test essentially verifies the versioning/deduplication logic in BaseMetadataProcessor
        """
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        http_msg = {
            'url': 'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json',
            'status_code': 200,
            'data': {
                'CERN': {
                    'country': 'Switzerland',
                    'latitude': 46.2044,
                    'longitude': 6.1432,
                    'rc_tier_level': 0,
                    'netroutes': {
                        'CERN_LHCONE': {
                            'netsite': 'CERN-PRIMARY',
                            'networks': {'ipv4': ['192.168.1.0/24'], 'ipv6': []}
                        }
                    }
                }
            }
        }
        
        # First call with no cache
        self.mock_clickhouse_cacher.lookup.return_value = None
        result1 = processor.build_message(http_msg, {})
        self.assertEqual(len(result1), 1)
        
        # Second call - mock cache returns record with same hash
        self.mock_clickhouse_cacher.lookup.return_value = {
            'id': 'CERN_LHCONE',
            'hash': result1[0]['hash'],
            'ref': result1[0]['ref']
        }
        result2 = processor.build_message(http_msg, {})
        self.assertEqual(len(result2), 0)  # Should skip unchanged record


if __name__ == '__main__':
    unittest.main()