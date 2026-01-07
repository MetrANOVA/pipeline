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

    def test_column_definitions(self):
        """Test that all required columns are defined."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        column_names = [col[0] for col in processor.column_defs]
        
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',
            'ip_subnet', 'name', 'latitude', 'longitude',
            'country_name', 'net_site', 'tier'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names,
                         f"Column '{expected_col}' not found in column definitions")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        self.assertEqual(column_types['ip_subnet'], 'Array(Tuple(IPv6, UInt8))')
        self.assertEqual(column_types['name'], 'LowCardinality(String)')
        self.assertEqual(column_types['latitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['longitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['country_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['net_site'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['tier'], 'Nullable(UInt8)')

    def test_create_table_command(self):
        """Test CREATE TABLE command generation."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        self.assertIn("CREATE TABLE IF NOT EXISTS meta_ip_cric", result)
        self.assertIn("ENGINE = ReplacingMergeTree", result)
        self.assertIn("PRIMARY KEY", result)
        self.assertIn("ORDER BY", result)
        self.assertIn("name", result)
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
        
        site_info = {
            'name': 'CERN',
            'tier': 0,
            'country_name': 'Switzerland',
            'latitude': 46.2044,
            'longitude': 6.1432
        }
        net_site = 'CERN-PRIMARY'
        ip_range = '192.168.1.0/24'
        
        result = processor._build_ip_record(ip_range, site_info, net_site)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], '192.168.1.0/24')
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24)])
        self.assertEqual(result['name'], 'CERN')
        self.assertEqual(result['latitude'], 46.2044)
        self.assertEqual(result['longitude'], 6.1432)
        self.assertEqual(result['country_name'], 'Switzerland')
        self.assertEqual(result['net_site'], 'CERN-PRIMARY')
        self.assertEqual(result['tier'], 0)

    def test_build_ip_record_invalid_subnet(self):
        """Test building IP record with invalid subnet."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        site_info = {'name': 'CERN', 'tier': 0, 'country_name': 'CH', 'latitude': 46.2, 'longitude': 6.1}
        net_site = 'CERN-PRIMARY'
        ip_range = 'invalid-subnet'
        
        result = processor._build_ip_record(ip_range, site_info, net_site)
        
        self.assertIsNone(result)

    # ==================== _extract_ip_records tests ====================

    def test_extract_ip_records_complete(self):
        """Test extracting IP records from complete CRIC data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        cric_data = {
            'CERN': {
                'country': 'Switzerland',  # CRIC API returns 'country'; _extract_ip_records renames it to 'country_name'
                'latitude': 46.2044,
                'longitude': 6.1432,
                'rc_tier_level': 0,
                'netroutes': {
                    'route1': {
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
        
        self.assertEqual(len(result), 3)  # 2 IPv4 + 1 IPv6
        
        # Check first record
        self.assertEqual(result[0]['id'], '192.168.1.0/24')
        self.assertEqual(result[0]['name'], 'CERN')
        self.assertEqual(result[0]['tier'], 0)

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

    # ==================== build_metadata_fields tests ====================

    def test_build_metadata_fields_basic(self):
        """Test building metadata fields with basic data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [('192.168.1.0', 24)],
            'name': 'CERN',
            'latitude': 46.2044,
            'longitude': 6.1432,
            'country_name': 'Switzerland',
            'net_site': 'CERN-PRIMARY',
            'tier': 0
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24)])
        self.assertEqual(result['name'], 'CERN')
        self.assertEqual(result['latitude'], 46.2044)
        self.assertEqual(result['longitude'], 6.1432)
        self.assertEqual(result['country_name'], 'Switzerland')
        self.assertEqual(result['net_site'], 'CERN-PRIMARY')
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
            'name': 'Test Site'
        }
        
        with patch.object(processor.logger, 'warning'):
            result = processor.build_metadata_fields(value)
            
            # Should only contain valid subnets
            self.assertEqual(len(result['ip_subnet']), 2)
            self.assertIn(('192.168.1.0', 24), result['ip_subnet'])
            self.assertIn(('10.0.0.0', 8), result['ip_subnet'])

    def test_build_metadata_fields_name_default(self):
        """Test that name has default value if missing."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {'ip_subnet': [('192.168.1.0', 24)]}
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['name'], 'Unknown')

    def test_build_metadata_fields_null_values(self):
        """Test handling of null/None values in fields."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [('192.168.1.0', 24)],
            'name': 'Test Site',
            'latitude': None,
            'longitude': None,
            'country_name': None,
            'net_site': None,
            'tier': None
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertIsNone(result['latitude'])
        self.assertIsNone(result['longitude'])
        self.assertIsNone(result['country_name'])
        self.assertIsNone(result['net_site'])
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
                        'route1': {
                            'netsite': 'CERN-PRIMARY',
                            'networks': {
                                'ipv4': ['192.168.1.0/24'],
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
        self.assertEqual(record['id'], '192.168.1.0/24')
        self.assertEqual(record['name'], 'CERN')
        self.assertEqual(record['country_name'], 'Switzerland')
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
                        'route1': {
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
                        'route1': {
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
        self.assertIn('10.0.0.0/8', ids)
        self.assertIn('172.16.0.0/12', ids)

    def test_build_message_skips_unchanged_records(self):
        """
        Test that unchanged records are skipped when cached.

        When the same data comes in twice, the processor should skip inserting it again if nothing changed. 
        This prevents unnecessary duplicate rows in ClickHouse
        This test essentially verifies the versioning/deduplication logic in BaseMetadataProcessor
        """
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        # Mock cache to return existing record with same hash
        existing_record = {
            'id': '192.168.1.0/24',
            'hash': 'somehash',
            'ref': '192.168.1.0/24__v1'
        }
        self.mock_clickhouse_cacher.lookup.return_value = existing_record
        
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
                        'route1': {
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
            'id': '192.168.1.0/24',
            'hash': result1[0]['hash'],
            'ref': result1[0]['ref']
        }
        result2 = processor.build_message(http_msg, {})
        self.assertEqual(len(result2), 0)  # Should skip unchanged record


if __name__ == '__main__':
    unittest.main()