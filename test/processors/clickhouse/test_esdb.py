#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
import sys

# Add the project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.esdb import MetaIPServiceProcessor, MetaServiceEdgeProcessor


class TestMetaIPServiceProcessor(unittest.TestCase):
    """Test suite for MetaIPServiceProcessor"""
    
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
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.table, 'meta_ip_esdb')
        self.assertEqual(processor.val_id_field, ['id'])
        self.assertEqual(processor.required_fields, [['id'], ['ip_subnet']])
        self.assertEqual(processor.int_fields, ['as_id'])
        self.assertEqual(processor.bool_fields, ['org_hide'])
        self.assertIn('service_prefix_group_name', processor.array_fields)
        self.assertIn('service_label', processor.array_fields)
        self.assertIn('service_type', processor.array_fields)
        self.assertIn('org_types', processor.array_fields)
        self.assertEqual(processor.ipservice_url_pattern, 'esdb')

    def test_init_custom_table_name(self):
        """Test initialization with custom table name."""
        with patch.dict(os.environ, {'CLICKHOUSE_ESDB_IP_METADATA_TABLE': 'custom_esdb_table'}):
            processor = MetaIPServiceProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_esdb_table')

    def test_init_custom_url_pattern(self):
        """Test initialization with custom URL match pattern."""
        with patch.dict(os.environ, {'IP_SERVICE_URL_MATCH_PATTERN': 'test-esdb.example.com'}):
            processor = MetaIPServiceProcessor(self.mock_pipeline)
            self.assertEqual(processor.ipservice_url_pattern, 'test-esdb.example.com')

    def test_column_definitions(self):
        """Test that all required columns are defined."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        column_names = [col[0] for col in processor.column_defs]
        
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',
            'ip_subnet', 'service_prefix_group_name', 'service_label',
            'service_type', 'org_short_name', 'org_full_name',
            'org_types', 'org_funding_agency', 'org_hide', 'as_id'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names,
                         f"Column '{expected_col}' not found in column definitions")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        self.assertEqual(column_types['ip_subnet'], 'Tuple(IPv6, UInt8)')
        self.assertEqual(column_types['service_prefix_group_name'], 'Array(String)')
        self.assertEqual(column_types['service_label'], 'Array(String)')
        self.assertEqual(column_types['service_type'], 'Array(String)')
        self.assertEqual(column_types['org_short_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['org_full_name'], 'Nullable(String)')
        self.assertEqual(column_types['org_types'], 'Array(String)')
        self.assertEqual(column_types['org_funding_agency'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['org_hide'], 'Bool')
        self.assertEqual(column_types['as_id'], 'Nullable(UInt32)')

    def test_create_table_command(self):
        """Test CREATE TABLE command generation."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        self.assertIn("CREATE TABLE IF NOT EXISTS meta_ip_esdb", result)
        self.assertIn("ENGINE = MergeTree", result)
        self.assertIn("ORDER BY", result)

    # ==================== match_message tests ====================

    def test_match_message_with_table_field(self):
        """Test match_message with standard table field (backward compatibility)."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        msg = {'table': 'meta_ip_esdb', 'data': []}
        result = processor.match_message(msg)
        
        self.assertTrue(result)

    def test_match_message_with_wrong_table(self):
        """Test match_message rejects messages with wrong table."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        msg = {'table': 'other_table', 'data': []}
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_with_esdb_url(self):
        """Test match_message matches GraphQLConsumer messages with ESDB URL."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {}
        }
        result = processor.match_message(msg)
        
        self.assertTrue(result)

    def test_match_message_with_non_esdb_url(self):
        """Test match_message rejects messages with non-ESDB URL."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        msg = {
            'url': 'https://other-api.example.com/data',
            'status_code': 200,
            'data': {}
        }
        result = processor.match_message(msg)
        
        self.assertFalse(result)

    def test_match_message_empty_message(self):
        """Test match_message handles empty message."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        result = processor.match_message({})
        
        self.assertFalse(result)

    def test_match_message_with_custom_url_pattern(self):
        """Test match_message uses custom URL pattern from env."""
        with patch.dict(os.environ, {'IP_SERVICE_URL_MATCH_PATTERN': 'custom-esdb.example.com'}):
            processor = MetaIPServiceProcessor(self.mock_pipeline)
            
            # Should match custom pattern
            msg_match = {
                'url': 'https://custom-esdb.example.com/graphql',
                'status_code': 200,
                'data': {}
            }
            self.assertTrue(processor.match_message(msg_match))
            
            # Should NOT match default pattern anymore
            msg_no_match = {
                'url': 'https://esdb.example.com/graphql',
                'status_code': 200,
                'data': {}
            }
            self.assertFalse(processor.match_message(msg_no_match))

    # ==================== _parse_ip_subnet tests ====================

    def test_parse_ip_subnet_valid_ipv4(self):
        """Test IP subnet parsing with valid IPv4 input."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        # Using RFC 5737 documentation ranges
        test_cases = [
            ('192.0.2.0/24', ('192.0.2.0', 24)),      # TEST-NET-1
            ('198.51.100.0/24', ('198.51.100.0', 24)), # TEST-NET-2
            ('203.0.113.0/24', ('203.0.113.0', 24)),   # TEST-NET-3
            ('10.0.0.0/8', ('10.0.0.0', 8)),           # Private
            ('172.16.0.0/12', ('172.16.0.0', 12))      # Private
        ]
        
        for ip_str, expected in test_cases:
            with self.subTest(ip_str=ip_str):
                result = processor._parse_ip_subnet(ip_str)
                self.assertEqual(result, expected)

    def test_parse_ip_subnet_valid_ipv6(self):
        """Test IP subnet parsing with valid IPv6 input."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        # Using RFC 3849 documentation range
        result = processor._parse_ip_subnet('2001:db8::/32')
        self.assertEqual(result, ('2001:db8::', 32))

    def test_parse_ip_subnet_invalid(self):
        """Test IP subnet parsing with invalid input."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        invalid_inputs = [
            '',
            None,
            'invalid',
            '192.0.2.1',  # Missing prefix
            '192.0.2.0/24/extra'  # Too many parts
        ]
        
        for invalid_input in invalid_inputs:
            with self.subTest(invalid_input=invalid_input):
                result = processor._parse_ip_subnet(invalid_input)
                self.assertIsNone(result)

    # ==================== _build_ip_record tests ====================

    def test_build_ip_record_basic(self):
        """Test building IP record with basic data."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        service_info = {
            'prefix_group_name': ['TESTLAB-Primary-v4'],
            'label': ['Primary'],
            'type': ['IPv4']
        }
        org_info = {
            'short_name': 'TESTLAB',
            'full_name': 'Test Laboratory',
            'types': ['Research Site'],
            'funding_agency': 'TESTFUND',
            'hide': False
        }
        asn = 64496  # RFC 5398 documentation ASN
        prefix_ip = '192.0.2.0/24'  # RFC 5737 TEST-NET-1
        
        result = processor._build_ip_record(prefix_ip, service_info, org_info, asn)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], '192.0.2.0/24')
        self.assertEqual(result['ip_subnet'], ('192.0.2.0', 24))
        self.assertEqual(result['service_prefix_group_name'], ['TESTLAB-Primary-v4'])
        self.assertEqual(result['service_label'], ['Primary'])
        self.assertEqual(result['service_type'], ['IPv4'])
        self.assertEqual(result['org_short_name'], 'TESTLAB')
        self.assertEqual(result['org_full_name'], 'Test Laboratory')
        self.assertEqual(result['org_types'], ['Research Site'])
        self.assertEqual(result['org_funding_agency'], 'TESTFUND')
        self.assertEqual(result['org_hide'], False)
        self.assertEqual(result['as_id'], 64496)

    def test_build_ip_record_multiple_services(self):
        """Test building IP record with multiple service values (merged)."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        service_info = {
            'prefix_group_name': ['TESTLAB-Primary-v4', 'TESTLAB-Science-v4'],
            'label': ['Primary', 'Science'],
            'type': ['IPv4', 'IPv4']
        }
        org_info = {
            'short_name': 'TESTLAB',
            'full_name': 'Test Laboratory',
            'types': ['Research Site', 'Research'],
            'funding_agency': 'TESTFUND',
            'hide': False
        }
        asn = 64496  # RFC 5398 documentation ASN
        prefix_ip = '192.0.2.0/24'  # RFC 5737 TEST-NET-1
        
        result = processor._build_ip_record(prefix_ip, service_info, org_info, asn)
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result['service_prefix_group_name']), 2)
        self.assertEqual(len(result['service_label']), 2)
        self.assertEqual(len(result['service_type']), 2)
        self.assertEqual(len(result['org_types']), 2)

    def test_build_ip_record_invalid_prefix(self):
        """Test building IP record with invalid prefix."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        service_info = {'prefix_group_name': [], 'label': [], 'type': []}
        org_info = {'short_name': None, 'full_name': None, 'types': [], 'funding_agency': None, 'hide': False}
        
        result = processor._build_ip_record('invalid', service_info, org_info, None)
        
        self.assertIsNone(result)

    def test_build_ip_record_empty_prefix(self):
        """Test building IP record with empty prefix."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        service_info = {'prefix_group_name': [], 'label': [], 'type': []}
        org_info = {'short_name': None, 'full_name': None, 'types': [], 'funding_agency': None, 'hide': False}
        
        result = processor._build_ip_record('', service_info, org_info, None)
        
        self.assertIsNone(result)

    def test_build_ip_record_hidden_org(self):
        """Test building IP record with hidden organization."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        service_info = {'prefix_group_name': ['Test'], 'label': ['Test'], 'type': ['IPv4']}
        org_info = {
            'short_name': 'HiddenOrg',
            'full_name': 'Hidden Organization',
            'types': [],
            'funding_agency': None,
            'hide': True
        }
        
        result = processor._build_ip_record('10.0.0.0/8', service_info, org_info, None)
        
        self.assertIsNotNone(result)
        self.assertTrue(result['org_hide'])

    def test_build_ip_record_null_asn(self):
        """Test building IP record with null ASN."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        service_info = {'prefix_group_name': ['Test'], 'label': ['Test'], 'type': ['IPv4']}
        org_info = {'short_name': 'Test', 'full_name': 'Test Org', 'types': [], 'funding_agency': None, 'hide': False}
        
        result = processor._build_ip_record('10.0.0.0/8', service_info, org_info, None)
        
        self.assertIsNotNone(result)
        self.assertIsNone(result['as_id'])

    # ==================== _extract_ip_records tests ====================

    def test_extract_ip_records_complete(self):
        """Test extracting IP records from complete ESDB GraphQL data."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'TESTLAB-Primary-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},  # RFC 5398 documentation ASN
                            'prefixes': [
                                {'prefixIp': '192.0.2.0/24'},    # RFC 5737 TEST-NET-1
                                {'prefixIp': '198.51.100.0/24'}  # RFC 5737 TEST-NET-2
                            ]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 2)
        
        # Check first record
        record1 = next(r for r in result if r['id'] == '192.0.2.0/24')
        self.assertEqual(record1['ip_subnet'], ('192.0.2.0', 24))
        self.assertEqual(record1['service_prefix_group_name'], ['TESTLAB-Primary-v4'])
        self.assertEqual(record1['service_label'], ['Primary'])
        self.assertEqual(record1['service_type'], ['IPv4'])
        self.assertEqual(record1['org_short_name'], 'TESTLAB')
        self.assertEqual(record1['org_full_name'], 'Test Laboratory')
        self.assertEqual(record1['org_types'], ['Research Site'])
        self.assertEqual(record1['org_funding_agency'], 'TESTFUND')
        self.assertFalse(record1['org_hide'])
        self.assertEqual(record1['as_id'], 64496)

    def test_extract_ip_records_duplicate_prefix_merging_deduplicates(self):
        """Test that duplicate values are deduplicated when merging prefixes."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'TESTLAB-Primary-v4',
                            'serviceType': {'shortName': 'IPv4'},  # Same type
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # TEST-NET-1
                        },
                        {
                            'id': '456',
                            'label': 'Science',
                            'prefixGroupName': 'TESTLAB-Science-v4',
                            'serviceType': {'shortName': 'IPv4'},  # Same type
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # Same prefix!
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        # Should be 1 record (merged)
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record['id'], '192.0.2.0/24')
        
        # Different values are merged
        self.assertEqual(len(record['service_prefix_group_name']), 2)
        self.assertIn('TESTLAB-Primary-v4', record['service_prefix_group_name'])
        self.assertIn('TESTLAB-Science-v4', record['service_prefix_group_name'])
        
        self.assertEqual(len(record['service_label']), 2)
        self.assertIn('Primary', record['service_label'])
        self.assertIn('Science', record['service_label'])
        
        # Same values are deduplicated - both services have 'IPv4'
        self.assertEqual(len(record['service_type']), 1)
        self.assertEqual(record['service_type'], ['IPv4'])

    def test_extract_ip_records_duplicate_prefix_merging_different_types(self):
        """Test that different service types are preserved when merging prefixes."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'TESTLAB-Primary-v4',
                            'serviceType': {'shortName': 'IPv4'},  # Type 1
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # TEST-NET-1
                        },
                        {
                            'id': '456',
                            'label': 'Transit',
                            'prefixGroupName': 'TESTLAB-Transit-v4',
                            'serviceType': {'shortName': 'Transit'},  # Type 2 - different!
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # Same prefix!
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        # Should be 1 record (merged)
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record['id'], '192.0.2.0/24')
        
        # All different values are preserved
        self.assertEqual(len(record['service_prefix_group_name']), 2)
        self.assertIn('TESTLAB-Primary-v4', record['service_prefix_group_name'])
        self.assertIn('TESTLAB-Transit-v4', record['service_prefix_group_name'])
        
        self.assertEqual(len(record['service_label']), 2)
        self.assertIn('Primary', record['service_label'])
        self.assertIn('Transit', record['service_label'])
        
        # Different service types are both preserved
        self.assertEqual(len(record['service_type']), 2)
        self.assertIn('IPv4', record['service_type'])
        self.assertIn('Transit', record['service_type'])

    def test_extract_ip_records_no_duplicate_values_in_merge(self):
        """Test that completely identical service entries don't create duplicates."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'TESTLAB-Primary-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {'shortName': 'TESTLAB', 'fullName': 'TESTLAB', 'types': [], 'visibility': 'Public'},
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        },
                        {
                            'id': '456',
                            'label': 'Primary',  # Same label
                            'prefixGroupName': 'TESTLAB-Primary-v4',  # Same prefix group name
                            'serviceType': {'shortName': 'IPv4'},  # Same type
                            'customer': {'shortName': 'TESTLAB', 'fullName': 'TESTLAB', 'types': [], 'visibility': 'Public'},
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        
        # Should NOT have duplicates - all values were identical
        self.assertEqual(len(record['service_prefix_group_name']), 1)
        self.assertEqual(len(record['service_label']), 1)
        self.assertEqual(len(record['service_type']), 1)

    def test_extract_ip_records_merge_different_orgs_same_prefix(self):
        """Test merging when different orgs share the same prefix (edge case)."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'ORG1-Primary-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'ORG1',
                                'fullName': 'Organization One',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'FUND1'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},  # RFC 5398 documentation ASN
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # TEST-NET-1
                        },
                        {
                            'id': '456',
                            'label': 'Secondary',
                            'prefixGroupName': 'ORG2-Secondary-v4',
                            'serviceType': {'shortName': 'Transit'},
                            'customer': {
                                'shortName': 'ORG2',
                                'fullName': 'Organization Two',
                                'types': [{'name': 'Commercial Peer'}],
                                'fundingAgency': {'shortName': 'FUND2'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64497},  # RFC 5398 documentation ASN
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # Same prefix!
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        # Should be 1 record (merged)
        self.assertEqual(len(result), 1)
        record = result[0]
        
        # Service info merged
        self.assertEqual(len(record['service_prefix_group_name']), 2)
        self.assertEqual(len(record['service_label']), 2)
        self.assertEqual(len(record['service_type']), 2)
        
        # Org info comes from first service encountered
        self.assertEqual(record['org_short_name'], 'ORG1')
        self.assertEqual(record['org_full_name'], 'Organization One')
        self.assertEqual(record['org_funding_agency'], 'FUND1')
        self.assertEqual(record['org_types'], ['Research Site'])
        self.assertEqual(record['as_id'], 64496)

    def test_extract_ip_records_hidden_visibility(self):
        """Test extraction with Hidden visibility."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Hidden Service',
                            'prefixGroupName': 'Hidden-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'HiddenOrg',
                                'fullName': 'Hidden Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Hidden'
                            },
                            'peer': None,
                            'prefixes': [{'prefixIp': '10.0.0.0/8'}]  # Private range
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]['org_hide'])

    def test_extract_ip_records_hide_flow_data_visibility(self):
        """Test extraction with Hide Flow Data visibility."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Test',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'TestOrg',
                                'fullName': 'Test Organization',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Hide Flow Data'
                            },
                            'peer': None,
                            'prefixes': [{'prefixIp': '10.0.0.0/8'}]  # Private range
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0]['org_hide'])

    def test_extract_ip_records_multiple_org_types(self):
        """Test extraction with multiple organization types."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'MultiTypeOrg',
                                'fullName': 'Multi Type Organization',
                                'types': [
                                    {'name': 'Research Site'},
                                    {'name': 'Commercial Peer'},
                                    {'name': 'Research'}
                                ],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '203.0.113.0/24'}]  # RFC 5737 TEST-NET-3
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0]['org_types']), 3)
        self.assertIn('Research Site', result[0]['org_types'])
        self.assertIn('Commercial Peer', result[0]['org_types'])
        self.assertIn('Research', result[0]['org_types'])

    def test_extract_ip_records_empty_data(self):
        """Test extracting IP records from empty data."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        result = processor._extract_ip_records({})
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_none_data(self):
        """Test extracting IP records from None data."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        result = processor._extract_ip_records(None)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_empty_service_list(self):
        """Test extracting IP records when serviceList is empty."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': []
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_no_prefixes(self):
        """Test extracting IP records when service has no prefixes."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {'shortName': 'Test', 'fullName': 'Test', 'types': [], 'visibility': 'Public'},
                            'peer': None,
                            'prefixes': []  # Empty prefixes
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_null_prefixes(self):
        """Test extracting IP records when prefixes is null."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {'shortName': 'Test', 'fullName': 'Test', 'types': [], 'visibility': 'Public'},
                            'peer': None,
                            'prefixes': None
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_null_customer(self):
        """Test extracting IP records when customer is null."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': None,
                            'peer': {'asn': 64496},
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]['org_short_name'])
        self.assertIsNone(result[0]['org_full_name'])
        self.assertEqual(result[0]['org_types'], [])
        self.assertFalse(result[0]['org_hide'])

    def test_extract_ip_records_null_peer(self):
        """Test extracting IP records when peer is null."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {'shortName': 'Test', 'fullName': 'Test', 'types': [], 'visibility': 'Public'},
                            'peer': None,
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]['as_id'])

    def test_extract_ip_records_null_service_type(self):
        """Test extracting IP records when serviceType is null."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': None,
                            'customer': {'shortName': 'Test', 'fullName': 'Test', 'types': [], 'visibility': 'Public'},
                            'peer': None,
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['service_type'], [])

    def test_extract_ip_records_null_funding_agency(self):
        """Test extracting IP records when fundingAgency is null."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'Test',
                                'fullName': 'Test',
                                'types': [],
                                'fundingAgency': None,
                                'visibility': 'Public'
                            },
                            'peer': None,
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]['org_funding_agency'])

    def test_extract_ip_records_string_funding_agency(self):
        """Test extracting IP records when fundingAgency is a string."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {
                                'shortName': 'Test',
                                'fullName': 'Test',
                                'types': [],
                                'fundingAgency': 'TESTFUND',  # String instead of object
                                'visibility': 'Public'
                            },
                            'peer': None,
                            'prefixes': [{'prefixIp': '192.0.2.0/24'}]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['org_funding_agency'], 'TESTFUND')

    def test_extract_ip_records_empty_prefix_ip(self):
        """Test that empty prefixIp values are skipped."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'Test-v4',
                            'serviceType': {'shortName': 'IPv4'},
                            'customer': {'shortName': 'Test', 'fullName': 'Test', 'types': [], 'visibility': 'Public'},
                            'peer': None,
                            'prefixes': [
                                {'prefixIp': ''},
                                {'prefixIp': '192.0.2.0/24'},
                                {'prefixIp': None}
                            ]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        # Should only have 1 record (the valid one)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], '192.0.2.0/24')

    def test_extract_ip_records_ipv6_prefix(self):
        """Test extraction with IPv6 prefixes."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary-v6',
                            'prefixGroupName': 'TESTLAB-Primary-v6',
                            'serviceType': {'shortName': 'IPv6'},
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [{'name': 'Research Site'}],
                                'fundingAgency': {'shortName': 'TESTFUND'},
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [
                                {'prefixIp': '2001:db8::/32'},      # RFC 3849 documentation
                                {'prefixIp': '2001:db8:1::/48'}     # RFC 3849 documentation
                            ]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 2)
        
        # Check IPv6 record
        record = next(r for r in result if r['id'] == '2001:db8::/32')
        self.assertEqual(record['ip_subnet'], ('2001:db8::', 32))
        self.assertEqual(record['service_type'], ['IPv6'])

    def test_extract_ip_records_mixed_ipv4_ipv6(self):
        """Test extraction with mixed IPv4 and IPv6 prefixes."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        ipservice_data = {
            'data': {
                'serviceList': {
                    'list': [
                        {
                            'id': '123',
                            'label': 'Primary',
                            'prefixGroupName': 'TESTLAB-Primary',
                            'serviceType': {'shortName': 'Dual-Stack'},
                            'customer': {
                                'shortName': 'TESTLAB',
                                'fullName': 'Test Laboratory',
                                'types': [],
                                'visibility': 'Public'
                            },
                            'peer': {'asn': 64496},
                            'prefixes': [
                                {'prefixIp': '192.0.2.0/24'},   # RFC 5737 TEST-NET-1
                                {'prefixIp': '2001:db8::/32'}   # RFC 3849 documentation
                            ]
                        }
                    ]
                }
            }
        }
        
        result = processor._extract_ip_records(ipservice_data)
        
        self.assertEqual(len(result), 2)
        
        ids = [r['id'] for r in result]
        self.assertIn('192.0.2.0/24', ids)
        self.assertIn('2001:db8::/32', ids)

    # ==================== build_metadata_fields tests ====================

    def test_build_metadata_fields_basic(self):
        """Test building metadata fields with basic data."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': ('192.0.2.0', 24),
            'service_prefix_group_name': ['Test-v4'],
            'service_label': ['Primary'],
            'service_type': ['IPv4'],
            'org_short_name': 'TestOrg',
            'org_full_name': 'Test Organization',
            'org_types': ['Research Site'],
            'org_funding_agency': 'TESTFUND',
            'org_hide': False,
            'as_id': 64496
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['ip_subnet'], ('192.0.2.0', 24))
        self.assertEqual(result['service_prefix_group_name'], ['Test-v4'])
        self.assertEqual(result['service_label'], ['Primary'])
        self.assertEqual(result['service_type'], ['IPv4'])
        self.assertEqual(result['org_short_name'], 'TestOrg')
        self.assertEqual(result['org_full_name'], 'Test Organization')
        self.assertEqual(result['org_types'], ['Research Site'])
        self.assertEqual(result['org_funding_agency'], 'TESTFUND')
        self.assertFalse(result['org_hide'])
        self.assertEqual(result['as_id'], 64496)

    def test_build_metadata_fields_ip_subnet_validation(self):
        """Test IP subnet validation in metadata fields."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': ['192.0.2.0', 24],  # List instead of tuple
            'service_prefix_group_name': ['Test'],
            'service_label': [],
            'service_type': [],
            'org_types': [],
            'org_hide': False
        }
        
        result = processor.build_metadata_fields(value)
        
        # Should convert to tuple
        self.assertEqual(result['ip_subnet'], ('192.0.2.0', 24))

    def test_build_metadata_fields_ensures_arrays(self):
        """Test that array fields are converted to lists."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': ('192.0.2.0', 24),
            'service_prefix_group_name': None,
            'service_label': 'not_a_list',
            'service_type': None,
            'org_types': None,
            'org_hide': False
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['service_prefix_group_name'], [])
        self.assertEqual(result['service_label'], [])
        self.assertEqual(result['service_type'], [])
        self.assertEqual(result['org_types'], [])

    def test_build_metadata_fields_ensures_bool(self):
        """Test that org_hide is converted to bool."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
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
                    'ip_subnet': ('192.0.2.0', 24),
                    'service_prefix_group_name': [],
                    'service_label': [],
                    'service_type': [],
                    'org_types': [],
                    'org_hide': input_val
                }
                result = processor.build_metadata_fields(value)
                self.assertEqual(result['org_hide'], expected)

    # ==================== build_message tests ====================

    def test_build_message_from_graphql_consumer(self):
        """Test building message from GraphQLConsumer data format."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceList': {
                        'list': [
                            {
                                'id': '123',
                                'label': 'Primary',
                                'prefixGroupName': 'TESTLAB-Primary-v4',
                                'serviceType': {'shortName': 'IPv4'},
                                'customer': {
                                    'shortName': 'TESTLAB',
                                    'fullName': 'Test Laboratory',
                                    'types': [{'name': 'Research Site'}],
                                    'fundingAgency': {'shortName': 'TESTFUND'},
                                    'visibility': 'Public'
                                },
                                'peer': {'asn': 64496},
                                'prefixes': [
                                    {'prefixIp': '192.0.2.0/24'},    # TEST-NET-1
                                    {'prefixIp': '198.51.100.0/24'}  # TEST-NET-2
                                ]
                            }
                        ]
                    }
                }
            }
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(len(result), 2)
        
        ids = [r['id'] for r in result]
        self.assertIn('192.0.2.0/24', ids)
        self.assertIn('198.51.100.0/24', ids)
        
        # Check one record in detail
        record = next(r for r in result if r['id'] == '192.0.2.0/24')
        self.assertEqual(record['org_short_name'], 'TESTLAB')
        self.assertEqual(record['service_prefix_group_name'], ['TESTLAB-Primary-v4'])
        self.assertIn('ref', record)
        self.assertIn('hash', record)

    def test_build_message_empty_data(self):
        """Test build_message with empty data."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {}
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(result, [])

    def test_build_message_no_data_field(self):
        """Test build_message when data field is missing."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(result, [])

    def test_build_message_multiple_services(self):
        """Test building message with multiple services."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceList': {
                        'list': [
                            {
                                'id': '123',
                                'label': 'Primary',
                                'prefixGroupName': 'SITE1-Primary-v4',
                                'serviceType': {'shortName': 'IPv4'},
                                'customer': {'shortName': 'SITE1', 'fullName': 'Site 1', 'types': [], 'visibility': 'Public'},
                                'peer': {'asn': 64496},
                                'prefixes': [{'prefixIp': '192.0.2.0/24'}]  # TEST-NET-1
                            },
                            {
                                'id': '456',
                                'label': 'Primary',
                                'prefixGroupName': 'SITE2-Primary-v4',
                                'serviceType': {'shortName': 'IPv4'},
                                'customer': {'shortName': 'SITE2', 'fullName': 'Site 2', 'types': [], 'visibility': 'Public'},
                                'peer': {'asn': 64497},
                                'prefixes': [{'prefixIp': '198.51.100.0/24'}]  # TEST-NET-2
                            }
                        ]
                    }
                }
            }
        }
        
        result = processor.build_message(graphql_msg, {})
        
        self.assertEqual(len(result), 2)
        ids = [r['id'] for r in result]
        self.assertIn('192.0.2.0/24', ids)
        self.assertIn('198.51.100.0/24', ids)

    def test_build_message_skips_unchanged_records(self):
        """Test that unchanged records are skipped when cached."""
        processor = MetaIPServiceProcessor(self.mock_pipeline)
        
        graphql_msg = {
            'url': 'https://esdb.example.com/graphql',
            'status_code': 200,
            'data': {
                'data': {
                    'serviceList': {
                        'list': [
                            {
                                'id': '123',
                                'label': 'Primary',
                                'prefixGroupName': 'Test-v4',
                                'serviceType': {'shortName': 'IPv4'},
                                'customer': {'shortName': 'Test', 'fullName': 'Test', 'types': [], 'visibility': 'Public'},
                                'peer': {'asn': 64496},
                                'prefixes': [{'prefixIp': '192.0.2.0/24'}]
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
            'id': '192.0.2.0/24',
            'hash': result1[0]['hash'],
            'ref': result1[0]['ref']
        }
        result2 = processor.build_message(graphql_msg, {})
        self.assertEqual(len(result2), 0)  # Should skip unchanged record


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
        
        self.assertEqual(processor.table, 'meta_interface_esdb')
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
            'connection_name', 'service_edge_id', 'org_short_name',
            'org_full_name', 'org_hide', 'org_tag',
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
        self.assertEqual(column_types['service_edge_id'], 'Nullable(String)')
        self.assertEqual(column_types['org_short_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['org_full_name'], 'Nullable(String)')
        self.assertEqual(column_types['org_hide'], 'Bool')
        self.assertEqual(column_types['org_tag'], 'Array(String)')
        self.assertEqual(column_types['org_type'], 'Array(String)')
        self.assertEqual(column_types['org_funding_agency'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['peer_as_id'], 'Nullable(UInt32)')
        self.assertEqual(column_types['peer_ipv4'], 'Nullable(String)')
        self.assertEqual(column_types['peer_ipv6'], 'Nullable(String)')

    def test_create_table_command(self):
        """Test CREATE TABLE command generation."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor.create_table_command()
        
        self.assertIn("CREATE TABLE IF NOT EXISTS meta_interface_esdb", result)
        self.assertIn("ENGINE = MergeTree", result)
        self.assertIn("ORDER BY", result)

    # ==================== match_message tests ====================

    def test_match_message_with_table_field(self):
        """Test match_message with standard table field (backward compatibility)."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        msg = {'table': 'meta_interface_esdb', 'data': []}
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
            service_edge_id='100',
            org_info=org_info,
            peer_info=peer_info
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'router-test1::se_test_100')
        self.assertEqual(result['connection_name'], 'test-connection-1')
        self.assertEqual(result['service_edge_id'], '100')
        self.assertEqual(result['org_short_name'], 'TESTORG')
        self.assertEqual(result['org_full_name'], 'Test Organization')
        self.assertEqual(result['org_type'], ['Research Site'])
        self.assertEqual(result['org_funding_agency'], 'TESTFUND')
        self.assertFalse(result['org_hide'])
        self.assertEqual(result['org_tag'], ['primary'])
        self.assertEqual(result['peer_as_id'], 64496)
        self.assertEqual(result['peer_ipv4'], '192.0.2.1')
        self.assertEqual(result['peer_ipv6'], '2001:db8::1')

    def test_build_service_edge_record_no_org_info(self):
        """Test building service edge record without organization info."""
        processor = MetaServiceEdgeProcessor(self.mock_pipeline)
        
        result = processor._build_service_edge_record(
            interface_id='router-test1::se_test_100',
            connection_name=None,
            service_edge_id='100',
            org_info=None,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertIsNone(result['org_short_name'])
        self.assertIsNone(result['org_full_name'])
        self.assertEqual(result['org_type'], [])
        self.assertEqual(result['org_tag'], [])
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
            service_edge_id='100',
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
            service_edge_id='100',
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
            service_edge_id='100',
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
            service_edge_id='100',
            org_info=org_info,
            peer_info=None
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result['org_tag']), 3)

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
            service_edge_id='100',
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
            service_edge_id='100',
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
        self.assertEqual(record['service_edge_id'], '100')
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
            'service_edge_id': '100',
            'org_short_name': 'TESTORG',
            'org_full_name': 'Test Organization',
            'org_hide': False,
            'org_tag': ['primary'],
            'org_type': ['Research Site'],
            'org_funding_agency': 'TESTFUND',
            'peer_as_id': 64496,
            'peer_ipv4': '192.0.2.1',
            'peer_ipv6': '2001:db8::1'
        }
        
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['connection_name'], 'test-connection-1')
        self.assertEqual(result['service_edge_id'], '100')
        self.assertEqual(result['org_short_name'], 'TESTORG')
        self.assertEqual(result['org_full_name'], 'Test Organization')
        self.assertFalse(result['org_hide'])
        self.assertEqual(result['org_tag'], ['primary'])
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