#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, mock_open, call
import sys

# Add the project root to Python path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.consumers.metadata import CRICIPConsumer
from metranova.processors.clickhouse.ip_cric import MetaIPCRICProcessor


class TestCRICIPConsumer(unittest.TestCase):
    """Test suite for CRICIPConsumer"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'CRIC_IP_CONSUMER_TABLE': 'meta_ip_cric',
            'CRIC_IP_CONSUMER_UPDATE_INTERVAL': '-1',
            'CRIC_IP_CONSUMER_URL': 'https://test-cric-api.cern.ch/api/core/rcsite/query/list/?json'
        })
        self.env_patcher.start()
        
    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_init_default_values(self):
        """Test that the consumer initializes with correct default values."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        self.assertEqual(consumer.table, 'meta_ip_cric')
        self.assertEqual(consumer.update_interval, -1)
        self.assertEqual(consumer.cric_url, 'https://test-cric-api.cern.ch/api/core/rcsite/query/list/?json')
        self.assertIsNone(consumer.custom_ip_file)

    def test_parse_ip_subnet_valid(self):
        """Test IP subnet parsing with valid input."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        test_cases = [
            ('192.168.1.0/24', ('192.168.1.0', 24)),
            ('10.0.0.0/8', ('10.0.0.0', 8)),
            ('2001:db8::/32', ('2001:db8::', 32)),
            ('172.16.0.0/12', ('172.16.0.0', 12))
        ]
        
        for ip_str, expected in test_cases:
            with self.subTest(ip_str=ip_str):
                result = consumer._parse_ip_subnet(ip_str)
                self.assertEqual(result, expected)

    def test_parse_ip_subnet_invalid(self):
        """Test IP subnet parsing with invalid input."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        invalid_inputs = [
            '',
            None,
            'invalid',
            '192.168.1.0',  # Missing prefix
            '192.168.1.0/invalid',  # Non-numeric prefix
            '192.168.1.0/24/extra'  # Too many parts
        ]
        
        for invalid_input in invalid_inputs:
            with self.subTest(invalid_input=invalid_input):
                result = consumer._parse_ip_subnet(invalid_input)
                self.assertIsNone(result)

    def test_fetch_cric_data_success(self):
        """Test successful CRIC data fetch."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        # Mock successful API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'SITE1': {'country': 'CH', 'latitude': 46.2, 'longitude': 6.1},
            'SITE2': {'country': 'US', 'latitude': 34.0, 'longitude': -118.2}
        }
        
        # Mock at the datasource client level
        with patch.object(consumer.datasource.client, 'get', return_value=mock_response):
            result = consumer._fetch_cric_data()
            
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 2)
            self.assertIn('SITE1', result)
            self.assertIn('SITE2', result)

    def test_fetch_cric_data_failure(self):
        """Test CRIC data fetch failure."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        # Mock failed API response
        with patch.object(consumer.datasource.client, 'get', side_effect=Exception("Connection error")):
            result = consumer._fetch_cric_data()
            
            self.assertIsNone(result)

    def test_load_custom_ip_data_file_not_found(self):
        """Test loading custom IP data when file doesn't exist."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        consumer.custom_ip_file = '/nonexistent/file.yml'
        
        result = consumer._load_custom_ip_data()
        
        self.assertEqual(result, {})

    @patch('builtins.open', new_callable=mock_open, read_data='data:\n  - id: "10.0.0.0/8"\n    name: "Custom Site"')
    @patch('metranova.consumers.metadata.yaml.safe_load')
    def test_load_custom_ip_data_success(self, mock_yaml, mock_file):
        """Test successful loading of custom IP data."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        consumer.custom_ip_file = '/path/to/custom.yml'
        
        mock_yaml.return_value = {
            'data': [
                {'id': '10.0.0.0/8', 'name': 'Custom Site', 'tier': 1}
            ]
        }
        
        result = consumer._load_custom_ip_data()
        
        self.assertEqual(len(result), 1)
        self.assertIn('10.0.0.0/8', result)
        self.assertEqual(result['10.0.0.0/8']['name'], 'Custom Site')

    def test_build_ip_record_basic(self):
        """Test building IP record with basic data."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        site_info = {
            'name': 'CERN',
            'tier': 0,
            'country': 'Switzerland',
            'latitude': 46.2044,
            'longitude': 6.1432
        }
        net_site = 'CERN-PRIMARY'
        ip_range = '192.168.1.0/24'
        custom_ip_data = {}
        
        result = consumer._build_ip_record(ip_range, site_info, net_site, custom_ip_data)
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], '192.168.1.0/24')
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24)])
        self.assertEqual(result['name'], 'CERN')
        self.assertEqual(result['latitude'], 46.2044)
        self.assertEqual(result['longitude'], 6.1432)
        self.assertEqual(result['country'], 'Switzerland')
        self.assertEqual(result['net_site'], 'CERN-PRIMARY')
        self.assertEqual(result['tier'], 0)

    def test_build_ip_record_with_custom_data(self):
        """Test building IP record with custom data override."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        site_info = {
            'name': 'CERN',
            'tier': 0,
            'country': 'Switzerland',
            'latitude': 46.2044,
            'longitude': 6.1432
        }
        net_site = 'CERN-PRIMARY'
        ip_range = '192.168.1.0/24'
        custom_ip_data = {
            '192.168.1.0/24': {
                'latitude': 46.5,  # Override
                'tag': ['production', 'critical']
            }
        }
        
        result = consumer._build_ip_record(ip_range, site_info, net_site, custom_ip_data)
        
        self.assertEqual(result['latitude'], 46.5)  # Should be overridden
        self.assertEqual(result['tag'], ['production', 'critical'])
        self.assertNotIn('192.168.1.0/24', custom_ip_data)  # Should be removed

    def test_build_ip_record_invalid_subnet(self):
        """Test building IP record with invalid subnet."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        site_info = {'name': 'CERN', 'tier': 0, 'country': 'CH', 'latitude': 46.2, 'longitude': 6.1}
        net_site = 'CERN-PRIMARY'
        ip_range = 'invalid-subnet'
        custom_ip_data = {}
        
        result = consumer._build_ip_record(ip_range, site_info, net_site, custom_ip_data)
        
        self.assertIsNone(result)

    def test_extract_ip_records_complete(self):
        """Test extracting IP records from complete CRIC data."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        cric_data = {
            'CERN': {
                'country': 'Switzerland',
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
        custom_ip_data = {}
        
        result = consumer._extract_ip_records(cric_data, custom_ip_data)
        
        self.assertEqual(len(result), 3)  # 2 IPv4 + 1 IPv6
        
        # Check first record
        self.assertEqual(result[0]['id'], '192.168.1.0/24')
        self.assertEqual(result[0]['name'], 'CERN')
        self.assertEqual(result[0]['tier'], 0)

    def test_extract_ip_records_no_netroutes(self):
        """Test extracting IP records when site has no netroutes."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        cric_data = {
            'SITE_NO_ROUTES': {
                'country': 'US',
                'latitude': 34.0,
                'longitude': -118.2,
                'rc_tier_level': 2
                # No netroutes
            }
        }
        custom_ip_data = {}
        
        result = consumer._extract_ip_records(cric_data, custom_ip_data)
        
        self.assertEqual(len(result), 0)

    def test_extract_ip_records_with_custom_additions(self):
        """Test that custom records are added to extraction results."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        # Provide minimal valid CRIC data so custom additions are processed
        cric_data = {
            'DUMMY_SITE': {
                'country': 'US',
                'latitude': 0.0,
                'longitude': 0.0,
                'rc_tier_level': 3,
                'netroutes': {}  # Empty netroutes - no IP records from CRIC
            }
        }
        custom_ip_data = {
            '172.16.0.0/12': {
                'id': '172.16.0.0/12',
                'ip_subnet': [('172.16.0.0', 12)],
                'name': 'Custom Network',
                'tier': 3
            }
        }
        
        result = consumer._extract_ip_records(cric_data, custom_ip_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['id'], '172.16.0.0/12')
        self.assertEqual(result[0]['name'], 'Custom Network')

    @patch.object(CRICIPConsumer, '_fetch_cric_data')
    @patch.object(CRICIPConsumer, '_load_custom_ip_data')
    @patch.object(CRICIPConsumer, '_extract_ip_records')
    def test_consume_messages_success(self, mock_extract, mock_load_custom, mock_fetch):
        """Test successful message consumption."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        # Mock the methods
        mock_fetch.return_value = {'SITE1': {}}
        mock_load_custom.return_value = {}
        mock_extract.return_value = [
            {'id': '192.168.1.0/24', 'name': 'SITE1'}
        ]
        
        consumer.consume_messages()
        
        # Verify pipeline was called with correct data
        self.mock_pipeline.process_message.assert_called_once()
        call_args = self.mock_pipeline.process_message.call_args[0][0]
        self.assertEqual(call_args['table'], 'meta_ip_cric')
        self.assertEqual(len(call_args['data']), 1)

    @patch.object(CRICIPConsumer, '_fetch_cric_data')
    def test_consume_messages_fetch_failure(self, mock_fetch):
        """Test consume_messages when fetch fails."""
        consumer = CRICIPConsumer(self.mock_pipeline)
        
        mock_fetch.return_value = None
        
        consumer.consume_messages()
        
        # Should not call pipeline
        self.mock_pipeline.process_message.assert_not_called()


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
        # Note: 'tag' is in array_fields from base class

    def test_init_custom_table_name(self):
        """Test initialization with custom table name."""
        with patch.dict(os.environ, {'CRIC_IP_METADATA_TABLE': 'custom_cric_table'}):
            processor = MetaIPCRICProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_cric_table')

    def test_column_definitions(self):
        """Test that all required columns are defined."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        column_names = [col[0] for col in processor.column_defs]
        
        # Use actual column names from your implementation
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',
            'ip_subnet', 'name', 'latitude', 'longitude',
            'country', 'net_site', 'tier'  # Actual names, not renamed
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
        self.assertEqual(column_types['country'], 'LowCardinality(Nullable(String))')
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
        # Just check that key fields are present, don't check exact format
        self.assertIn("name", result)
        self.assertIn("id", result)

    def test_build_metadata_fields_basic(self):
        """Test building metadata fields with basic data."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {
            'ip_subnet': [('192.168.1.0', 24)],
            'name': 'CERN',
            'latitude': 46.2044,
            'longitude': 6.1432,
            'country': 'Switzerland',
            'net_site': 'CERN-PRIMARY',
            'tier': 0
        }
        
        result = processor.build_metadata_fields(value)
        
        # Fields should pass through unchanged (no renaming in your implementation)
        self.assertEqual(result['ip_subnet'], [('192.168.1.0', 24)])
        self.assertEqual(result['name'], 'CERN')
        self.assertEqual(result['latitude'], 46.2044)
        self.assertEqual(result['longitude'], 6.1432)
        self.assertEqual(result['country'], 'Switzerland')
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

    def test_build_metadata_fields_tier_validation(self):
        """Test tier value validation."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        # Test valid tier
        value1 = {'tier': 2, 'name': 'Site1', 'ip_subnet': [('192.168.1.0', 24)]}
        result1 = processor.build_metadata_fields(value1)
        self.assertEqual(result1['tier'], 2)

    def test_build_metadata_fields_name_default(self):
        """Test that name has default value if missing."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        value = {'ip_subnet': [('192.168.1.0', 24)]}
        result = processor.build_metadata_fields(value)
        
        self.assertEqual(result['name'], 'Unknown')

    def test_build_message_valid_record(self):
        """Test building message with valid record."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': '192.168.1.0/24',
                'ip_subnet': [('192.168.1.0', 24)],
                'name': 'CERN',
                'latitude': 46.2044,
                'longitude': 6.1432,
                'country': 'Switzerland',
                'net_site': 'CERN-PRIMARY',
                'tier': 0
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record['id'], '192.168.1.0/24')
        self.assertEqual(record['country'], 'Switzerland')
        self.assertEqual(record['net_site'], 'CERN-PRIMARY')
        self.assertIn('ref', record)
        self.assertIn('hash', record)

    def test_build_message_missing_required_fields(self):
        """Test that missing required fields are handled."""
        processor = MetaIPCRICProcessor(self.mock_pipeline)
        
        # Missing ip_subnet
        input_data1 = {'data': [{'id': 'test'}]}
        result1 = processor.build_message(input_data1, {})
        self.assertEqual(result1, [])
        
        # Missing id
        input_data2 = {'data': [{'ip_subnet': [('192.168.1.0', 24)]}]}
        result2 = processor.build_message(input_data2, {})
        self.assertEqual(result2, [])


if __name__ == '__main__':
    unittest.main()