#!/usr/bin/env python3

import unittest
import pytricia
from unittest.mock import patch, MagicMock
from typing import Any, Dict, Iterator

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.file.base import BaseFileProcessor, IPTriePickleFileProcessor


class TestBaseFileProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = BaseFileProcessor(self.mock_pipeline)
        
        # Test that logger is set
        self.assertEqual(processor.logger, processor.logger)
        
        # Test that required_fields is correctly set
        expected_required_fields = [
            ['table'],
            ['rows']
        ]
        self.assertEqual(processor.required_fields, expected_required_fields)
        
        # Test that it inherits from BaseProcessor
        from metranova.processors.base import BaseProcessor
        self.assertIsInstance(processor, BaseProcessor)

    def test_inheritance_from_base_processor(self):
        """Test that BaseFileProcessor properly inherits from BaseProcessor."""
        processor = BaseFileProcessor(self.mock_pipeline)
        
        # Check that it has inherited methods from BaseProcessor
        self.assertTrue(hasattr(processor, 'has_required_fields'))
        self.assertTrue(hasattr(processor, 'match_message'))
        
        # Check that pipeline is set
        self.assertEqual(processor.pipeline, self.mock_pipeline)


class TestIPTriePickleFileProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        # Test that it inherits from BaseFileProcessor
        self.assertIsInstance(processor, BaseFileProcessor)
        
        # Test that logger is set
        self.assertEqual(processor.logger, processor.logger)
        
        # Test that required_fields is correctly inherited
        expected_required_fields = [
            ['table'],
            ['rows'],
            ['column_names']
        ]
        self.assertEqual(processor.required_fields, expected_required_fields)

    def test_build_message_basic_functionality(self):
        """Test build_message with basic valid input."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24], ['10.0.0.0', 8]]],
                ['ip-block-2', 'ref-2', [['172.16.0.0', 12]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        
        # Should return a list with one dictionary
        self.assertEqual(len(result), 1)
        
        output = result[0]
        self.assertIn('name', output)
        self.assertIn('data', output)
        
        # Check file name
        self.assertEqual(output['name'], 'ip_trie_meta_ip.pickle')
        
        # Check that data is a PyTricia object
        self.assertIsInstance(output['data'], pytricia.PyTricia)
        
        # Verify trie contents
        trie = output['data']
        self.assertEqual(trie['192.168.1.0/24'], ('ref-1',))
        self.assertEqual(trie['10.0.0.0/8'], ('ref-1',))
        self.assertEqual(trie['172.16.0.0/12'], ('ref-2',))

    def test_build_message_ipv6_subnets(self):
        """Test build_message with IPv6 subnets."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ipv6',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['ipv6-block-1', 'ref-ipv6-1', [['2001:db8::', 32], ['fe80::', 10]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        
        self.assertEqual(len(result), 1)
        output = result[0]
        
        # Check file name
        self.assertEqual(output['name'], 'ip_trie_meta_ipv6.pickle')
        
        # Verify IPv6 entries in trie
        trie = output['data']
        self.assertEqual(trie['2001:db8::/32'], ('ref-ipv6-1',))
        self.assertEqual(trie['fe80::/10'], ('ref-ipv6-1',))

    def test_build_message_mixed_ip_versions(self):
        """Test build_message with mixed IPv4 and IPv6 subnets."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_mixed_ip',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['mixed-block-1', 'ref-mixed-1', [['192.168.1.0', 24], ['2001:db8::', 32]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        
        self.assertEqual(len(result), 1)
        output = result[0]
        
        # Verify both IPv4 and IPv6 entries
        trie = output['data']
        self.assertEqual(trie['192.168.1.0/24'], ('ref-mixed-1',))
        self.assertEqual(trie['2001:db8::/32'], ('ref-mixed-1',))

    def test_build_message_empty_rows(self):
        """Test build_message with empty rows list."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_empty',
            'column_names': [],
            'rows': []
        }
        
        result = list(processor.build_message(input_data, {}))
        
        self.assertEqual(len(result), 1)
        output = result[0]
        
        # Should still create trie with correct name
        self.assertEqual(output['name'], 'ip_trie_meta_empty.pickle')
        self.assertIsInstance(output['data'], pytricia.PyTricia)
        
        # Trie should be empty
        trie = output['data']
        # PyTricia doesn't have a direct length method, but we can check if it's empty
        # by trying to access a key that shouldn't exist
        with self.assertRaises(KeyError):
            _ = trie['192.168.1.0/24']

    def test_build_message_null_values_in_rows(self):
        """Test build_message handles null values in rows gracefully."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_with_nulls',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24]]],  # Valid row
                [None, 'ref-2', [['10.0.0.0', 8]]],  # Null id
                ['ip-block-3', None, [['172.16.0.0', 12]]],  # Null ref
                ['ip-block-4', 'ref-4', None],  # Null ip_subnet
                ['ip-block-5', 'ref-5', [['203.0.113.0', 24]]]  # Valid row
            ]
        }
        
        with patch.object(processor.logger, 'debug') as mock_debug:
            result = list(processor.build_message(input_data, {}))
            
            # Should have logged debug messages for null values
            self.assertEqual(mock_debug.call_count, 3)  # 3 rows with null values
        
        self.assertEqual(len(result), 1)
        output = result[0]
        
        # Verify only valid rows were added to trie
        trie = output['data']
        self.assertEqual(trie['192.168.1.0/24'], ('ref-1',))
        self.assertEqual(trie['203.0.113.0/24'], ('ref-5',))

        # Verify null value rows were skipped
        with self.assertRaises(KeyError):
            _ = trie['10.0.0.0/8']  # Should not exist due to null id
        with self.assertRaises(KeyError):
            _ = trie['172.16.0.0/12']  # Should not exist due to null ref

    def test_build_message_invalid_ip_subnet_type(self):
        """Test build_message handles invalid ip_subnet types."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_invalid',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24]]],  # Valid
                ['ip-block-2', 'ref-2', 'not-a-list'],  # Invalid - string
                ['ip-block-3', 'ref-3', 12345],  # Invalid - integer
                ['ip-block-4', 'ref-4', [['10.0.0.0', 8]]]  # Valid
            ]
        }
        
        with patch.object(processor.logger, 'warning') as mock_warning:
            result = list(processor.build_message(input_data, {}))
            
            # Should have logged warnings for invalid types
            self.assertEqual(mock_warning.call_count, 2)  # 2 invalid ip_subnet types
        
        self.assertEqual(len(result), 1)
        output = result[0]
        
        # Verify only valid rows were added to trie
        trie = output['data']
        self.assertEqual(trie['192.168.1.0/24'], ('ref-1',))
        self.assertEqual(trie['10.0.0.0/8'], ('ref-4',))

    def test_build_message_missing_rows_key(self):
        """Test build_message handles missing 'rows' key gracefully."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_no_rows'
            # Missing 'rows' key
        }
        
        result = list(processor.build_message(input_data, {}))
        
        self.assertEqual(len(result), 0)

    def test_build_message_missing_column_names_key(self):
        """Test build_message handles missing 'column_names' key gracefully."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_no_column_names',
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24]]],
                ['ip-block-2', 'ref-2', [['10.0.0.0', 8]]]
            ]
        }

        result = list(processor.build_message(input_data, {}))

        self.assertEqual(len(result), 0)


    def test_build_message_comprehensive_scenario(self):
        """Test build_message with a comprehensive real-world scenario."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_comprehensive',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                # Valid IPv4 entries
                ['ucla-ipv4-1', 'ucla-ref-1', [['164.67.0.0', 16], ['128.97.0.0', 16]]],
                ['private-rfc1918-1', 'private-ref-1', [['192.168.0.0', 16], ['10.0.0.0', 8]]],
                
                # Valid IPv6 entries
                ['ucla-ipv6-1', 'ucla-ref-2', [['2607:f010::', 32]]],
                ['link-local', 'link-local-ref', [['fe80::', 10]]],
                
                # Mixed entry
                ['mixed-blocks', 'mixed-ref', [['172.16.0.0', 12], ['2001:db8::', 32]]],
                
                # Entry with null values (should be skipped)
                [None, 'null-id-ref', [['203.0.113.0', 24]]],
                
                # Entry with invalid ip_subnet type (should be skipped)
                ['invalid-subnet', 'invalid-ref', 'not-a-list']
            ]
        }
        
        with patch.object(processor.logger, 'debug') as mock_debug, \
             patch.object(processor.logger, 'warning') as mock_warning:
            
            result = list(processor.build_message(input_data, {}))
            
            # Should log 1 debug for null value and 1 warning for invalid type
            mock_debug.assert_called_once()
            mock_warning.assert_called_once()
        
        self.assertEqual(len(result), 1)
        output = result[0]
        
        # Check file name
        self.assertEqual(output['name'], 'ip_trie_meta_ip_comprehensive.pickle')
        
        # Verify all valid entries are in trie
        trie = output['data']
        
        # UCLA IPv4 blocks
        self.assertEqual(trie['164.67.0.0/16'], ('ucla-ref-1',))
        self.assertEqual(trie['128.97.0.0/16'], ('ucla-ref-1',))

        # Private IPv4 blocks
        self.assertEqual(trie['192.168.0.0/16'], ('private-ref-1',))
        self.assertEqual(trie['10.0.0.0/8'], ('private-ref-1',))
        
        # IPv6 blocks
        self.assertEqual(trie['2607:f010::/32'], ('ucla-ref-2',))
        self.assertEqual(trie['fe80::/10'], ('link-local-ref',))

        # Mixed blocks
        self.assertEqual(trie['172.16.0.0/12'], ('mixed-ref',))
        self.assertEqual(trie['2001:db8::/32'], ('mixed-ref',))

        # Verify invalid entries were not added
        with self.assertRaises(KeyError):
            _ = trie['203.0.113.0/24']  # Should not exist due to null id

    def test_build_message_trie_is_frozen(self):
        """Test that the returned trie is frozen (read-only)."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_frozen_test',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['test-block', 'test-ref', [['192.168.1.0', 24]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        output = result[0]
        trie = output['data']
        
        # Verify trie is frozen by attempting to modify it
        # PyTricia raises an exception when trying to modify a frozen trie
        with self.assertRaises(Exception):  # PyTricia raises a generic Exception
            trie['192.168.2.0/24'] = 'new-ref'

    def test_build_message_returns_iterator(self):
        """Test that build_message returns an iterator."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_iterator_test',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['test-block', 'test-ref', [['192.168.1.0', 24]]]
            ]
        }
        
        result = processor.build_message(input_data, {})
        
        # The method returns a list wrapped in an iterator-like structure
        # Let's check that it's iterable and yields the expected result
        self.assertTrue(hasattr(result, '__iter__'))
        
        # Convert to list and verify content
        result_list = list(result)
        self.assertEqual(len(result_list), 1)
        self.assertIn('name', result_list[0])
        self.assertIn('data', result_list[0])

    def test_build_message_table_name_variations(self):
        """Test build_message with various table name formats."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        test_cases = [
            'meta_ip',
            'meta_ip_test',
            'ip_metadata_table',
            'custom_table_name_123'
        ]
        
        for table_name in test_cases:
            with self.subTest(table_name=table_name):
                input_data = {
                    'table': table_name,
                    'column_names': ['id', 'ref', 'ip_subnet'],
                    'rows': [
                        ['test-block', 'test-ref', [['192.168.1.0', 24]]]
                    ]
                }
                
                result = list(processor.build_message(input_data, {}))
                output = result[0]
                
                expected_filename = f'ip_trie_{table_name}.pickle'
                self.assertEqual(output['name'], expected_filename)

    def test_build_message_edge_case_subnets(self):
        """Test build_message with edge case subnet formats."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_edge_cases',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                # /32 IPv4 (single host)
                ['single-host-ipv4', 'host-ref-1', [['192.168.1.1', 32]]],
                
                # /24 IPv4 (common subnet)
                ['common-ipv4', 'common-ipv4-ref', [['10.0.1.0', 24]]],
                
                # /128 IPv6 (single host)
                ['single-host-ipv6', 'host-ref-2', [['2001:db8::1', 128]]],
                
                # /64 IPv6 (common subnet)
                ['common-ipv6', 'common-ipv6-ref', [['2001:db8::', 64]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        output = result[0]
        trie = output['data']
        
        # Verify edge case entries
        self.assertEqual(trie['192.168.1.1/32'], ('host-ref-1',))
        self.assertEqual(trie['10.0.1.0/24'], ('common-ipv4-ref',))
        self.assertEqual(trie['2001:db8::1/128'], ('host-ref-2',))
        self.assertEqual(trie['2001:db8::/64'], ('common-ipv6-ref',))

    def test_build_message_with_additional_columns(self):
        """Test build_message processes additional columns beyond id, ref, ip_subnet."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_with_extra_cols',
            'column_names': ['id', 'ref', 'ip_subnet', 'latest_as_id', 'latest_country', 'latest_org_name'],
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24]], 65001, 'US', 'Organization A'],
                ['ip-block-2', 'ref-2', [['10.0.0.0', 8]], 65002, 'CA', 'Organization B']
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        output = result[0]
        trie = output['data']
        
        # Verify additional fields are included in lookup values
        lookup_value_1 = trie['192.168.1.0/24']
        self.assertEqual(lookup_value_1[0], 'ref-1')
        self.assertEqual(lookup_value_1[1], 65001)
        self.assertEqual(lookup_value_1[2], 'US')
        self.assertEqual(lookup_value_1[3], 'Organization A')
        
        lookup_value_2 = trie['10.0.0.0/8']
        self.assertEqual(lookup_value_2[0], 'ref-2')
        self.assertEqual(lookup_value_2[1], 65002)
        self.assertEqual(lookup_value_2[2], 'CA')
        self.assertEqual(lookup_value_2[3], 'Organization B')

    def test_build_message_handles_ipv4_mapped_ipv6_addresses(self):
        """Test build_message converts IPv4-mapped IPv6 addresses to IPv4."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_mapped',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['mapped-ipv4', 'ref-mapped', [['::ffff:192.168.1.0', 24]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        output = result[0]
        trie = output['data']
        
        # Should be converted to plain IPv4
        self.assertEqual(trie['192.168.1.0/24'], ('ref-mapped',))

    def test_build_message_with_invalid_subnet_tuple_format(self):
        """Test build_message handles invalid subnet tuple formats."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        input_data = {
            'table': 'meta_ip_invalid_tuples',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['valid-block', 'ref-valid', [['192.168.1.0', 24]]],  # Valid
                ['empty-tuple', 'ref-empty', [[]]],  # Empty tuple
                ['single-element', 'ref-single', [['192.168.2.0']]],  # Missing prefix
                ['null-address', 'ref-null-addr', [[None, 24]]],  # Null address
                ['null-prefix', 'ref-null-prefix', [['192.168.3.0', None]]],  # Null prefix
                ['not-tuple', 'ref-not-tuple', ['not-a-tuple']],  # Not a tuple
                ['valid-block-2', 'ref-valid-2', [['10.0.0.0', 8]]]  # Valid
            ]
        }
        
        with patch.object(processor.logger, 'warning') as mock_warning, \
             patch.object(processor.logger, 'debug') as mock_debug:
            
            result = list(processor.build_message(input_data, {}))
            
            # Should log warnings for invalid formats
            self.assertGreater(mock_warning.call_count, 0)
            # Should log debug for null values
            self.assertGreater(mock_debug.call_count, 0)
        
        output = result[0]
        trie = output['data']
        
        # Only valid blocks should be in trie
        self.assertEqual(trie['192.168.1.0/24'], ('ref-valid',))
        self.assertEqual(trie['10.0.0.0/8'], ('ref-valid-2',))
        
        # Invalid entries should not be in trie
        with self.assertRaises(KeyError):
            _ = trie['192.168.2.0/24']
        with self.assertRaises(KeyError):
            _ = trie['192.168.3.0/24']

    def test_build_message_with_use_simple_table_name_false(self):
        """Test build_message filename generation with use_simple_table_name=False."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        processor.use_simple_table_name = False
        
        input_data = {
            'table': 'meta_ip:some_variant',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        output = result[0]
        
        # Should replace : with . when use_simple_table_name is False
        self.assertEqual(output['name'], 'ip_trie_meta_ip.some_variant.pickle')

    def test_build_message_with_use_simple_table_name_true(self):
        """Test build_message filename generation with use_simple_table_name=True."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        processor.use_simple_table_name = True
        
        input_data = {
            'table': 'meta_ip:some_variant',
            'column_names': ['id', 'ref', 'ip_subnet'],
            'rows': [
                ['ip-block-1', 'ref-1', [['192.168.1.0', 24]]]
            ]
        }
        
        result = list(processor.build_message(input_data, {}))
        output = result[0]
        
        # Should only use part before : when use_simple_table_name is True
        self.assertEqual(output['name'], 'ip_trie_meta_ip.pickle')

    @patch.dict(os.environ, {'IP_FILE_USE_SIMPLE_FILE_NAME': 'false'})
    def test_init_use_simple_table_name_from_env_false(self):
        """Test initialization reads use_simple_table_name from environment variable (false)."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.use_simple_table_name)

    @patch.dict(os.environ, {'IP_FILE_USE_SIMPLE_FILE_NAME': '0'})
    def test_init_use_simple_table_name_from_env_zero(self):
        """Test initialization reads use_simple_table_name from environment variable (0)."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.use_simple_table_name)

    @patch.dict(os.environ, {'IP_FILE_USE_SIMPLE_FILE_NAME': 'no'})
    def test_init_use_simple_table_name_from_env_no(self):
        """Test initialization reads use_simple_table_name from environment variable (no)."""
        processor = IPTriePickleFileProcessor(self.mock_pipeline)
        
        self.assertFalse(processor.use_simple_table_name)


if __name__ == '__main__':
    unittest.main()