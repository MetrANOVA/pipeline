#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock, Mock
from ipaddress import IPv6Address

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from metranova.cachers.clickhouse import ClickHouseCacher


class TestClickHouseCacher(unittest.TestCase):
    """Unit tests for ClickHouseCacher class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the ClickHouseConnector to avoid actual connections
        with patch('metranova.cachers.clickhouse.ClickHouseConnector') as mock_connector:
            mock_instance = Mock()
            mock_instance.client = None
            mock_connector.return_value = mock_instance
            
            with patch.dict(os.environ, {
                'CLICKHOUSE_CACHER_TABLES': 'meta_device,meta_interface',
                'CLICKHOUSE_CACHER_REFRESH_INTERVAL': '0',  # Disable refresh for tests
                'CLICKHOUSE_CACHER_MAX_SIZE': '1000',
                'CLICKHOUSE_CACHER_MAX_TTL': '60'
            }):
                self.cacher = ClickHouseCacher()

    def tearDown(self):
        """Clean up after each test."""
        if hasattr(self.cacher, 'close'):
            self.cacher.close()

    def test_init_with_environment_variables(self):
        """Test initialization with environment variables."""
        self.assertEqual(self.cacher.cache_max_size, 1000)
        self.assertEqual(self.cacher.cache_max_ttl, 60)
        self.assertEqual(self.cacher.cache_refresh_interval, 0)
        self.assertEqual(self.cacher.tables, ['meta_device', 'meta_interface'])

    @patch('metranova.cachers.clickhouse.ClickHouseConnector')
    def test_init_default_values(self, mock_connector):
        """Test initialization with default values when no env vars set."""
        mock_instance = Mock()
        mock_instance.client = None
        mock_connector.return_value = mock_instance
        
        with patch.dict(os.environ, {}, clear=True):
            cacher = ClickHouseCacher()
            
            self.assertEqual(cacher.cache_max_size, 100000000)
            self.assertEqual(cacher.cache_max_ttl, 86400)
            self.assertEqual(cacher.cache_refresh_interval, 600)
            self.assertEqual(cacher.tables, [])
            cacher.close()

    def test_inheritance_from_base_cacher(self):
        """Test that ClickHouseCacher inherits from BaseCacher."""
        from metranova.cachers.base import BaseCacher
        self.assertIsInstance(self.cacher, BaseCacher)

    def test_prime_calls_prime_table_for_each_table(self):
        """Test that prime calls prime_table for each configured table."""
        with patch.object(self.cacher, 'prime_table') as mock_prime_table:
            self.cacher.prime()
            
            self.assertEqual(mock_prime_table.call_count, 2)
            mock_prime_table.assert_any_call('meta_device')
            mock_prime_table.assert_any_call('meta_interface')

    def test_prime_table_no_client(self):
        """Test prime_table when client is not available."""
        self.cacher.cache.client = None
        
        # Should not raise an error
        self.cacher.prime_table('meta_device')
        
        # Local cache should not be populated
        self.assertNotIn('meta_device', self.cacher.local_cache)

    def test_prime_table_no_rows(self):
        """Test prime_table when query returns no rows."""
        mock_client = Mock()
        self.cacher.cache.client = mock_client
        
        with patch.object(self.cacher, 'query_table') as mock_query:
            mock_query.return_value = None
            
            self.cacher.prime_table('meta_device')
            
            # Should handle gracefully
            self.assertNotIn('meta_device', self.cacher.local_cache)

    def test_prime_table_with_data(self):
        """Test prime_table successfully loads data."""
        mock_client = Mock()
        self.cacher.cache.client = mock_client
        
        # Mock query results: (max_insert_time, id, ref, hash)
        mock_rows = [
            ('2024-01-01 00:00:00', 'device1', 'Device 1', 'hash1'),
            ('2024-01-01 00:00:00', 'device2', 'Device 2', 'hash2')
        ]
        
        with patch.object(self.cacher, 'query_table') as mock_query:
            mock_query.return_value = mock_rows
            
            self.cacher.prime_table('meta_device')
            
            # Should have populated local_cache
            self.assertIn('meta_device', self.cacher.local_cache)
            self.assertEqual(len(self.cacher.local_cache['meta_device']), 2)
            self.assertIn('device1', self.cacher.local_cache['meta_device'])
            self.assertEqual(self.cacher.local_cache['meta_device']['device1']['ref'], 'Device 1')

    def test_prime_table_with_additional_fields(self):
        """Test prime_table with additional index fields."""
        mock_client = Mock()
        self.cacher.cache.client = mock_client
        
        # Mock query results with additional fields: (max_insert_time, id, ref, hash, device_id, flow_index, edge)
        mock_rows = [
            ('2024-01-01 00:00:00', 'if1', 'Interface 1', 'hash1', 'device1', 10, True),
            ('2024-01-01 00:00:00', 'if2', 'Interface 2', 'hash2', 'device2', 20, False)
        ]
        
        with patch.object(self.cacher, 'query_table') as mock_query:
            mock_query.return_value = mock_rows
            
            self.cacher.prime_table('meta_interface')
            
            # Should build composite keys
            self.assertIn('meta_interface', self.cacher.local_cache)
            self.assertIn('device1:10:True', self.cacher.local_cache['meta_interface'])
            self.assertIn('device2:20:False', self.cacher.local_cache['meta_interface'])

    def test_prime_table_with_ipv6_mapped_addresses(self):
        """Test prime_table converts IPv6-mapped IPv4 addresses."""
        mock_client = Mock()
        self.cacher.cache.client = mock_client
        
        # Create IPv6-mapped IPv4 address
        ipv6_mapped = IPv6Address('::ffff:192.168.1.1')
        
        mock_rows = [
            ('2024-01-01 00:00:00', 'device1', 'Device 1', 'hash1', ipv6_mapped)
        ]
        
        with patch.object(self.cacher, 'query_table') as mock_query:
            mock_query.return_value = mock_rows
            
            self.cacher.prime_table('meta_device')
            
            # Should convert to IPv4 format
            self.assertIn('192.168.1.1', self.cacher.local_cache['meta_device'])

    def test_prime_table_skips_none_fields(self):
        """Test prime_table skips None values in composite keys."""
        mock_client = Mock()
        self.cacher.cache.client = mock_client
        
        mock_rows = [
            ('2024-01-01 00:00:00', 'device1', 'Device 1', 'hash1', 'router1', None, 'value3')
        ]
        
        with patch.object(self.cacher, 'query_table') as mock_query:
            mock_query.return_value = mock_rows
            
            self.cacher.prime_table('meta_device')
            
            # Should skip None value
            self.assertIn('router1:value3', self.cacher.local_cache['meta_device'])

    def test_lookup_simple_key(self):
        """Test lookup with simple key."""
        self.cacher.local_cache['meta_device'] = {
            'device1': {'id': 'device1', 'ref': 'Device 1', 'hash': 'hash1'}
        }
        
        result = self.cacher.lookup('meta_device', 'device1')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['ref'], 'Device 1')

    def test_lookup_not_found(self):
        """Test lookup when key not found."""
        self.cacher.local_cache['meta_device'] = {}
        
        result = self.cacher.lookup('meta_device', 'nonexistent')
        
        self.assertIsNone(result)

    def test_lookup_table_not_cached(self):
        """Test lookup when table not in cache."""
        result = self.cacher.lookup('nonexistent_table', 'key')
        
        self.assertIsNone(result)

    def test_lookup_with_none_parameters(self):
        """Test lookup with None parameters."""
        result1 = self.cacher.lookup(None, 'key')
        result2 = self.cacher.lookup('table', None)
        
        self.assertIsNone(result1)
        self.assertIsNone(result2)

    def test_lookup_dict_key(self):
        """Test lookup_dict_key extracts specific field from cached record."""
        self.cacher.local_cache['meta_device'] = {
            'device1': {'id': 'device1', 'ref': 'Device 1', 'hash': 'hash1'}
        }
        
        result = self.cacher.lookup_dict_key('meta_device', 'device1', 'ref')
        
        self.assertEqual(result, 'Device 1')

    def test_lookup_dict_key_not_found(self):
        """Test lookup_dict_key when key not found."""
        result = self.cacher.lookup_dict_key('meta_device', 'nonexistent', 'ref')
        
        self.assertIsNone(result)

    def test_lookup_dict_key_field_not_found(self):
        """Test lookup_dict_key when field not in record."""
        self.cacher.local_cache['meta_device'] = {
            'device1': {'id': 'device1', 'ref': 'Device 1'}
        }
        
        result = self.cacher.lookup_dict_key('meta_device', 'device1', 'nonexistent_field')
        
        self.assertIsNone(result)

    def test_build_query_simple_table(self):
        """Test build_query with simple table name."""
        query = self.cacher.build_query('meta_device')
        
        self.assertIn('SELECT MAX(insert_time)', query)
        self.assertIn('argMax(id, insert_time)', query)
        self.assertIn('argMax(ref, insert_time)', query)
        self.assertIn('argMax(hash, insert_time)', query)
        self.assertIn('FROM meta_device', query)
        self.assertIn('GROUP BY id ORDER BY id', query)
        self.assertNotIn('ARRAY JOIN', query)

    def test_build_query_with_additional_fields(self):
        """Test build_query with additional index fields."""
        query = self.cacher.build_query('meta_interface:device_id:flow_index')
        
        self.assertIn('FROM meta_interface', query)
        self.assertIn('argMax(device_id, insert_time)', query)
        self.assertIn('argMax(flow_index, insert_time)', query)
        self.assertIn('AND device_id IS NOT NULL', query)
        self.assertIn('AND flow_index IS NOT NULL', query)
        self.assertNotIn('ARRAY JOIN', query)

    def test_build_query_with_array_field(self):
        """Test build_query with array field (@ prefix)."""
        query = self.cacher.build_query('meta_device:@loopback_ip')
        
        self.assertIn('argMax(loopback_ip, insert_time)', query)
        self.assertIn('ARRAY JOIN latest_loopback_ip', query)
        self.assertIn('AND loopback_ip IS NOT NULL', query)

    def test_build_query_with_multiple_fields_including_array(self):
        """Test build_query with mix of regular and array fields."""
        query = self.cacher.build_query('meta_interface:device_id:@loopback_ip:edge')
        
        self.assertIn('argMax(device_id, insert_time)', query)
        self.assertIn('argMax(loopback_ip, insert_time)', query)
        self.assertIn('argMax(edge, insert_time)', query)
        self.assertIn('ARRAY JOIN latest_loopback_ip', query)

    def test_query_table_success(self):
        """Test query_table executes query successfully."""
        mock_result = Mock()
        mock_result.result_rows = [('2024-01-01', 'id1', 'ref1', 'hash1')]
        
        mock_client = Mock()
        mock_client.query.return_value = mock_result
        self.cacher.cache.client = mock_client
        
        rows = self.cacher.query_table('meta_device')
        
        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 1)
        mock_client.query.assert_called_once()

    def test_query_table_no_result(self):
        """Test query_table when query returns no result."""
        mock_client = Mock()
        mock_client.query.return_value = None
        self.cacher.cache.client = mock_client
        
        rows = self.cacher.query_table('meta_device')
        
        self.assertIsNone(rows)

    def test_query_table_empty_rows(self):
        """Test query_table when result has no rows."""
        mock_result = Mock()
        mock_result.result_rows = []
        
        mock_client = Mock()
        mock_client.query.return_value = mock_result
        self.cacher.cache.client = mock_client
        
        rows = self.cacher.query_table('meta_device')
        
        self.assertIsNone(rows)

    def test_query_table_exception(self):
        """Test query_table handles exceptions."""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")
        self.cacher.cache.client = mock_client
        
        with self.assertRaises(Exception):
            self.cacher.query_table('meta_device')


if __name__ == '__main__':
    unittest.main(verbosity=2)
