import os
import tempfile
import unittest
from unittest.mock import Mock, patch, mock_open
from collections import defaultdict
import orjson
import json

from metranova.consumers.metadata import YAMLFileConsumer, CAIDAOrgASConsumer

class TestYAMLFileConsumer(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_handle_file_data_valid_metadata(self):
        """Test handle_file_data with valid metadata structure."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'test_table',
            'data': [
                {'id': 'record1', 'name': 'Test Record 1'},
                {'id': 'record2', 'name': 'Test Record 2'}
            ]
        }
        
        consumer.handle_file_data('/path/to/file.yml', data)
        
        # Records are handles all at once
        self.assertEqual(self.mock_pipeline.process_message.call_count, 1)
        
        calls = self.mock_pipeline.process_message.call_args_list
        self.assertEqual(calls[0][0][0], {'table': 'test_table', 'data': [{'id': 'record1', 'name': 'Test Record 1'},{'id': 'record2', 'name': 'Test Record 2'}]})
    def test_handle_file_data_no_table(self):
        """Test handle_file_data when table field is missing."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'data': [{'id': 'record1', 'name': 'Test Record 1'}]
        }
        
        with patch.object(consumer.logger, 'error') as mock_error:
            consumer.handle_file_data('/path/to/file.yml', data)
            
            mock_error.assert_called_once()
            self.assertIn("No table found in file", mock_error.call_args[0][0])

        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_empty_data(self):
        """Test handle_file_data with empty data array."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_no_data_field(self):
        """Test handle_file_data when data field is missing."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages (empty list default)
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_none_input(self):
        """Test handle_file_data with None input data."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        consumer.handle_file_data('/path/to/file.yml', None)
        
        # Should not process any messages
        self.mock_pipeline.process_message.assert_not_called()

    def test_handle_file_data_single_record(self):
        """Test handle_file_data with single record."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'interface_table',
            'data': [
                {
                    'id': 'intf-001',
                    'name': 'eth0',
                    'type': 'ethernet',
                    'description': 'Management interface'
                }
            ]
        }
        
        consumer.handle_file_data('/path/to/interfaces.yml', data)
        
        self.mock_pipeline.process_message.assert_called_once_with({
            'table': 'interface_table',
            'data': [{
                'id': 'intf-001',
                'name': 'eth0',
                'type': 'ethernet',
                'description': 'Management interface'
            }]
        })

    def test_handle_file_data_complex_records(self):
        """Test handle_file_data with complex nested record structures."""
        
        consumer = YAMLFileConsumer(self.mock_pipeline)
        
        data = {
            'table': 'complex_table',
            'data': [
                {
                    'id': 'complex-001',
                    'metadata': {
                        'tags': ['production', 'critical'],
                        'properties': {
                            'region': 'us-west-2',
                            'environment': 'prod'
                        }
                    },
                    'array_field': [1, 2, 3, 4]
                }
            ]
        }
        
        consumer.handle_file_data('/path/to/complex.yml', data)
        
        expected_call = {
            'table': 'complex_table',
            'data': [{
                'id': 'complex-001',
                'metadata': {
                    'tags': ['production', 'critical'],
                    'properties': {
                        'region': 'us-west-2',
                        'environment': 'prod'
                    }
                },
                'array_field': [1, 2, 3, 4]
            }]
        }
        
        self.mock_pipeline.process_message.assert_called_once_with(expected_call)

class TestMetadataConsumersIntegration(unittest.TestCase):
    """Integration tests for file consumers."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()

    def test_yaml_consumer_with_real_file(self):
        """Test YAMLFileConsumer with a real temporary file."""
        
        yaml_content = """
table: meta_interface
data:
  - id: interface-001
    name: eth0
    type: ethernet
    description: Management interface
    properties:
      speed: 1000
      duplex: full
  - id: interface-002
    name: eth1
    type: ethernet
    description: Data interface
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as temp_file:
            temp_file.write(yaml_content)
            temp_file_path = temp_file.name
        
        try:
            # Test YAMLFileConsumer
            consumer = YAMLFileConsumer(self.mock_pipeline)
            consumer.file_paths = [temp_file_path]
            
            consumer.consume_messages()
            
            # Should process each record in the data array
            self.assertEqual(self.mock_pipeline.process_message.call_count, 1)
            
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)


class TestCAIDAOrgASConsumer(unittest.TestCase):
    """Unit tests for CAIDAOrgASConsumer class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        self.mock_pipeline.process_message = Mock()
        self.mock_pipeline.cacher = Mock()
        self.mock_cacher = Mock()
        self.mock_cacher.prime = Mock()
        self.mock_pipeline.cacher.return_value = self.mock_cacher
        
        # Patch environment variables
        self.env_patcher = patch.dict(os.environ, {
            'CAIDA_ORG_AS_CONSUMER_UPDATE_INTERVAL': '3600',
            'CAIDA_ORG_AS_CONSUMER_AS_TABLE': 'test_meta_as',
            'CAIDA_ORG_AS_CONSUMER_ORG_TABLE': 'test_meta_organization',
            'CAIDA_ORG_AS_CONSUMER_AS2ORG_FILE': '/test/caida_as_org2info.jsonl',
            'CAIDA_ORG_AS_CONSUMER_PEERINGDB_FILE': '/test/caida_peeringdb.json',
            'CAIDA_ORG_AS_CONSUMER_CUSTOM_ORG_FILE': '/test/custom_org.yml',
            'CAIDA_ORG_AS_CONSUMER_CUSTOM_AS_FILE': '/test/custom_as.yml'
        })
        self.env_patcher.start()
        
        self.consumer = CAIDAOrgASConsumer(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_init_with_environment_variables(self):
        """Test that the consumer initializes properly with environment variables."""
        self.assertEqual(self.consumer.update_interval, 3600)
        self.assertEqual(self.consumer.as_table, 'test_meta_as')
        self.assertEqual(self.consumer.org_table, 'test_meta_organization')
        self.assertEqual(self.consumer.as2org_file, '/test/caida_as_org2info.jsonl')
        self.assertEqual(self.consumer.peeringdb_file, '/test/caida_peeringdb.json')
        self.assertEqual(self.consumer.custom_org_file, '/test/custom_org.yml')
        self.assertEqual(self.consumer.custom_as_file, '/test/custom_as.yml')

    def test_lookup_country_name_valid(self):
        """Test _lookup_country_name with valid country code."""
        result = self.consumer._lookup_country_name('US')
        self.assertEqual(result, 'United States')
        
        result = self.consumer._lookup_country_name('CA')
        self.assertEqual(result, 'Canada')

    def test_lookup_country_name_invalid(self):
        """Test _lookup_country_name with invalid country code."""
        result = self.consumer._lookup_country_name('XX')
        self.assertIsNone(result)
        
        result = self.consumer._lookup_country_name('')
        self.assertIsNone(result)
        
        result = self.consumer._lookup_country_name(None)
        self.assertIsNone(result)

    def test_lookup_continent_name_valid(self):
        """Test _lookup_continent_name with valid country code."""
        result = self.consumer._lookup_continent_name('US')
        self.assertEqual(result, 'North America')
        
        result = self.consumer._lookup_continent_name('FR')
        self.assertEqual(result, 'Europe')

    def test_lookup_continent_name_invalid(self):
        """Test _lookup_continent_name with invalid country code."""
        result = self.consumer._lookup_continent_name('XX')
        self.assertIsNone(result)
        
        result = self.consumer._lookup_continent_name('')
        self.assertIsNone(result)
        
        result = self.consumer._lookup_continent_name(None)
        self.assertIsNone(result)

    def test_lookup_subdivision_name_valid(self):
        """Test _lookup_subdivision_name with valid codes."""
        result = self.consumer._lookup_subdivision_name('US', 'CA')
        self.assertEqual(result, 'California')
        
        result = self.consumer._lookup_subdivision_name('US', 'NY')
        self.assertEqual(result, 'New York')

    def test_lookup_subdivision_name_invalid(self):
        """Test _lookup_subdivision_name with invalid codes."""
        result = self.consumer._lookup_subdivision_name('US', 'XX')
        self.assertIsNone(result)
        
        result = self.consumer._lookup_subdivision_name('XX', 'YY')
        self.assertIsNone(result)

    @patch('builtins.open', new_callable=mock_open)
    @patch.object(CAIDAOrgASConsumer, '_lookup_country_name')
    @patch.object(CAIDAOrgASConsumer, '_lookup_continent_name')
    def test_load_caida_data_organizations(self, mock_continent_lookup, mock_country_lookup, mock_file):
        """Test _load_caida_data with organization records."""
        # Mock the lookup functions to return predictable values
        mock_country_lookup.return_value = 'United States'
        mock_continent_lookup.return_value = 'North America'
        
        caida_data = [
            '{"type": "Organization", "organizationId": "org1", "name": "Test Org 1", "country": "US"}',
            '{"type": "Organization", "organizationId": "org2", "name": "Test Org 2", "country": "CA"}'
        ]
        mock_file.return_value.__iter__ = lambda self: iter(caida_data)
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        
        self.consumer._load_caida_data(as_objs, org_objs)
        
        self.assertEqual(len(org_objs), 2)
        self.assertIn('caida:org1', org_objs)
        self.assertIn('caida:org2', org_objs)
        
        org1 = org_objs['caida:org1']
        self.assertEqual(org1['id'], 'caida:org1')
        self.assertEqual(org1['name'], 'Test Org 1')
        self.assertEqual(org1['country_code'], 'US')
        self.assertEqual(org1['country_name'], 'United States')
        self.assertEqual(org1['continent_name'], 'North America')
        self.assertEqual(org1['ext']['data_source'], ['CAIDA'])

    @patch('builtins.open', new_callable=mock_open)
    def test_load_caida_data_asns(self, mock_file):
        """Test _load_caida_data with ASN records."""
        caida_data = [
            '{"type": "ASN", "asn": "65001", "organizationId": "org1", "name": "Test AS 1"}',
            '{"type": "ASN", "asn": "65002", "organizationId": "org2", "name": "Test AS 2"}'
        ]
        mock_file.return_value.__iter__ = lambda self: iter(caida_data)
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        
        self.consumer._load_caida_data(as_objs, org_objs)
        
        self.assertEqual(len(as_objs), 2)
        self.assertIn(65001, as_objs)
        self.assertIn(65002, as_objs)
        
        as1 = as_objs[65001]
        self.assertEqual(as1['id'], 65001)
        self.assertEqual(as1['organization_id'], 'caida:org1')
        self.assertEqual(as1['name'], 'Test AS 1')
        self.assertEqual(as1['ext']['data_source'], ['CAIDA'])

    @patch('builtins.open', new_callable=mock_open)
    def test_load_caida_data_file_not_found(self, mock_file):
        """Test _load_caida_data with file not found error."""
        mock_file.side_effect = FileNotFoundError("File not found")
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer._load_caida_data(as_objs, org_objs)
            mock_error.assert_called_once()
            self.assertIn("AS to Org file not found", mock_error.call_args[0][0])

    @patch('builtins.open', new_callable=mock_open)
    def test_load_peeringdb_data_success(self, mock_file):
        """Test _load_peeringdb_data with successful file load."""
        peeringdb_data = {
            "org": {
                "data": [
                    {"id": 1, "name": "PeeringDB Org 1", "country": "US", "city": "San Francisco"}
                ]
            },
            "net": {
                "data": [
                    {"asn": 65001, "org_id": 1, "name": "PeeringDB AS 1"}
                ]
            }
        }
        mock_file.return_value.read.return_value = orjson.dumps(peeringdb_data)
        
        result = self.consumer._load_peeringdb_data()
        
        self.assertIsNotNone(result)
        self.assertEqual(result["org"]["data"][0]["name"], "PeeringDB Org 1")

    @patch('builtins.open', new_callable=mock_open)
    def test_load_peeringdb_data_file_not_found(self, mock_file):
        """Test _load_peeringdb_data with file not found error."""
        mock_file.side_effect = FileNotFoundError("File not found")
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            result = self.consumer._load_peeringdb_data()
            self.assertIsNone(result)
            mock_error.assert_called_once()
            self.assertIn("PeeringDB file not found", mock_error.call_args[0][0])

    def test_process_peeringdb_organizations(self):
        """Test _process_peeringdb_organizations with valid data."""
        peeringdb_data = {
            "org": {
                "data": [
                    {"id": 1, "name": "Org 1", "country": "US"},
                    {"id": 2, "name": "Org 2", "country": "CA"}
                ]
            }
        }
        
        result = self.consumer._process_peeringdb_organizations(peeringdb_data)
        
        self.assertEqual(len(result), 2)
        self.assertIn("peeringdb:1", result)
        self.assertIn("peeringdb:2", result)
        self.assertEqual(result["peeringdb:1"]["name"], "Org 1")

    def test_process_peeringdb_organizations_none_data(self):
        """Test _process_peeringdb_organizations with None data."""
        result = self.consumer._process_peeringdb_organizations(None)
        self.assertEqual(result, {})

    def test_process_peeringdb_networks_new_as(self):
        """Test _process_peeringdb_networks creating new AS records."""
        peeringdb_data = {
            "net": {
                "data": [
                    {
                        "asn": 65001,
                        "org_id": 1,
                        "name": "Test AS",
                        "info_ipv6": True,
                        "info_prefixes4": 100,
                        "info_prefixes6": 50
                    }
                ]
            }
        }
        
        peeringdb_org_objs = {
            "peeringdb:1": {
                "id": 1,
                "name": "Test Org",
                "country": "US",
                "city": "San Francisco",
                "latitude": 37.7749,
                "longitude": -122.4194,
                "state": "CA"
            }
        }
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        
        self.consumer._process_peeringdb_networks(peeringdb_data, peeringdb_org_objs, as_objs, org_objs)
        
        # Check AS was created
        self.assertIn(65001, as_objs)
        as_record = as_objs[65001]
        self.assertEqual(as_record['id'], 65001)
        self.assertEqual(as_record['organization_id'], 'peeringdb:1')
        self.assertEqual(as_record['name'], 'Test AS')
        self.assertIn('PeeringDB', as_record['ext']['data_source'])
        self.assertEqual(as_record['ext']['peeringdb_ipv6'], True)
        self.assertEqual(as_record['ext']['peeringdb_prefixes4'], 100)
        
        # Check organization was created
        self.assertIn('peeringdb:1', org_objs)
        org_record = org_objs['peeringdb:1']
        self.assertEqual(org_record['id'], 'peeringdb:1')
        self.assertEqual(org_record['name'], 'Test Org')
        self.assertEqual(org_record['country_code'], 'US')
        self.assertEqual(org_record['country_name'], 'United States')
        self.assertEqual(org_record['city_name'], 'San Francisco')
        self.assertEqual(org_record['latitude'], 37.7749)
        self.assertEqual(org_record['longitude'], -122.4194)
        self.assertEqual(org_record['country_sub_code'], 'CA')
        self.assertEqual(org_record['country_sub_name'], 'California')

    def test_process_peeringdb_networks_existing_as(self):
        """Test _process_peeringdb_networks updating existing AS records."""
        peeringdb_data = {
            "net": {
                "data": [
                    {
                        "asn": 65001,
                        "org_id": 1,
                        "name": "Updated AS Name",
                        "info_traffic": "1Gbps"
                    }
                ]
            }
        }
        
        peeringdb_org_objs = {
            "peeringdb:1": {"id": 1, "name": "Test Org", "country": "US"}
        }
        
        # Pre-populate with existing AS and org
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        as_objs[65001] = {
            'id': 65001,
            'organization_id': 'peeringdb:1',
            'name': 'Original AS Name',
            'ext': {'data_source': ['CAIDA']}
        }
        org_objs['peeringdb:1'] = {
            'id': 'peeringdb:1',
            'name': 'Test Org',
            'ext': {'data_source': ['CAIDA']}
        }
        
        self.consumer._process_peeringdb_networks(peeringdb_data, peeringdb_org_objs, as_objs, org_objs)
        
        # Check AS was updated
        as_record = as_objs[65001]
        self.assertIn('PeeringDB', as_record['ext']['data_source'])
        self.assertEqual(as_record['name'], 'Updated AS Name')
        self.assertEqual(as_record['ext']['peeringdb_traffic'], '1Gbps')
        
        # Check organization was updated
        org_record = org_objs['peeringdb:1']
        self.assertIn('PeeringDB', org_record['ext']['data_source'])

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    def test_load_custom_data_organizations(self, mock_yaml_load, mock_file):
        """Test _load_custom_data with custom organization data."""
        custom_org_data = {
            "data": [
                {"id": "custom:org1", "name": "Custom Org 1", "custom_field": "custom_value"}
            ]
        }
        mock_yaml_load.return_value = custom_org_data
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        # Pre-populate with existing org
        org_objs["custom:org1"] = {"id": "custom:org1", "name": "Original Name"}
        
        self.consumer._load_custom_data(as_objs, org_objs)
        
        # Check organization was updated
        org_record = org_objs["custom:org1"]
        self.assertEqual(org_record["name"], "Custom Org 1")
        self.assertEqual(org_record["custom_field"], "custom_value")

    @patch('builtins.open', new_callable=mock_open)
    @patch('yaml.safe_load')
    def test_load_custom_data_as_records(self, mock_yaml_load, mock_file):
        """Test _load_custom_data with custom AS data."""
        custom_as_data = {
            "data": [
                {"id": 65001, "name": "Custom AS 1", "custom_field": "custom_value"}
            ]
        }
        mock_yaml_load.return_value = custom_as_data
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        # Pre-populate with existing AS
        as_objs[65001] = {"id": 65001, "name": "Original AS Name"}
        
        self.consumer._load_custom_data(as_objs, org_objs)
        
        # Check AS was updated
        as_record = as_objs[65001]
        self.assertEqual(as_record["name"], "Custom AS 1")
        self.assertEqual(as_record["custom_field"], "custom_value")

    @patch('builtins.open', new_callable=mock_open)
    def test_load_custom_data_file_not_found(self, mock_file):
        """Test _load_custom_data with missing custom files."""
        mock_file.side_effect = FileNotFoundError("File not found")
        
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        
        with patch.object(self.consumer.logger, 'error') as mock_error:
            self.consumer._load_custom_data(as_objs, org_objs)
            # Should log errors for both custom org and AS files
            self.assertEqual(mock_error.call_count, 2)

    @patch('time.sleep')
    def test_emit_messages(self, mock_sleep):
        """Test _emit_messages emits organization and AS data properly."""
        as_objs = {65001: {"id": 65001, "name": "Test AS"}}
        org_objs = {"org1": {"id": "org1", "name": "Test Org"}}
        
        self.consumer._emit_messages(as_objs, org_objs)
        
        # Check that messages were emitted in correct order
        self.assertEqual(self.mock_pipeline.process_message.call_count, 2)
        
        # First call should be organizations
        first_call = self.mock_pipeline.process_message.call_args_list[0]
        self.assertEqual(first_call[0][0]['table'], 'test_meta_organization')
        self.assertEqual(len(first_call[0][0]['data']), 1)
        
        # Second call should be AS records
        second_call = self.mock_pipeline.process_message.call_args_list[1]
        self.assertEqual(second_call[0][0]['table'], 'test_meta_as')
        self.assertEqual(len(second_call[0][0]['data']), 1)
        
        # Check that sleep was called for timing
        mock_sleep.assert_called_once_with(10)
        
        # Check that cacher was primed
        self.mock_pipeline.cacher.assert_called_once_with("clickhouse")
        self.mock_cacher.prime.assert_called_once()

    @patch.object(CAIDAOrgASConsumer, '_emit_messages')
    @patch.object(CAIDAOrgASConsumer, '_load_custom_data')
    @patch.object(CAIDAOrgASConsumer, '_process_peeringdb_networks')
    @patch.object(CAIDAOrgASConsumer, '_process_peeringdb_organizations')
    @patch.object(CAIDAOrgASConsumer, '_load_peeringdb_data')
    @patch.object(CAIDAOrgASConsumer, '_load_caida_data')
    def test_consume_messages_integration(self, mock_load_caida, mock_load_peeringdb, 
                                        mock_process_orgs, mock_process_networks,
                                        mock_load_custom, mock_emit):
        """Test consume_messages calls all methods in correct order."""
        # Mock return value for PeeringDB data
        mock_peeringdb_data = {"test": "data"}
        mock_load_peeringdb.return_value = mock_peeringdb_data
        mock_process_orgs.return_value = {"peeringdb:1": {"id": 1}}
        
        self.consumer.consume_messages()
        
        # Verify all methods were called in correct order
        mock_load_caida.assert_called_once()
        mock_load_peeringdb.assert_called_once()
        mock_process_orgs.assert_called_once_with(mock_peeringdb_data)
        mock_process_networks.assert_called_once()
        mock_load_custom.assert_called_once()
        mock_emit.assert_called_once()
        
        # Verify the parameters passed to process_networks include the org objects
        call_args = mock_process_networks.call_args[0]
        self.assertEqual(call_args[0], mock_peeringdb_data)  # peeringdb_data
        self.assertEqual(call_args[1], {"peeringdb:1": {"id": 1}})  # peeringdb_org_objs

    def test_consumer_without_custom_files(self):
        """Test consumer initialization without custom file environment variables."""
        # Create consumer without custom file environment variables
        with patch.dict(os.environ, {
            'CAIDA_ORG_AS_CONSUMER_CUSTOM_ORG_FILE': '',
            'CAIDA_ORG_AS_CONSUMER_CUSTOM_AS_FILE': ''
        }, clear=False):
            consumer = CAIDAOrgASConsumer(self.mock_pipeline)
            self.assertEqual(consumer.custom_org_file, '')
            self.assertEqual(consumer.custom_as_file, '')


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)