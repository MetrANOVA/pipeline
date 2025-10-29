#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock
import hashlib
import orjson

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.organization import OrganizationMetadataProcessor


class TestOrganizationMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
        # Mock clickhouse cacher
        self.mock_clickhouse_cacher = MagicMock()
        self.mock_clickhouse_cacher.lookup.return_value = None  # No cached record by default
        
        # Set up cacher method to return the appropriate mock based on the type
        def mock_cacher(cache_type):
            if cache_type == "clickhouse":
                return self.mock_clickhouse_cacher
            else:
                return MagicMock()
        
        self.mock_pipeline.cacher.side_effect = mock_cacher

    def test_init_default_values(self):
        """Test that the processor initializes with correct default values."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        # Test default table name
        self.assertEqual(processor.table, 'meta_organization')
        
        # Test that val_id_field is correctly set
        self.assertEqual(processor.val_id_field, ['id'])
        
        # Test that required_fields is correctly set
        self.assertEqual(processor.required_fields, [['id'], ['name']])

        # Test that float_fields and array_fields are set
        self.assertEqual(processor.float_fields, ['latitude', 'longitude'])
        self.assertEqual(processor.array_fields, ['type'])
        
        # Test that additional column definitions were added
        column_names = [col[0] for col in processor.column_defs]
        expected_columns = [
            'id', 'ref', 'hash', 'insert_time', 'ext', 'tag',  # from base class
            'name', 'type', 'city_name', 'continent_name',
            'country_name', 'country_code', 'country_sub_name', 'country_sub_code',
            'latitude', 'longitude'
        ]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, column_names, f"Column '{expected_col}' not found in column definitions")

    def test_init_custom_table_name(self):
        """Test initialization with custom table name from environment."""
        with patch.dict(os.environ, {'CLICKHOUSE_ORGANIZATION_METADATA_TABLE': 'custom_organization_table'}):
            processor = OrganizationMetadataProcessor(self.mock_pipeline)
            self.assertEqual(processor.table, 'custom_organization_table')

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        with patch.dict(os.environ, {'CLICKHOUSE_ORGANIZATION_METADATA_TABLE': 'test_organization_table'}):
            processor = OrganizationMetadataProcessor(self.mock_pipeline)
            
            # Call the method
            result = processor.create_table_command()
            
            # Print the result for inspection
            print("\n" + "="*80)
            print("CREATE TABLE COMMAND OUTPUT (OrganizationMetadataProcessor - Basic):")
            print("="*80)
            print(result)
            print("="*80)
            
            # Basic assertions
            self.assertIsInstance(result, str)
            self.assertIn("CREATE TABLE IF NOT EXISTS test_organization_table", result)
            self.assertIn("ENGINE = MergeTree()", result)
            self.assertIn("ORDER BY", result)
            
            # Check that organization-specific columns are included
            organization_columns = [
                'name', 'type', 'city_name', 'continent_name',
                'country_name', 'country_code', 'latitude', 'longitude'
            ]
            for column in organization_columns:
                self.assertIn(f"`{column}`", result, f"Column {column} not found in CREATE TABLE command")

    def test_create_table_command_default_table_name(self):
        """Test create_table_command with default table name when env var not set."""
        # Ensure the environment variable is not set
        with patch.dict(os.environ, {}, clear=True):
            processor = OrganizationMetadataProcessor(self.mock_pipeline)
            result = processor.create_table_command()
            
            self.assertIn("CREATE TABLE IF NOT EXISTS meta_organization", result)

    def test_column_definitions_structure(self):
        """Test that column definitions are properly structured."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        # Check each column definition has correct structure [name, type, insert_flag]
        for col_def in processor.column_defs:
            self.assertIsInstance(col_def, list, "Column definition should be a list")
            self.assertEqual(len(col_def), 3, "Column definition should have 3 elements")
            self.assertIsInstance(col_def[0], str, "Column name should be a string")
            # col_def[1] can be string or None (for ext field)
            self.assertIsInstance(col_def[2], bool, "Insert flag should be a boolean")

    def test_column_types_correctness(self):
        """Test that specific column types are correctly defined."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        # Create a mapping of column names to their types
        column_types = {col[0]: col[1] for col in processor.column_defs}
        
        # Test specific column types
        self.assertEqual(column_types['name'], 'LowCardinality(String)')
        self.assertEqual(column_types['type'], 'Array(LowCardinality(String))')
        self.assertEqual(column_types['latitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['longitude'], 'Nullable(Float64)')
        self.assertEqual(column_types['city_name'], 'LowCardinality(Nullable(String))')
        self.assertEqual(column_types['country_code'], 'LowCardinality(Nullable(String))')

    def test_build_message_missing_required_fields(self):
        """Test build_message with missing required fields."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        # Missing 'data' field entirely
        input_data_1 = {'other': 'value'}
        result_1 = processor.build_message(input_data_1, {})
        self.assertEqual(result_1, [])
        
        # Missing 'id' field in data
        input_data_2 = {'data': [{'name': 'Test University', 'city_name': 'Los Angeles'}]}
        result_2 = processor.build_message(input_data_2, {})
        self.assertEqual(result_2, [])
        
        # Missing 'name' field in data
        input_data_3 = {'data': [{'id': 'test-org', 'city_name': 'Los Angeles'}]}
        result_3 = processor.build_message(input_data_3, {})
        self.assertEqual(result_3, [])

    def test_build_message_valid_single_record(self):
        """Test build_message with a single valid record."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'ucla',
                'name': 'University of California, Los Angeles',
                'type': ['university', 'research', 'education'],
                'city_name': 'Los Angeles',
                'continent_name': 'North America',
                'country_name': 'United States',
                'country_code': 'US',
                'country_sub_name': 'California',
                'country_sub_code': 'CA',
                'latitude': 34.0689,
                'longitude': -118.4452
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'ucla')
        self.assertEqual(record['name'], 'University of California, Los Angeles')
        self.assertEqual(record['type'], ['university', 'research', 'education'])
        self.assertEqual(record['city_name'], 'Los Angeles')
        self.assertEqual(record['continent_name'], 'North America')
        self.assertEqual(record['country_name'], 'United States')
        self.assertEqual(record['country_code'], 'US')
        self.assertEqual(record['country_sub_name'], 'California')
        self.assertEqual(record['country_sub_code'], 'CA')
        self.assertEqual(record['latitude'], 34.0689)
        self.assertEqual(record['longitude'], -118.4452)
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('ucla__v'))

    def test_build_message_minimal_required_fields(self):
        """Test build_message with only required fields."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-org',
                'name': 'Test Organization'
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test-org')
        self.assertEqual(record['name'], 'Test Organization')
        
        # Other fields should be None or default values
        self.assertIsNone(record['city_name'])
        self.assertIsNone(record['latitude'])
        self.assertIsNone(record['longitude'])
        self.assertEqual(record['ext'], '{}')
        self.assertEqual(record['tag'], [])

    def test_build_message_array_field_handling(self):
        """Test build_message with various array field formats."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        test_cases = [
            # Arrays as lists
            {
                'data': [{
                    'id': 'org1',
                    'name': 'Organization 1',
                    'type': ['university', 'research']
                }]
            },
            # Single values (should be handled by base class)
            {
                'data': [{
                    'id': 'org2',
                    'name': 'Organization 2',
                    'type': 'university'
                }]
            }
        ]
        
        for i, input_data in enumerate(test_cases):
            with self.subTest(case=i):
                # Reset cacher mock for each test case
                self.mock_clickhouse_cacher.lookup.return_value = None
                
                result = processor.build_message(input_data, {})
                
                self.assertIsInstance(result, list)
                self.assertEqual(len(result), 1)
                
                record = result[0]
                self.assertEqual(record['name'], f'Organization {i+1}')

    def test_build_message_existing_record_no_change(self):
        """Test build_message skips unchanged records."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-org',
                'name': 'Test Organization',
                'city_name': 'Los Angeles'
            }]
        }
        
        # Calculate the expected hash based on how the processor builds the record
        formatted_record = {
            "id": "test-org",
            "name": "Test Organization",
            "city_name": "Los Angeles",
            "ext": "{}",
            "tag": [],
            "type": [],  # Arrays default to empty arrays, not None
            "continent_name": None,
            "country_name": None,
            "country_code": None,
            "country_sub_name": None,
            "country_sub_code": None,
            "latitude": None,
            "longitude": None
        }
        record_json = orjson.dumps(formatted_record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_hash = hashlib.md5(record_json.encode('utf-8')).hexdigest()
        
        # Mock existing record with same hash
        mock_existing = {'hash': record_hash, 'ref': 'test-org__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertEqual(result, [])

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'test-org',
                'name': 'Test Organization Updated',  # Changed value
                'city_name': 'Los Angeles'
            }]
        }
        
        # Mock existing record with different hash
        mock_existing = {'hash': 'different_hash', 'ref': 'test-org__v1'}
        self.mock_clickhouse_cacher.lookup.return_value = mock_existing
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test-org')
        self.assertEqual(record['name'], 'Test Organization Updated')
        self.assertEqual(record['ref'], 'test-org__v2')  # Should increment version

    def test_build_message_comprehensive_scenario(self):
        """Test build_message with a comprehensive real-world scenario."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        input_data = {
            'data': [{
                'id': 'caltech',
                'name': 'California Institute of Technology',
                'type': ['university', 'research', 'technology'],
                'city_name': 'Pasadena',
                'continent_name': 'North America',
                'country_name': 'United States',
                'country_code': 'US',
                'country_sub_name': 'California',
                'country_sub_code': 'CA',
                'latitude': 34.1377,
                'longitude': -118.1253,
                'ext': '{"website": "https://www.caltech.edu", "established": 1891}',
                'tag': ['tier1', 'research-intensive', 'stem']
            }]
        }
        
        result = processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        
        # Verify all fields are properly processed
        self.assertEqual(record['id'], 'caltech')
        self.assertEqual(record['name'], 'California Institute of Technology')
        self.assertEqual(record['type'], ['university', 'research', 'technology'])
        self.assertEqual(record['city_name'], 'Pasadena')
        self.assertEqual(record['continent_name'], 'North America')
        self.assertEqual(record['country_name'], 'United States')
        self.assertEqual(record['country_code'], 'US')
        self.assertEqual(record['country_sub_name'], 'California')
        self.assertEqual(record['country_sub_code'], 'CA')
        self.assertEqual(record['latitude'], 34.1377)
        self.assertEqual(record['longitude'], -118.1253)
        
        # Verify ext and tag from base class processing
        self.assertEqual(record['ext'], '{"website": "https://www.caltech.edu", "established": 1891}')
        self.assertEqual(record['tag'], ['tier1', 'research-intensive', 'stem'])
        
        # Check that ref and hash are set
        self.assertIn('ref', record)
        self.assertIn('hash', record)
        self.assertTrue(record['ref'].startswith('caltech__v'))

    def test_build_message_geographic_coordinates_edge_cases(self):
        """Test build_message with various geographic coordinate formats."""
        processor = OrganizationMetadataProcessor(self.mock_pipeline)
        
        test_cases = [
            # Valid coordinates as numbers
            {'latitude': 34.0522, 'longitude': -118.2437},
            # Valid coordinates as strings (should be handled by base class)
            {'latitude': '34.0522', 'longitude': '-118.2437'},
            # Null/None coordinates
            {'latitude': None, 'longitude': None},
            # Missing coordinates (should default to None)
            {}
        ]
        
        for i, coords in enumerate(test_cases):
            with self.subTest(case=i):
                # Reset cacher mock for each test case
                self.mock_clickhouse_cacher.lookup.return_value = None
                
                input_data = {
                    'data': [{
                        'id': f'org-{i}',
                        'name': f'Test Organization {i}',
                        **coords
                    }]
                }
                
                result = processor.build_message(input_data, {})
                
                self.assertIsInstance(result, list)
                self.assertEqual(len(result), 1)
                
                record = result[0]
                self.assertEqual(record['name'], f'Test Organization {i}')
                
                # Check coordinate handling
                if i == 0:  # Numbers
                    self.assertEqual(record['latitude'], 34.0522)
                    self.assertEqual(record['longitude'], -118.2437)
                elif i == 1:  # Strings (base class may convert these)
                    # Base class handles string conversion
                    self.assertIn('latitude', record)
                    self.assertIn('longitude', record)
                else:  # None or missing
                    self.assertIn('latitude', record)
                    self.assertIn('longitude', record)


if __name__ == '__main__':
    unittest.main()
