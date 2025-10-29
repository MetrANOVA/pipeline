#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.processors.clickhouse.base import BaseMetadataProcessor, DataCounterProcessor, DataGaugeProcessor, BaseDataProcessor


class TestBaseMetadataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline with a mock cacher
        self.mock_pipeline = Mock()
        mock_cacher = Mock()
        mock_cacher.lookup.return_value = None
        self.mock_pipeline.cacher.return_value = mock_cacher
        
        # Create an instance of BaseMetadataProcessor
        self.processor = BaseMetadataProcessor(self.mock_pipeline)
        
        # Set a test table name
        self.processor.table = "test_metadata_table"
        
        # Verify column_defs are set (they should be set by BaseMetadataProcessor.__init__)
        print(f"Column defs: {self.processor.column_defs}")
        print(f"Order by: {self.processor.order_by}")
        
        # If column_defs is empty, set them manually (fallback)
        if not self.processor.column_defs:
            self.processor.column_defs = [
                ['id', 'String', True],
                ['ref', 'String', True],
                ['hash', 'String', True],
                ['insert_time', 'DateTime DEFAULT now()', False],
                ['ext', None, True],
                ['tag', 'Array(LowCardinality(String))', True]
            ]
        
        # Set order_by to match what BaseMetadataProcessor should have
        self.processor.order_by = ['ref', 'id', 'insert_time']

    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings."""
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (Basic):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure
        self.assertIn("CREATE TABLE IF NOT EXISTS test_metadata_table", result)
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertIn("ORDER BY (`ref`,`id`,`insert_time`)", result)
        self.assertIn("SETTINGS index_granularity = 8192", result)
        
        # Check for specific columns
        self.assertIn("`id` String", result)
        self.assertIn("`ref` String", result)
        self.assertIn("`hash` String", result)
        self.assertIn("`insert_time` DateTime DEFAULT now()", result)
        self.assertIn("`ext` JSON", result)
        self.assertIn("`tag` Array(LowCardinality(String))", result)
        
        # Verify that insert_time is not included in the column list (include_in_insert = False)
        # We can't easily test this without examining the exact structure, but we can verify it exists

    def test_create_table_command_with_extension_definitions(self):
        """Test create_table_command with extension column definitions."""
        
        # Add some extension definitions
        self.processor.extension_defs = {
            "ext": [
                ["device_type", "String", True],
                ["location", "String", True],
                ["capacity", "UInt64", True],
                ["metadata", "JSON", True]
            ]
        }
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (With Extensions):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for extension column structure
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`device_type` String", result)
        self.assertIn("`location` String", result)
        self.assertIn("`capacity` UInt64", result)
        self.assertIn("`metadata` JSON", result)

    def test_create_table_command_with_primary_keys(self):
        """Test create_table_command with primary keys defined."""
        
        # Set primary keys
        self.processor.primary_keys = ["ref", "id"]
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (With Primary Keys):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for primary key clause
        self.assertIn("PRIMARY KEY (`ref`,`id`)", result)

    def test_create_table_command_with_partition(self):
        """Test create_table_command with partition defined."""
        
        # Set partition
        self.processor.partition_by = "toYYYYMM(insert_time)"
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (With Partition):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for partition clause
        self.assertIn("PARTITION BY toYYYYMM(insert_time)", result)

    def test_create_table_command_validation_errors(self):
        """Test that create_table_command raises appropriate errors for invalid configurations."""
        
        # Test with no table name
        self.processor.table = ""
        with self.assertRaises(ValueError) as context:
            self.processor.create_table_command()
        self.assertIn("Table name is not set", str(context.exception))
        
        # Reset table name and test with no column definitions
        self.processor.table = "test_table"
        self.processor.column_defs = []
        with self.assertRaises(ValueError) as context:
            self.processor.create_table_command()
        self.assertIn("Column definitions are not set", str(context.exception))
        
        # Reset column definitions and test with no table engine
        self.processor.column_defs = [['id', 'String', True]]
        self.processor.table_engine = ""
        with self.assertRaises(ValueError) as context:
            self.processor.create_table_command()
        self.assertIn("Table engine is not set", str(context.exception))

    @patch.dict(os.environ, {'TEST_EXTENSION_VAR': 'ext1,ext2,invalid_ext'})
    def test_get_extension_defs(self):
        """Test get_extension_defs method with various scenarios."""
        
        # Test with valid extension options
        extension_options = {
            'ext1': [['field1', 'String', True], ['field2', 'UInt32', True]],
            'ext2': [['field3', 'Float64', True]]
        }
        
        result = self.processor.get_extension_defs('TEST_EXTENSION_VAR', extension_options)
        
        # Should return all fields from valid extensions
        expected = [['field1', 'String', True], ['field2', 'UInt32', True], ['field3', 'Float64', True]]
        self.assertEqual(result, expected)
        
        # Check that extension_enabled is populated
        self.assertTrue(self.processor.extension_is_enabled('ext1'))
        self.assertTrue(self.processor.extension_is_enabled('ext2'))
        self.assertTrue(self.processor.extension_is_enabled('invalid_ext'))  # Still enabled even without options

    def test_get_extension_defs_empty_env_var(self):
        """Test get_extension_defs with empty environment variable."""
        
        extension_options = {'ext1': [['field1', 'String', True]]}
        result = self.processor.get_extension_defs('NON_EXISTENT_VAR', extension_options)
        
        self.assertEqual(result, [])

    def test_extension_is_enabled(self):
        """Test extension_is_enabled method."""
        
        # Initially no extensions should be enabled
        self.assertFalse(self.processor.extension_is_enabled('test_ext'))
        
        # Enable an extension manually
        self.processor.extension_enabled['ext']['test_ext'] = True
        self.assertTrue(self.processor.extension_is_enabled('test_ext'))
        
        # Test with different json_column_name
        self.processor.extension_enabled['custom_col']['another_ext'] = True
        self.assertTrue(self.processor.extension_is_enabled('another_ext', 'custom_col'))
        self.assertFalse(self.processor.extension_is_enabled('another_ext', 'ext'))

    def test_column_names(self):
        """Test column_names method returns only columns marked for insertion."""
        
        result = self.processor.column_names()
        
        # Should only include columns where include_in_insert is True
        expected = ['id', 'ref', 'hash', 'ext', 'tag']  # insert_time has False
        self.assertEqual(result, expected)

    def test_message_to_columns(self):
        """Test message_to_columns method."""
        
        # Test with valid message
        message = {
            'id': 'test_id',
            'ref': 'test_ref',
            'hash': 'test_hash',
            'ext': '{}',
            'tag': []
        }
        
        result = self.processor.message_to_columns(message)
        expected = ['test_id', 'test_ref', 'test_hash', '{}', []]
        self.assertEqual(result, expected)

    def test_message_to_columns_missing_field(self):
        """Test message_to_columns with missing required field."""
        
        # Missing 'ref' field
        message = {
            'id': 'test_id',
            'hash': 'test_hash',
            'ext': '{}',
            'tag': []
        }
        
        with self.assertRaises(ValueError) as context:
            self.processor.message_to_columns(message)
        self.assertIn("Missing column 'ref' in message", str(context.exception))


class TestBaseMetadataProcessorBuildMessage(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures for build_message tests."""
        # Create a mock pipeline with cachers
        self.mock_pipeline = Mock()
        mock_clickhouse_cacher = Mock()
        mock_clickhouse_cacher.lookup.return_value = None  # No existing record
        self.mock_pipeline.cacher.return_value = mock_clickhouse_cacher
        
        # Create processor
        self.processor = BaseMetadataProcessor(self.mock_pipeline)
        self.processor.table = "test_table"
        self.processor.val_id_field = ['id']
        self.processor.required_fields = [['id'], ['name']]

    def test_build_message_new_record(self):
        """Test build_message creates new record with v1 ref."""
        
        input_data = {
            'data': [{
                'id': 'test_id',
                'name': 'test_name',
                'description': 'test_desc'
            }]
        }
        
        result = self.processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        record = result[0]
        self.assertEqual(record['id'], 'test_id')
        self.assertEqual(record['ref'], 'test_id__v1')
        self.assertIn('hash', record)
        self.assertEqual(record['ext'], '{}')
        self.assertEqual(record['tag'], [])

    def test_build_message_existing_record_no_change(self):
        """Test build_message skips unchanged records."""
        
        input_data = {
            'data': [{
                'id': 'test_id',
                'name': 'test_name'
            }]
        }
        
        # Mock existing record with same hash - calculate hash same way as current code
        import hashlib
        import orjson
        
        # Build record the same way the current code does to get correct hash
        # Only include fields that are actually in the column_defs
        formatted_record = {
            "id": "test_id",
            "ext": "{}",
            "tag": []
        }
        record_json = orjson.dumps(formatted_record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_hash = hashlib.md5(record_json.encode('utf-8')).hexdigest()
        
        mock_existing = {'hash': record_hash, 'ref': 'test_id__v1'}
        self.mock_pipeline.cacher.return_value.lookup.return_value = mock_existing
        
        result = self.processor.build_message(input_data, {})
        
        self.assertEqual(result, [])

    def test_build_message_existing_record_changed(self):
        """Test build_message creates new version for changed records."""
        
        input_data = {
            'data': [{
                'id': 'test_id',
                'name': 'test_name_updated'
            }]
        }
        
        # Mock existing record with different hash
        mock_existing = {'hash': 'different_hash', 'ref': 'test_id__v2'}
        self.mock_pipeline.cacher.return_value.lookup.return_value = mock_existing
        
        result = self.processor.build_message(input_data, {})
        
        self.assertIsInstance(result, list)
        record = result[0]
        self.assertEqual(record['ref'], 'test_id__v3')  # Should increment version

    def test_build_message_missing_required_fields(self):
        """Test build_message returns empty list for missing required fields."""
        
        input_data = {
            'data': [{
                'id': 'test_id'
                # Missing 'name' field
            }]
        }
        
        result = self.processor.build_message(input_data, {})
        self.assertEqual(result, [])

    def test_build_message_missing_id_field(self):
        """Test build_message returns empty list when id field is missing."""
        
        input_data = {
            'data': [{
                'name': 'test_name'
                # Missing 'id' field
            }]
        }
        
        result = self.processor.build_message(input_data, {})
        self.assertEqual(result, [])

    @patch.dict(os.environ, {'CLICKHOUSE_METADATA_FORCE_UPDATE': 'true'})
    def test_build_message_force_update(self):
        """Test build_message with force update enabled."""
        
        # Create a new processor instance with force_update enabled
        force_processor = BaseMetadataProcessor(self.mock_pipeline)
        force_processor.table = "test_table"
        force_processor.val_id_field = ['id']
        force_processor.required_fields = [['id'], ['name']]
        
        input_data = {
            'data': [{
                'id': 'test_id',
                'name': 'test_name'
            }]
        }
        
        # Mock existing record with same hash
        import hashlib
        import orjson
        value_json = orjson.dumps(input_data, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_hash = hashlib.md5(value_json.encode('utf-8')).hexdigest()
        
        mock_existing = {'hash': record_hash, 'ref': 'test_id__v1'}
        self.mock_pipeline.cacher.return_value.lookup.return_value = mock_existing
        
        result = force_processor.build_message(input_data, {})
        
        # Should create new version even with same hash due to force update
        self.assertIsInstance(result, list)
        record = result[0]
        self.assertEqual(record['ref'], 'test_id__v2')

    def test_match_message(self):
        """Test match_message method."""
        
        # Should match when table matches
        self.assertTrue(self.processor.match_message({'table': 'test_table'}))
        
        # Should not match when table doesn't match
        self.assertFalse(self.processor.match_message({'table': 'other_table'}))
        
        # Should not match when no table specified
        self.assertFalse(self.processor.match_message({}))


class TestBaseDataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures for BaseDataProcessor tests."""
        self.mock_pipeline = Mock()
        self.processor = BaseDataProcessor(self.mock_pipeline)

    def test_policy_scope_parsing(self):
        """Test that policy_scope environment variable is properly parsed."""
        
        with patch.dict(os.environ, {'CLICKHOUSE_POLICY_SCOPE': 'scope1,scope2,scope3'}):
            processor = BaseDataProcessor(self.mock_pipeline)
            self.assertEqual(processor.policy_scope, ['scope1', 'scope2', 'scope3'])

    def test_policy_scope_empty(self):
        """Test policy_scope when environment variable is not set."""
        
        if 'CLICKHOUSE_POLICY_SCOPE' in os.environ:
            del os.environ['CLICKHOUSE_POLICY_SCOPE']
        
        processor = BaseDataProcessor(self.mock_pipeline)
        self.assertEqual(processor.policy_scope, [])


class TestDataCounterProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = Mock()
        
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'interface'})
    def test_create_table_command_data_counter(self):
        """Test the create_table_command method for DataCounterProcessor."""
        
        # Create an instance of DataCounterProcessor
        processor = DataCounterProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (DataCounterProcessor):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure
        self.assertIn("CREATE TABLE IF NOT EXISTS data_interface_counter", result)
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertIn("PARTITION BY toYYYYMMDD(observation_time)", result)
        self.assertIn("ORDER BY (`metric_name`,`interface_id`,`observation_time`)", result)
        self.assertIn("SETTINGS index_granularity = 8192", result)
        
        # Check for specific columns
        self.assertIn("`observation_time` DateTime64(3, 'UTC')", result)
        self.assertIn("`insert_time` DateTime64(3, 'UTC') DEFAULT now64()", result)
        self.assertIn("`collector_id` LowCardinality(String)", result)
        self.assertIn("`policy_originator` LowCardinality(String)", result)
        self.assertIn("`policy_level` LowCardinality(String)", result)
        self.assertIn("`policy_scope` Array(LowCardinality(String))", result)
        self.assertIn("`ext` JSON", result)
        self.assertIn("`interface_id` LowCardinality(String)", result)
        self.assertIn("`interface_ref` Nullable(String)", result)
        self.assertIn("`metric_name` String", result)
        self.assertIn("`metric_value` UInt64", result)
        
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'device'})
    def test_create_table_command_data_counter_different_resource(self):
        """Test the create_table_command method for DataCounterProcessor with different resource name."""
        
        # Create an instance of DataCounterProcessor
        processor = DataCounterProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (DataCounterProcessor - Device):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure with different resource name
        self.assertIn("CREATE TABLE IF NOT EXISTS data_device_counter", result)
        self.assertIn("`device_id` LowCardinality(String)", result)
        self.assertIn("`device_ref` Nullable(String)", result)
        self.assertIn("ORDER BY (`metric_name`,`device_id`,`observation_time`)", result)
        
    def test_data_counter_processor_missing_env_var(self):
        """Test that DataCounterProcessor raises error when environment variable is missing."""
        
        # Ensure the environment variable is not set
        if 'CLICKHOUSE_METRIC_RESOURCE_NAME' in os.environ:
            del os.environ['CLICKHOUSE_METRIC_RESOURCE_NAME']
            
        with self.assertRaises(ValueError) as context:
            DataCounterProcessor(self.mock_pipeline)
        self.assertIn("CLICKHOUSE_METRIC_RESOURCE_NAME environment variable not set", str(context.exception))


class TestDataGaugeProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = Mock()
        
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'interface'})
    def test_create_table_command_data_gauge(self):
        """Test the create_table_command method for DataGaugeProcessor."""
        
        # Create an instance of DataGaugeProcessor
        processor = DataGaugeProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (DataGaugeProcessor):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure
        self.assertIn("CREATE TABLE IF NOT EXISTS data_interface_gauge", result)
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertIn("PARTITION BY toYYYYMMDD(observation_time)", result)
        self.assertIn("ORDER BY (`metric_name`,`interface_id`,`observation_time`)", result)
        self.assertIn("SETTINGS index_granularity = 8192", result)
        
        # Check for specific columns
        self.assertIn("`observation_time` DateTime64(3, 'UTC')", result)
        self.assertIn("`insert_time` DateTime64(3, 'UTC') DEFAULT now64()", result)
        self.assertIn("`collector_id` LowCardinality(String)", result)
        self.assertIn("`policy_originator` LowCardinality(String)", result)
        self.assertIn("`policy_level` LowCardinality(String)", result)
        self.assertIn("`policy_scope` Array(LowCardinality(String))", result)
        self.assertIn("`ext` JSON", result)
        self.assertIn("`interface_id` LowCardinality(String)", result)
        self.assertIn("`interface_ref` Nullable(String)", result)
        self.assertIn("`metric_name` String", result)
        self.assertIn("`metric_value` Float64", result)
        
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'server'})
    def test_create_table_command_data_gauge_different_resource(self):
        """Test the create_table_command method for DataGaugeProcessor with different resource name."""
        
        # Create an instance of DataGaugeProcessor
        processor = DataGaugeProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (DataGaugeProcessor - Server):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure with different resource name
        self.assertIn("CREATE TABLE IF NOT EXISTS data_server_gauge", result)
        self.assertIn("`server_id` LowCardinality(String)", result)
        self.assertIn("`server_ref` Nullable(String)", result)
        self.assertIn("ORDER BY (`metric_name`,`server_id`,`observation_time`)", result)
        
    def test_data_gauge_processor_missing_env_var(self):
        """Test that DataGaugeProcessor raises error when environment variable is missing."""
        
        # Ensure the environment variable is not set
        if 'CLICKHOUSE_METRIC_RESOURCE_NAME' in os.environ:
            del os.environ['CLICKHOUSE_METRIC_RESOURCE_NAME']
            
        with self.assertRaises(ValueError) as context:
            DataGaugeProcessor(self.mock_pipeline)
        self.assertIn("CLICKHOUSE_METRIC_RESOURCE_NAME environment variable not set", str(context.exception))


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)