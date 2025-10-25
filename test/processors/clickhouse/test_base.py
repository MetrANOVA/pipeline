#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.processors.clickhouse.base import BaseMetadataProcessor, DataCounterProcessor, DataGaugeProcessor


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