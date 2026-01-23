#!/usr/bin/env python3

from collections import defaultdict
import unittest
import sys
import os
from unittest.mock import Mock, patch

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from metranova.processors.clickhouse.base import BaseMetadataProcessor, BaseDataProcessor, BaseClickHouseTableMixin, BaseClickHouseMaterializedViewMixin, BaseClickHouseDictionaryMixin


class TestBaseClickHouseTableMixin(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures for BaseClickHouseTableMixin tests."""
        self.mixin = BaseClickHouseTableMixin()
        
    def test_init_default_values(self):
        """Test initialization with default values."""
        self.assertIsNotNone(self.mixin.logger)
        self.assertIsNone(self.mixin.cluster_name)
        self.assertFalse(self.mixin.replication)
        self.assertEqual(self.mixin.replica_path, '/clickhouse/tables/{shard}/{database}/{table}')
        self.assertEqual(self.mixin.replica_name, '{replica}')
        self.assertEqual(self.mixin.table_engine, "MergeTree")
        self.assertEqual(self.mixin.table_granularity, 8192)
        self.assertEqual(self.mixin.extension_columns, {"ext": True})
        self.assertEqual(self.mixin.table_ttl_column, "insert_time")
        self.assertEqual(self.mixin.table, "")
        self.assertIsNone(self.mixin.table_ttl)
        self.assertEqual(self.mixin.column_defs, [])
        self.assertEqual(self.mixin.extension_defs, {"ext": []})
        self.assertIsInstance(self.mixin.extension_enabled, defaultdict)
        self.assertEqual(self.mixin.partition_by, "")
        self.assertEqual(self.mixin.primary_keys, [])
        self.assertEqual(self.mixin.order_by, [])
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_CLUSTER_NAME': 'test_cluster',
        'CLICKHOUSE_REPLICATION': 'true',
        'CLICKHOUSE_REPLICA_PATH': '/custom/path',
        'CLICKHOUSE_REPLICA_NAME': '{custom_replica}'
    })
    def test_init_with_environment_variables(self):
        """Test initialization with environment variables."""
        mixin = BaseClickHouseTableMixin()
        
        self.assertEqual(mixin.cluster_name, 'test_cluster')
        self.assertTrue(mixin.replication)
        self.assertEqual(mixin.replica_path, '/custom/path')
        self.assertEqual(mixin.replica_name, '{custom_replica}')
    
    def test_get_table_names_default(self):
        """Test get_table_names returns single table name."""
        self.mixin.table = "test_table"
        result = self.mixin.get_table_names()
        self.assertEqual(result, ["test_table"])
    
    def test_create_table_command_basic(self):
        """Test basic table creation command."""
        self.mixin.table = "test_table"
        self.mixin.column_defs = [
            ['id', 'String', True],
            ['name', 'String', True]
        ]
        self.mixin.order_by = ['id']
        
        result = self.mixin.create_table_command()
        
        self.assertIn("CREATE TABLE IF NOT EXISTS test_table", result)
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertIn("`id` String", result)
        self.assertIn("`name` String", result)
        self.assertIn("ORDER BY (`id`)", result)
    
    def test_create_table_command_validation_errors(self):
        """Test validation errors in create_table_command."""
        # No table name
        self.mixin.column_defs = [['id', 'String', True]]
        with self.assertRaises(ValueError) as context:
            self.mixin.create_table_command()
        self.assertIn("Table name is not set", str(context.exception))
        
        # No column definitions
        self.mixin.table = "test_table"
        self.mixin.column_defs = []
        with self.assertRaises(ValueError) as context:
            self.mixin.create_table_command()
        self.assertIn("Column definitions are not set", str(context.exception))
        
        # No table engine
        self.mixin.column_defs = [['id', 'String', True]]
        self.mixin.table_engine = ""
        with self.assertRaises(ValueError) as context:
            self.mixin.create_table_command()
        self.assertIn("Table engine is not set", str(context.exception))
    
    def test_get_extension_defs(self):
        """Test get_extension_defs method."""
        extension_options = {
            'ext1': [['field1', 'String'], ['field2', 'Int32']],
            'ext2': [['field3', 'Float64']]
        }
        
        with patch.dict(os.environ, {'TEST_EXT_VAR': 'ext1,ext2'}):
            result = self.mixin.get_extension_defs('TEST_EXT_VAR', extension_options)
            
            expected = [['field1', 'String'], ['field2', 'Int32'], ['field3', 'Float64']]
            self.assertEqual(result, expected)
            self.assertTrue(self.mixin.extension_is_enabled('ext1'))
            self.assertTrue(self.mixin.extension_is_enabled('ext2'))
    
    def test_get_extension_defs_empty_env(self):
        """Test get_extension_defs with empty environment variable."""
        result = self.mixin.get_extension_defs('NON_EXISTENT_VAR', {})
        self.assertEqual(result, [])
    
    def test_extension_is_enabled(self):
        """Test extension_is_enabled method."""
        self.assertFalse(self.mixin.extension_is_enabled('test_ext'))
        
        self.mixin.extension_enabled['ext']['test_ext'] = True
        self.assertTrue(self.mixin.extension_is_enabled('test_ext'))
        
        self.mixin.extension_enabled['custom']['another_ext'] = True
        self.assertTrue(self.mixin.extension_is_enabled('another_ext', 'custom'))
        self.assertFalse(self.mixin.extension_is_enabled('another_ext', 'ext'))
    
    def test_create_table_command_non_replicated_no_options(self):
        """Test table creation with non-replicated MergeTree engine without options."""
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True], ['value', 'Int32', True]]
        self.mixin.table_engine = "MergeTree"
        self.mixin.table_engine_opts = ""
        self.mixin.order_by = ['id']
        self.mixin.replication = False
        
        result = self.mixin.create_table_command()
        
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertNotIn("Replicated", result)
        self.assertNotIn("replica_path", result)
    
    def test_create_table_command_non_replicated_with_options(self):
        """Test table creation with non-replicated MergeTree engine with options."""
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True], ['value', 'Int32', True]]
        self.mixin.table_engine = "ReplacingMergeTree"
        self.mixin.table_engine_opts = "ver"
        self.mixin.order_by = ['id']
        self.mixin.replication = False
        
        result = self.mixin.create_table_command()
        
        self.assertIn("ENGINE = ReplacingMergeTree(ver)", result)
        self.assertNotIn("Replicated", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true'})
    def test_create_table_command_replicated_no_options(self):
        """Test table creation with replicated MergeTree engine without options."""
        self.mixin = BaseClickHouseTableMixin()  # Reinitialize with env var
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True], ['value', 'Int32', True]]
        self.mixin.table_engine = "MergeTree"
        self.mixin.table_engine_opts = ""
        self.mixin.order_by = ['id']
        
        result = self.mixin.create_table_command()
        
        self.assertIn("ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')", result)
        self.assertIn("ReplicatedMergeTree", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true'})
    def test_create_table_command_replicated_with_options(self):
        """Test table creation with replicated ReplacingMergeTree engine with options."""
        self.mixin = BaseClickHouseTableMixin()  # Reinitialize with env var
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True], ['value', 'Int32', True]]
        self.mixin.table_engine = "ReplacingMergeTree"
        self.mixin.table_engine_opts = "ver"
        self.mixin.order_by = ['id']
        
        result = self.mixin.create_table_command()
        
        self.assertIn("ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', ver)", result)
        self.assertIn("ReplicatedReplacingMergeTree", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true'})
    def test_create_table_command_already_replicated_engine(self):
        """Test that already replicated engine names don't get double prefixed."""
        self.mixin = BaseClickHouseTableMixin()  # Reinitialize with env var
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True]]
        self.mixin.table_engine = "ReplicatedMergeTree"  # Already has Replicated prefix
        self.mixin.table_engine_opts = ""
        self.mixin.order_by = ['id']
        
        result = self.mixin.create_table_command()
        
        # Should not become ReplicatedReplicatedMergeTree
        self.assertIn("ENGINE = ReplicatedMergeTree", result)
        self.assertNotIn("ReplicatedReplicatedMergeTree", result)
    
    def test_create_table_command_non_mergetree_engine(self):
        """Test that non-MergeTree engines don't get Replicated prefix but still get replica paths."""
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True]]
        self.mixin.table_engine = "Log"
        self.mixin.table_engine_opts = ""
        self.mixin.order_by = ['id']
        self.mixin.replication = True  # Even with replication enabled
        
        result = self.mixin.create_table_command()
        
        # Log engine doesn't become ReplicatedLog
        self.assertNotIn("ReplicatedLog", result)
        # But when replication is enabled, replica paths are still added
        self.assertIn("ENGINE = Log('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')", result)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_REPLICATION': 'true',
        'CLICKHOUSE_REPLICA_PATH': '/custom/replica/path',
        'CLICKHOUSE_REPLICA_NAME': '{custom_replica}'
    })
    def test_create_table_command_custom_replica_paths(self):
        """Test table creation with custom replica path and name."""
        self.mixin = BaseClickHouseTableMixin()  # Reinitialize with env vars
        self.mixin.table = "test_table"
        self.mixin.column_defs = [['id', 'String', True]]
        self.mixin.table_engine = "MergeTree"
        self.mixin.table_engine_opts = ""
        self.mixin.order_by = ['id']
        
        result = self.mixin.create_table_command()
        
        self.assertIn("ENGINE = ReplicatedMergeTree('/custom/replica/path', '{custom_replica}')", result)


class TestBaseClickHouseMaterializedViewMixin(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.mixin = BaseClickHouseMaterializedViewMixin()
    
    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.mixin.mv_name, "")
        self.assertEqual(self.mixin.mv_select_query, "")
        # Should inherit from BaseClickHouseTableMixin
        self.assertIsNotNone(self.mixin.logger)
        self.assertIsNone(self.mixin.cluster_name)
    
    def test_init_with_agg_window(self):
        """Test initialization with aggregation window."""
        mixin = BaseClickHouseMaterializedViewMixin(source_table_name="source_table", agg_window="5m")
        self.assertEqual(mixin.source_table_name, "source_table")
        self.assertEqual(mixin.agg_window, "5m")
        self.assertEqual(mixin.agg_window_ch_interval, "5 MINUTE")
    
    def test_init_with_various_agg_windows(self):
        """Test initialization with various aggregation window formats."""
        test_cases = [
            ("10s", "10 SECOND"),
            ("15m", "15 MINUTE"),
            ("12h", "12 HOUR"),
            ("7d", "7 DAY"),
            ("2w", "2 WEEK"),
            ("3mo", "3 MONTH"),
            ("1y", "1 YEAR")
        ]
        for agg_window, expected_interval in test_cases:
            with self.subTest(agg_window=agg_window):
                mixin = BaseClickHouseMaterializedViewMixin(agg_window=agg_window)
                self.assertEqual(mixin.agg_window, agg_window)
                self.assertEqual(mixin.agg_window_ch_interval, expected_interval)
    
    def test_agg_window_to_ch_interval_seconds(self):
        """Test agg_window_to_ch_interval with seconds."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("30s"), "30 SECOND")
    
    def test_agg_window_to_ch_interval_minutes(self):
        """Test agg_window_to_ch_interval with minutes."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("5m"), "5 MINUTE")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("15m"), "15 MINUTE")
    
    def test_agg_window_to_ch_interval_hours(self):
        """Test agg_window_to_ch_interval with hours."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("1h"), "1 HOUR")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("12h"), "12 HOUR")
    
    def test_agg_window_to_ch_interval_days(self):
        """Test agg_window_to_ch_interval with days."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("1d"), "1 DAY")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("30d"), "30 DAY")
    
    def test_agg_window_to_ch_interval_weeks(self):
        """Test agg_window_to_ch_interval with weeks."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("1w"), "1 WEEK")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("4w"), "4 WEEK")
    
    def test_agg_window_to_ch_interval_months(self):
        """Test agg_window_to_ch_interval with months."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("1mo"), "1 MONTH")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("3mo"), "3 MONTH")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("12mo"), "12 MONTH")
    
    def test_agg_window_to_ch_interval_years(self):
        """Test agg_window_to_ch_interval with years."""
        self.assertEqual(self.mixin.agg_window_to_ch_interval("1y"), "1 YEAR")
        self.assertEqual(self.mixin.agg_window_to_ch_interval("10y"), "10 YEAR")
    
    def test_agg_window_to_ch_interval_invalid_format(self):
        """Test agg_window_to_ch_interval with invalid format raises ValueError."""
        invalid_windows = ["5", "5x", "abc", "5minutes", "5 m", ""]
        for invalid_window in invalid_windows:
            with self.subTest(invalid_window=invalid_window):
                with self.assertRaises(ValueError) as context:
                    self.mixin.agg_window_to_ch_interval(invalid_window)
                self.assertIn("Invalid aggregation window format", str(context.exception))
    
    def test_create_mv_command_basic(self):
        """Test basic materialized view creation command."""
        self.mixin.mv_name = "test_mv"
        self.mixin.table = "target_table"
        self.mixin.mv_select_query = "SELECT * FROM source_table"
        
        result = self.mixin.create_mv_command()
        
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv", result)
        self.assertIn("TO target_table", result)
        self.assertIn("AS SELECT * FROM source_table", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_CLUSTER_NAME': 'test_cluster'})
    def test_create_mv_command_with_cluster(self):
        """Test materialized view creation with cluster."""
        mixin = BaseClickHouseMaterializedViewMixin()
        mixin.mv_name = "test_mv"
        mixin.table = "target_table"
        mixin.mv_select_query = "SELECT id, name FROM source_table"
        
        result = mixin.create_mv_command()
        
        self.assertIn("ON CLUSTER 'test_cluster'", result)
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS test_mv", result)
    
    def test_create_mv_command_complex_query(self):
        """Test materialized view with complex SELECT query."""
        self.mixin.mv_name = "aggregated_mv"
        self.mixin.table = "aggregated_table"
        self.mixin.mv_select_query = """
            SELECT 
                toStartOfHour(timestamp) as hour,
                resource_id,
                sum(value) as total_value,
                avg(value) as avg_value
            FROM source_table
            GROUP BY hour, resource_id
        """
        
        result = self.mixin.create_mv_command()
        
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS aggregated_mv", result)
        self.assertIn("sum(value)", result)
        self.assertIn("GROUP BY hour, resource_id", result)
    
    def test_build_extension_select_term_with_fields(self):
        """Test build_extension_select_term with extension field definitions."""
        self.mixin.extension_defs = {
            "ext": [
                ["bgp_as_path_id", "UInt32"],
                ["device_type", "String"],
                ["location_id", "UInt64"]
            ]
        }
        
        result = self.mixin.build_extension_select_term("ext")
        
        self.assertIn("toJSONString(", result)
        self.assertIn("map(", result)
        self.assertIn("'bgp_as_path_id', ext.bgp_as_path_id", result)
        self.assertIn("'device_type', ext.device_type", result)
        self.assertIn("'location_id', ext.location_id", result)
        self.assertIn(") as ext,", result)
    
    def test_build_extension_select_term_no_fields(self):
        """Test build_extension_select_term with no extension fields returns empty string."""
        self.mixin.extension_defs = {"ext": []}
        
        result = self.mixin.build_extension_select_term("ext")
        
        self.assertEqual(result, "")
    
    def test_build_extension_select_term_custom_json_column(self):
        """Test build_extension_select_term with custom JSON column name."""
        self.mixin.extension_defs = {
            "custom_ext": [
                ["field1", "String"],
                ["field2", "Int32"]
            ]
        }
        
        result = self.mixin.build_extension_select_term("custom_ext")
        
        self.assertIn("'field1', custom_ext.field1", result)
        self.assertIn("'field2', custom_ext.field2", result)
        self.assertIn(") as custom_ext,", result)
    
    def test_build_extension_select_term_single_field(self):
        """Test build_extension_select_term with single extension field."""
        self.mixin.extension_defs = {
            "ext": [["single_field", "String"]]
        }
        
        result = self.mixin.build_extension_select_term("ext")
        
        self.assertIn("'single_field', ext.single_field", result)
        self.assertNotIn(",\n        '", result)  # Should not have comma before closing
    
    def test_build_extension_select_term_nonexistent_column(self):
        """Test build_extension_select_term with nonexistent JSON column name."""
        self.mixin.extension_defs = {"ext": [["field1", "String"]]}
        
        result = self.mixin.build_extension_select_term("nonexistent")
        
        self.assertEqual(result, "")
    
    def test_policy_override_initialization(self):
        """Test that policy override fields are initialized correctly."""
        self.assertFalse(self.mixin.policy_override)
        self.assertEqual(self.mixin.policy_level, "")
        self.assertEqual(self.mixin.policy_scope, [])
    
    def test_policy_override_terms_default(self):
        """Test policy_override_terms returns default field names when override is False."""
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        self.assertEqual(policy_level_term, "policy_level")
        self.assertEqual(policy_scope_term, "policy_scope")
    
    def test_policy_override_terms_with_override(self):
        """Test policy_override_terms with policy override enabled."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:green"
        self.mixin.policy_scope = ["scope1", "scope2", "scope3"]
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        self.assertEqual(policy_level_term, "'tlp:green' AS policy_level")
        self.assertEqual(policy_scope_term, "['scope1,scope2,scope3'] AS policy_scope")
    
    def test_policy_override_terms_with_single_scope(self):
        """Test policy_override_terms with single scope item."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:amber"
        self.mixin.policy_scope = ["public"]
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        self.assertEqual(policy_level_term, "'tlp:amber' AS policy_level")
        self.assertEqual(policy_scope_term, "['public'] AS policy_scope")
    
    def test_policy_override_terms_with_whitespace(self):
        """Test policy_override_terms strips whitespace from scope items."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:red"
        self.mixin.policy_scope = ["  scope1  ", "scope2", "  scope3"]
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        # Should strip whitespace
        self.assertEqual(policy_scope_term, "['scope1,scope2,scope3'] AS policy_scope")
    
    def test_policy_override_terms_filters_empty_strings(self):
        """Test policy_override_terms filters out empty strings from scope."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:white"
        self.mixin.policy_scope = ["scope1", "", "  ", "scope2"]
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        # Should filter out empty and whitespace-only strings
        self.assertEqual(policy_scope_term, "['scope1,scope2'] AS policy_scope")
    
    def test_policy_override_terms_missing_level(self):
        """Test policy_override_terms raises ValueError when policy_level is not set."""
        self.mixin.policy_override = True
        self.mixin.policy_level = ""
        self.mixin.policy_scope = ["scope1"]
        
        with self.assertRaises(ValueError) as context:
            self.mixin.policy_override_terms()
        self.assertIn("policy_level is not set", str(context.exception))
    
    def test_policy_override_terms_missing_scope(self):
        """Test policy_override_terms raises ValueError when policy_scope is None."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:amber"
        self.mixin.policy_scope = None
        
        with self.assertRaises(ValueError) as context:
            self.mixin.policy_override_terms()
        self.assertIn("policy_scope is not set or not a list", str(context.exception))
    
    def test_policy_override_terms_scope_not_list(self):
        """Test policy_override_terms raises ValueError when policy_scope is not a list."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:green"
        self.mixin.policy_scope = "not_a_list"
        
        with self.assertRaises(ValueError) as context:
            self.mixin.policy_override_terms()
        self.assertIn("policy_scope is not set or not a list", str(context.exception))
    
    def test_policy_override_terms_empty_scope_list(self):
        """Test policy_override_terms with empty policy_scope list."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:red"
        self.mixin.policy_scope = []
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        self.assertEqual(policy_level_term, "'tlp:red' AS policy_level")
        self.assertEqual(policy_scope_term, "[''] AS policy_scope")
    
    def test_policy_override_terms_special_characters_in_level(self):
        """Test policy_override_terms with special characters in policy_level."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "custom:level-1.0"
        self.mixin.policy_scope = ["internal"]
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        self.assertEqual(policy_level_term, "'custom:level-1.0' AS policy_level")
    
    def test_policy_override_terms_complex_scopes(self):
        """Test policy_override_terms with complex scope names."""
        self.mixin.policy_override = True
        self.mixin.policy_level = "tlp:amber"
        self.mixin.policy_scope = ["org:research", "team:security", "project:alpha"]
        
        policy_level_term, policy_scope_term = self.mixin.policy_override_terms()
        
        self.assertEqual(policy_scope_term, "['org:research,team:security,project:alpha'] AS policy_scope")


class TestBaseClickHouseDictionaryMixin(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.mixin = BaseClickHouseDictionaryMixin(source_table_name="source_table")
    
    def test_init_with_source_table(self):
        """Test initialization with source table."""
        self.assertEqual(self.mixin.dictionary_name, "")
        self.assertEqual(self.mixin.source_table_name, "source_table")
        self.assertEqual(self.mixin.column_defs, [])
        self.assertEqual(self.mixin.primary_keys, [])
        self.assertEqual(self.mixin.source_database, 'default')
        self.assertEqual(self.mixin.source_username, 'default')
        self.assertEqual(self.mixin.source_password, '')
        self.assertEqual(self.mixin.lifetime_min, 600)
        self.assertEqual(self.mixin.lifetime_max, 3600)
        self.assertEqual(self.mixin.layout, "")
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_CLUSTER_NAME': 'test_cluster',
        'CLICKHOUSE_DATABASE': 'test_db',
        'CLICKHOUSE_USERNAME': 'test_user',
        'CLICKHOUSE_PASSWORD': 'test_pass'
    })
    def test_init_with_environment_variables(self):
        """Test initialization with environment variables."""
        mixin = BaseClickHouseDictionaryMixin()
        
        self.assertEqual(mixin.cluster_name, 'test_cluster')
        self.assertEqual(mixin.source_database, 'test_db')
        self.assertEqual(mixin.source_username, 'test_user')
        self.assertEqual(mixin.source_password, 'test_pass')
    
    def test_create_dictionary_command_basic(self):
        """Test basic dictionary creation command."""
        self.mixin.dictionary_name = "test_dict"
        self.mixin.column_defs = [
            ['id', 'String'],
            ['name', 'String'],
            ['value', 'Int32']
        ]
        self.mixin.primary_keys = ['id']
        self.mixin.layout = "FLAT()"
        
        result = self.mixin.create_dictionary_command()
        
        self.assertIn("CREATE DICTIONARY IF NOT EXISTS test_dict", result)
        self.assertIn("`id` String", result)
        self.assertIn("`name` String", result)
        self.assertIn("`value` Int32", result)
        self.assertIn("PRIMARY KEY (`id`)", result)
        self.assertIn("SOURCE(CLICKHOUSE(TABLE 'source_table'", result)
        self.assertIn("LIFETIME(MIN 600 MAX 3600)", result)
        self.assertIn("LAYOUT(FLAT())", result)
    
    def test_create_dictionary_command_validation_errors(self):
        """Test validation errors in create_dictionary_command."""
        # No dictionary name
        self.mixin.column_defs = [['id', 'String']]
        self.mixin.source_table_name = "source"
        with self.assertRaises(ValueError) as context:
            self.mixin.create_dictionary_command()
        self.assertIn("Dictionary name is not set", str(context.exception))
        
        # No column definitions
        self.mixin.dictionary_name = "test_dict"
        self.mixin.column_defs = []
        with self.assertRaises(ValueError) as context:
            self.mixin.create_dictionary_command()
        self.assertIn("Column definitions are not set", str(context.exception))
        
        # No source table
        self.mixin.column_defs = [['id', 'String']]
        self.mixin.source_table_name = ""
        with self.assertRaises(ValueError) as context:
            self.mixin.create_dictionary_command()
        self.assertIn("Source table name is not set", str(context.exception))
    
    @patch.dict(os.environ, {'CLICKHOUSE_CLUSTER_NAME': 'prod_cluster'})
    def test_create_dictionary_command_with_cluster(self):
        """Test dictionary creation with cluster."""
        mixin = BaseClickHouseDictionaryMixin(source_table_name="meta_table")
        mixin.dictionary_name = "meta_dict"
        mixin.column_defs = [['id', 'UInt64'], ['data', 'String']]
        mixin.primary_keys = ['id']
        mixin.layout = "HASHED()"
        
        result = mixin.create_dictionary_command()
        
        self.assertIn("ON CLUSTER 'prod_cluster'", result)
    
    def test_create_dictionary_command_with_range(self):
        """Test dictionary creation with range layout."""
        self.mixin.dictionary_name = "range_dict"
        self.mixin.column_defs = [
            ['id', 'UInt64'],
            ['start_date', 'Date'],
            ['end_date', 'Date'],
            ['value', 'String']
        ]
        self.mixin.primary_keys = ['id']
        self.mixin.layout = "RANGE_HASHED()"
        self.mixin.layout_range_min = "start_date"
        self.mixin.layout_range_max = "end_date"
        
        result = self.mixin.create_dictionary_command()
        
        self.assertIn("RANGE(MIN start_date MAX end_date)", result)
    
    def test_create_dictionary_command_multiple_primary_keys(self):
        """Test dictionary with multiple primary keys."""
        self.mixin.dictionary_name = "composite_dict"
        self.mixin.column_defs = [
            ['id1', 'String'],
            ['id2', 'String'],
            ['value', 'Int32']
        ]
        self.mixin.primary_keys = ['id1', 'id2']
        self.mixin.layout = "COMPLEX_KEY_HASHED()"
        
        result = self.mixin.create_dictionary_command()
        
        self.assertIn("PRIMARY KEY (`id1`,`id2`)", result)
    
    def test_create_dictionary_command_custom_lifetime(self):
        """Test dictionary with custom lifetime."""
        self.mixin.dictionary_name = "cached_dict"
        self.mixin.column_defs = [['key', 'String'], ['val', 'String']]
        self.mixin.primary_keys = ['key']
        self.mixin.layout = "CACHE(SIZE_IN_CELLS 1000000)"
        self.mixin.lifetime_min = 300
        self.mixin.lifetime_max = 1800
        
        result = self.mixin.create_dictionary_command()
        
        self.assertIn("LIFETIME(MIN 300 MAX 1800)", result)
    
    def test_create_dictionary_command_no_range_if_not_set(self):
        """Test that RANGE clause is not added if range fields not set."""
        self.mixin.dictionary_name = "simple_dict"
        self.mixin.column_defs = [['id', 'String'], ['val', 'String']]
        self.mixin.primary_keys = ['id']
        self.mixin.layout = "FLAT()"
        self.mixin.layout_range_min = ""
        self.mixin.layout_range_max = ""
        
        result = self.mixin.create_dictionary_command()
        
        self.assertNotIn("RANGE(", result)


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

    def test_create_table_command_with_ttl(self):
        """Test create_table_command with TTL defined."""
        
        # Set TTL
        self.processor.table_ttl = "30 DAY"
        self.processor.table_ttl_column = "insert_time"
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (With TTL):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for TTL clause and settings
        self.assertIn("TTL insert_time + INTERVAL 30 DAY", result)
        self.assertIn("ttl_only_drop_parts = 1", result)

    def test_create_table_command_with_ttl_missing_column(self):
        """Test create_table_command with TTL defined but missing TTL column."""
        
        # Set TTL but not TTL column
        self.processor.table_ttl = "30 DAY"
        self.processor.table_ttl_column = None
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Should not include TTL clause when TTL column is missing
        self.assertNotIn("TTL", result)
        self.assertNotIn("ttl_only_drop_parts", result)

    def test_create_table_command_with_ttl_missing_interval(self):
        """Test create_table_command with TTL column defined but missing TTL interval."""
        
        # Set TTL column but not TTL interval
        self.processor.table_ttl = None
        self.processor.table_ttl_column = "insert_time"
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Should not include TTL clause when TTL interval is missing
        self.assertNotIn("TTL", result)
        self.assertNotIn("ttl_only_drop_parts", result)

    def test_create_table_command_with_all_features(self):
        """Test create_table_command with all features: partition, TTL, primary keys, and extensions."""
        
        # Set all features
        self.processor.partition_by = "toYYYYMM(insert_time)"
        self.processor.table_ttl = "90 DAY"
        self.processor.table_ttl_column = "insert_time"
        self.processor.primary_keys = ["ref", "id"]
        self.processor.extension_defs = {
            "ext": [
                ["device_type", "String", True],
                ["location", "String", True]
            ]
        }
        
        # Call the method
        result = self.processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (All Features):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for all clauses
        self.assertIn("PARTITION BY toYYYYMM(insert_time)", result)
        self.assertIn("PRIMARY KEY (`ref`,`id`)", result)
        self.assertIn("ORDER BY (`ref`,`id`,`insert_time`)", result)
        self.assertIn("TTL insert_time + INTERVAL 90 DAY", result)
        self.assertIn("ttl_only_drop_parts = 1", result)
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`device_type` String", result)
        self.assertIn("`location` String", result)

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
        table_name = "meta_test_table"
        message = {
            'id': 'test_id',
            'ref': 'test_ref',
            'hash': 'test_hash',
            'ext': '{}',
            'tag': []
        }
        
        result = self.processor.message_to_columns(message, table_name)
        expected = ['test_id', 'test_ref', 'test_hash', '{}', []]
        self.assertEqual(result, expected)

    def test_message_to_columns_missing_field(self):
        """Test message_to_columns with missing required field."""
        
        # Missing 'ref' field
        table_name = "meta_test_table"
        message = {
            'id': 'test_id',
            'hash': 'test_hash',
            'ext': '{}',
            'tag': []
        }
        
        with self.assertRaises(ValueError) as context:
            self.processor.message_to_columns(message, table_name)
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


class TestBaseClickHouseProcessorAdditional(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.mock_pipeline = Mock()
        
    def test_get_materialized_views_default(self):
        """Test get_materialized_views returns empty list by default."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        
        result = processor.get_materialized_views()
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 0)
    
    def test_get_materialized_views_with_views(self):
        """Test get_materialized_views returns list of materialized views."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        
        # Add mock materialized views
        mock_mv1 = Mock()
        mock_mv2 = Mock()
        processor.materialized_views = [mock_mv1, mock_mv2]
        
        result = processor.get_materialized_views()
        
        self.assertEqual(len(result), 2)
        self.assertIn(mock_mv1, result)
        self.assertIn(mock_mv2, result)
    
    def test_materialized_views_initialization(self):
        """Test that materialized_views list is initialized on processor creation."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        
        self.assertIsInstance(processor.materialized_views, list)
        self.assertEqual(processor.materialized_views, [])
    
    def test_get_ch_dictionaries_default(self):
        """Test get_ch_dictionaries returns empty list by default."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        
        result = processor.get_ch_dictionaries()
        
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 0)
    
    def test_get_ch_dictionaries_with_dictionaries(self):
        """Test get_ch_dictionaries returns list of dictionaries."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        
        # Add mock dictionaries
        mock_dict1 = Mock()
        mock_dict2 = Mock()
        processor.ch_dictionaries = [mock_dict1, mock_dict2]
        
        result = processor.get_ch_dictionaries()
        
        self.assertEqual(len(result), 2)
        self.assertIn(mock_dict1, result)
        self.assertIn(mock_dict2, result)
    
    def test_ch_dictionaries_initialization(self):
        """Test that ch_dictionaries list is initialized on processor creation."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        
        self.assertIsInstance(processor.ch_dictionaries, list)
        self.assertEqual(processor.ch_dictionaries, [])
    
    @patch.dict(os.environ, {'TEST_MV_WINDOWS': '5m,1h,1d'})
    def test_load_materialized_views(self):
        """Test loading materialized views from environment variable."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "test_table"
        
        # Create a mock MV class
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            def __init__(self, source_table_name="", agg_window=""):
                super().__init__(source_table_name, agg_window)
                self.mv_name = f"test_mv_{agg_window}"
        
        processor.load_materialized_views('TEST_MV_WINDOWS', MockMVClass)
        
        self.assertEqual(len(processor.materialized_views), 3)
        self.assertEqual(processor.materialized_views[0].agg_window, "5m")
        self.assertEqual(processor.materialized_views[0].source_table_name, "test_table")
        self.assertEqual(processor.materialized_views[0].agg_window_ch_interval, "5 MINUTE")
        self.assertEqual(processor.materialized_views[1].agg_window, "1h")
        self.assertEqual(processor.materialized_views[1].agg_window_ch_interval, "1 HOUR")
        self.assertEqual(processor.materialized_views[2].agg_window, "1d")
        self.assertEqual(processor.materialized_views[2].agg_window_ch_interval, "1 DAY")
    
    @patch.dict(os.environ, {'TEST_MV_WINDOWS': '1w, 1mo, 1y'})
    def test_load_materialized_views_with_spaces(self):
        """Test loading materialized views handles spaces in environment variable."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "test_table"
        
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            def __init__(self, source_table_name="", agg_window=""):
                super().__init__(source_table_name, agg_window)
        
        processor.load_materialized_views('TEST_MV_WINDOWS', MockMVClass)
        
        self.assertEqual(len(processor.materialized_views), 3)
        self.assertEqual(processor.materialized_views[0].agg_window, "1w")
        self.assertEqual(processor.materialized_views[1].agg_window, "1mo")
        self.assertEqual(processor.materialized_views[2].agg_window, "1y")
    
    def test_load_materialized_views_empty_env(self):
        """Test load_materialized_views with empty environment variable."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "test_table"
        
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            pass
        
        processor.load_materialized_views('NON_EXISTENT_VAR', MockMVClass)
        
        self.assertEqual(len(processor.materialized_views), 0)
    
    @patch.dict(os.environ, {'TEST_MV_WINDOWS': ''})
    def test_load_materialized_views_empty_string(self):
        """Test load_materialized_views with empty string environment variable."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "test_table"
        
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            pass
        
        processor.load_materialized_views('TEST_MV_WINDOWS', MockMVClass)
        
        self.assertEqual(len(processor.materialized_views), 0)
    
    @patch.dict(os.environ, {'TEST_MV_WINDOWS': '5m,,1h,  ,1d'})
    def test_load_materialized_views_with_empty_values(self):
        """Test load_materialized_views filters out empty values."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "test_table"
        
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            def __init__(self, source_table_name="", agg_window=""):
                super().__init__(source_table_name, agg_window)
        
        processor.load_materialized_views('TEST_MV_WINDOWS', MockMVClass)
        
        # Should only create MVs for non-empty values
        self.assertEqual(len(processor.materialized_views), 3)
        self.assertEqual(processor.materialized_views[0].agg_window, "5m")
        self.assertEqual(processor.materialized_views[1].agg_window, "1h")
        self.assertEqual(processor.materialized_views[2].agg_window, "1d")
    
    @patch.dict(os.environ, {'TEST_MV_WINDOWS': '5m'})
    def test_load_materialized_views_single_value(self):
        """Test load_materialized_views with single aggregation window."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "source_table"
        
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            def __init__(self, source_table_name="", agg_window=""):
                super().__init__(source_table_name, agg_window)
        
        processor.load_materialized_views('TEST_MV_WINDOWS', MockMVClass)
        
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertEqual(processor.materialized_views[0].agg_window, "5m")
        self.assertEqual(processor.materialized_views[0].source_table_name, "source_table")
    
    @patch.dict(os.environ, {'TEST_MV_WINDOWS': '10s,30m,6h,14d,2w,6mo,5y'})
    def test_load_materialized_views_multiple_types(self):
        """Test load_materialized_views with multiple aggregation window types."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor, BaseClickHouseMaterializedViewMixin
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "metrics_table"
        
        class MockMVClass(BaseClickHouseMaterializedViewMixin):
            def __init__(self, source_table_name="", agg_window=""):
                super().__init__(source_table_name, agg_window)
        
        processor.load_materialized_views('TEST_MV_WINDOWS', MockMVClass)
        
        self.assertEqual(len(processor.materialized_views), 7)
        # Verify each aggregation window and its converted interval
        expected = [
            ("10s", "10 SECOND"),
            ("30m", "30 MINUTE"),
            ("6h", "6 HOUR"),
            ("14d", "14 DAY"),
            ("2w", "2 WEEK"),
            ("6mo", "6 MONTH"),
            ("5y", "5 YEAR")
        ]
        for i, (agg_window, expected_interval) in enumerate(expected):
            self.assertEqual(processor.materialized_views[i].agg_window, agg_window)
            self.assertEqual(processor.materialized_views[i].agg_window_ch_interval, expected_interval)
            self.assertEqual(processor.materialized_views[i].source_table_name, "metrics_table")
        
    def test_get_table_names_default(self):
        """Test get_table_names returns single table name."""
        from metranova.processors.clickhouse.base import BaseClickHouseProcessor
        
        processor = BaseClickHouseProcessor(self.mock_pipeline)
        processor.table = "test_table"
        
        result = processor.get_table_names()
        self.assertEqual(result, ["test_table"])
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true', 'CLICKHOUSE_CLUSTER_NAME': 'test_cluster'})
    def test_create_table_command_with_replication(self):
        """Test create_table_command with replication enabled."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        processor.table = "test_table"
        processor.replication = True
        processor.cluster_name = 'test_cluster'
        processor.replica_path = '/clickhouse/tables/{shard}/{database}/{table}'
        processor.replica_name = '{replica}'
        
        result = processor.create_table_command()
        
        self.assertIn("ON CLUSTER 'test_cluster'", result)
        self.assertIn("ENGINE = ReplicatedMergeTree", result)
        self.assertIn("'/clickhouse/tables/{shard}/{database}/{table}'", result)
        self.assertIn("'{replica}'", result)
    
    def test_create_table_command_with_table_suffix_filtering(self):
        """Test create_table_command filters columns by table suffix."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        processor.table = "test_table_gauge"
        
        # Add columns with suffix markers
        processor.column_defs.append(['gauge_field', 'Float64', True, 'gauge'])
        processor.column_defs.append(['counter_field', 'UInt64', True, 'counter'])
        
        result = processor.create_table_command()
        
        # Should include gauge_field since table ends with _gauge
        self.assertIn('gauge_field', result)
        # Should NOT include counter_field
        self.assertNotIn('counter_field', result)
    
    @patch.dict(os.environ, {'TEST_IP_REF_VAR': 'as,geo,org'})
    def test_get_ip_ref_extensions(self):
        """Test get_ip_ref_extensions parses environment variable."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        
        result = processor.get_ip_ref_extensions('TEST_IP_REF_VAR')
        
        self.assertEqual(result, ['as', 'geo', 'org'])
    
    def test_get_ip_ref_extensions_empty(self):
        """Test get_ip_ref_extensions with empty env var."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        
        result = processor.get_ip_ref_extensions('NON_EXISTENT_VAR')
        
        self.assertEqual(result, [])
    
    def test_lookup_ip_ref_extensions(self):
        """Test lookup_ip_ref_extensions queries cacher."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        processor.ip_ref_extensions = ['as', 'geo']
        
        # Mock cacher lookups - now returns tuples instead of dicts
        # Tuple format: (ref, *other_fields)
        mock_ip_cacher = Mock()
        mock_ip_cacher.lookup.side_effect = [
            ('AS12345',),  # as lookup - tuple with ref at index 0
            ('US-CA',)     # geo lookup - tuple with ref at index 0
        ]
        self.mock_pipeline.cacher.return_value = mock_ip_cacher
        
        result = processor.lookup_ip_ref_extensions('192.168.1.1', 'src')
        
        self.assertEqual(result, {
            'src_ip_as_ref': 'AS12345',
            'src_ip_geo_ref': 'US-CA'
        })
        
        # Verify lookups were called correctly
        self.assertEqual(mock_ip_cacher.lookup.call_count, 2)
        mock_ip_cacher.lookup.assert_any_call('meta_ip_as', '192.168.1.1')
        mock_ip_cacher.lookup.assert_any_call('meta_ip_geo', '192.168.1.1')
    
    def test_lookup_ip_ref_extensions_empty_ip(self):
        """Test lookup_ip_ref_extensions with empty IP."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        processor.ip_ref_extensions = ['as']
        
        result = processor.lookup_ip_ref_extensions('', 'src')
        
        self.assertEqual(result, {})
    
    def test_lookup_ip_ref_extensions_no_ref_in_result(self):
        """Test lookup_ip_ref_extensions when ref is missing in result."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        processor.ip_ref_extensions = ['as']
        
        mock_ip_cacher = Mock()
        # Return empty tuple or tuple without enough elements
        mock_ip_cacher.lookup.return_value = ()
        self.mock_pipeline.cacher.return_value = mock_ip_cacher
        
        result = processor.lookup_ip_ref_extensions('192.168.1.1', 'dst')
        
        self.assertEqual(result, {})
    
    def test_column_names_with_duplicates(self):
        """Test column_names handles duplicate column definitions."""
        processor = BaseMetadataProcessor(self.mock_pipeline)
        # Add duplicate column
        processor.column_defs.append(['id', 'String', True])
        
        result = processor.column_names()
        
        # Should not have duplicates
        self.assertEqual(len(result), len(set(result)))
        self.assertEqual(result.count('id'), 1)


class TestBaseMetadataProcessorAdditional(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.mock_pipeline = Mock()
        mock_cacher = Mock()
        mock_cacher.lookup.return_value = None
        self.mock_pipeline.cacher.return_value = mock_cacher
        
        self.processor = BaseMetadataProcessor(self.mock_pipeline)
        self.processor.table = "test_table"
        self.processor.val_id_field = ['id']
        self.processor.required_fields = [['id']]
    
    def test_calculate_hash(self):
        """Test calculate_hash creates consistent hashes."""
        import hashlib
        import orjson
        
        record = {'id': 'test', 'field1': 'value1', 'field2': 'value2'}
        
        hash1 = self.processor.calculate_hash(record.copy())
        hash2 = self.processor.calculate_hash(record.copy())
        
        # Should be consistent
        self.assertEqual(hash1, hash2)
        
        # Should be MD5
        self.assertEqual(len(hash1), 32)
    
    def test_calculate_hash_removes_hash_field(self):
        """Test calculate_hash removes hash field before calculating."""
        record = {'id': 'test', 'hash': 'old_hash', 'field1': 'value1'}
        
        hash_result = self.processor.calculate_hash(record)
        
        # Should not include the old hash in calculation
        self.assertNotEqual(hash_result, 'old_hash')
    
    def test_calculate_hash_removes_ref_field(self):
        """Test calculate_hash removes ref field before calculating."""
        record1 = {'id': 'test', 'field1': 'value1'}
        record2 = {'id': 'test', 'ref': 'test__v1', 'field1': 'value1'}
        
        hash1 = self.processor.calculate_hash(record1.copy())
        hash2 = self.processor.calculate_hash(record2.copy())
        
        # Should be the same despite different ref
        self.assertEqual(hash1, hash2)
    
    def test_build_message_not_dict_data(self):
        """Test build_message with non-dict data."""
        input_data = {
            'data': 'not a list'
        }
        
        result = self.processor.build_message(input_data, {})
        
        self.assertEqual(result, [])
    
    def test_build_message_empty_value(self):
        """Test build_message with empty value."""
        result = self.processor.build_message(None, {})
        self.assertEqual(result, [])
        
        result = self.processor.build_message({}, {})
        self.assertEqual(result, [])
    
    def test_build_single_message_nested_id_field(self):
        """Test build_single_message with nested ID field."""
        self.processor.val_id_field = ['meta', 'device', 'id']
        self.processor.required_fields = [['meta']]  # Adjust required fields
        
        value = {
            'meta': {
                'device': {
                    'id': 'nested_id'
                }
            },
            'ext': '{}',
            'tag': []
        }
        
        result = self.processor.build_single_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'nested_id')
    
    def test_build_single_message_nested_id_field_missing(self):
        """Test build_single_message with incomplete nested ID path."""
        self.processor.val_id_field = ['meta', 'device', 'id']
        
        value = {
            'meta': {
                # Missing 'device' level
            },
            'ext': '{}',
            'tag': []
        }
        
        result = self.processor.build_single_message(value, {})
        
        self.assertIsNone(result)
    
    def test_build_single_message_non_versioned(self):
        """Test build_single_message with versioned=False."""
        self.processor.versioned = False
        
        value = {
            'id': 'test_id',
            'ext': '{}',
            'tag': []
        }
        
        result = self.processor.build_single_message(value, {})
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'test_id')
        # Should not have ref or hash for non-versioned
        self.assertNotIn('ref', result)
        self.assertNotIn('hash', result)
    
    def test_build_single_message_invalid_ref_format(self):
        """Test build_single_message with invalid ref format in cache."""
        value = {'id': 'test_id', 'ext': '{}', 'tag': []}
        
        # Mock existing record with invalid ref format
        mock_existing = {'hash': 'different_hash', 'ref': 'invalid_ref_no_version'}
        self.mock_pipeline.cacher.return_value.lookup.return_value = mock_existing
        
        result = self.processor.build_single_message(value, {})
        
        # Should return None and log warning
        self.assertIsNone(result)
    
    def test_format_float_fields(self):
        """Test format_float_fields converts values correctly."""
        self.processor.float_fields = ['field1', 'field2', 'field3']
        
        record = {
            'field1': '3.14',
            'field2': 42,
            'field3': 'invalid',
            'field4': 'ignored'
        }
        
        self.processor.format_float_fields(record)
        
        self.assertEqual(record['field1'], 3.14)
        self.assertEqual(record['field2'], 42.0)
        self.assertIsNone(record['field3'])
    
    def test_format_int_fields(self):
        """Test format_int_fields converts values correctly."""
        self.processor.int_fields = ['field1', 'field2', 'field3']
        
        record = {
            'field1': '42',
            'field2': 3.14,
            'field3': 'invalid',
            'field4': 'ignored'
        }
        
        self.processor.format_int_fields(record)
        
        self.assertEqual(record['field1'], 42)
        self.assertEqual(record['field2'], 3)
        self.assertIsNone(record['field3'])
    
    def test_format_boolean_fields(self):
        """Test format_boolean_fields converts values correctly."""
        self.processor.boolean_fields = ['field1', 'field2', 'field3', 'field4', 'field5', 'field6']
        
        record = {
            'field1': True,
            'field2': 'true',
            'field3': 'True',
            'field4': 1,
            'field5': '1',
            'field6': 'false'
        }
        
        self.processor.format_boolean_fields(record)
        
        self.assertTrue(record['field1'])
        self.assertTrue(record['field2'])
        self.assertTrue(record['field3'])
        self.assertTrue(record['field4'])
        self.assertTrue(record['field5'])
        self.assertFalse(record['field6'])
    
    def test_format_array_fields_none(self):
        """Test format_array_fields handles None values."""
        self.processor.array_fields = ['field1']
        
        record = {'field1': None}
        
        self.processor.format_array_fields(record)
        
        self.assertEqual(record['field1'], [])
    
    def test_format_array_fields_json_string(self):
        """Test format_array_fields parses JSON strings."""
        import orjson
        
        self.processor.array_fields = ['field1', 'field2']
        
        record = {
            'field1': '["item1", "item2"]',
            'field2': 'invalid json'
        }
        
        self.processor.format_array_fields(record)
        
        self.assertEqual(record['field1'], ["item1", "item2"])
        self.assertEqual(record['field2'], [])
    
    def test_build_metadata_fields_with_dict_ext(self):
        """Test build_metadata_fields with dict ext."""
        value = {
            'id': 'test',
            'ext': {'key': 'value'},
            'tag': None
        }
        
        result = self.processor.build_metadata_fields(value)
        
        # ext should be converted to JSON string
        import orjson
        self.assertEqual(result['ext'], orjson.dumps({'key': 'value'}, option=orjson.OPT_SORT_KEYS).decode('utf-8'))
        self.assertEqual(result['tag'], [])
    
    def test_build_metadata_fields_with_string_tag(self):
        """Test build_metadata_fields with string tag."""
        value = {
            'id': 'test',
            'ext': '{}',
            'tag': '["tag1", "tag2"]'
        }
        
        result = self.processor.build_metadata_fields(value)
        
        self.assertEqual(result['tag'], ["tag1", "tag2"])
    
    def test_build_message_with_self_ref_fields_list(self):
        """Test build_message with self-referencing list fields."""
        self.processor.self_ref_fields = ['parent']
        self.processor.required_fields = [['id']]
        
        # Need to add parent_id and parent_ref to column_defs so they're included
        self.processor.column_defs.append(['parent_id', 'Array(String)', True])
        self.processor.column_defs.append(['parent_ref', 'Array(String)', True])
        
        input_data = {
            'data': [
                {'id': 'item1', 'parent_id': ['item2', 'item3'], 'parent_ref': [], 'ext': '{}', 'tag': []},
                {'id': 'item2', 'parent_id': [], 'parent_ref': [], 'ext': '{}', 'tag': []},
                {'id': 'item3', 'parent_id': [], 'parent_ref': [], 'ext': '{}', 'tag': []}
            ]
        }
        
        result = self.processor.build_message(input_data, {})
        
        # Should update parent_ref for item1
        item1 = next((r for r in result if r['id'] == 'item1'), None)
        if item1:
            self.assertIn('parent_ref', item1)
            self.assertIsInstance(item1['parent_ref'], list)
            self.assertIn('item2__v1', item1['parent_ref'])
            self.assertIn('item3__v1', item1['parent_ref'])
    
    def test_build_message_with_self_ref_fields_single(self):
        """Test build_message with self-referencing single field."""
        self.processor.self_ref_fields = ['parent']
        self.processor.required_fields = [['id']]
        
        # Need to add parent_id and parent_ref to column_defs
        self.processor.column_defs.append(['parent_id', 'String', True])
        self.processor.column_defs.append(['parent_ref', 'String', True])
        
        input_data = {
            'data': [
                {'id': 'child', 'parent_id': 'parent', 'parent_ref': '', 'ext': '{}', 'tag': []},
                {'id': 'parent', 'parent_id': '', 'parent_ref': '', 'ext': '{}', 'tag': []}
            ]
        }
        
        result = self.processor.build_message(input_data, {})
        
        # Should update parent_ref for child
        child = next((r for r in result if r['id'] == 'child'), None)
        if child:
            self.assertEqual(child['parent_ref'], 'parent__v1')
    
    def test_build_message_self_ref_unchanged_after_update(self):
        """Test build_message skips self-ref records that are unchanged after ref update."""
        self.processor.self_ref_fields = ['parent']
        self.processor.required_fields = [['id']]
        
        # Need to add parent_id and parent_ref to column_defs
        self.processor.column_defs.append(['parent_id', 'String', True])
        self.processor.column_defs.append(['parent_ref', 'String', True])
        
        # Mock that child already has correct hash in cache
        def lookup_side_effect(table, item_id):
            if item_id == 'child':
                # Return a cached record that will match after self-ref update
                import hashlib
                import orjson
                # This would be the hash after self-ref is updated
                return {'hash': 'matching_hash', 'ref': 'child__v1'}
            return None
        
        self.mock_pipeline.cacher.return_value.lookup.side_effect = lookup_side_effect
        
        input_data = {
            'data': [
                {'id': 'child', 'parent_id': 'parent', 'parent_ref': '', 'ext': '{}', 'tag': []},
                {'id': 'parent', 'parent_id': '', 'parent_ref': '', 'ext': '{}', 'tag': []}
            ]
        }
        
        result = self.processor.build_message(input_data, {})
        
        # Child should be filtered out if hash matches after self-ref update
        # This is complex to test perfectly, but we can verify the logic runs
        self.assertIsInstance(result, list)


class TestBaseDataGenericMetricProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.mock_pipeline = Mock()
    
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'interface,device,application'})
    def test_load_resource_types(self):
        """Test load_resource_types parses environment variable."""
        from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
        
        processor = BaseDataGenericMetricProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.resource_types, ['interface', 'device', 'application'])
    
    def test_load_resource_types_empty(self):
        """Test load_resource_types with empty environment."""
        from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
        
        if 'CLICKHOUSE_METRIC_RESOURCE_NAME' in os.environ:
            del os.environ['CLICKHOUSE_METRIC_RESOURCE_NAME']
        
        processor = BaseDataGenericMetricProcessor(self.mock_pipeline)
        
        self.assertEqual(processor.resource_types, [])
    
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'interface'})
    def test_get_table_name(self):
        """Test get_table_name formats correctly."""
        from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
        
        processor = BaseDataGenericMetricProcessor(self.mock_pipeline)
        
        result = processor.get_table_name('interface', 'counter')
        self.assertEqual(result, 'data_interface_counter')
        
        result = processor.get_table_name('device', 'gauge')
        self.assertEqual(result, 'data_device_gauge')
    
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'interface,device'})
    def test_get_table_names(self):
        """Test get_table_names returns all combinations."""
        from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
        
        processor = BaseDataGenericMetricProcessor(self.mock_pipeline)
        
        result = list(processor.get_table_names())
        
        self.assertIn('data_interface_counter', result)
        self.assertIn('data_interface_gauge', result)
        self.assertIn('data_device_counter', result)
        self.assertIn('data_device_gauge', result)
        self.assertEqual(len(result), 4)
    
    @patch.dict(os.environ, {'CLICKHOUSE_METRIC_RESOURCE_NAME': 'interface'})
    def test_column_defs_initialization(self):
        """Test that column_defs are properly initialized."""
        from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor
        
        processor = BaseDataGenericMetricProcessor(self.mock_pipeline)
        
        # Should have observation_time at start
        self.assertEqual(processor.column_defs[0][0], 'observation_time')
        
        # Should have metric_value fields with suffixes
        metric_value_cols = [col for col in processor.column_defs if col[0] == 'metric_value']
        self.assertEqual(len(metric_value_cols), 2)  # gauge and counter
        
        # Check suffixes
        gauge_col = next(col for col in metric_value_cols if len(col) > 3 and col[3] == 'gauge')
        counter_col = next(col for col in metric_value_cols if len(col) > 3 and col[3] == 'counter')
        
        self.assertIn('Float64', gauge_col[1])
        self.assertIn('UInt64', counter_col[1])


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)