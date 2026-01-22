#!/usr/bin/env python3

import unittest
import os
from unittest.mock import patch, MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.flow import BaseFlowProcessor


class TestBaseFlowProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline
        self.mock_pipeline = MagicMock()
        
    def test_create_table_command_basic(self):
        """Test the create_table_command method with default settings (no extensions)."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - Basic):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Assertions to verify the command structure
        self.assertIn("CREATE TABLE IF NOT EXISTS data_flow", result)
        self.assertIn("ENGINE = MergeTree()", result)
        self.assertIn("PARTITION BY toYYYYMMDD(start_time)", result)
        self.assertIn("ORDER BY (`src_as_id`,`dst_as_id`,`src_ip`,`dst_ip`,`start_time`)", result)
        self.assertIn("SETTINGS index_granularity = 8192", result)
        
        # Check for specific core columns
        self.assertIn("`start_time` DateTime64(3, 'UTC')", result)
        self.assertIn("`end_time` DateTime64(3, 'UTC')", result)
        self.assertIn("`insert_time` DateTime64(3, 'UTC') DEFAULT now64()", result)
        self.assertIn("`collector_id` LowCardinality(String)", result)
        self.assertIn("`policy_originator` LowCardinality(String)", result)
        self.assertIn("`policy_level` LowCardinality(String)", result)
        self.assertIn("`policy_scope` Array(LowCardinality(String))", result)
        self.assertIn("`flow_type` LowCardinality(String)", result)
        self.assertIn("`device_id` LowCardinality(String)", result)
        self.assertIn("`device_ref` Nullable(String)", result)
        self.assertIn("`src_as_id` UInt32", result)
        self.assertIn("`src_as_ref` Nullable(String)", result)
        self.assertIn("`src_ip` IPv6", result)
        self.assertIn("`src_ip_ref` Nullable(String)", result)
        self.assertIn("`src_port` UInt16", result)
        self.assertIn("`dst_as_id` UInt32", result)
        self.assertIn("`dst_as_ref` Nullable(String)", result)
        self.assertIn("`dst_ip` IPv6", result)
        self.assertIn("`dst_ip_ref` Nullable(String)", result)
        self.assertIn("`dst_port` UInt16", result)
        self.assertIn("`protocol` LowCardinality(String)", result)
        self.assertIn("`in_interface_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`in_interface_ref` Nullable(String)", result)
        self.assertIn("`out_interface_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`out_interface_ref` Nullable(String)", result)
        self.assertIn("`peer_as_id` Nullable(UInt32)", result)
        self.assertIn("`peer_as_ref` Nullable(String)", result)
        self.assertIn("`peer_ip` Nullable(IPv6)", result)
        self.assertIn("`peer_ip_ref` Nullable(String)", result)
        self.assertIn("`ip_version` UInt8", result)
        self.assertIn("`application_port` UInt16", result)
        self.assertIn("`bit_count` UInt64", result)
        self.assertIn("`packet_count` UInt64", result)
        self.assertIn("`ext` JSON", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'bgp'})
    def test_create_table_command_with_bgp_extensions(self):
        """Test the create_table_command method with BGP extensions enabled."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - BGP Extensions):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for BGP extension columns
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`bgp_as_path_id` Array(UInt32)", result)
        self.assertIn("`bgp_as_path_padding` Array(UInt16)", result)
        self.assertIn("`bgp_community` Array(LowCardinality(String))", result)
        self.assertIn("`bgp_ext_community` Array(LowCardinality(String))", result)
        self.assertIn("`bgp_large_community` Array(LowCardinality(String))", result)
        self.assertIn("`bgp_local_pref` Nullable(UInt32)", result)
        self.assertIn("`bgp_med` Nullable(UInt32)", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'ipv4,ipv6'})
    def test_create_table_command_with_ip_extensions(self):
        """Test the create_table_command method with IPv4 and IPv6 extensions enabled."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - IP Extensions):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for IP extension columns
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`ipv4_dscp` Nullable(UInt8)", result)
        self.assertIn("`ipv4_tos` Nullable(UInt8)", result)
        self.assertIn("`ipv6_flow_label` Nullable(UInt32)", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'mpls'})
    def test_create_table_command_with_mpls_extensions(self):
        """Test the create_table_command method with MPLS extensions enabled."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - MPLS Extensions):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for MPLS extension columns
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`mpls_bottom_label` Nullable(UInt32)", result)
        self.assertIn("`mpls_exp` Array(UInt8)", result)
        self.assertIn("`mpls_label` Array(UInt32)", result)
        self.assertIn("`mpls_pw` Nullable(UInt32)", result)
        self.assertIn("`mpls_top_label_ip` Nullable(IPv6)", result)
        self.assertIn("`mpls_top_label_type` Nullable(UInt32)", result)
        self.assertIn("`mpls_vpn_rd` LowCardinality(Nullable(String))", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'bgp,ipv4,ipv6,mpls'})
    def test_create_table_command_with_all_extensions(self):
        """Test the create_table_command method with all extensions enabled."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - All Extensions):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for all extension columns
        self.assertIn("`ext` JSON(", result)
        
        # BGP columns
        self.assertIn("`bgp_as_path_id` Array(UInt32)", result)
        self.assertIn("`bgp_as_path_padding` Array(UInt16)", result)
        self.assertIn("`bgp_community` Array(LowCardinality(String))", result)
        self.assertIn("`bgp_ext_community` Array(LowCardinality(String))", result)
        self.assertIn("`bgp_large_community` Array(LowCardinality(String))", result)
        self.assertIn("`bgp_local_pref` Nullable(UInt32)", result)
        self.assertIn("`bgp_med` Nullable(UInt32)", result)
        
        # IP columns
        self.assertIn("`ipv4_dscp` Nullable(UInt8)", result)
        self.assertIn("`ipv4_tos` Nullable(UInt8)", result)
        self.assertIn("`ipv6_flow_label` Nullable(UInt32)", result)
        
        # MPLS columns
        self.assertIn("`mpls_bottom_label` Nullable(UInt32)", result)
        self.assertIn("`mpls_exp` Array(UInt8)", result)
        self.assertIn("`mpls_label` Array(UInt32)", result)
        self.assertIn("`mpls_pw` Nullable(UInt32)", result)
        self.assertIn("`mpls_top_label_ip` Nullable(IPv6)", result)
        self.assertIn("`mpls_top_label_type` Nullable(UInt32)", result)
        self.assertIn("`mpls_vpn_rd` LowCardinality(Nullable(String))", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'invalid,bgp,nonexistent'})
    def test_create_table_command_with_invalid_extensions(self):
        """Test the create_table_command method with some invalid extensions (should ignore invalid ones)."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - Invalid Extensions):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Should only include BGP extensions, invalid ones should be ignored
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`bgp_as_path_id` Array(UInt32)", result)
        self.assertIn("`bgp_as_path_padding` Array(UInt16)", result)

        # Should NOT include any invalid extension columns
        self.assertNotIn("`invalid_", result)
        self.assertNotIn("`nonexistent_", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_TABLE': 'custom_flow_table'})
    def test_create_table_command_with_custom_table_name(self):
        """Test the create_table_command method with custom table name."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - Custom Table Name):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Should use custom table name
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_flow_table", result)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS data_flow", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_PARTITION_BY': 'toYYYYMM(start_time)'})
    def test_create_table_command_with_custom_partition(self):
        """Test the create_table_command method with custom partition configuration."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - Custom Partition):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for the custom partition
        self.assertIn("PARTITION BY toYYYYMM(start_time)", result)

    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_TTL': '60 DAY', 'CLICKHOUSE_FLOW_TTL_COLUMN': 'end_time'})
    def test_create_table_command_with_ttl(self):
        """Test the create_table_command method with TTL configuration."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - TTL):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for TTL clause and settings
        self.assertIn("TTL end_time + INTERVAL 60 DAY", result)
        self.assertIn("ttl_only_drop_parts = 1", result)

    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_TABLE': 'custom_flow_data',
        'CLICKHOUSE_FLOW_PARTITION_BY': 'toYYYYMMDD(end_time)',
        'CLICKHOUSE_FLOW_TTL': '180 DAY',
        'CLICKHOUSE_FLOW_TTL_COLUMN': 'start_time',
        'CLICKHOUSE_FLOW_EXTENSIONS': 'bgp,mpls'
    })
    def test_create_table_command_comprehensive_configuration(self):
        """Test the create_table_command method with comprehensive custom configuration."""
        
        # Create an instance of BaseFlowProcessor
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Call the method
        result = processor.create_table_command()
        
        # Print the result for inspection
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (BaseFlowProcessor - Comprehensive):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check for all custom configurations
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_flow_data", result)
        self.assertIn("PARTITION BY toYYYYMMDD(end_time)", result)
        self.assertIn("TTL start_time + INTERVAL 180 DAY", result)
        self.assertIn("ttl_only_drop_parts = 1", result)
        
        # Check for BGP extensions
        self.assertIn("`bgp_as_path_id` Array(UInt32)", result)
        self.assertIn("`bgp_community` Array(LowCardinality(String))", result)
        
        # Check for MPLS extensions
        self.assertIn("`mpls_label` Array(UInt32)", result)
        self.assertIn("`mpls_vpn_rd` LowCardinality(Nullable(String))", result)

    def test_default_configuration_values(self):
        """Test that the processor has correct default values."""
        
        # Create an instance of BaseFlowProcessor with no environment overrides
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Check default values
        self.assertEqual(processor.table, 'data_flow')
        self.assertEqual(processor.partition_by, 'toYYYYMMDD(start_time)')
        self.assertEqual(processor.table_ttl, '30 DAY')
        self.assertEqual(processor.table_ttl_column, 'start_time')
        self.assertEqual(processor.flow_type, 'unknown')
        
        # Check that order_by is properly set
        expected_order_by = ["src_as_id", "dst_as_id", "src_ip", "dst_ip", "start_time"]
        self.assertEqual(processor.order_by, expected_order_by)

    def test_materialized_view_not_loaded_by_default(self):
        """Test that MaterializedViewByEdgeAS5m is not loaded by default."""
        
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Should have no materialized views by default
        self.assertEqual(len(processor.materialized_views), 0)
        self.assertEqual(processor.get_materialized_views(), [])
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_ENABLED': 'false'})
    def test_materialized_view_not_loaded_when_disabled(self):
        """Test that MaterializedViewByEdgeAS5m is not loaded when explicitly disabled."""
        
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Should have no materialized views when disabled
        self.assertEqual(len(processor.materialized_views), 0)
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_ENABLED': 'true'})
    def test_materialized_view_loaded_when_enabled(self):
        """Test that MaterializedViewByEdgeAS5m is loaded when enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Should have one materialized view
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertIsInstance(processor.materialized_views[0], MaterializedViewByEdgeAS5m)
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_ENABLED': '1'})
    def test_materialized_view_loaded_with_numeric_true(self):
        """Test that MaterializedViewByEdgeAS5m is loaded with numeric '1'."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertIsInstance(processor.materialized_views[0], MaterializedViewByEdgeAS5m)
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_ENABLED': 'yes'})
    def test_materialized_view_loaded_with_yes(self):
        """Test that MaterializedViewByEdgeAS5m is loaded with 'yes'."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        self.assertEqual(len(processor.materialized_views), 1)
        self.assertIsInstance(processor.materialized_views[0], MaterializedViewByEdgeAS5m)


class TestMaterializedViewByEdgeAS5m(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock pipeline (not used in MV but needed for parent)
        self.mock_pipeline = MagicMock()
    
    def test_initialization_basic(self):
        """Test basic initialization of MaterializedViewByEdgeAS5m."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        
        # Check basic attributes
        self.assertEqual(mv.source_table_name, "data_flow")
        self.assertEqual(mv.table, 'data_flow_by_edge_as_5m')
        self.assertEqual(mv.table_ttl, '5 YEAR')
        self.assertEqual(mv.table_ttl_column, 'start_time')
        self.assertEqual(mv.partition_by, 'toYYYYMMDD(start_time)')
        self.assertEqual(mv.table_engine, 'SummingMergeTree')
        self.assertEqual(mv.table_engine_opts, '(flow_count, bit_count, packet_count)')
        self.assertEqual(mv.mv_name, 'data_flow_by_edge_as_5m_mv')
        self.assertTrue(mv.allow_nullable_key)
    
    def test_initialization_column_defs(self):
        """Test that column_defs are properly set."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        
        # Check that key columns exist
        column_names = [col[0] for col in mv.column_defs]
        self.assertIn('start_time', column_names)
        self.assertIn('policy_originator', column_names)
        self.assertIn('policy_level', column_names)
        self.assertIn('policy_scope', column_names)
        self.assertIn('ext', column_names)
        self.assertIn('src_as_id', column_names)
        self.assertIn('src_as_ref', column_names)
        self.assertIn('dst_as_id', column_names)
        self.assertIn('dst_as_ref', column_names)
        self.assertIn('device_id', column_names)
        self.assertIn('application_id', column_names)
        self.assertIn('in_interface_id', column_names)
        self.assertIn('in_interface_edge', column_names)
        self.assertIn('out_interface_id', column_names)
        self.assertIn('out_interface_edge', column_names)
        self.assertIn('ip_version', column_names)
        self.assertIn('flow_count', column_names)
        self.assertIn('bit_count', column_names)
        self.assertIn('packet_count', column_names)
    
    def test_initialization_primary_and_order_keys(self):
        """Test that primary and order by keys are properly set."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        
        # Check primary keys (note: plural)
        self.assertEqual(mv.primary_keys, ['src_as_id', 'dst_as_id', 'start_time'])
        
        # Check order by includes all necessary fields
        self.assertIn('src_as_id', mv.order_by)
        self.assertIn('dst_as_id', mv.order_by)
        self.assertIn('start_time', mv.order_by)
        self.assertIn('policy_originator', mv.order_by)
        self.assertIn('ext.bgp_as_path_id', mv.order_by)
        self.assertIn('application_id', mv.order_by)
    
    def test_create_table_command(self):
        """Test create_table_command generates correct SQL."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        result = mv.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (MaterializedViewByEdgeAS5m):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check basic structure
        self.assertIn("CREATE TABLE IF NOT EXISTS data_flow_by_edge_as_5m", result)
        self.assertIn("ENGINE = SummingMergeTree((flow_count, bit_count, packet_count))", result)
        self.assertIn("PARTITION BY toYYYYMMDD(start_time)", result)
        self.assertIn("PRIMARY KEY (`src_as_id`,`dst_as_id`,`start_time`)", result)
        self.assertIn("TTL start_time + INTERVAL 5 YEAR", result)
        self.assertIn("allow_nullable_key = 1", result)
        
        # Check key columns
        self.assertIn("`start_time` DateTime", result)
        self.assertIn("`src_as_id` UInt32", result)
        self.assertIn("`dst_as_id` UInt32", result)
        self.assertIn("`flow_count` UInt64", result)
        self.assertIn("`bit_count` UInt64", result)
        self.assertIn("`packet_count` UInt64", result)
    
    def test_create_mv_command(self):
        """Test create_mv_command generates correct SQL."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        result = mv.create_mv_command()
        
        print("\n" + "="*80)
        print("CREATE MATERIALIZED VIEW COMMAND OUTPUT (MaterializedViewByEdgeAS5m):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check basic structure
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS data_flow_by_edge_as_5m_mv", result)
        self.assertIn("TO data_flow_by_edge_as_5m", result)
        self.assertIn("AS", result)
        
        # Check SELECT clause
        self.assertIn("SELECT", result)
        self.assertIn("toStartOfInterval(start_time, INTERVAL 5 MINUTE) AS start_time", result)
        self.assertIn("FROM data_flow", result)
        
        # Check WHERE clause
        self.assertIn("WHERE in_interface_edge = True OR out_interface_edge = True", result)
        
        # Check key fields in SELECT
        self.assertIn("src_as_id", result)
        self.assertIn("dst_as_id", result)
        self.assertIn("1 AS flow_count", result)
        self.assertIn("bit_count", result)
        self.assertIn("packet_count", result)
        
        # Check dictionary lookup
        self.assertIn("dictGetOrNull('meta_application_dict', 'id', protocol, application_port) AS application_id", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'bgp'})
    def test_create_mv_command_with_bgp_extension(self):
        """Test create_mv_command with BGP extensions."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        result = mv.create_mv_command()
        
        # Should include BGP extension select term
        self.assertIn("toJSONString", result)
        self.assertIn("'bgp_as_path_id', ext.bgp_as_path_id", result)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TABLE': 'custom_mv_table',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TTL': '10 YEAR',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TTL_COLUMN': 'end_time',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_PARTITION_BY': 'toYYYYMM(start_time)'
    })
    def test_initialization_with_custom_env_vars(self):
        """Test initialization with custom environment variables."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        
        # Check custom values
        self.assertEqual(mv.table, 'custom_mv_table')
        self.assertEqual(mv.table_ttl, '10 YEAR')
        self.assertEqual(mv.table_ttl_column, 'end_time')
        self.assertEqual(mv.partition_by, 'toYYYYMM(start_time)')
        self.assertEqual(mv.mv_name, 'custom_mv_table_mv')
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TABLE': 'aggregated_flow'})
    def test_create_table_command_with_custom_table_name(self):
        """Test create_table_command with custom table name."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        result = mv.create_table_command()
        
        # Should use custom table name
        self.assertIn("CREATE TABLE IF NOT EXISTS aggregated_flow", result)
        self.assertNotIn("data_flow_by_edge_as_5m", result)
    
    def test_mv_select_query_structure(self):
        """Test that mv_select_query has correct structure."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
        
        # Check that mv_select_query is set
        self.assertIsNotNone(mv.mv_select_query)
        self.assertIsInstance(mv.mv_select_query, str)
        
        # Check key components
        self.assertIn("SELECT", mv.mv_select_query)
        self.assertIn("FROM data_flow", mv.mv_select_query)
        self.assertIn("toStartOfInterval(start_time, INTERVAL 5 MINUTE)", mv.mv_select_query)
        self.assertIn("WHERE in_interface_edge = True OR out_interface_edge = True", mv.mv_select_query)
    
    def test_extension_defs_with_bgp(self):
        """Test that extension_defs are set correctly with BGP extensions."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        with patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'bgp'}):
            mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
            
            # Check that BGP extension is in extension_defs
            self.assertIn('ext', mv.extension_defs)
            ext_fields = mv.extension_defs['ext']
            
            # Should have bgp_as_path_id field
            field_names = [field[0] for field in ext_fields]
            self.assertIn('bgp_as_path_id', field_names)
    
    def test_extension_defs_without_extensions(self):
        """Test that extension_defs are empty without extensions."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS5m
        
        with patch.dict(os.environ, {}, clear=True):
            mv = MaterializedViewByEdgeAS5m(source_table_name="data_flow")
            
            # extension_defs should be empty or have empty ext
            self.assertEqual(mv.extension_defs.get('ext', []), [])

if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)