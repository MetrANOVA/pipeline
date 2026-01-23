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
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS': '5m,1h,1d'})
    def test_load_materialized_views(self):
        """Test loading materialized views from environment variable."""
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Should have 3 materialized views
        self.assertEqual(len(processor.materialized_views), 3)
        self.assertEqual(processor.materialized_views[0].agg_window, "5m")
        self.assertEqual(processor.materialized_views[0].source_table_name, "data_flow")
        self.assertEqual(processor.materialized_views[1].agg_window, "1h")
        self.assertEqual(processor.materialized_views[2].agg_window, "1d")
    
    def test_no_materialized_views_by_default(self):
        """Test that no materialized views are loaded without environment variable."""
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Should have no materialized views
        self.assertEqual(len(processor.materialized_views), 0)
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_EDGE_AS': '1w, 1mo, 1y'})
    def test_load_materialized_views_with_spaces(self):
        """Test loading materialized views handles spaces in environment variable."""
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        self.assertEqual(len(processor.materialized_views), 3)
        self.assertEqual(processor.materialized_views[0].agg_window, "1w")
        self.assertEqual(processor.materialized_views[1].agg_window, "1mo")
        self.assertEqual(processor.materialized_views[2].agg_window, "1y")
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_MV_BY_INTERFACE': '5m,1h'})
    def test_load_materialized_views_by_interface(self):
        """Test loading MaterializedViewByInterface from environment variable."""
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Count only interface MVs
        interface_mvs = [mv for mv in processor.materialized_views if 'interface' in mv.table]
        self.assertEqual(len(interface_mvs), 2)
        self.assertEqual(interface_mvs[0].agg_window, "5m")
        self.assertEqual(interface_mvs[1].agg_window, "1h")
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS': '5m,1h',
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE': '1d'
    })
    def test_load_multiple_mv_types(self):
        """Test loading both edge_as and interface materialized views."""
        processor = BaseFlowProcessor(self.mock_pipeline)
        
        # Should have 3 total MVs: 2 edge_as + 1 interface
        self.assertEqual(len(processor.materialized_views), 3)
        
        # Check for edge_as MVs
        edge_as_mvs = [mv for mv in processor.materialized_views if 'edge_as' in mv.table]
        self.assertEqual(len(edge_as_mvs), 2)
        
        # Check for interface MVs
        interface_mvs = [mv for mv in processor.materialized_views if 'interface' in mv.table]
        self.assertEqual(len(interface_mvs), 1)


class TestMaterializedViewByEdgeAS(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
    
    def test_initialization_requires_agg_window(self):
        """Test that MaterializedViewByEdgeAS requires agg_window parameter."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        with self.assertRaises(ValueError) as context:
            MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="")
        self.assertIn("agg_window must be provided", str(context.exception))
    
    def test_initialization_with_agg_window(self):
        """Test MaterializedViewByEdgeAS initialization with aggregation window."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="5m")
        
        self.assertEqual(mv.source_table_name, "data_flow")
        self.assertEqual(mv.agg_window, "5m")
        self.assertEqual(mv.agg_window_ch_interval, "5 MINUTE")
        self.assertEqual(mv.table, "data_flow_by_edge_as_5m")
        self.assertEqual(mv.mv_name, "data_flow_by_edge_as_5m_mv")
        self.assertEqual(mv.table_engine, "SummingMergeTree")
        self.assertEqual(mv.table_engine_opts, "(flow_count, bit_count, packet_count)")
    
    def test_create_table_command_basic(self):
        """Test create_table_command for MaterializedViewByEdgeAS."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        result = mv.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (MaterializedViewByEdgeAS - 1h):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check table name and engine
        self.assertIn("CREATE TABLE IF NOT EXISTS data_flow_by_edge_as_1h", result)
        self.assertIn("ENGINE = SummingMergeTree((flow_count, bit_count, packet_count))", result)
        
        # Check for required columns
        self.assertIn("`start_time` DateTime", result)
        self.assertIn("`src_as_id` UInt32", result)
        self.assertIn("`dst_as_id` UInt32", result)
        self.assertIn("`device_id` LowCardinality(String)", result)
        self.assertIn("`flow_count` UInt64", result)
        self.assertIn("`bit_count` UInt64", result)
        self.assertIn("`packet_count` UInt64", result)
        self.assertIn("`in_interface_edge` Bool", result)
        self.assertIn("`out_interface_edge` Bool", result)
        
        # Check settings
        self.assertIn("PARTITION BY toYYYYMMDD(start_time)", result)
        self.assertIn("PRIMARY KEY (`src_as_id`,`dst_as_id`,`start_time`)", result)
        self.assertIn("TTL start_time + INTERVAL 5 YEAR", result)
    
    def test_create_mv_command_basic(self):
        """Test create_mv_command for MaterializedViewByEdgeAS."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1d")
        result = mv.create_mv_command()
        
        print("\n" + "="*80)
        print("CREATE MV COMMAND OUTPUT (MaterializedViewByEdgeAS - 1d):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check materialized view structure
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS data_flow_by_edge_as_1d_mv", result)
        self.assertIn("TO data_flow_by_edge_as_1d", result)
        self.assertIn("AS", result)
        
        # Check SELECT query elements
        self.assertIn("toStartOfInterval(start_time, INTERVAL 1 DAY)", result)
        self.assertIn("FROM data_flow", result)
        self.assertIn("WHERE in_interface_edge = True OR out_interface_edge = True", result)
        self.assertIn("policy_originator", result)
        self.assertIn("policy_level", result)
        self.assertIn("policy_scope", result)
        self.assertIn("src_as_id", result)
        self.assertIn("dst_as_id", result)
        self.assertIn("device_id", result)
        self.assertIn("1 AS flow_count", result)
        self.assertIn("bit_count", result)
        self.assertIn("packet_count", result)
    
    def test_various_aggregation_windows(self):
        """Test MaterializedViewByEdgeAS with various aggregation windows."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        test_cases = [
            ("5m", "5 MINUTE", "data_flow_by_edge_as_5m"),
            ("1h", "1 HOUR", "data_flow_by_edge_as_1h"),
            ("12h", "12 HOUR", "data_flow_by_edge_as_12h"),
            ("1d", "1 DAY", "data_flow_by_edge_as_1d"),
            ("1w", "1 WEEK", "data_flow_by_edge_as_1w"),
            ("1mo", "1 MONTH", "data_flow_by_edge_as_1mo"),
            ("1y", "1 YEAR", "data_flow_by_edge_as_1y")
        ]
        
        for agg_window, expected_interval, expected_table in test_cases:
            with self.subTest(agg_window=agg_window):
                mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window=agg_window)
                self.assertEqual(mv.agg_window, agg_window)
                self.assertEqual(mv.agg_window_ch_interval, expected_interval)
                self.assertEqual(mv.table, expected_table)
                self.assertEqual(mv.mv_name, expected_table + "_mv")
    
    @patch.dict(os.environ, {'CLICKHOUSE_FLOW_EXTENSIONS': 'bgp'})
    def test_with_bgp_extensions(self):
        """Test MaterializedViewByEdgeAS with BGP extensions."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="5m")
        result = mv.create_table_command()
        
        # Should include BGP extension column
        self.assertIn("`ext` JSON(", result)
        self.assertIn("`bgp_as_path_id` Array(UInt32)", result)
        
        # Check MV command includes extension select term
        mv_result = mv.create_mv_command()
        self.assertIn("toJSONString", mv_result)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1H_TABLE': 'custom_flow_1h',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1H_TTL': '10 YEAR',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1H_PARTITION_BY': 'toYYYYMM(start_time)'
    })
    def test_custom_environment_variables(self):
        """Test MaterializedViewByEdgeAS with custom environment variables."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        # Check custom values
        self.assertEqual(mv.table, "custom_flow_1h")
        self.assertEqual(mv.table_ttl, "10 YEAR")
        self.assertEqual(mv.partition_by, "toYYYYMM(start_time)")
        
        result = mv.create_table_command()
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_flow_1h", result)
        self.assertIn("TTL start_time + INTERVAL 10 YEAR", result)
        self.assertIn("PARTITION BY toYYYYMM(start_time)", result)
    
    def test_order_by_configuration(self):
        """Test that MaterializedViewByEdgeAS has correct order_by configuration."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="5m")
        
        # Check that order_by includes all necessary columns
        expected_start = ["src_as_id", "dst_as_id", "start_time"]
        self.assertEqual(mv.order_by[:3], expected_start)
        
        # Check that it includes policy and extension fields
        self.assertIn("policy_originator", mv.order_by)
        self.assertIn("policy_level", mv.order_by)
        self.assertIn("policy_scope", mv.order_by)
        self.assertIn("ext.bgp_as_path_id", mv.order_by)
    
    def test_primary_keys_configuration(self):
        """Test that MaterializedViewByEdgeAS has correct primary_keys configuration."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1d")
        
        expected_primary_keys = ["src_as_id", "dst_as_id", "start_time"]
        self.assertEqual(mv.primary_keys, expected_primary_keys)
    
    def test_allow_nullable_key(self):
        """Test that MaterializedViewByEdgeAS allows nullable keys."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        self.assertTrue(mv.allow_nullable_key)
        
        # Verify it's in the table command
        result = mv.create_table_command()
        self.assertIn("allow_nullable_key = 1", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true'})
    def test_with_replication(self):
        """Test MaterializedViewByEdgeAS with replication enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="5m")
        result = mv.create_table_command()
        
        # Should use ReplicatedSummingMergeTree
        self.assertIn("ReplicatedSummingMergeTree", result)
        self.assertIn("/clickhouse/tables/{shard}/{database}/{table}", result)
        self.assertIn("{replica}", result)
    
    def test_mv_select_query_uses_interval(self):
        """Test that the MV SELECT query uses the correct interval."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        test_cases = [
            ("5m", "5 MINUTE"),
            ("1h", "1 HOUR"),
            ("1d", "1 DAY"),
            ("1w", "1 WEEK")
        ]
        
        for agg_window, expected_interval in test_cases:
            with self.subTest(agg_window=agg_window):
                mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window=agg_window)
                self.assertIn(f"INTERVAL {expected_interval}", mv.mv_select_query)
    
    def test_mv_select_query_filters_edge_interfaces(self):
        """Test that the MV SELECT query filters for edge interfaces."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        # Should filter for edge interfaces
        self.assertIn("WHERE in_interface_edge = True OR out_interface_edge = True", mv.mv_select_query)
    
    def test_mv_select_query_uses_application_dict(self):
        """Test that the MV SELECT query uses application dictionary lookup."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        # Should use dictionary lookup for application
        self.assertIn("dictGetOrNull('meta_application_dict'", mv.mv_select_query)
        self.assertIn("protocol, application_port", mv.mv_select_query)
        self.assertIn("AS application_id", mv.mv_select_query)
    
    def test_policy_override_default_enabled(self):
        """Test that policy override is enabled by default for MaterializedViewByEdgeAS."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        # Should be enabled by default
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "tlp:green")
        self.assertEqual(mv.policy_scope, ["comm:re"])
    
    def test_policy_override_in_mv_query_when_enabled(self):
        """Test that MV SELECT query uses policy override terms when enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        # Should have override terms in query
        self.assertIn("'tlp:green' AS policy_level", mv.mv_select_query)
        self.assertIn("['comm:re'] AS policy_scope", mv.mv_select_query)
        # Should still reference policy_originator from source
        self.assertIn("policy_originator,", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1H_POLICY_OVERRIDE': 'false'
    })
    def test_policy_override_disabled(self):
        """Test MaterializedViewByEdgeAS with policy override disabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        self.assertFalse(mv.policy_override)
        # Query should use source fields without override
        self.assertIn("policy_level", mv.mv_select_query)
        self.assertIn("policy_scope", mv.mv_select_query)
        # Should NOT have the override terms
        self.assertNotIn("'tlp:green' AS policy_level", mv.mv_select_query)
        self.assertNotIn("AS policy_scope", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_POLICY_LEVEL': 'tlp:amber',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_POLICY_SCOPE': 'internal,restricted',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_POLICY_OVERRIDE': 'true'
    })
    def test_policy_override_custom_settings(self):
        """Test MaterializedViewByEdgeAS with custom policy override settings."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="5m")
        
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "tlp:amber")
        self.assertEqual(mv.policy_scope, ["internal", "restricted"])
        
        # Query should use custom override values
        self.assertIn("'tlp:amber' AS policy_level", mv.mv_select_query)
        self.assertIn("['internal,restricted'] AS policy_scope", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1D_POLICY_LEVEL': 'tlp:red',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1D_POLICY_SCOPE': 'confidential',
        'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_1D_POLICY_OVERRIDE': '1'  # Test with '1' instead of 'true'
    })
    def test_policy_override_with_numeric_true(self):
        """Test that policy override works with '1' value."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        mv = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1d")
        
        self.assertTrue(mv.policy_override)
        self.assertIn("'tlp:red' AS policy_level", mv.mv_select_query)
        self.assertIn("['confidential'] AS policy_scope", mv.mv_select_query)
    
    def test_policy_settings_per_aggregation_window(self):
        """Test that different aggregation windows can have different policy settings."""
        from metranova.processors.clickhouse.flow import MaterializedViewByEdgeAS
        
        # Each agg window should use its own environment variables
        mv_5m = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="5m")
        mv_1h = MaterializedViewByEdgeAS(source_table_name="data_flow", agg_window="1h")
        
        # Both should have defaults
        self.assertEqual(mv_5m.policy_level, "tlp:green")
        self.assertEqual(mv_1h.policy_level, "tlp:green")
        self.assertTrue(mv_5m.policy_override)
        self.assertTrue(mv_1h.policy_override)


class TestMaterializedViewByInterface(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
    
    def test_initialization_requires_agg_window(self):
        """Test that MaterializedViewByInterface requires agg_window parameter."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        with self.assertRaises(ValueError) as context:
            MaterializedViewByInterface(source_table_name="data_flow", agg_window="")
        self.assertIn("agg_window must be provided", str(context.exception))
    
    def test_initialization_with_agg_window(self):
        """Test MaterializedViewByInterface initialization with aggregation window."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="5m")
        
        self.assertEqual(mv.source_table_name, "data_flow")
        self.assertEqual(mv.agg_window, "5m")
        self.assertEqual(mv.agg_window_ch_interval, "5 MINUTE")
        self.assertEqual(mv.table, "data_flow_by_interface_5m")
        self.assertEqual(mv.mv_name, "data_flow_by_interface_5m_mv")
        self.assertEqual(mv.table_engine, "SummingMergeTree")
        self.assertEqual(mv.table_engine_opts, "(flow_count, bit_count, packet_count)")
    
    def test_create_table_command_basic(self):
        """Test create_table_command for MaterializedViewByInterface."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        result = mv.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (MaterializedViewByInterface - 1h):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check table name and engine
        self.assertIn("CREATE TABLE IF NOT EXISTS data_flow_by_interface_1h", result)
        self.assertIn("ENGINE = SummingMergeTree((flow_count, bit_count, packet_count))", result)
        
        # Check for required columns
        self.assertIn("`start_time` DateTime", result)
        self.assertIn("`device_id` LowCardinality(String)", result)
        self.assertIn("`in_interface_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`in_interface_ref` Nullable(String)", result)
        self.assertIn("`in_interface_edge` Bool", result)
        self.assertIn("`out_interface_id` LowCardinality(Nullable(String))", result)
        self.assertIn("`out_interface_ref` Nullable(String)", result)
        self.assertIn("`out_interface_edge` Bool", result)
        self.assertIn("`flow_count` UInt64", result)
        self.assertIn("`bit_count` UInt64", result)
        self.assertIn("`packet_count` UInt64", result)
        
        # Check settings
        self.assertIn("PARTITION BY toYYYYMMDD(start_time)", result)
        self.assertIn("PRIMARY KEY (`in_interface_id`,`out_interface_id`,`start_time`)", result)
        self.assertIn("TTL start_time + INTERVAL 5 YEAR", result)
    
    def test_create_mv_command_basic(self):
        """Test create_mv_command for MaterializedViewByInterface."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1d")
        result = mv.create_mv_command()
        
        print("\n" + "="*80)
        print("CREATE MV COMMAND OUTPUT (MaterializedViewByInterface - 1d):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check materialized view structure
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS data_flow_by_interface_1d_mv", result)
        self.assertIn("TO data_flow_by_interface_1d", result)
        self.assertIn("AS", result)
        
        # Check SELECT query elements
        self.assertIn("toStartOfInterval(start_time, INTERVAL 1 DAY)", result)
        self.assertIn("FROM data_flow", result)
        self.assertIn("WHERE in_interface_ref IS NOT NULL AND out_interface_ref IS NOT NULL", result)
        self.assertIn("policy_originator", result)
        self.assertIn("policy_level", result)
        self.assertIn("policy_scope", result)
        self.assertIn("device_id", result)
        self.assertIn("in_interface_id", result)
        self.assertIn("out_interface_id", result)
        self.assertIn("1 AS flow_count", result)
        self.assertIn("bit_count", result)
        self.assertIn("packet_count", result)
    
    def test_various_aggregation_windows(self):
        """Test MaterializedViewByInterface with various aggregation windows."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        test_cases = [
            ("5m", "5 MINUTE", "data_flow_by_interface_5m"),
            ("1h", "1 HOUR", "data_flow_by_interface_1h"),
            ("12h", "12 HOUR", "data_flow_by_interface_12h"),
            ("1d", "1 DAY", "data_flow_by_interface_1d"),
            ("1w", "1 WEEK", "data_flow_by_interface_1w"),
            ("1mo", "1 MONTH", "data_flow_by_interface_1mo"),
            ("1y", "1 YEAR", "data_flow_by_interface_1y")
        ]
        
        for agg_window, expected_interval, expected_table in test_cases:
            with self.subTest(agg_window=agg_window):
                mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window=agg_window)
                self.assertEqual(mv.agg_window, agg_window)
                self.assertEqual(mv.agg_window_ch_interval, expected_interval)
                self.assertEqual(mv.table, expected_table)
                self.assertEqual(mv.mv_name, expected_table + "_mv")
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_1H_TABLE': 'custom_interface_1h',
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_1H_TTL': '10 YEAR',
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_1H_PARTITION_BY': 'toYYYYMM(start_time)'
    })
    def test_custom_environment_variables(self):
        """Test MaterializedViewByInterface with custom environment variables."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        # Check custom values
        self.assertEqual(mv.table, "custom_interface_1h")
        self.assertEqual(mv.table_ttl, "10 YEAR")
        self.assertEqual(mv.partition_by, "toYYYYMM(start_time)")
        
        result = mv.create_table_command()
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_interface_1h", result)
        self.assertIn("TTL start_time + INTERVAL 10 YEAR", result)
        self.assertIn("PARTITION BY toYYYYMM(start_time)", result)
    
    def test_order_by_configuration(self):
        """Test that MaterializedViewByInterface has correct order_by configuration."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="5m")
        
        # Check that order_by includes all necessary columns
        expected_start = ["in_interface_id", "out_interface_id", "start_time"]
        self.assertEqual(mv.order_by[:3], expected_start)
        
        # Check that it includes policy and interface fields
        self.assertIn("in_interface_ref", mv.order_by)
        self.assertIn("out_interface_ref", mv.order_by)
        self.assertIn("in_interface_edge", mv.order_by)
        self.assertIn("out_interface_edge", mv.order_by)
        self.assertIn("policy_originator", mv.order_by)
        self.assertIn("policy_level", mv.order_by)
        self.assertIn("policy_scope", mv.order_by)
        self.assertIn("device_id", mv.order_by)
        self.assertIn("device_ref", mv.order_by)
    
    def test_primary_keys_configuration(self):
        """Test that MaterializedViewByInterface has correct primary_keys configuration."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1d")
        
        expected_primary_keys = ["in_interface_id", "out_interface_id", "start_time"]
        self.assertEqual(mv.primary_keys, expected_primary_keys)
    
    def test_allow_nullable_key(self):
        """Test that MaterializedViewByInterface allows nullable keys."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        self.assertTrue(mv.allow_nullable_key)
        
        # Verify it's in the table command
        result = mv.create_table_command()
        self.assertIn("allow_nullable_key = 1", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true'})
    def test_with_replication(self):
        """Test MaterializedViewByInterface with replication enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="5m")
        result = mv.create_table_command()
        
        # Should use ReplicatedSummingMergeTree
        self.assertIn("ReplicatedSummingMergeTree", result)
        self.assertIn("/clickhouse/tables/{shard}/{database}/{table}", result)
        self.assertIn("{replica}", result)
    
    def test_mv_select_query_uses_interval(self):
        """Test that the MV SELECT query uses the correct interval."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        test_cases = [
            ("5m", "5 MINUTE"),
            ("1h", "1 HOUR"),
            ("1d", "1 DAY"),
            ("1w", "1 WEEK")
        ]
        
        for agg_window, expected_interval in test_cases:
            with self.subTest(agg_window=agg_window):
                mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window=agg_window)
                self.assertIn(f"INTERVAL {expected_interval}", mv.mv_select_query)
    
    def test_mv_select_query_filters_null_interfaces(self):
        """Test that the MV SELECT query filters for non-null interface refs."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        # Should filter for non-null interface refs
        self.assertIn("WHERE in_interface_ref IS NOT NULL AND out_interface_ref IS NOT NULL", mv.mv_select_query)
    
    def test_mv_select_query_structure(self):
        """Test that the MV SELECT query has correct structure."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        # Check for required fields
        self.assertIn("start_time", mv.mv_select_query)
        self.assertIn("policy_originator", mv.mv_select_query)
        self.assertIn("policy_level", mv.mv_select_query)
        self.assertIn("policy_scope", mv.mv_select_query)
        self.assertIn("device_id", mv.mv_select_query)
        self.assertIn("device_ref", mv.mv_select_query)
        self.assertIn("in_interface_id", mv.mv_select_query)
        self.assertIn("in_interface_ref", mv.mv_select_query)
        self.assertIn("in_interface_edge", mv.mv_select_query)
        self.assertIn("out_interface_id", mv.mv_select_query)
        self.assertIn("out_interface_ref", mv.mv_select_query)
        self.assertIn("out_interface_edge", mv.mv_select_query)
        self.assertIn("1 AS flow_count", mv.mv_select_query)
        self.assertIn("bit_count", mv.mv_select_query)
        self.assertIn("packet_count", mv.mv_select_query)
    
    def test_column_definitions(self):
        """Test that MaterializedViewByInterface has correct column definitions."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="5m")
        
        # Extract column names from column_defs
        column_names = [col[0] for col in mv.column_defs]
        
        # Check for required columns
        self.assertIn("start_time", column_names)
        self.assertIn("policy_originator", column_names)
        self.assertIn("policy_level", column_names)
        self.assertIn("policy_scope", column_names)
        self.assertIn("device_id", column_names)
        self.assertIn("device_ref", column_names)
        self.assertIn("in_interface_id", column_names)
        self.assertIn("in_interface_ref", column_names)
        self.assertIn("in_interface_edge", column_names)
        self.assertIn("out_interface_id", column_names)
        self.assertIn("out_interface_ref", column_names)
        self.assertIn("out_interface_edge", column_names)
        self.assertIn("flow_count", column_names)
        self.assertIn("bit_count", column_names)
        self.assertIn("packet_count", column_names)
    
    def test_policy_override_default_enabled(self):
        """Test that policy override is enabled by default for MaterializedViewByInterface."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        # Should be enabled by default
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "tlp:green")
        self.assertEqual(mv.policy_scope, ["comm:re"])
    
    def test_policy_override_in_mv_query_when_enabled(self):
        """Test that MV SELECT query uses policy override terms when enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        # Should have override terms in query
        self.assertIn("'tlp:green' AS policy_level", mv.mv_select_query)
        self.assertIn("['comm:re'] AS policy_scope", mv.mv_select_query)
        # Should still reference policy_originator from source
        self.assertIn("policy_originator,", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_1H_POLICY_OVERRIDE': 'false'
    })
    def test_policy_override_disabled(self):
        """Test MaterializedViewByInterface with policy override disabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="1h")
        
        self.assertFalse(mv.policy_override)
        # Query should use source fields without override
        self.assertNotIn("'tlp:green' AS policy_level", mv.mv_select_query)
        self.assertNotIn("AS policy_scope", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_5M_POLICY_LEVEL': 'tlp:white',
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_5M_POLICY_SCOPE': 'public,external',
        'CLICKHOUSE_FLOW_MV_BY_INTERFACE_5M_POLICY_OVERRIDE': 'yes'  # Test with 'yes'
    })
    def test_policy_override_custom_settings(self):
        """Test MaterializedViewByInterface with custom policy override settings."""
        from metranova.processors.clickhouse.flow import MaterializedViewByInterface
        
        mv = MaterializedViewByInterface(source_table_name="data_flow", agg_window="5m")
        
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "tlp:white")
        self.assertEqual(mv.policy_scope, ["public", "external"])
        
        # Query should use custom override values
        self.assertIn("'tlp:white' AS policy_level", mv.mv_select_query)
        self.assertIn("['public,external'] AS policy_scope", mv.mv_select_query)


class TestMaterializedViewByIPVersion(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = MagicMock()
    
    def test_initialization_requires_agg_window(self):
        """Test that MaterializedViewByIPVersion requires agg_window parameter."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        with self.assertRaises(ValueError) as context:
            MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="")
        self.assertIn("agg_window must be provided", str(context.exception))
    
    def test_initialization_with_agg_window(self):
        """Test MaterializedViewByIPVersion initialization with aggregation window."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="5m")
        
        self.assertEqual(mv.source_table_name, "data_flow")
        self.assertEqual(mv.agg_window, "5m")
        self.assertEqual(mv.agg_window_ch_interval, "5 MINUTE")
        self.assertEqual(mv.table, "data_flow_by_ip_version_5m")
        self.assertEqual(mv.mv_name, "data_flow_by_ip_version_5m_mv")
        self.assertEqual(mv.table_engine, "SummingMergeTree")
        self.assertEqual(mv.table_engine_opts, "(flow_count, bit_count, packet_count)")
    
    def test_create_table_command_basic(self):
        """Test create_table_command for MaterializedViewByIPVersion."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        result = mv.create_table_command()
        
        print("\n" + "="*80)
        print("CREATE TABLE COMMAND OUTPUT (MaterializedViewByIPVersion - 1h):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check table name and engine
        self.assertIn("CREATE TABLE IF NOT EXISTS data_flow_by_ip_version_1h", result)
        self.assertIn("ENGINE = SummingMergeTree((flow_count, bit_count, packet_count))", result)
        
        # Check for required columns
        self.assertIn("`start_time` DateTime", result)
        self.assertIn("`policy_originator` LowCardinality(Nullable(String))", result)
        self.assertIn("`policy_level` LowCardinality(Nullable(String))", result)
        self.assertIn("`policy_scope` Array(LowCardinality(String))", result)
        self.assertIn("`ip_version` UInt8", result)
        self.assertIn("`flow_count` UInt64", result)
        self.assertIn("`bit_count` UInt64", result)
        self.assertIn("`packet_count` UInt64", result)
        
        # Check settings
        self.assertIn("PARTITION BY toYYYYMMDD(start_time)", result)
        self.assertIn("PRIMARY KEY (`ip_version`,`start_time`)", result)
        self.assertIn("TTL start_time + INTERVAL 5 YEAR", result)
    
    def test_create_mv_command_basic(self):
        """Test create_mv_command for MaterializedViewByIPVersion."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1d")
        result = mv.create_mv_command()
        
        print("\n" + "="*80)
        print("CREATE MV COMMAND OUTPUT (MaterializedViewByIPVersion - 1d):")
        print("="*80)
        print(result)
        print("="*80)
        
        # Check materialized view structure
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS data_flow_by_ip_version_1d_mv", result)
        self.assertIn("TO data_flow_by_ip_version_1d", result)
        self.assertIn("AS", result)
        
        # Check SELECT query elements
        self.assertIn("toStartOfInterval(start_time, INTERVAL 1 DAY)", result)
        self.assertIn("FROM data_flow", result)
        self.assertIn("policy_originator", result)
        self.assertIn("policy_level", result)
        self.assertIn("policy_scope", result)
        self.assertIn("ip_version", result)
        self.assertIn("1 AS flow_count", result)
        self.assertIn("bit_count", result)
        self.assertIn("packet_count", result)
        
        # Should NOT have a WHERE clause (aggregates all flows)
        self.assertNotIn("WHERE", result)
    
    def test_various_aggregation_windows(self):
        """Test MaterializedViewByIPVersion with various aggregation windows."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        test_cases = [
            ("5m", "5 MINUTE", "data_flow_by_ip_version_5m"),
            ("1h", "1 HOUR", "data_flow_by_ip_version_1h"),
            ("12h", "12 HOUR", "data_flow_by_ip_version_12h"),
            ("1d", "1 DAY", "data_flow_by_ip_version_1d"),
            ("1w", "1 WEEK", "data_flow_by_ip_version_1w"),
            ("1mo", "1 MONTH", "data_flow_by_ip_version_1mo"),
            ("1y", "1 YEAR", "data_flow_by_ip_version_1y")
        ]
        
        for agg_window, expected_interval, expected_table in test_cases:
            with self.subTest(agg_window=agg_window):
                mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window=agg_window)
                self.assertEqual(mv.agg_window, agg_window)
                self.assertEqual(mv.agg_window_ch_interval, expected_interval)
                self.assertEqual(mv.table, expected_table)
                self.assertEqual(mv.mv_name, expected_table + "_mv")
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1H_TABLE': 'custom_ip_version_1h',
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1H_TTL': '10 YEAR',
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1H_PARTITION_BY': 'toYYYYMM(start_time)'
    })
    def test_custom_environment_variables(self):
        """Test MaterializedViewByIPVersion with custom environment variables."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        
        # Check custom values
        self.assertEqual(mv.table, "custom_ip_version_1h")
        self.assertEqual(mv.table_ttl, "10 YEAR")
        self.assertEqual(mv.partition_by, "toYYYYMM(start_time)")
        
        result = mv.create_table_command()
        self.assertIn("CREATE TABLE IF NOT EXISTS custom_ip_version_1h", result)
        self.assertIn("TTL start_time + INTERVAL 10 YEAR", result)
        self.assertIn("PARTITION BY toYYYYMM(start_time)", result)
    
    def test_order_by_configuration(self):
        """Test that MaterializedViewByIPVersion has correct order_by configuration."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="5m")
        
        # Check that order_by includes all necessary columns
        expected_order = ["ip_version", "start_time", "policy_originator", "policy_level", "policy_scope"]
        self.assertEqual(mv.order_by, expected_order)
    
    def test_primary_keys_configuration(self):
        """Test that MaterializedViewByIPVersion has correct primary_keys configuration."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1d")
        
        expected_primary_keys = ["ip_version", "start_time"]
        self.assertEqual(mv.primary_keys, expected_primary_keys)
    
    def test_allow_nullable_key(self):
        """Test that MaterializedViewByIPVersion allows nullable keys."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        
        self.assertTrue(mv.allow_nullable_key)
        
        # Verify it's in the table command
        result = mv.create_table_command()
        self.assertIn("allow_nullable_key = 1", result)
    
    @patch.dict(os.environ, {'CLICKHOUSE_REPLICATION': 'true'})
    def test_with_replication(self):
        """Test MaterializedViewByIPVersion with replication enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="5m")
        result = mv.create_table_command()
        
        # Should use ReplicatedSummingMergeTree
        self.assertIn("ReplicatedSummingMergeTree", result)
        self.assertIn("/clickhouse/tables/{shard}/{database}/{table}", result)
        self.assertIn("{replica}", result)
    
    def test_mv_select_query_uses_interval(self):
        """Test that the MV SELECT query uses the correct interval."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        test_cases = [
            ("5m", "5 MINUTE"),
            ("1h", "1 HOUR"),
            ("1d", "1 DAY"),
            ("1w", "1 WEEK")
        ]
        
        for agg_window, expected_interval in test_cases:
            with self.subTest(agg_window=agg_window):
                mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window=agg_window)
                self.assertIn(f"INTERVAL {expected_interval}", mv.mv_select_query)
    
    def test_mv_select_query_no_filtering(self):
        """Test that the MV SELECT query does not filter flows (aggregates all)."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        
        # Should NOT have any WHERE clause - aggregates all flows
        self.assertNotIn("WHERE", mv.mv_select_query)
    
    def test_mv_select_query_structure(self):
        """Test that the MV SELECT query has correct structure."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        
        # Check for required fields
        self.assertIn("start_time", mv.mv_select_query)
        self.assertIn("policy_originator", mv.mv_select_query)
        self.assertIn("policy_level", mv.mv_select_query)
        self.assertIn("policy_scope", mv.mv_select_query)
        self.assertIn("ip_version", mv.mv_select_query)
        self.assertIn("1 AS flow_count", mv.mv_select_query)
        self.assertIn("bit_count", mv.mv_select_query)
        self.assertIn("packet_count", mv.mv_select_query)
        
        # Should NOT have device or interface fields
        self.assertNotIn("device_id", mv.mv_select_query)
        self.assertNotIn("in_interface", mv.mv_select_query)
        self.assertNotIn("out_interface", mv.mv_select_query)
        self.assertNotIn("src_as", mv.mv_select_query)
        self.assertNotIn("dst_as", mv.mv_select_query)
    
    def test_column_definitions(self):
        """Test that MaterializedViewByIPVersion has correct column definitions."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="5m")
        
        # Extract column names from column_defs
        column_names = [col[0] for col in mv.column_defs]
        
        # Check for required columns (minimal set)
        self.assertIn("start_time", column_names)
        self.assertIn("policy_originator", column_names)
        self.assertIn("policy_level", column_names)
        self.assertIn("policy_scope", column_names)
        self.assertIn("ip_version", column_names)
        self.assertIn("flow_count", column_names)
        self.assertIn("bit_count", column_names)
        self.assertIn("packet_count", column_names)
        
        # Should be exactly 8 columns
        self.assertEqual(len(column_names), 8)
    
    def test_policy_override_default_enabled(self):
        """Test that policy override is enabled by default for MaterializedViewByIPVersion."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        
        # Should be enabled by default
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "tlp:green")
        self.assertEqual(mv.policy_scope, ["comm:re"])
    
    def test_policy_override_in_mv_query_when_enabled(self):
        """Test that MV SELECT query uses policy override terms when enabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1h")
        
        # Should have override terms in query
        self.assertIn("'tlp:green' AS policy_level", mv.mv_select_query)
        self.assertIn("['comm:re'] AS policy_scope", mv.mv_select_query)
        # Should still reference policy_originator from source
        self.assertIn("policy_originator,", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1D_POLICY_OVERRIDE': '0'  # Test with '0' for false
    })
    def test_policy_override_disabled(self):
        """Test MaterializedViewByIPVersion with policy override disabled."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1d")
        
        self.assertFalse(mv.policy_override)
        # Query should use source fields without override
        self.assertNotIn("'tlp:green' AS policy_level", mv.mv_select_query)
        self.assertNotIn("AS policy_scope", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1W_POLICY_LEVEL': 'custom:level',
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1W_POLICY_SCOPE': 'vpn:internal,region:us',
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1W_POLICY_OVERRIDE': 'TRUE'  # Test case insensitive
    })
    def test_policy_override_custom_settings(self):
        """Test MaterializedViewByIPVersion with custom policy override settings."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1w")
        
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "custom:level")
        self.assertEqual(mv.policy_scope, ["vpn:internal", "region:us"])
        
        # Query should use custom override values
        self.assertIn("'custom:level' AS policy_level", mv.mv_select_query)
        self.assertIn("['vpn:internal,region:us'] AS policy_scope", mv.mv_select_query)
    
    @patch.dict(os.environ, {
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1MO_POLICY_LEVEL': 'tlp:amber',
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1MO_POLICY_SCOPE': 'research',
        'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_1MO_POLICY_OVERRIDE': 'true'
    })
    def test_policy_override_single_scope(self):
        """Test MaterializedViewByIPVersion with single scope value."""
        from metranova.processors.clickhouse.flow import MaterializedViewByIPVersion
        
        mv = MaterializedViewByIPVersion(source_table_name="data_flow", agg_window="1mo")
        
        self.assertTrue(mv.policy_override)
        self.assertEqual(mv.policy_level, "tlp:amber")
        self.assertEqual(mv.policy_scope, ["research"])
        
        # Query should use single scope value
        self.assertIn("'tlp:amber' AS policy_level", mv.mv_select_query)
        self.assertIn("['research'] AS policy_scope", mv.mv_select_query)

if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)