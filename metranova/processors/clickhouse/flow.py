import orjson
import os
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.base import BaseClickHouseMaterializedViewMixin, BaseDataProcessor

class BaseFlowProcessor(BaseDataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        # environment settings
        self.table = os.getenv('CLICKHOUSE_FLOW_TABLE', 'data_flow')
        self.table_ttl = os.getenv('CLICKHOUSE_FLOW_TTL', '30 DAY')
        self.table_ttl_column = os.getenv('CLICKHOUSE_FLOW_TTL_COLUMN', 'start_time')
        self.flow_type = os.getenv('CLICKHOUSE_FLOW_TYPE', 'unknown')
        self.partition_by = os.getenv('CLICKHOUSE_FLOW_PARTITION_BY', "toYYYYMMDD(start_time)")
        self.policy_auto_scopes = os.getenv('CLICKHOUSE_FLOW_POLICY_AUTO_SCOPES', 'true').lower() in ('true', '1', 'yes')
        #A comma separated list of key-value pairs in form key:value, mapping a community id (such as a l3vpn rd) to a scope string
        policy_community_scope_map_str = os.getenv('CLICKHOUSE_FLOW_POLICY_COMMUNITY_SCOPE_MAP', None)
        # Parse the community scope map into a dictionary
        self.policy_community_scope_map = self.parse_env_map_list(policy_community_scope_map_str)
        # add time fields to front of column names
        self.column_defs.insert(0, ["start_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.insert(1, ["end_time", "DateTime64(3, 'UTC')", True])
        self.column_defs.append(['flow_type', 'LowCardinality(String)', True])
        self.column_defs.append(['device_id', 'LowCardinality(String)', True])
        self.column_defs.append(['device_ref', 'Nullable(String)', True])
        self.column_defs.append(['src_as_id', 'UInt32', True])
        self.column_defs.append(['src_as_ref', 'Nullable(String)', True])
        self.column_defs.append(['src_ip', 'IPv6', True])
        self.column_defs.append(['src_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['src_port', 'UInt16', True])
        self.column_defs.append(['dst_as_id', 'UInt32', True])
        self.column_defs.append(['dst_as_ref', 'Nullable(String)', True])
        self.column_defs.append(['dst_ip', 'IPv6', True])
        self.column_defs.append(['dst_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['dst_port', 'UInt16', True])
        self.column_defs.append(['protocol', 'LowCardinality(String)', True])
        self.column_defs.append(['in_interface_id', 'LowCardinality(Nullable(String))', True])
        self.column_defs.append(['in_interface_ref', 'Nullable(String)', True])
        self.column_defs.append(['in_interface_edge', 'Bool', True])
        self.column_defs.append(['out_interface_id', 'LowCardinality(Nullable(String))', True])
        self.column_defs.append(['out_interface_ref', 'Nullable(String)', True])
        self.column_defs.append(['out_interface_edge', 'Bool', True])
        self.column_defs.append(['peer_as_id', 'Nullable(UInt32)', True])
        self.column_defs.append(['peer_as_ref', 'Nullable(String)', True])
        self.column_defs.append(['peer_ip', 'Nullable(IPv6)', True])
        self.column_defs.append(['peer_ip_ref', 'Nullable(String)', True])
        self.column_defs.append(['ip_version', 'UInt8', True])
        self.column_defs.append(['application_port', 'UInt16', True])
        self.column_defs.append(['bit_count', 'UInt64', True])
        self.column_defs.append(['packet_count', 'UInt64', True])
        # build list of potential ext type hints
        extension_options = {
                "bgp": [
                    ["bgp_as_path_id", "Array(UInt32)"],
                    ["bgp_as_path_padding", "Array(UInt16)"],
                    ["bgp_community", "Array(LowCardinality(String))"],
                    ["bgp_ext_community", "Array(LowCardinality(String))"],
                    ["bgp_large_community", "Array(LowCardinality(String))"],
                    ["bgp_local_pref", "Nullable(UInt32)"],
                    ["bgp_med", "Nullable(UInt32)"]
                ],
                "ipv4": [
                    ["ipv4_dscp", "Nullable(UInt8)"],
                    ["ipv4_tos", "Nullable(UInt8)"]
                ],
                "ipv6": [
                    ["ipv6_flow_label", "Nullable(UInt32)"]
                ],
                "mpls": [
                    ["mpls_bottom_label", "Nullable(UInt32)"],
                    ["mpls_exp", "Array(UInt8)"],
                    ["mpls_label", "Array(UInt32)"],
                    ["mpls_pw", "Nullable(UInt32)"],
                    ["mpls_top_label_ip", "Nullable(IPv6)"],
                    ["mpls_top_label_type", "Nullable(UInt32)"],
                    ["mpls_vpn_rd", "LowCardinality(Nullable(String))"]
                ],
                "vlan": [
                    ["vlan_id", "Nullable(UInt32)"],
                    ["vlan_in_id", "Nullable(UInt32)"],
                    ["vlan_out_id", "Nullable(UInt32)"],
                    ["vlan_in_inner_id", "Nullable(UInt32)"],
                    ["vlan_out_inner_id", "Nullable(UInt32)"],
                ]
            }
        # determine columns to use from environment
        self.extension_defs['ext'] = self.get_extension_defs('CLICKHOUSE_FLOW_EXTENSIONS', extension_options)
        #grab references to extension tables for IP ref lookups
        self.ip_ref_extensions = self.get_ip_ref_extensions("CLICKHOUSE_FLOW_IP_REF_EXTENSIONS")
        # set additional table settings
        self.order_by = ["src_as_id", "dst_as_id", "src_ip", "dst_ip", "start_time"]
        # Enable materialized views - they accept a comma separated list of agg windows - e.g. 5m, 12h, 1d, 1w, 3mo, 10y, etc
        self.load_materialized_views('CLICKHOUSE_FLOW_MV_BY_EDGE_AS', MaterializedViewByEdgeAS)
        self.load_materialized_views('CLICKHOUSE_FLOW_MV_BY_INTERFACE', MaterializedViewByInterface)
        self.load_materialized_views('CLICKHOUSE_FLOW_MV_BY_IP_VERSION', MaterializedViewByIPVersion)

    def parse_env_map_list(self, map_list_str: str | None) -> Dict[str, str]:
        result = {}
        if map_list_str:
            pairs = map_list_str.split(',')
            for pair in pairs:
                if ':' in pair:
                    key, value = pair.split(':', 1)
                    result[key.strip()] = value.strip()
        return result

class MaterializedViewByEdgeAS(BaseClickHouseMaterializedViewMixin):
    
    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        super().__init__(source_table_name, agg_window)  
        #check required parameters
        if not agg_window:
            raise ValueError("agg_window must be provided for MaterializedViewByEdgeAS")
        
        #Target Table settings
        self.column_defs = [
            ['start_time', 'DateTime', True],
            ['policy_originator', 'LowCardinality(Nullable(String))', True],
            ['policy_level', 'LowCardinality(Nullable(String))', True],
            ['policy_scope', 'Array(LowCardinality(String))', True],
            ['ext', None, True],
            ['src_as_id', 'UInt32', True],
            ['src_as_ref', 'Nullable(String)', True],
            ['dst_as_id', 'UInt32', True],
            ['dst_as_ref', 'Nullable(String)', True],
            ['device_id', 'LowCardinality(String)', True],
            ['device_ref', 'Nullable(String)', True],
            ['application_id', 'LowCardinality(Nullable(String))', True],
            ['in_interface_id', 'LowCardinality(Nullable(String))', True],
            ['in_interface_ref', 'Nullable(String)', True],
            ['in_interface_edge', 'Bool', True],
            ['out_interface_id', 'LowCardinality(Nullable(String))', True],
            ['out_interface_ref', 'Nullable(String)', True],
            ['out_interface_edge', 'Bool', True],
            ['ip_version', 'UInt8', True],
            ['flow_count', 'UInt64', True],
            ['bit_count', 'UInt64', True],
            ['packet_count', 'UInt64', True]
        ]
        # determine columns to use from environment
        extension_options = {
            "bgp": [
                ["bgp_as_path_id", "Array(UInt32)"],
            ]
        }
        self.extension_defs['ext'] = self.get_extension_defs('CLICKHOUSE_FLOW_EXTENSIONS', extension_options)
        agg_window_upper = agg_window.upper()
        self.table = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_TABLE', f'data_flow_by_edge_as_{agg_window}')
        self.table_ttl = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_TTL', '5 YEAR')
        self.table_ttl_column = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_TTL_COLUMN', 'start_time')
        self.partition_by = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_PARTITION_BY', "toYYYYMMDD(start_time)")
        self.policy_level = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_POLICY_LEVEL', 'tlp:green')
        self.policy_scope = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_POLICY_SCOPE', 'comm:re').split(',')
        self.policy_override = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_EDGE_AS_{agg_window_upper}_POLICY_OVERRIDE', 'true').lower() in ('true', '1', 'yes')
        self.table_engine = 'SummingMergeTree'
        # note; The opts are a single parameter passed as a tuple. The create_tabla_command will add the surrounding parantheses
        # Example: SummingMergeTree((flow_count, bit_count, packet_count))
        # Replication Example: ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}, (flow_count, bit_count, packet_count))
        self.table_engine_opts = '(flow_count, bit_count, packet_count)'
        self.primary_keys = [
            "src_as_id",
            "dst_as_id",
            "start_time"
        ]
        self.order_by = [
            "src_as_id",
            "dst_as_id",
            "start_time",
            "policy_originator",
            "policy_level",
            "policy_scope",
            "ext.bgp_as_path_id",
            "src_as_ref",
            "dst_as_ref",
            "device_id",
            "device_ref",
            "application_id",
            "in_interface_id",
            "in_interface_ref",
            "in_interface_edge",
            "out_interface_id",
            "out_interface_ref",
            "out_interface_edge",
            "ip_version"
        ]
        self.allow_nullable_key = True
        # Materialized View settings
        extension_select_term = self.build_extension_select_term()
        self.mv_name = self.table + "_mv"
        #determine how we are handling policy fields
        policy_level_term, policy_scope_term = self.policy_override_terms()
        self.mv_select_query = f"""
            SELECT
                toStartOfInterval(start_time, INTERVAL {self.agg_window_ch_interval}) AS start_time, 
                policy_originator,
                {policy_level_term},
                {policy_scope_term},
                {extension_select_term}src_as_id,
                src_as_ref,
                dst_as_id,
                dst_as_ref,
                device_id,
                device_ref,
                dictGetOrNull('meta_application_dict', 'id', protocol, application_port) AS application_id,
                in_interface_id,
                in_interface_ref,
                in_interface_edge,
                out_interface_id,
                out_interface_ref,
                out_interface_edge,
                ip_version,
                1 AS flow_count,
                bit_count,
                packet_count
            FROM {self.source_table_name}
            WHERE in_interface_edge = True OR out_interface_edge = True
        """

class MaterializedViewByInterface(BaseClickHouseMaterializedViewMixin):
    
    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        super().__init__(source_table_name, agg_window)  
        #check required parameters
        if not agg_window:
            raise ValueError("agg_window must be provided for MaterializedViewByInterface")
        """
        CREATE TABLE IF NOT EXISTS data_flow_by_interface_5m (
            `start_ts` DateTime,
            `ifin_ref` String,
            `ifout_ref` String,
            `device_name` LowCardinality(Nullable(String)),
            `device_ip` Nullable(IPv6),
            `flow_count` UInt32,
            `num_bits` Float64,
            `num_pkts` Float64
        )
        ENGINE = SummingMergeTree((flow_count, num_bits, num_pkts))
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`ifin_ref`,`ifout_ref`, `device_name`, `device_ip`, `start_ts`)
        SETTINGS index_granularity = 8192, allow_nullable_key = 1
        """
        #Target Table settings
        self.column_defs = [
            ['start_time', 'DateTime', True],
            ['policy_originator', 'LowCardinality(Nullable(String))', True],
            ['policy_level', 'LowCardinality(Nullable(String))', True],
            ['policy_scope', 'Array(LowCardinality(String))', True],
            ['device_id', 'LowCardinality(String)', True],
            ['device_ref', 'Nullable(String)', True],
            ['in_interface_id', 'LowCardinality(Nullable(String))', True],
            ['in_interface_ref', 'Nullable(String)', True],
            ['in_interface_edge', 'Bool', True],
            ['out_interface_id', 'LowCardinality(Nullable(String))', True],
            ['out_interface_ref', 'Nullable(String)', True],
            ['out_interface_edge', 'Bool', True],
            ['flow_count', 'UInt64', True],
            ['bit_count', 'UInt64', True],
            ['packet_count', 'UInt64', True]
        ]
        agg_window_upper = agg_window.upper()
        self.table = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_TABLE', f'data_flow_by_interface_{agg_window}')
        self.table_ttl = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_TTL', '5 YEAR')
        self.table_ttl_column = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_TTL_COLUMN', 'start_time')
        self.partition_by = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_PARTITION_BY', "toYYYYMMDD(start_time)")
        self.policy_level = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_POLICY_LEVEL', 'tlp:green')
        self.policy_scope = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_POLICY_SCOPE', 'comm:re').split(',')
        self.policy_override = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_INTERFACE_{agg_window_upper}_POLICY_OVERRIDE', 'true').lower() in ('true', '1', 'yes')
        self.table_engine = 'SummingMergeTree'
        # note: The opts are a single parameter passed as a tuple. The create_table_command will add the surrounding parantheses
        # Example: SummingMergeTree((flow_count, bit_count, packet_count))
        # Replication Example: ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}, (flow_count, bit_count, packet_count))
        self.table_engine_opts = '(flow_count, bit_count, packet_count)'
        self.primary_keys = [
            "in_interface_id",
            "out_interface_id",
            "start_time"
        ]
        self.order_by = [
            "in_interface_id",
            "out_interface_id",
            "start_time",
            "in_interface_ref",
            "in_interface_edge",
            "out_interface_ref",
            "out_interface_edge",
            "policy_originator",
            "policy_level",
            "policy_scope",
            "device_id",
            "device_ref"
        ]
        self.allow_nullable_key = True
        self.mv_name = self.table + "_mv"
        #determine how we are handling policy fields
        policy_level_term, policy_scope_term = self.policy_override_terms()
        self.mv_select_query = f"""
            SELECT
                toStartOfInterval(start_time, INTERVAL {self.agg_window_ch_interval}) AS start_time, 
                policy_originator,
                {policy_level_term},
                {policy_scope_term},
                device_id,
                device_ref,
                in_interface_id,
                in_interface_ref,
                in_interface_edge,
                out_interface_id,
                out_interface_ref,
                out_interface_edge,
                1 AS flow_count,
                bit_count,
                packet_count
            FROM {self.source_table_name}
            WHERE in_interface_ref IS NOT NULL AND out_interface_ref IS NOT NULL
        """

class MaterializedViewByIPVersion(BaseClickHouseMaterializedViewMixin):
    
    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        super().__init__(source_table_name, agg_window)  
        #check required parameters
        if not agg_window:
            raise ValueError("agg_window must be provided for MaterializedViewByIPVersion")
        
        #Target Table settings
        self.column_defs = [
            ['start_time', 'DateTime', True],
            ['policy_originator', 'LowCardinality(Nullable(String))', True],
            ['policy_level', 'LowCardinality(Nullable(String))', True],
            ['policy_scope', 'Array(LowCardinality(String))', True],
            ['ip_version', 'UInt8', True],
            ['flow_count', 'UInt64', True],
            ['bit_count', 'UInt64', True],
            ['packet_count', 'UInt64', True]
        ]
        agg_window_upper = agg_window.upper()
        self.table = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_TABLE', f'data_flow_by_ip_version_{agg_window}')
        self.table_ttl = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_TTL', '5 YEAR')
        self.table_ttl_column = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_TTL_COLUMN', 'start_time')
        self.partition_by = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_PARTITION_BY', "toYYYYMMDD(start_time)")
        self.policy_level = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_POLICY_LEVEL', 'tlp:green')
        self.policy_scope = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_POLICY_SCOPE', 'comm:re').split(',')
        self.policy_override = os.getenv(f'CLICKHOUSE_FLOW_MV_BY_IP_VERSION_{agg_window_upper}_POLICY_OVERRIDE', 'true').lower() in ('true', '1', 'yes')
        self.table_engine = 'SummingMergeTree'
        # note: The opts are a single parameter passed as a tuple. The create_table_command will add the surrounding parantheses
        # Example: SummingMergeTree((flow_count, bit_count, packet_count))
        # Replication Example: ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}, (flow_count, bit_count, packet_count))
        self.table_engine_opts = '(flow_count, bit_count, packet_count)'
        self.primary_keys = [
            "ip_version",
            "start_time"
        ]
        self.order_by = [
            "ip_version",
            "start_time",
            "policy_originator",
            "policy_level",
            "policy_scope"
        ]
        self.allow_nullable_key = True
        self.mv_name = self.table + "_mv"
        
        #determine how we are handling policy fields
        policy_level_term, policy_scope_term = self.policy_override_terms()
        # Note: this doesn't do any de-duplication or filtering, just aggregates by IP version across all flows seen
        self.mv_select_query = f"""
            SELECT
                toStartOfInterval(start_time, INTERVAL {self.agg_window_ch_interval}) AS start_time, 
                policy_originator,
                {policy_level_term},
                {policy_scope_term},
                ip_version,
                1 AS flow_count,
                bit_count,
                packet_count
            FROM {self.source_table_name}
        """