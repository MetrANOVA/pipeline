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
        # Enable materialized views - default to false
        if os.getenv('CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_ENABLED', 'false').lower() in ('true', '1', 'yes'):
            self.materialized_views.append(MaterializedViewByEdgeAS5m(source_table_name=self.table))

    def parse_env_map_list(self, map_list_str: str | None) -> Dict[str, str]:
        result = {}
        if map_list_str:
            pairs = map_list_str.split(',')
            for pair in pairs:
                if ':' in pair:
                    key, value = pair.split(':', 1)
                    result[key.strip()] = value.strip()
        return result

class MaterializedViewByEdgeAS5m(BaseClickHouseMaterializedViewMixin):
    
    def __init__(self, source_table_name: str = ""):
        super().__init__(source_table_name)
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
        self.table = os.getenv('CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TABLE', 'data_flow_by_edge_as_5m')
        self.table_ttl = os.getenv('CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TTL', '5 YEAR')
        self.table_ttl_column = os.getenv('CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_TTL_COLUMN', 'start_time')
        self.partition_by = os.getenv('CLICKHOUSE_FLOW_MV_BY_EDGE_AS_5M_PARTITION_BY', "toYYYYMMDD(start_time)")
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
        self.mv_select_query = f"""
            SELECT
                toStartOfInterval(start_time, INTERVAL 5 MINUTE) AS start_time, 
                policy_originator,
                policy_level,
                policy_scope,
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