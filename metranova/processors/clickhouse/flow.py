import orjson
import os
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.base import BaseDataProcessor

class BaseFlowProcessor(BaseDataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_FLOW_TABLE', 'data_flow')
        
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
        self.column_defs.append(['out_interface_id', 'LowCardinality(Nullable(String))', True])
        self.column_defs.append(['out_interface_ref', 'Nullable(String)', True])
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
                    ["mpls_labels", "Array(UInt32)"],
                    ["mpls_pw", "Nullable(UInt32)"],
                    ["mpls_top_label_ip", "Nullable(IPv6)"],
                    ["mpls_top_label_type", "Nullable(UInt32)"],
                    ["mpls_vpn_rd", "LowCardinality(Nullable(String))"]
                ]
            }
        # determine columns to use from environment
        self.extension_defs['ext'] = self.get_extension_defs('CLICKHOUSE_FLOW_EXTENSIONS', extension_options)

        # set additional table settings
        self.partition_by = "toYYYYMMDD(start_time)"
        self.order_by = ["src_as_id", "dst_as_id", "src_ip", "dst_ip", "start_time"]

class StardustFlowProcessor(BaseFlowProcessor):
    """Processor for Stardust flow data - will be deleted in future"""
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [
            ['start'], 
            ['end'], 
            ['meta', 'dst_asn'], 
            ['meta', 'dst_ip'], 
            ['meta', 'dst_port'], 
            ['meta', 'src_asn'], 
            ['meta', 'src_ip'], 
            ['meta', 'src_port']
        ]

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        #build padding array
        bgp_padding = []
        for i in range(0, 20):
            padding = value.get("meta", {}).get("bgp", {}).get("as_hop{}_padding".format(i), None)
            if padding is None:
                break
            try:
                padding = int(padding)
            except ValueError:
                self.logger.error(f"Invalid bgp padding value: {padding}")
                break
            bgp_padding.append(padding)

        # build mpls_exp array
        mpls_exp = []
        for i in range(0, 20):
            exp = value.get("meta", {}).get("mpls", {}).get("exp{}".format(i), None)
            if exp is None:
                break
            try:
                exp = int(exp)
            except ValueError:
                self.logger.error(f"Invalid mpls exp value: {exp}")
                break
            mpls_exp.append(exp)

        #build ext
        ext = {}
        if self.extension_is_enabled("bgp"):
            ext.update({
                "bgp_as_path_id": value.get("meta", {}).get("bgp", {}).get("as_path", []),
                "bgp_as_path_padding": bgp_padding,
                "bgp_community": value.get("meta", {}).get("bgp", {}).get("comms", []),
                "bgp_ext_community": value.get("meta", {}).get("bgp", {}).get("ecomms", []),
                "bgp_large_community": value.get("meta", {}).get("bgp", {}).get("lcomms", []),
                "bgp_local_pref": value.get("meta", {}).get("bgp", {}).get("local_pref", None),
                "bgp_med": value.get("meta", {}).get("bgp", {}).get("med", None)
            })
        if self.extension_is_enabled("ipv4"):
            ext.update({
                "ipv4_dscp": value.get("meta", {}).get("dscp", None),
                "ipv4_tos": value.get("meta", {}).get("ip_tos", None)
            })
        if self.extension_is_enabled("ipv6"):
            ext.update({
                "ipv6_flow_label": value.get("meta", {}).get("ipv6", {}).get("flow_label", None)
            })
        if self.extension_is_enabled("mpls"):
            ext.update({
                "mpls_bottom_label": value.get("meta", {}).get("mpls", {}).get("bottom_label", None),
                "mpls_exp": mpls_exp,
                "mpls_labels": value.get("meta", {}).get("mpls", {}).get("labels", []),
                "mpls_pw": value.get("meta", {}).get("mpls", {}).get("pw_id", None),
                "mpls_top_label_ip": value.get("meta", {}).get("mpls", {}).get("top_label_ip", None),
                "mpls_top_label_type": value.get("meta", {}).get("mpls", {}).get("top_label_type", None),
                "mpls_vpn_rd": value.get("meta", {}).get("mpls", {}).get("vpn_rd", None)
            })
        if self.extension_is_enabled("esdb"):
            ext.update({
                "src_ip_esdb_ref": self.pipeline.cacher("redis").lookup_list("meta_esdb_ipsvc", value.get("meta", {}).get("esdb", {}).get("src", {}).get("service", {}).get("prefix_group_name", [])),
                "dst_ip_esdb_ref": self.pipeline.cacher("redis").lookup_list("meta_esdb_ipsvc", value.get("meta", {}).get("esdb", {}).get("dst", {}).get("service", {}).get("prefix_group_name", []))
            })
        if self.extension_is_enabled("scireg"):
            ext.update({
                "dst_scireg_ref": self.pipeline.cacher("ip").lookup("meta_scireg", value.get("meta", {}).get("dst_ip", None)),
                "src_scireg_ref": self.pipeline.cacher("ip").lookup("meta_scireg", value.get("meta", {}).get("src_ip", None))
            })

        return [{
            "start_time": value.get("start"),
            "end_time": value.get("end"),
            "collector_id": value.get("meta", {}).get("sensor_id", None),
            "policy_originator": value.get("policy", {}).get("originator", None),
            "policy_level": value.get("policy", {}).get("level", None),
            "policy_scope": value.get("policy", {}).get("scopes", []),
            "ext": orjson.dumps(ext),
            "flow_type": value.get("meta", {}).get("flow_type", None),
            "device_id": value.get("meta", {}).get("router", {}).get("name", None),
            "device_ref": self.pipeline.cacher("redis").lookup("meta_device", value.get("meta", {}).get("router", {}).get("name", None)),
            "src_as_id": value.get("meta", {}).get("src_asn", None),
            "src_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value.get("meta", {}).get("src_asn", None)),
            "src_ip": value.get("meta", {}).get("src_ip", None),
            "src_ip_ref": self.pipeline.cacher("ip").lookup("meta_ip", value.get("meta", {}).get("src_ip", None)),
            "src_port": value.get("meta", {}).get("src_port", None),
            "dst_as_id":  value.get("meta", {}).get("dst_asn", None),
            "dst_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value.get("meta", {}).get("dst_asn", None)),
            "dst_ip": value.get("meta", {}).get("dst_ip"),
            "dst_ip_ref": self.pipeline.cacher("ip").lookup("meta_ip", value.get("meta", {}).get("dst_ip", None)),
            "dst_port": value.get("meta", {}).get("dst_port", None),
            "protocol": value.get("meta", {}).get("protocol", None),
            "in_interface_id": value.get("meta", {}).get("iface_in", {}).get("id", None),
            "in_interface_ref": self.pipeline.cacher("redis").lookup("meta_interface", value.get("meta", {}).get("iface_in", {}).get("id", None)),
            "out_interface_id": value.get("meta", {}).get("iface_out", {}).get("id", None),
            "out_interface_ref": self.pipeline.cacher("redis").lookup("meta_interface", value.get("meta", {}).get("iface_out", {}).get("id", None)),
            "peer_as_id": value.get("meta", {}).get("bgp", {}).get("peer_as_dst", None),
            "peer_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value.get("meta", {}).get("bgp", {}).get("peer_as_dst", None)),
            "peer_ip": value.get("meta", {}).get("bgp", {}).get("next_hop", None),
            "peer_ip_ref": self.pipeline.cacher("ip").lookup("meta_ip", value.get("meta", {}).get("bgp", {}).get("next_hop", None)),
            "ip_version":  value.get("meta", {}).get("ip_version", None),
            "application_port": value.get("meta", {}).get("app_port", 0),
            "bit_count": value.get("values", {}).get("num_bits", None),
            "packet_count": value.get("values", {}).get("num_packets", None)
        }]