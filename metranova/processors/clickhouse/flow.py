from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.base import BaseDataProcessor
import os

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
                    ["bgp_asn_path", "Array(UInt32)"],
                    ["bgp_asn_path_padding", "Array(UInt16)"],
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

        return [{
            "start_ts": value.get("start"),
            "end_ts": value.get("end"),
            "policy_originator": value.get("policy", {}).get("originator", None),
            "policy_level": value.get("policy", {}).get("level", None),
            "policy_scopes": value.get("policy", {}).get("scopes", []),
            "app_name": value.get("meta", {}).get("app_name", None),
            "app_port": value.get("meta", {}).get("app_port", 0),
            "bgp_as_path": value.get("meta", {}).get("bgp", {}).get("as_path", []),
            "bgp_as_path_name": value.get("meta", {}).get("bgp", {}).get("as_path_name", []),
            "bgp_as_path_org": value.get("meta", {}).get("bgp", {}).get("as_path_org", []),
            "bgp_as_path_padding": bgp_padding,
            "bgp_as_path_padded_len": value.get("meta", {}).get("bgp", {}).get("as_path_padded_len", None),
            "bgp_comms": value.get("meta", {}).get("bgp", {}).get("comms", []),
            "bgp_ecomms": value.get("meta", {}).get("bgp", {}).get("ecomms", []),
            "bgp_lcomms": value.get("meta", {}).get("bgp", {}).get("lcomms", []),
            "bgp_local_pref": value.get("meta", {}).get("bgp", {}).get("local_pref", None),
            "bgp_med": value.get("meta", {}).get("bgp", {}).get("med", None),
            "bgp_next_hop": value.get("meta", {}).get("bgp", {}).get("next_hop", None),
            "bgp_peer_as_dst": value.get("meta", {}).get("bgp", {}).get("peer_as_dst", None),
            "bgp_peer_as_dst_name": value.get("meta", {}).get("bgp", {}).get("peer_as_dst_name", None),
            "bgp_peer_as_dst_org": value.get("meta", {}).get("bgp", {}).get("peer_as_dst_org", None),
            "collector_id": value.get("meta", {}).get("sensor_id", None),
            "country_scope": value.get("meta", {}).get("country_scope", None),
            "device_ip":  value.get("meta", {}).get("router", {}).get("ip", None),
            "device_name": value.get("meta", {}).get("router", {}).get("name", None),
            "device_loc_name": value.get("meta", {}).get("device_info", {}).get("loc_name", None),
            "device_loc_type": value.get("meta", {}).get("device_info", {}).get("loc_type", None),
            "device_loc_lat": value.get("meta", {}).get("device_info", {}).get("location", {}).get("lat", None),
            "device_loc_lon": value.get("meta", {}).get("device_info", {}).get("location", {}).get("lon", None),
            "device_manufac": value.get("meta", {}).get("device_info", {}).get("manufacturer", None),
            "device_model": value.get("meta", {}).get("device_info", {}).get("model", None),
            "device_network": value.get("meta", {}).get("device_info", {}).get("network", None),
            "device_os": value.get("meta", {}).get("device_info", {}).get("os", None),
            "device_role": value.get("meta", {}).get("device_info", {}).get("role", None),
            "device_state": value.get("meta", {}).get("device_info", {}).get("state", None),
            "dscp": value.get("meta", {}).get("dscp", None),
            "dst_as_name": value.get("meta", {}).get("dst_as_name", None),
            "dst_asn":  value.get("meta", {}).get("dst_asn", None),
            "dst_continent": value.get("meta", {}).get("dst_continent", None),
            "dst_country_name": value.get("meta", {}).get("dst_country_name", None),
            "dst_esdb_ipsvc_ref": self.pipeline.cacher("redis").lookup_list("meta_esdb_ipsvc", value.get("meta", {}).get("esdb", {}).get("dst", {}).get("service", {}).get("prefix_group_name", [])),
            "dst_ip": value.get("meta", {}).get("dst_ip"),
            "dst_loc_lat": value.get("meta", {}).get("dst_location", {}).get("lat", None),
            "dst_loc_lon": value.get("meta", {}).get("dst_location", {}).get("lon", None),
            "dst_org": value.get("meta", {}).get("dst_organization", None),
            "dst_port": value.get("meta", {}).get("dst_port", None),
            "dst_pref_loc_lat": value.get("meta", {}).get("dst_preferred_location", {}).get("lat", None),
            "dst_pref_loc_lon": value.get("meta", {}).get("dst_preferred_location", {}).get("lon", None),
            "dst_pref_org": value.get("meta", {}).get("dst_preferred_org", None),
            "dst_pub_asn":  value.get("meta", {}).get("dst_pub_asn", None),
            "dst_region_iso_code": value.get("meta", {}).get("dst_region_iso_code", None),
            "dst_region_name": value.get("meta", {}).get("dst_region_name", None),
            "dst_scireg_ref": self.pipeline.cacher("ip").lookup("meta_scireg", value.get("meta", {}).get("dst_ip", None)),
            "flow_type": value.get("meta", {}).get("flow_type", None),
            "ifin_ref": self.pipeline.cacher("redis").lookup("meta_if", value.get("meta", {}).get("iface_in", {}).get("id", None)),
            "ifout_ref": self.pipeline.cacher("redis").lookup("meta_if", value.get("meta", {}).get("iface_out", {}).get("id", None)),
            "ip_tos": value.get("meta", {}).get("ip_tos", None),
            "ip_version":  value.get("meta", {}).get("ip_version", None),
            "ipv6_flow_label": value.get("meta", {}).get("ipv6", {}).get("flow_label", None),
            "mpls_bottom_label": value.get("meta", {}).get("mpls", {}).get("bottom_label", None),
            "mpls_exp": mpls_exp,
            "mpls_labels": value.get("meta", {}).get("mpls", {}).get("labels", []),
            "mpls_pw_id": value.get("meta", {}).get("mpls", {}).get("pw_id", None),
            "mpls_stack_depth": value.get("meta", {}).get("mpls", {}).get("stack_depth", None),
            "mpls_top_label": value.get("meta", {}).get("mpls", {}).get("top_label", None),
            "mpls_top_label_ip": value.get("meta", {}).get("mpls", {}).get("top_label_ip", None),
            "mpls_top_label_type": value.get("meta", {}).get("mpls", {}).get("top_label_type", None),
            "mpls_vpn_rd": value.get("meta", {}).get("mpls", {}).get("vpn_rd", None),
            "protocol": value.get("meta", {}).get("protocol", None),
            "src_as_name": value.get("meta", {}).get("src_as_name", "unknown"),
            "src_asn": value.get("meta", {}).get("src_asn", None),
            "src_continent": value.get("meta", {}).get("src_continent", None),
            "src_country_name": value.get("meta", {}).get("src_country_name", None),
            "src_esdb_ipsvc_ref": self.pipeline.cacher("redis").lookup_list("meta_esdb_ipsvc", value.get("meta", {}).get("esdb", {}).get("src", {}).get("service", {}).get("prefix_group_name", [])),
            "src_ip": value.get("meta", {}).get("src_ip", None),
            "src_loc_lat": value.get("meta", {}).get("src_location", {}).get("latitude", None),
            "src_loc_lon": value.get("meta", {}).get("src_location", {}).get("longitude", None),
            "src_org": value.get("meta", {}).get("src_organization", None),
            "src_port": value.get("meta", {}).get("src_port", None),
            "src_pref_loc_lat": value.get("meta", {}).get("src_preferred_location", {}).get("lat", None),
            "src_pref_loc_lon": value.get("meta", {}).get("src_preferred_location", {}).get("lon", None),
            "src_pref_org": value.get("meta", {}).get("src_preferred_org", None),
            "src_pub_asn": value.get("meta", {}).get("src_pub_asn", None),
            "src_region_iso_code": value.get("meta", {}).get("src_region_iso_code", None),
            "src_region_name": value.get("meta", {}).get("src_region_name", None),
            "src_scireg_ref": self.pipeline.cacher("ip").lookup("meta_scireg", value.get("meta", {}).get("src_ip", None)),
            "traffic_class": value.get("meta", {}).get("traffic_class", None),
            "vrf_egress_id": value.get("meta", {}).get("vrf", {}).get("egress_id", None),
            "vrf_ingress_id": value.get("meta", {}).get("vrf", {}).get("ingress_id", None),
            "bits_per_sec": value.get("values", {}).get("bits_per_second", None),
            "duration": value.get("values", {}).get("duration", None),
            "max_pkt_len": value.get("values", {}).get("max_packet_len", None),
            "max_ttl": value.get("values", {}).get("max_ttl", None),
            "min_pkt_len": value.get("values", {}).get("min_packet_len", None),
            "min_ttl": value.get("values", {}).get("min_ttl", None),
            "num_bits": value.get("values", {}).get("num_bits", None),
            "num_pkts": value.get("values", {}).get("num_packets", None),
            "pkts_per_sec": value.get("values", {}).get("packets_per_second", None)
        }]