"""
Classes that import data from Stardust logstash pipelines
"""
from datetime import datetime, timedelta
import os
import orjson
from typing import Iterator, Dict, Any
from metranova.processors.clickhouse.base import BaseDataProcessor
from metranova.processors.clickhouse.flow import BaseFlowProcessor
import logging

from metranova.processors.clickhouse.interface import BaseInterfaceTrafficProcessor

logger = logging.getLogger(__name__)

class FlowProcessor(BaseFlowProcessor):
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
                "mpls_label": value.get("meta", {}).get("mpls", {}).get("labels", []),
                "mpls_pw": value.get("meta", {}).get("mpls", {}).get("pw_id", None),
                "mpls_top_label_ip": value.get("meta", {}).get("mpls", {}).get("top_label_ip", None),
                "mpls_top_label_type": value.get("meta", {}).get("mpls", {}).get("top_label_type", None),
                "mpls_vpn_rd": value.get("meta", {}).get("mpls", {}).get("vpn_rd", None)
            })
        if self.extension_is_enabled("esdb"):
            ext.update({
                "src_ip_esdb_ref": self.pipeline.cacher("redis").lookup_list("meta_ip_esdb", value.get("meta", {}).get("esdb", {}).get("src", {}).get("service", {}).get("prefix_group_name", [])),
                "dst_ip_esdb_ref": self.pipeline.cacher("redis").lookup_list("meta_ip_esdb", value.get("meta", {}).get("esdb", {}).get("dst", {}).get("service", {}).get("prefix_group_name", []))
            })
        if self.extension_is_enabled("scireg"):
            ext.update({
                "dst_scireg_ref": self.pipeline.cacher("ip").lookup("meta_ip_scireg", value.get("meta", {}).get("dst_ip", None)),
                "src_scireg_ref": self.pipeline.cacher("ip").lookup("meta_ip_scireg", value.get("meta", {}).get("src_ip", None))
            })

        return [{
            "start_time": value.get("start"),
            "end_time": value.get("end"),
            "collector_id": value.get("meta", {}).get("sensor_id", None),
            "policy_originator": value.get("policy", {}).get("originator", None),
            "policy_level": value.get("policy", {}).get("level", None),
            "policy_scope": value.get("policy", {}).get("scopes", []),
            "ext": orjson.dumps(ext).decode('utf-8'),
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

class InterfaceTrafficProcessor(BaseInterfaceTrafficProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.meta_if_lookup_table = os.getenv('CLICKHOUSE_IF_METADATA_TABLE', 'meta_interface')
        self.meta_if_traffic_interval = int(os.getenv('METRANOVA_IF_TRAFFIC_INTERVAL', '30'))
        self.required_fields = [
            ['start'], 
            ['meta', 'id'], 
            ['meta', 'device']
        ]
        #set this to true in subclass if want to skip checking for meta.name field
        self.skip_required_name = False
    
    def has_required_fields(self, value):
        if not super().has_required_fields(value):
            return False
        
        #special case: check for missing name due to issue in source data
        if self.skip_required_name:
            return True
        if value.get('meta', {}).get('name', None) is None:
            meta_id = value.get('meta', {}).get('id', '')
            if '::' in meta_id:
                parts = meta_id.split('::', 1)
                if len(parts) == 2:
                    value.setdefault('meta', {})['name'] = parts[1]
        if value.get('meta', {}).get('name', None) is None:
            self.logger.error(f"Missing required field 'name' in message value and has no parsable meta.id: {value}")
            return False

        #everything is present so return true
        return True

    def match_message(self, value):
        #make sure we have at least one of values fields we care about
        values_fields = ['in_bits', 'in_pkts', 'in_errors', 'in_discards', 'in_ucast_pkts', 'in_mcast_pkts', 'in_bcast_pkts', 'out_bits', 'out_pkts', 'out_errors', 'out_discards', 'out_ucast_pkts', 'out_mcast_pkts', 'out_bcast_pkts', 'admin_state', 'oper_state']
        if not any(value.get('values', {}).get(f, {}).get("val", None) is not None for f in values_fields):
            self.logger.debug(f"Missing all values fields in message value")
            return False

        return True

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # Build message dictionary
        return [{
            "start_time": value.get("start"),
            "end_time": datetime.fromisoformat(value.get("start").replace('Z', '+00:00')) + timedelta(seconds=self.meta_if_traffic_interval),
            "collector_id": value.get("meta", {}).get("sensor_id")[0] if isinstance(value.get("meta", {}).get("sensor_id", None), list) else value.get("meta", {}).get("sensor_id", "unknown"),
            "policy_originator": value.get("policy", {}).get("originator", None),
            "policy_level": value.get("policy", {}).get("level", None),
            "policy_scope": value.get("policy", {}).get("scopes", []),
            "ext": '{}',
            "interface_id": value.get("meta", {}).get("id", None),
            "interface_ref": self.pipeline.cacher("redis").lookup(self.meta_if_lookup_table, value.get("meta", {}).get("id", None)),
            "admin_status": value.get("values", {}).get("admin_state", {}).get("val", None),
            "oper_status": value.get("values", {}).get("oper_state", {}).get("val", None),
            "in_bit_count": value.get("values", {}).get("in_bits", {}).get("val", None),
            "in_error_packet_count": value.get("values", {}).get("in_errors", {}).get("val", None),
            "in_discard_packet_count": value.get("values", {}).get("in_discards", {}).get("val", None),
            "in_ucast_packet_count": value.get("values", {}).get("in_ucast_pkts", {}).get("val", None),
            "in_mcast_packet_count": value.get("values", {}).get("in_mcast_pkts", {}).get("val", None),
            "in_bcast_packet_count": value.get("values", {}).get("in_bcast_pkts", {}).get("val", None),
            "out_bit_count": value.get("values", {}).get("out_bits", {}).get("val", None),
            "out_error_packet_count": value.get("values", {}).get("out_errors", {}).get("val", None),
            "out_discard_packet_count": value.get("values", {}).get("out_discards", {}).get("val", None),
            "out_ucast_packet_count": value.get("values", {}).get("out_ucast_pkts", {}).get("val", None),
            "out_mcast_packet_count": value.get("values", {}).get("out_mcast_pkts", {}).get("val", None),
            "out_bcast_packet_count": value.get("values", {}).get("out_bcast_pkts", {}).get("val", None)
        }]