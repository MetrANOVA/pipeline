import logging
import orjson
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.flow import BaseFlowProcessor

logger = logging.getLogger(__name__)

class NFAcctdFlowProcessor(BaseFlowProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)

        #default to netflow if not set
        if self.flow_type == "unknown":
            self.flow_type = "netflow"

        # Define required fields - these are examples, adjust as needed
        self.required_fields = [
            ['timestamp_start'],
            ['timestamp_end'],
            ['ip_src'], 
            ['ip_dst'],
            ['port_src'],
            ['port_dst'],
            ['ip_proto']
        ]

    def add_bgp_extensions(self, value: dict, ext: dict):
        ########### BGP Extensions ############
        # Calculate bgp_as_path_padding - count consecutive identical ASNs and record counts in padding. Removed duplicates from as_path_id
        if value.get("as_path", None):
            as_path_str = value.get("as_path", "").split("_")
            bgp_as_path_id = []
            bgp_as_path_padding = []
            padding_count = 1
            for as_str in as_path_str:
                #convert to int
                try:
                    asn = int(as_str)
                except ValueError:
                    continue
                if bgp_as_path_id and asn == bgp_as_path_id[-1]:
                    padding_count += 1
                else:
                    bgp_as_path_id.append(asn)
                    bgp_as_path_padding.append(padding_count)
                    padding_count = 1
            ext["bgp_as_path_id"] = bgp_as_path_id
            ext["bgp_as_path_padding"] = bgp_as_path_padding
            # Other BGP fields
            if value.get("comms", None):
                ext["bgp_community"] = value.get("comms", "").split("_")
            if value.get("ecomms", None):
                ext["bgp_ext_community"] = value.get("ecomms", "").split("_")
            if value.get("lcomms", None):
                ext["bgp_large_community"] = value.get("lcomms", "").split("_")
            if value.get("local_pref", None) is not None:
                try:
                    ext["bgp_local_pref"] = int(value.get("local_pref"))
                except ValueError:
                    pass
            if value.get("med", None) is not None:
                try:
                    ext["bgp_med"] = int(value.get("med"))
                except ValueError:
                    pass

    def add_ipv4_extensions(self, value: dict, ext: dict):
        if value.get("tos", None) is not None:
            try:
                ext["ipv4_tos"] = int(value.get("tos"))
                ext["ipv4_dscp"] = ext["ipv4_tos"] >> 2  # DSCP is upper 6 bits of TOS
            except ValueError:
                pass

    def add_ipv6_extensions(self, value: dict, ext: dict):
        if value.get("ipv6_flow_label", None) is not None:
            try:
                ext["ipv6_flow_label"] = int(value.get("ipv6_flow_label"))
            except ValueError:
                pass

    def add_mpls_extensions(self, value: dict, ext: dict):
        # depending on pmacct version, mpls_labels may be string of labels separated by _ or maybe separate fields mpls_label_1, mpls_label_2, etc (up to 10)
        mpls_labels = []
        # try the separate fields first
        if value.get("mpls_label1", None):
            for i in range(1, 11):
                label_key = f"mpls_label{i}"
                if value.get(label_key, None):
                    mpls_labels.append(value.get(label_key))
                else:
                    break
        #if that didn't work, try the single string field
        if not mpls_labels:
            mpls_labels_str = value.get("mpls_labels", None)
            if mpls_labels_str:
                mpls_labels = mpls_labels_str.split("_")
        # now that we have the labels they are each hex string with hyphens, convert to integers
        if mpls_labels:
            ext["mpls_labels"] = []
            ext["mpls_exp"] = []
            for raw_label in mpls_labels:
                try:
                    hex_label = raw_label.replace("-", "")
                    label_int = int(hex_label, 16)
                    label = label_int >> 4
                    exp = (label_int & 0b1110) >> 1
                    ext["mpls_labels"].append(label)
                    ext["mpls_exp"].append(exp)
                    # last non-zero label processed will be bottom label
                    if label != 0:
                        ext["mpls_bottom_label"] = label  
                except ValueError:
                    continue
        if value.get("mpls_pw_id", None) is not None:
            try:
                ext["mpls_pw"] = int(value.get("mpls_pw_id"))
            except ValueError:
                pass
        if value.get("mpls_top_label_ipv4", None):
            ext["mpls_top_label_ip"] = value.get("mpls_top_label_ipv4")
        if value.get("mpls_top_label_type", None) is not None:
            try:
                ext["mpls_top_label_type"] = int(value.get("mpls_top_label_type"))
            except ValueError:
                pass
        if value.get("mpls_vpn_rd", None):
            ext["mpls_vpn_rd"] = value.get("mpls_vpn_rd")

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # todo: map to device id
        device_id = value.get("peer_ip_src", None)  
        
        #todo: map to in and out interface ids as strings
        interface_in_id = str(value.get("iface_in"))
        interface_out_id = str(value.get("iface_out"))
        #todo : uncomment this when we have reliable device_id mapping
        # interface_in_index = str(value.get("iface_in"))
        # interface_in_id = self.pipeline.cacher("redis").lookup("meta_interface__device_id__flow_index", "{}:{}".format(device_id, interface_in_index))
        # interface_out_index = str(value.get("iface_out"))
        # interface_out_id = self.pipeline.cacher("redis").lookup("meta_interface__device_id__flow_index", "{}:{}".format(device_id, interface_out_index))
        
        # todo: determine application port - use dst port if available, else src port
        application_port = value.get("port_dst", None)
        if application_port is None:
            application_port = value.get("port_src", None)

        # todo: all the ref lookups
        
        # determine ip version - use quick method based on presence of ':' in ip address
        ip_version = 4
        if ':' in value["ip_src"]:
            ip_version = 6

        # Calculate start and end times. Need to be ms since epoch since type is DateTime64(3, 'UTC')
        try:
            start_time = int(float(value.get("timestamp_start", 0)) * 1000)
            end_time = int(float(value.get("timestamp_end", 0)) * 1000)
        except (ValueError, TypeError):
            self.logger.debug("Invalid timestamp format in flow record")
            return None

        # Build extension fields
        ext = {}
        self.add_bgp_extensions(value, ext)
        self.add_ipv4_extensions(value, ext)
        self.add_ipv6_extensions(value, ext)
        self.add_mpls_extensions(value, ext)

        # Pull the relevant fields from the value dict as needed.
        # you may also need to do some formattingto make sure theya re the right data type, etc
        # if you are not sure about something, just set it to None for now
        formatted_record = { 
            "start_time": start_time,
            "end_time": end_time,
            "collector_id": value.get("label", "unknown"),
            "policy_originator": self.policy_originator,
            "policy_level": self.policy_level,
            "ext": orjson.dumps(ext).decode('utf-8'),
            "flow_type": self.flow_type,
            "device_id": device_id,
            "device_ref": self.pipeline.cacher("redis").lookup("meta_device", device_id),
            "src_as_id": value.get("as_src", None),
            "src_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value.get("as_src", None)),
            "src_ip": value.get("ip_src", None),
            "src_ip_ref": self.pipeline.cacher("ip").lookup("meta_ip", value.get("ip_src", None)),
            "src_port": value.get("port_src", None),
            "dst_as_id":  value.get("as_dst", None),
            "dst_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value.get("as_dst", None)),
            "dst_ip": value.get("ip_dst", None),
            "dst_ip_ref": self.pipeline.cacher("ip").lookup("meta_ip", value.get("ip_dst", None)),
            "dst_port": value.get("port_dst", None),
            "protocol": value.get("ip_proto", None),
            "in_interface_id": interface_in_id,
            "in_interface_ref": self.pipeline.cacher("redis").lookup("meta_interface", interface_in_id),
            "out_interface_id": interface_out_id,
            "out_interface_ref": self.pipeline.cacher("redis").lookup("meta_interface", interface_out_id),
            "peer_as_id": value.get("peer_as_dst", None),
            "peer_as_ref": self.pipeline.cacher("redis").lookup("meta_as", value.get("peer_as_dst", None)),
            "peer_ip": value.get("peer_ip_dst", None),
            "peer_ip_ref": self.pipeline.cacher("ip").lookup("meta_ip", value.get("peer_ip_dst", None)),
            "ip_version": ip_version,
            "application_port": application_port,
            "bit_count": value.get("bytes", 0),
            "packet_count": value.get("packets", 0),
        }

        # format integers
        int_fields = ['src_as_id', 'src_port', 'dst_as_id', 'dst_port', 'peer_as_id', 'application_port', 'bit_count', 'packet_count']
        for field in int_fields:
            if formatted_record[field] is not None:
                try:
                    formatted_record[field] = int(formatted_record[field])
                except ValueError:
                    formatted_record[field] = 0

        #scale bit_count to bits
        formatted_record['bit_count'] *= 8

        #auto fill policy scope - copy to avoid modifying original list
        formatted_record["policy_scope"] = self.policy_scope.copy()
        if self.policy_auto_scopes:
            scopes = set()
            if formatted_record["src_as_id"]:
                scopes.add(f"as:{formatted_record['src_as_id']}")
            if formatted_record["dst_as_id"]:
                scopes.add(f"as:{formatted_record['dst_as_id']}")
            #todo: handle community based scopes when we have vrtr_name
        formatted_record["policy_scope"].extend(list(scopes))

        # expects a list returned (there are cases that aren;t this where you produce multiple records)
        # just wraps our one dict in an array
        return [formatted_record]

class SFAcctdFlowProcessor(BaseFlowProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        # Define required fields - these are examples, adjust as needed
        self.required_fields = [
            ['src_ip'], 
            ['dst_ip'], 
            ['toplevel', 'nested']
        ]

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None
        
        # Value should be a dict of the JSON object sent from sfacctd to Kafka
        self.logger.info( f"Processing sfacctd flow record: {value}" )
        
        # build the record here. The keys match the ClickHouse column names.
        # Printing column names for reference:
        self.logger.info(f"Column names: {self.column_names()}")
        # Pull the relevant fields from the value dict as needed. 
        # you may also need to do some formattingto make sure theya re the right data type, etc
        # if you are not sure about something, just set it to None for now
        formatted_record = {}

        # expects a list returned (there are cases that aren;t this where you produce multiple records)
        # just wraps our one dict in an array
        return [formatted_record]