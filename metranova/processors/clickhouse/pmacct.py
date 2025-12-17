import logging
import orjson
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.flow import BaseFlowProcessor

logger = logging.getLogger(__name__)

class PMAcctFlowProcessor(BaseFlowProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.flow_type_map = {
            "nfacctd": "netflow",
            "sfacctd": "sflow"
        }
        # Define required fields - these are examples, adjust as needed
        self.required_fields = [
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
            mpls_labels_str = value.get("mpls_label", None)
            if mpls_labels_str:
                mpls_labels = mpls_labels_str.split("_")
        # now that we have the labels they are each hex string with hyphens, convert to integers
        if mpls_labels:
            ext["mpls_label"] = []
            ext["mpls_exp"] = []
            for raw_label in mpls_labels:
                try:
                    hex_label = raw_label.replace("-", "")
                    label_int = int(hex_label, 16)
                    label = label_int >> 4
                    exp = (label_int & 0b1110) >> 1
                    ext["mpls_label"].append(label)
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

    def lookup_ip_as_fields(self, ip_field: str, target_ip_field: str, as_field: str, target_as_field: str, value: dict, formatted_record: dict):
        """ Lookup IP address and AS fields in cache to get associated metadata. If AS not provided, try to get from IP cache."""
        formatted_record[target_ip_field] = value.get(ip_field, None)
        #ignore if no IP provided including if empty string
        if not formatted_record[target_ip_field]:
            formatted_record[target_ip_field] = None
            formatted_record[f"{target_ip_field}_ref"] = None
            formatted_record[f"{target_as_field}_id"] = None
            formatted_record[f"{target_as_field}_ref"] = None
            return
        as_id_field = f"{target_as_field}_id"
        formatted_record[as_id_field] = value.get(as_field, None)
        ip_cache_result = self.pipeline.cacher("ip").lookup("meta_ip", formatted_record[target_ip_field])
        if ip_cache_result:
            formatted_record[f"{target_ip_field}_ref"] = ip_cache_result.get("ref", None)
            #if no AS provided in value (0 or None), see if we have one in the cached result
            try:
                if formatted_record[as_id_field] is None or int(formatted_record[as_id_field]) == 0:
                    formatted_record[as_id_field] = ip_cache_result.get("as_id", None)
            except ValueError:
                pass
        else:
            formatted_record[f"{target_ip_field}_ref"] = None

        #set the as ref field
        formatted_record[f"{target_as_field}_ref"] = self.pipeline.cacher("clickhouse").lookup_dict_key("meta_as", formatted_record[as_id_field], "ref")

    def add_vlan_extensions(self, value: dict, ext: dict):
        # namespace: vlan, direction: in or out (optional), type: id or inner_id
        if value.get("vlan", None) is not None:
            try:
                ext["vlan_id"] = int(value.get("vlan"))
            except ValueError:
                pass
        if value.get("vlan_in", None) is not None:
            try:
                ext["vlan_in_id"] = int(value.get("vlan_in"))
            except ValueError:
                pass
        if value.get("vlan_out", None) is not None:
            try:
                ext["vlan_out_id"] = int(value.get("vlan_out"))
            except ValueError:
                pass
        if value.get("cvlan_in", None) is not None:
            try:
                ext["vlan_in_inner_id"] = int(value.get("cvlan_in"))
            except ValueError:
                pass
        if value.get("cvlan_out", None) is not None:
            try:
                ext["vlan_out_inner_id"] = int(value.get("cvlan_out"))
            except ValueError:
                pass

    def lookup_interface(self, device_id: str, flow_index: Any, formatted_record: dict, direction: str) -> None:
        """ Lookup interface based on device_id and flow_index. Sets interface_id, interface_ref, and interface_edge in formatted_record."""
        #Initialize values
        interface_id = None
        interface_ref = None
        interface_edge = False

        # make sure we have the keys we need
        if device_id is not None and flow_index is not None:
            # Try edge true first
            flow_index = str(flow_index)
            interface_details = self.pipeline.cacher("clickhouse").lookup("meta_interface:device_id:flow_index:edge", f"{device_id}:{flow_index}:True")
            # check if we got details
            if interface_details:
                # set edge to true if we did
                interface_edge = True
            else:
                # otherwise try edge false
                interface_details = self.pipeline.cacher("clickhouse").lookup("meta_interface:device_id:flow_index:edge", f"{device_id}:{flow_index}:False")
            #set id and ref if we found details
            if interface_details:
                interface_id = interface_details.get("id", None)
                interface_ref = interface_details.get("ref", None)
        
        #update record
        formatted_record[f"{direction}_interface_id"] = interface_id
        formatted_record[f"{direction}_interface_ref"] = interface_ref
        formatted_record[f"{direction}_interface_edge"] = interface_edge

    def lookup_device(self, device_ip: str, formatted_record: dict):
        #initialize values
        device_id = "unknown"
        device_ref = None
        if device_ip:
            #if we have an ip, at a minimum device_id should be that
            device_id = device_ip
            device_details = self.pipeline.cacher("clickhouse").lookup("meta_device:@loopback_ip", device_ip)
            if device_details:
                #if we got this far, set to id of fallback to ip
                device_id = device_details.get("id", device_id)
                device_ref = device_details.get("ref", None)
        formatted_record["device_id"] = device_id
        formatted_record["device_ref"] = device_ref

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None
        #check time fields:
        # - nfacctd will have timestamp_start and timestamp_end
        # - sfacctd will have timestamp_arrival
        if value.get("timestamp_arrival", None) is not None and value.get("timestamp_start", None) is None:
            # must be sfacctd, set start and end to arrival
            value["timestamp_start"] = value["timestamp_arrival"]
            value["timestamp_end"] = value["timestamp_arrival"]
        elif value.get("timestamp_start", None) is None:
            # if no start (and already checked for arrival), then we can't process this record
            self.logger.debug("Missing timestamp fields in flow record")
            return None
        elif value.get("timestamp_end", None) is None:
            #just in case end is not specified, set it to start - probably should not happen
            value["timestamp_end"] = value["timestamp_start"]

        # Determine flow type from writer_id if available, otherwise use default
        flow_type = self.flow_type
        if self.flow_type_map.get(value.get("writer_id", None), None):
            flow_type = self.flow_type_map[value["writer_id"]]

        #Initialize formatted record
        formatted_record = {}

        #Lookup IPs in cache
        self.lookup_ip_as_fields("ip_src", "src_ip", "as_src", "src_as", value, formatted_record)
        self.lookup_ip_as_fields("ip_dst", "dst_ip", "as_dst", "dst_as", value, formatted_record)
        self.lookup_ip_as_fields("peer_ip_dst", "peer_ip", "peer_as_dst", "peer_as", value, formatted_record)

        # Lookup device id based on IP address
        self.lookup_device(value.get("peer_ip_src", None), formatted_record)

        #map to in and out interface ids as strings
        self.lookup_interface(formatted_record["device_id"], value.get("iface_in", None), formatted_record, "in")
        self.lookup_interface(formatted_record["device_id"], value.get("iface_out", None), formatted_record, "out")

        # todo: determine application port - use dst port if available, else src port
        application_port = value.get("port_dst", None)
        if application_port is None:
            application_port = value.get("port_src", None)

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
        self.add_vlan_extensions(value, ext)
        ext.update(self.lookup_ip_ref_extensions(formatted_record.get("src_ip", None), "src"))
        ext.update(self.lookup_ip_ref_extensions(formatted_record.get("dst_ip", None), "dst"))

        # Pull the relevant fields from the value dict as needed.
        # you may also need to do some formatting to make sure they are the right data type, etc
        formatted_record.update({ 
            "start_time": start_time,
            "end_time": end_time,
            "collector_id": value.get("label", "unknown"),
            "policy_originator": self.policy_originator,
            "policy_level": self.policy_level,
            "ext": orjson.dumps(ext).decode('utf-8'),
            "flow_type": flow_type,
            "src_port": value.get("port_src", None),
            "dst_port": value.get("port_dst", None),
            "protocol": value.get("ip_proto", None),
            "ip_version": ip_version,
            "application_port": application_port,
            "bit_count": value.get("bytes", 0),
            "packet_count": value.get("packets", 0),
        })

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
            if ext.get("mpls_vpn_rd", None) and ":" in ext["mpls_vpn_rd"]:
                #split rd into parts by colon and grab the last part
                rd_key = ext["mpls_vpn_rd"].split(":")[-1]
                if rd_key in self.policy_community_scope_map:
                    scopes.add(f"comm:{self.policy_community_scope_map[rd_key]}")
        formatted_record["policy_scope"].extend(list(scopes))

        # expects a list returned (there are cases that aren;t this where you produce multiple records)
        # just wraps our one dict in an array
        return [formatted_record]
