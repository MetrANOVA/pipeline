import logging
import orjson
import ipaddress
from datetime import datetime, timezone
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.flow import BaseFlowProcessor
from metranova.processors.clickhouse.bmp import BaseBMPRouteProcessor, BaseBMPStatProcessor

logger = logging.getLogger(__name__)

class PMAcctUtilMixin:
    def __init__(self):
        pass

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

    def lookup_ip(self, ip_field: str, target_ip_field: str, value: dict, formatted_record: dict):
        formatted_record[target_ip_field] = value.get(ip_field, None)
        ip_cache_result = self.pipeline.cacher("ip").lookup("meta_ip", formatted_record[target_ip_field])
        if ip_cache_result:
            formatted_record[f"{target_ip_field}_ref"] = ip_cache_result[0]
        else:
            formatted_record[f"{target_ip_field}_ref"] = None

class PMAcctFlowProcessor(BaseFlowProcessor, PMAcctUtilMixin):
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
        
        #Lookup ip ref, we'll lookup AS later
        ip_cache_result = self.pipeline.cacher("ip").lookup("meta_ip", formatted_record[target_ip_field])
        if ip_cache_result:
            formatted_record[f"{target_ip_field}_ref"] = ip_cache_result[0]
        else:
            formatted_record[f"{target_ip_field}_ref"] = None

        #lookup as_id if not provided, using ip preference order (may be different from cacher above which always uses meta_ip)
        provided_as_id = formatted_record[as_id_field]
        if provided_as_id is not None:
            try:
                provided_as_id = int(provided_as_id)
            except ValueError:
                provided_as_id = None
            # give the formatted as_id
            formatted_record[as_id_field] = provided_as_id
        if provided_as_id is None or provided_as_id == 0:
            for ip_to_as_table in self.ip_to_as_lookup_order:
                as_cache_result = None
                #optimize for the case where ip_to_as_table is meta_ip since we already did that lookup above
                if ip_to_as_table == "meta_ip":
                    as_cache_result = ip_cache_result
                else:
                    as_cache_result = self.pipeline.cacher("ip").lookup(ip_to_as_table, formatted_record[target_ip_field])
                if as_cache_result and len(as_cache_result) > 1:
                    formatted_record[as_id_field] = as_cache_result[1]
                    break
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

        #determine application port - by finding lowest recognized port in src and dst, since we don't always know which is client and which is server. If neither are recognized ports, will just use dst port as application port if available
        src_port = value.get("port_src", 0)
        dst_port = value.get("port_dst", 0)
        src_app_lookup = self.pipeline.cacher("clickhouse_ranged").lookup_key_range("meta_application", value.get("ip_proto", None), src_port)
        dst_app_lookup = self.pipeline.cacher("clickhouse_ranged").lookup_key_range("meta_application", value.get("ip_proto", None), dst_port)
        application_port = 0
        if src_app_lookup and dst_app_lookup:
            #use the smaller port number of the two if both are recognized application ports
            try:
                application_port = min(int(src_port), int(dst_port))
            except ValueError:
                application_port = 0
        elif src_app_lookup:
            application_port = src_port
        elif dst_app_lookup:
            #default to dst port if no recognized app port in src - this is common since often only server port is well known app port
            application_port = dst_port

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
            "src_port": src_port,
            "dst_port": dst_port,
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

class PMAcctBMPRouteProcessor(BaseBMPRouteProcessor, PMAcctUtilMixin):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [
            ['bmp_router'], 
            ['event_type'],
            ['peer_ip'],
            ['bmp_msg_type'],
            ['log_type'],
            ['timestamp'],
            ['timestamp_arrival']

        ]

    def match_message(self, value):
        if value.get("bmp_msg_type", "") != "route_monitor":
            return False
        return super().match_message(value)

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None

        #init
        formatted_record = {}
        ext = {}

        #determine log type
        formatted_record["action"] = value.get("log_type", None)
        if formatted_record["action"] in ["update", "withdraw"]:
            #updates and withdraws require an ip_prefix
            if not value.get("ip_prefix", None):
                return None
        elif formatted_record["action"] == "delete":
            # no prefix needed, set to ::/0
            if not value.get("ip_prefix", None):
                value["ip_prefix"] = "::/0"
        else:
            #unknown log_type
            return None
        
        # lookup device
        self.lookup_device(value.get("bmp_router", ""), formatted_record)

        #lookup ip field
        self.lookup_ip("ip_prefix", "ip_prefix", value, formatted_record)
        self.lookup_ip("bgp_nexthop", "bgp_nexthop", value, formatted_record)
        self.lookup_ip("peer_ip", "peer_ip", value, formatted_record)

        #Parse BGP fields
        as_path_str = value.get("as_path", "").split(" ")
        bgp_as_path_id = []
        bgp_as_path_ref = []
        for as_str in as_path_str:
            #convert to int
            try:
                asn = int(as_str)
            except ValueError:
                continue
            bgp_as_path_id.append(asn)
            # lookup AS ref
            as_info = self.pipeline.cacher("clickhouse").lookup("meta_as", asn)
            if as_info:
                bgp_as_path_ref.append(as_info.get("ref", None))
            else:
                bgp_as_path_ref.append(None)
        formatted_record["as_path_id"] = bgp_as_path_id

        # Other BGP fields
        if value.get("comms", None):
            formatted_record["community"] = value.get("comms", "").split(" ")
        else:
            formatted_record["community"] = []
        if value.get("ecomms", None):
            formatted_record["ext_community"] = value.get("ecomms", "").split(" ")
        else:
            formatted_record["ext_community"] = []
        if value.get("local_pref", None) is not None:
            try:
                formatted_record["local_pref"] = int(value.get("local_pref"))
            except ValueError:
                pass
        if value.get("med", None) is not None:
            try:
                formatted_record["med"] = int(value.get("med"))
            except ValueError:
                pass

        # Parse IP prefix a tuple
        ip_prefix_str = value.get("ip_prefix") #required so should always return
        ip_prefix_tuple = tuple(ip_prefix_str.split("/"))
        if len(ip_prefix_tuple) == 2:
            formatted_record["ip_prefix_address"] = ip_prefix_tuple[0]
            formatted_record["ip_prefix_length"] = int(ip_prefix_tuple[1])
        elif ":" in ip_prefix_tuple[0]:
            #ipv6 address without a prefix length, default to /128
            formatted_record["ip_prefix_address"] = ip_prefix_tuple[0]
            formatted_record["ip_prefix_length"] = 128
        else:
            #ipv4 address without a prefix length, default to /32
            formatted_record["ip_prefix_address"] = ip_prefix_tuple[0]
            formatted_record["ip_prefix_length"] = 32
        #calculate range start and end based on address and length
        try:
            network = ipaddress.ip_network(f"{formatted_record['ip_prefix_address']}/{formatted_record['ip_prefix_length']}", strict=False)
            formatted_record["ip_prefix_range_start"] = str(network.network_address)
            formatted_record["ip_prefix_range_end"] = str(network.broadcast_address)
        except ValueError:
            # We need a valid IP address and prefix length to calculate the range. If the provided values are invalid, we cannot proceed with this record.
            return None

        #get add_path_id if present, otherwise default to 0
        try:
            formatted_record["add_path_id"] = int(value.get("add_path_id", 0))
        except ValueError:
            formatted_record["add_path_id"] = 0

        # set IP version
        afi = value.get("afi", None)
        if afi == 1:
            formatted_record["ip_version"] = 4
        elif afi == 2:
            formatted_record["ip_version"] = 6
        else:
            formatted_record["ip_version"] = None

        # Update formatted_record with additional fields
        formatted_record.update({
            "observation_time": int(float(value.get("timestamp", 0)) * 1000),
            "export_time": int(float(value.get("timestamp_arrival", 0)) * 1000),
            "collector_id": value.get("label", None),
            "policy_originator": "unknown",
            "policy_level": "unknown",
            "policy_scope": ["unknown"],
            "ext": orjson.dumps(ext).decode('utf-8'),
            "is_in": value.get("is_in", False),
            "is_out": value.get("is_out", False),
            "is_post": value.get("is_post", False),
            "afi": afi,
            "safi": value.get("safi", None),
            "as_path_ref": [], #TODO: lookup
            "origin": value.get("origin", ""),
            "local_pref": value.get("local_pref", None),
            "med": value.get("med", None),
            "rd": value.get("rd", None),
            "mpls_label": value.get("mpls_label", None),
            "aigp": value.get("aigp", None),
            "psid_li": value.get("psid_li", None),
            "path_status": value.get("path_status", []),
            "reason_code": value.get("reason_code", [])
        })
        return [formatted_record]


class PMAcctBMPStatProcessor(BaseBMPStatProcessor, PMAcctUtilMixin):
    """Parse BMP Statistics Report messages (bmp_msg_type: "stats") from pmacct/pbmpd.

    Each JSON object from the source represents a single IANA statistics counter
    observation for one BGP peer on one router.  The processor maps those fields
    onto the ``data_bmp_stat`` table columns defined by ``BaseBMPStatProcessor``.
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [
            ['bmp_router'],
            ['event_type'],
            ['peer_ip'],
            ['bmp_msg_type'],
            ['timestamp'],
            ['counter_type'],
        ]

    def match_message(self, value):
        return value.get("bmp_msg_type", "") == "stats"

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        if not self.has_required_fields(value):
            return None

        formatted_record = {}
        ext = {}

        # observation_time maps to pbmpd `timestamp` — when the BMP stats
        # message was generated on the router. pbmpd sends this as an ISO 8601
        # string; fall back to epoch 0 if missing or unparseable.
        ts_raw = value.get("timestamp", "")
        try:
            ts_ms = int(datetime.fromisoformat(ts_raw).timestamp() * 1000)
        except (ValueError, TypeError):
            try:
                ts_ms = int(float(ts_raw) * 1000)
            except (ValueError, TypeError):
                ts_ms = 0
        formatted_record["observation_time"] = ts_ms

        # Device / router identity
        self.lookup_device(value.get("bmp_router", ""), formatted_record)

        # BGP peer IP — sets peer_ip and peer_ip_ref
        self.lookup_ip("peer_ip", "peer_ip", value, formatted_record)

        # BGP peer AS — provided directly in the message; look up the ref separately
        peer_asn = value.get("peer_asn", None)
        if peer_asn is not None:
            try:
                peer_asn = int(peer_asn)
            except (ValueError, TypeError):
                peer_asn = None
        formatted_record["peer_as_id"] = peer_asn
        formatted_record["peer_as_ref"] = self.pipeline.cacher("clickhouse").lookup_dict_key(
            "meta_as", peer_asn, "ref"
        )

        # BGP peer type (IANA peer type code)
        peer_type = value.get("peer_type", None)
        if peer_type is not None:
            try:
                peer_type = int(peer_type)
            except (ValueError, TypeError):
                peer_type = None
        formatted_record["peer_type"] = peer_type

        # BGP router ID of the remote peer
        formatted_record["bgp_id"] = value.get("bgp_id", None)

        # Route Distinguisher
        formatted_record["rd"] = value.get("rd", None)

        # Loc-RIB / filtered scope flags
        formatted_record["is_loc"] = bool(value.get("is_loc", False))
        formatted_record["is_filtered"] = bool(value.get("is_filtered", False))

        # Statistics metric fields — map pbmpd counter_type / counter_type_str /
        # counter_value onto metric_type / metric_type_str / metric_value.
        metric_type = value.get("counter_type", None)
        if metric_type is not None:
            try:
                metric_type = int(metric_type)
            except (ValueError, TypeError):
                metric_type = None
        if metric_type is None:
            return None
        formatted_record["metric_type"] = metric_type
        formatted_record["metric_type_str"] = value.get("counter_type_str", "")

        metric_value = value.get("counter_value", 0)
        try:
            metric_value = int(metric_value)
        except (ValueError, TypeError):
            metric_value = 0
        formatted_record["metric_value"] = metric_value

        # Standard policy / collector fields
        formatted_record.update({
            "collector_id": value.get("label", None),
            "policy_originator": "unknown",
            "policy_level": "unknown",
            "policy_scope": ["unknown"],
            "ext": orjson.dumps(ext).decode('utf-8'),
        })

        return [formatted_record]
