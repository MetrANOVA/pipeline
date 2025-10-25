import logging
import orjson
from metranova.processors.redis.interface import BaseInterfaceMetadataProcessor

logger = logging.getLogger(__name__)

class InterfaceMetadataProcessor(BaseInterfaceMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [["meta", "id"], ["meta", "name"], ["meta", "device"]]
        self.match_fields = [
            ["meta", "id"], ["meta", "name"], ["meta", "device"], ["meta", "description"],
            ["meta", "if_index"], ["meta", "intercloud"], ["meta", "ipv4"], ["meta", "ipv6"],
            ["meta", "netflow_index"], ["meta", "peer", "asn"], ["meta", "peer", "ipv4"],
            ["meta", "peer", "ipv6"], ["meta", "port_name"], ["meta", "remote", "device"],
            ["meta", "remote", "full_name"], ["meta", "remote", "id"], ["meta", "remote", "lldp_device"],
            ["meta", "remote", "lldp_port"], ["meta", "remote", "loc_name"], ["meta", "remote", "loc_type"],
            ["meta", "remote", "location", "lat"], ["meta", "remote", "location", "lon"],
            ["meta", "remote", "manufacturer"], ["meta", "remote", "model"], ["meta", "remote", "network"],
            ["meta", "remote", "os"], ["meta", "remote", "port"], ["meta", "remote", "role"],
            ["meta", "remote", "short_name"], ["meta", "remote", "state"], ["meta", "site"],
            ["meta", "speed"], ["meta", "visibility"], ["meta", "vrtr_ifglobalindex"],
            ["meta", "vrtr_ifindex"], ["meta", "vrtr_name"]
        ]
    
    def match_message(self, value):
        return self.has_match_field(value)
    
    def build_message(self, value, msg_metadata):
        # check required fields
        if not self.has_required_fields(value):
            return None

        # handle special case for name. If missing parse from meta.id which is a string of form <device>::<name>
        if value.get('meta', {}).get('name', None) is None:
            meta_id = value.get('meta', {}).get('id', '')
            if '::' in meta_id:
                parts = meta_id.split('::', 1)
                if len(parts) == 2:
                    value.setdefault('meta', {})['name'] = parts[1]

        #format edge boolean as a string. if it is a list, take the first element
        if isinstance(value.get('meta', {}).get('intercloud', None), bool):
            value['meta']['intercloud'] = 'true' if value['meta']['intercloud'] else 'false'
        elif isinstance(value.get('meta', {}).get('intercloud', None), list) and len(value['meta']['intercloud']) > 0:
            #no idea why this is ever a list, but handle it anyway
            value['meta']['intercloud'] = 'true' if value['meta']['intercloud'][0] else 'false'

        # figure out type
        type = 'port'
        if value.get('meta', {}).get('service_type', None):
            type = "service_{}".format(value['meta']['service_type'].lower())
        elif value.get('meta', {}).get('vrtr_name', None):
            type = 'service_l3vpn'
        elif value.get('meta', {}).get('is_lag', False):
            type = 'lag'
        elif value.get('meta', {}).get('port_name', None):
            type = 'interface'

        # Figure out port
        port_name = None
        if value.get('meta', {}).get('port_name', None):
            port_name = "{}::{}".format(value['meta']['device'], value['meta']['port_name'])

        # Figure out tag
        tags = []
        if value.get('meta', {}).get('visibility', None) == False:
            tags.append("hide")

        # lookup flow index
        flow_index = None
        if value.get('meta', {}).get('if_index', None):
            flow_index = value['meta']['if_index']
        elif value.get('meta', {}).get('vrtr_ifglobalindex', None):
            flow_index = value['meta']['vrtr_ifglobalindex']

        #build initial data dict
        data = {
            "id": value.get('meta', {}).get('id', None),
            "type": type,
            "device_id": value.get('meta', {}).get('device', None),
            "description": value.get('meta', {}).get('descr', None),
            "edge": value.get('meta', {}).get('intercloud', None),
            "flow_index": flow_index,
            "ipv4": value.get('meta', {}).get('ipv4', None),
            "ipv6": value.get('meta', {}).get('ipv6', None),
            "name": value.get('meta', {}).get('name', None),
            "speed": value.get('meta', {}).get('speed', None),
            "circuit_id": orjson.dumps(value.get('meta', {}).get('circuits', {}).get('id', [])).decode('utf-8'),
            "peer_as_id": value.get('meta', {}).get('peer', {}).get('asn', None),
            "peer_interface_ipv4": value.get('meta', {}).get('peer', {}).get('ipv4', None),
            "peer_interface_ipv6": value.get('meta', {}).get('peer', {}).get('ipv6', None),
            "lag_member_interface_id": orjson.dumps(value.get('meta', {}).get('lag_members', [])).decode('utf-8'),
            "port_interface_id": port_name,
            "remote_interface_id": value.get('meta', {}).get('remote', {}).get('id', None),
            "remote_organization_id": value.get('meta', {}).get('org', {}).get('short_name', None),
            "tags": orjson.dumps(tags).decode('utf-8')
        }

        # build extension fields
        ext = {}
        if value.get('meta', {}).get('vrtr_ifglobalindex', None):
            ext["vrtr_interface_global_index"] = value['meta']['vrtr_ifglobalindex']
        if value.get('meta', {}).get('vrtr_ifindex', None):
            ext["vrtr_interface_index"] = value['meta']['vrtr_ifindex']
        if value.get('meta', {}).get('vrtr_name', None):
            ext["vrtr_id"] = value['meta']['vrtr_name']
        if value.get('meta', {}).get('vrtr_ifencapvalue', None):
            ext["vrtr_interface_encap"] = value['meta']['vrtr_ifencapvalue']
        if value.get('meta', {}).get('sap_name', None):
            ext["sap_name"] = value['meta']['sap_name']
        data['ext'] = orjson.dumps(ext).decode('utf-8')

        return [{
            "table": self.table,
            "key": value.get('meta', {}).get('id', None),
            "data": data,
            "expires": self.expires
        }]


