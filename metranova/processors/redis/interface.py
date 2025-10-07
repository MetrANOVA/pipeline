import logging
import os
from metranova.processors.redis.base import BaseRedisProcessor

logger = logging.getLogger(__name__)

class InterfaceMetadataProcessor(BaseRedisProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('REDIS_IF_METADATA_TABLE', 'meta_if_cache')
        self.expires = int(os.getenv('REDIS_IF_METADATA_EXPIRES', '86400'))  # default 1 day
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

        return [{
            "table": self.table,
            "key": value.get('meta', {}).get('id', None),
            "data": {
                "id": value.get('meta', {}).get('id', None),
                "name": value.get('meta', {}).get('name', None),
                "device": value.get('meta', {}).get('device', None),
                "description": value.get('meta', {}).get('description', None),
                "ifindex": value.get('meta', {}).get('if_index', None),
                "edge": value.get('meta', {}).get('intercloud', None),
                "ipv4": value.get('meta', {}).get('ipv4', None),
                "ipv6": value.get('meta', {}).get('ipv6', None),
                "netflow_index": value.get('meta', {}).get('netflow_index', None),
                "peer_asn": value.get('meta', {}).get('peer', {}).get('asn', None),
                "peer_ipv4": value.get('meta', {}).get('peer', {}).get('ipv4', None),
                "peer_ipv6": value.get('meta', {}).get('peer', {}).get('ipv6', None),
                "port_name": value.get('meta', {}).get('port_name', None),
                "remote_device": value.get('meta', {}).get('remote', {}).get('device', None),
                "remote_full_name": value.get('meta', {}).get('remote', {}).get('full_name', None),
                "remote_id": value.get('meta', {}).get('remote', {}).get('id', None),
                "remote_lldp_device": value.get('meta', {}).get('remote', {}).get('lldp_device', None),
                "remote_lldp_port": value.get('meta', {}).get('remote', {}).get('lldp_port', None),
                "remote_loc_name": value.get('meta', {}).get('remote', {}).get('loc_name', None),
                "remote_loc_type": value.get('meta', {}).get('remote', {}).get('loc_type', None),
                "remote_loc_lat": value.get('meta', {}).get('remote', {}).get('location', {}).get('lat', None),
                "remote_loc_lon": value.get('meta', {}).get('remote', {}).get('location', {}).get('lon', None),
                "remote_manufacturer": value.get('meta', {}).get('remote', {}).get('manufacturer', None),
                "remote_model": value.get('meta', {}).get('remote', {}).get('model', None),
                "remote_network": value.get('meta', {}).get('remote', {}).get('network', None),
                "remote_os": value.get('meta', {}).get('remote', {}).get('os', None),
                "remote_port": value.get('meta', {}).get('remote', {}).get('port', None),
                "remote_role": value.get('meta', {}).get('remote', {}).get('role', None),
                "remote_short_name": value.get('meta', {}).get('remote', {}).get('short_name', None),
                "remote_state": value.get('meta', {}).get('remote', {}).get('state', None),
                "site": value.get('meta', {}).get('site', None),
                "speed": value.get('meta', {}).get('speed', None),
                "visibility": value.get('meta', {}).get('visibility', None),
                "vrtr_ifglobalindex": value.get('meta', {}).get('vrtr_ifglobalindex', None),
                "vrtr_ifindex": value.get('meta', {}).get('vrtr_ifindex', None),
                "vrtr_name": value.get('meta', {}).get('vrtr_name', None),
            },
            "expires": self.expires
        }]


