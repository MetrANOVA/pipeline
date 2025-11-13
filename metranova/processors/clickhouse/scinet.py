import ipaddress
import logging
import os

from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class SCinetMetadataProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_SCINET_METADATA_TABLE', 'meta_ip_scinet')
        self.val_id_field = ['resource_name']
        self.column_defs.extend([
            ['ip_subnet', 'Array(Tuple(IPv6,UInt8))', True],
            ['organization_name', 'Nullable(String)', True],
            ['coordinate_x', 'Nullable(Float64)', True],
            ['coordinate_y', 'Nullable(Float64)', True],
        ])
        self.required_fields = [
            ["addresses"],
            ["org_name"],
            ["resource_name"]
        ]
    
    def match_message(self, value):
        #override base since don't set table in url
        return self.has_match_field(value)
    
    def build_metadata_fields(self, value: dict) -> dict | None:
        #iterate over strings in value['addresses'] and build a new list of tuples where first element is IP address and second is prefix length.
        ip_subnets = []
        for addr in value['addresses']:
            #if has slash use otherwise default to 32 for ipv4 and 128 for ipv6
            if '/' in addr:
                ip, prefix = addr.split('/', 1)
                ip_subnets.append((ip, int(prefix)))
            else:
                try:
                    ip_obj = ipaddress.ip_address(addr)
                    default_prefix = 32 if ip_obj.version == 4 else 128
                    ip_subnets.append((addr, default_prefix))
                except (ipaddress.AddressValueError, ValueError):
                    self.logger.warning(f"Invalid IP address format: {addr}")
                    continue

        #init record
        formatted_record = {
            'ip_subnet': ip_subnets,
            'organization_name': value.get('org_name', None),
            'coordinate_x': value.get('latitude', None),
            'coordinate_y': value.get('longitude', None),
            'ext': '{}',
            'tag': []
        }

        return formatted_record
