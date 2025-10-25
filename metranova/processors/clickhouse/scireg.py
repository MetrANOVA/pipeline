import logging
import os
import ipaddress
from datetime import datetime
from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class ScienceRegistryProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_SCIREG_METADATA_TABLE', 'meta_ip_scireg')
        self.val_id_field = ['scireg_id']
        self.column_defs.extend([
            ['scireg_update_time', 'Date', True],
            ['ip_subnet', 'Array(Tuple(IPv6,UInt8))', True],
            ['organization_name', 'Nullable(String)', True],
            ['organization_id', 'LowCardinality(Nullable(String))', True],
            ['organization_ref', 'Nullable(String)', True],
            ['discipline', 'Nullable(String)', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True],
            ['resource_name', 'Nullable(String)', True],
            ['project_name', 'Nullable(String)', True],
            ['contact_email', 'Nullable(String)', True]
        ])
        self.required_fields = [
            ["scireg_id"],
            ["addresses"]
        ]

    def build_message(self, value: dict, msg_metadata: dict) -> list[dict]:
        # Get a JSON list so need to iterate and then call super().build_message for each record
        if not value or not value.get("data", None):
            return []
        
        # check if value["data"] is a list
        if not isinstance(value["data"], list):
            self.logger.warning("Expected 'data' to be a list, got %s", type(value["data"]))
            return []
        
        #iterate through each record in value["data"]
        records = []
        for record in value["data"]:
            formatted_records = super().build_message(record, msg_metadata)
            self.logger.debug(f"Formatted record: {formatted_records}")
            if formatted_records:
                records.extend(formatted_records)

        return records

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
            'scireg_update_time': value.get('last_updated', 'unknown'),
            'ip_subnet': ip_subnets,
            'organization_name': value.get('org_name', None),
            'organization_id': value.get('org_name', None),
            'organization_ref': self.pipeline.cacher("redis").lookup("meta_organization", value.get('org_name', None)),
            'discipline': value.get('discipline', None),
            'latitude': value.get('latitude', None),
            'longitude': value.get('longitude', None),
            'resource_name': value.get('resource_name', None),
            'project_name': value.get('project_name', None),
            'contact_email': value.get('contact_email', None)
        }

        #format scireg_update_time if equals "unknown"
        if formatted_record['scireg_update_time'] == "unknown":
            formatted_record['scireg_update_time'] = '1970-01-01'
        #convert scireg_update_time to a datetime object
        try:
            formatted_record['scireg_update_time'] = datetime.strptime(formatted_record['scireg_update_time'], '%Y-%m-%d').date()
        except ValueError:
            formatted_record['scireg_update_time'] = datetime(1970, 1, 1).date()

        #cast latitude and longitude to a float, and set to None if exception when casting
        float_fields = ['latitude', 'longitude']
        for field in float_fields:
            if formatted_record[field] is not None:
                try:
                    formatted_record[field] = float(formatted_record[field])
                except ValueError:
                    formatted_record[field] = None

        return formatted_record