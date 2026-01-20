"""
Processor for IP Service/ESDB metadata
Handles Service Database IP service information including prefixes, organizations, and ASNs.

Data model:
- One record per IP prefix
- service_prefix_group_name: Name of the service in ESDB (e.g., AMES-Main-v4)
- service_label: Label applied to the service (e.g., Main)
- service_type: Type ESDB applies to the service (e.g., LHCONEv4)
- org_short_name: Short name for the organization (e.g., LBNL)
- org_full_name: Full organization name (e.g., Lawrence Berkeley National Lab)
- org_types: List of types for this organization (e.g., ESnet Site, Commercial Peer)
- org_funding_agency: Funding source (e.g., NSF)
- org_hide: True if visibility is Hidden or Hide Flow Data
- asn: AS number of this organization
"""
import logging
import os
from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class MetaIPServiceProcessor(BaseMetadataProcessor):
    """Processor for meta_ip_esdb table - ESnet IP Service/ESDB service IP metadata"""
    
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_ESDB_IP_METADATA_TABLE', 'meta_ip_esdb')
        
        # IP Service/ESDB URL pattern for matching GraphQLConsumer messages
        self.ipservice_url_pattern = os.getenv('IP_SERVICE_URL_MATCH_PATTERN', 'esdb')
        
        # Type formatting
        self.int_fields = ['asn']
        self.bool_fields = ['org_hide']
        self.array_fields = ['service_prefix_group_name', 'service_label', 'service_type', 'org_types']
        
        # ID and required fields
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['ip_subnet']]
        
        # Column definitions
        self.column_defs.extend([
            # IP subnet
            ['ip_subnet', 'Tuple(IPv6, UInt8)', True],
            
            # Service fields (arrays to handle multiple services per prefix)
            ['service_prefix_group_name', 'Array(String)', True],
            ['service_label', 'Array(String)', True],
            ['service_type', 'Array(String)', True],
            
            # Organization fields
            ['org_short_name', 'LowCardinality(Nullable(String))', True],
            ['org_full_name', 'Nullable(String)', True],
            ['org_types', 'Array(String)', True],
            ['org_funding_agency', 'LowCardinality(Nullable(String))', True],
            ['org_hide', 'Bool', True],
            
            # ASN
            ['asn', 'Nullable(UInt32)', True],
        ])

    def match_message(self, value):
        """
        Match messages for this processor.
        
        Matches messages that either:
        1. Have table field matching this processor's table (standard metadata pattern)
        2. Come from GraphQLConsumer with IP Service/ESDB URL
        """
        # Check for table match (standard metadata pattern)
        if value.get('table') == self.table:
            return super().match_message(value)
        
        # Check for GraphQLConsumer message with IP Service/ESDB URL
        url = value.get('url', '')
        if self.ipservice_url_pattern in url:
            return True
        
        return False
    
    def _parse_ip_subnet(self, ip_subnet_str):
        """Parse IP subnet string into tuple format"""
        if not ip_subnet_str:
            return None
        try:
            parts = ip_subnet_str.split('/')
            if len(parts) != 2:
                self.logger.warning(f"Invalid IP subnet format: {ip_subnet_str}")
                return None
            return (parts[0], int(parts[1]))
        except Exception as e:
            self.logger.error(f"Error parsing IP subnet {ip_subnet_str}: {e}")
            return None
    
    def _build_ip_record(self, prefix_ip, service_info, org_info, asn):
        """Build an IP record for a prefix"""
        try:
            ip_subnet = self._parse_ip_subnet(prefix_ip)
            if not ip_subnet:
                return None
            
            ip_record = {
                'id': prefix_ip,  # Use prefix as unique ID
                'ip_subnet': ip_subnet,
                'service_prefix_group_name': service_info.get('prefix_group_name', []),
                'service_label': service_info.get('label', []),
                'service_type': service_info.get('type', []),
                'org_short_name': org_info.get('short_name'),
                'org_full_name': org_info.get('full_name'),
                'org_types': org_info.get('types', []),
                'org_funding_agency': org_info.get('funding_agency'),
                'org_hide': org_info.get('hide', False),
                'asn': asn,
            }
            
            return ip_record
            
        except Exception as e:
            self.logger.error(f"Error building IP record for {prefix_ip}: {e}")
            return None

    def _extract_ip_records(self, ipservice_data):
        """
        Extract IP records from IP Service/ESDB GraphQL data.
        
        Expected GraphQL response structure:
        {
            "data": {
                "serviceList": {
                    "list": [
                        {
                            "id": "...",
                            "label": "...",
                            "prefixGroupName": "...",
                            "serviceType": {"shortName": "..."},
                            "customer": {
                                "shortName": "...",
                                "fullName": "...",
                                "types": [{"name": "..."}],
                                "fundingAgency": {"shortName": "..."},
                                "visibility": "..."
                            },
                            "peer": {"asn": 123},
                            "prefixes": [{"prefixIp": "..."}]
                        }
                    ]
                }
            }
        }
        """
        ip_records = []
        prefix_tracker = {}  # Track prefixes to handle duplicates
        
        if not ipservice_data:
            self.logger.warning("No IP Service/ESDB data to process")
            return ip_records
        
        # Navigate to services list - serviceList.list structure
        services = ipservice_data.get('data', {}).get('serviceList', {}).get('list', [])
        
        self.logger.debug(f"Found {len(services)} services in IP Service/ESDB response")
        
        if not services:
            self.logger.warning("No services found in IP Service/ESDB data")
            return ip_records
        
        for service in services:
            try:
                # Extract service info (camelCase from GraphQL)
                service_info = {
                    'prefix_group_name': [service.get('prefixGroupName', '')] if service.get('prefixGroupName') else [],
                    'label': [service.get('label', '')] if service.get('label') else [],
                    'type': [],
                }
                
                # Get service type
                service_type = service.get('serviceType') or {}
                if isinstance(service_type, dict):
                    type_name = service_type.get('shortName', '')
                    if type_name:
                        service_info['type'] = [type_name]
                
                # Extract organization info from customer
                customer = service.get('customer') or {}
                org_info = {
                    'short_name': customer.get('shortName'),
                    'full_name': customer.get('fullName'),
                    'types': [],
                    'funding_agency': None,
                    'hide': False,
                }
                
                # Get org types
                org_types = customer.get('types', []) or []
                for org_type in org_types:
                    if isinstance(org_type, dict):
                        type_name = org_type.get('name', '')
                        if type_name:
                            org_info['types'].append(type_name)
                    elif isinstance(org_type, str):
                        org_info['types'].append(org_type)
                
                # Get funding agency
                funding_agency = customer.get('fundingAgency') or {}
                if isinstance(funding_agency, dict):
                    org_info['funding_agency'] = funding_agency.get('shortName')
                elif isinstance(funding_agency, str):
                    org_info['funding_agency'] = funding_agency
                
                # Get visibility/hide status
                visibility = customer.get('visibility', '')
                org_info['hide'] = visibility in ('Hidden', 'Hide Flow Data')
                
                # Get ASN from peer
                peer = service.get('peer') or {}
                asn = peer.get('asn')
                
                # Process prefixes
                prefixes = service.get('prefixes', []) or []
                for prefix in prefixes:
                    if isinstance(prefix, dict):
                        prefix_ip = prefix.get('prefixIp', '')
                    else:
                        prefix_ip = str(prefix)
                    
                    if not prefix_ip:
                        continue
                    
                    # Handle duplicate prefixes (merge service info)
                    if prefix_ip in prefix_tracker:
                        existing = prefix_tracker[prefix_ip]
                        # Merge service arrays
                        for key in ['prefix_group_name', 'label', 'type']:
                            for val in service_info.get(key, []):
                                if val and val not in existing['service_info'][key]:
                                    existing['service_info'][key].append(val)
                    else:
                        prefix_tracker[prefix_ip] = {
                            'service_info': {
                                'prefix_group_name': list(service_info['prefix_group_name']),
                                'label': list(service_info['label']),
                                'type': list(service_info['type']),
                            },
                            'org_info': org_info,
                            'asn': asn,
                        }
                        
            except Exception as e:
                self.logger.error(f"Error processing service: {e}")
                continue
        
        # Build final records from tracked prefixes
        for prefix_ip, data in prefix_tracker.items():
            ip_record = self._build_ip_record(
                prefix_ip,
                data['service_info'],
                data['org_info'],
                data['asn']
            )
            if ip_record:
                ip_records.append(ip_record)
        
        self.logger.info(f"Extracted {len(ip_records)} IP records from IP Service/ESDB data")
        return ip_records
    
    def build_message(self, msg: dict, metadata: dict) -> list:
        """
        Build message from GraphQLConsumer data.
        
        GraphQLConsumer passes: {'url': url, 'status_code': ..., 'data': result.json()}
        where 'data' is the raw GraphQL API response
        """
        # Get the raw IP Service/ESDB data from the message
        ipservice_data = msg.get('data', {})
        
        if not ipservice_data:
            self.logger.warning("No IP Service/ESDB data in message")
            return []
        
        # Extract IP records from IP Service/ESDB data
        ip_records = self._extract_ip_records(ipservice_data)
        
        if not ip_records:
            self.logger.warning("No IP records extracted from IP Service/ESDB data")
            return []
        
        # Process each record through the parent's build_message
        formatted_msg = {'data': ip_records}
        return super().build_message(formatted_msg, metadata)
    
    def build_metadata_fields(self, value: dict) -> dict:
        """Extract and format IP Service/ESDB IP metadata fields"""
        formatted_record = super().build_metadata_fields(value)
        
        # Validate and format IP subnet
        ip_subnet = formatted_record.get('ip_subnet')
        if isinstance(ip_subnet, (list, tuple)) and len(ip_subnet) == 2:
            try:
                formatted_record['ip_subnet'] = (str(ip_subnet[0]), int(ip_subnet[1]))
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid ip_subnet: {ip_subnet}")
                formatted_record['ip_subnet'] = None
        
        # Ensure arrays are lists
        for field in self.array_fields:
            if not isinstance(formatted_record.get(field), list):
                formatted_record[field] = []
        
        # Ensure bool field
        formatted_record['org_hide'] = bool(formatted_record.get('org_hide', False))
        
        return formatted_record