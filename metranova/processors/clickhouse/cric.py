"""
Processor for CRIC IP metadata
Handles WLCG CRIC site network information including IP ranges, sites, and network routes.

Data model:
- One record per netroute (e.g., AGLT2_LHCONE_RT)
- All IP subnets within a netroute are combined into ip_subnet array
- rcsite_name: Resource Center Site (e.g., AGLT2 = ATLAS Great Lakes Tier 2)
- netsite_name: Physical location (e.g., US-AGLT2 University of Michigan)
"""
import logging
import os
from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class MetaIPCRICProcessor(BaseMetadataProcessor):
    """Processor for meta_ip_cric table - WLCG CRIC site IP metadata"""
    
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_CRIC_IP_METADATA_TABLE', 'meta_ip_cric')
        
        # CRIC URL pattern for matching HTTPConsumer messages
        self.cric_url_pattern = os.getenv('CRIC_URL_MATCH_PATTERN', 'cric.cern.ch')
        
        # Type formatting - only the fields we need
        self.float_fields = ['latitude', 'longitude']
        self.int_fields = ['tier']
        self.array_fields = ['ip_subnet']
        
        # ID and required fields
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['ip_subnet']]
        
        # Extend column definitions - only 6 core fields
        self.column_defs.extend([
            # IP subnet - array of all networks in the netroute
            ['ip_subnet', 'Array(Tuple(IPv6, UInt8))', True],
            
            # Core CRIC fields
            ['rcsite_name', 'LowCardinality(String)', True],           # Resource Center Site name (e.g., AGLT2)
            ['latitude', 'Nullable(Float64)', True],                    # Latitude
            ['longitude', 'Nullable(Float64)', True],                   # Longitude
            ['country_name', 'LowCardinality(Nullable(String))', True], # Country Name
            ['netsite_name', 'LowCardinality(Nullable(String))', True], # Network site name (e.g., US-AGLT2 Michigan State University)
            ['tier', 'Nullable(UInt8)', True],                          # Tier level
        ])


    def match_message(self, value):
        """
        Match messages for this processor.
        
        This override is necessary because we use the generic HTTPConsumer instead of 
        a dedicated CRICIPConsumer. The base class BaseMetadataProcessor.match_message 
        only matches messages that have a 'table' field equal to self.table.
        
        HTTPConsumer sends messages in the format:
            {'url': 'https://...', 'status_code': 200, 'data': {...}}
        
        Since there is no 'table' field in HTTPConsumer messages, the base class 
        match_message would return False and skip the message. We override to also 
        check for the CRIC URL pattern in the message.
        
        Unlike SciRegProcessor or Scinet which can use has_match_field() because the SciReg/Scinet API 
        response contains identifiable fields (scireg_id, addresses) at the top level,
        the CRIC API returns raw site data where our required fields (id, ip_subnet) 
        are extracted during processing, not present in the raw response.
        
        Matches messages that either:
        1. Have table field matching this processor's table (standard metadata pattern)
        2. Come from HTTPConsumer with CRIC URL (identified by 'cric.cern.ch' in url)
        """
        # Check for table match (standard metadata pattern)
        if value.get('table') == self.table:
            return super().match_message(value)
        
        # Check for HTTPConsumer message with CRIC URL
        url = value.get('url', '')
        if self.cric_url_pattern in url:
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
    
    def _build_ip_record(self, netroute_id, ip_subnets, rcsite_info, netsite_name):
        """Build an IP record for a netroute with all its IP subnets combined"""
        try:
            if not ip_subnets:
                return None
            
            # Build the IP record
            ip_record = {
                'id': netroute_id,
                'ip_subnet': ip_subnets,
                'rcsite_name': rcsite_info['rcsite_name'],    
                'latitude': rcsite_info['latitude'],
                'longitude': rcsite_info['longitude'],
                'country_name': rcsite_info['country_name'],
                'netsite_name': netsite_name,
                'tier': rcsite_info['tier'],
            }
            
            return ip_record
            
        except Exception as e:
            self.logger.error(f"Error building IP record for {netroute_id}: {e}")
            return None
    
    def _extract_ip_records(self, cric_data):
        """Extract IP records from CRIC data - one record per netroute"""
        ip_records = []
        
        if not cric_data:
            self.logger.warning("No CRIC data to process")
            return ip_records
        
        for rcsite_name, site_data in cric_data.items():
            try:
                # Get the rcsite (Resource Center Site) info
                rcsite_info = {
                    'rcsite_name': rcsite_name,
                    'tier': site_data.get('rc_tier_level', None),
                    'country_name': site_data.get('country', None),
                    'latitude': site_data.get('latitude', None),
                    'longitude': site_data.get('longitude', None),
                }
                
                # Process netroutes (network routes)
                netroutes = site_data.get('netroutes', {})
                if not netroutes:
                    self.logger.debug(f"No netroutes found for rcsite {rcsite_name}")
                    continue
                
                # Iterate through each network route
                for netroute_id, netroute_data in netroutes.items():
                    try:
                        # Get netsite_name from netsite field
                        netsite_name = netroute_data.get('netsite', None)

                        # Get networks (contains IPv4 and IPv6 ranges)
                        networks = netroute_data.get('networks', {})
                        if not networks:
                            self.logger.debug(f"No networks found for netroute {netroute_id} in rcsite {rcsite_name}")
                            continue
                        
                        # Combine all IPv4 and IPv6 ranges into one list
                        ip_subnets = []
                        
                        # Process IPv4 ranges
                        for ip_range in networks.get('ipv4', []):
                            ip_subnet_tuple = self._parse_ip_subnet(ip_range)
                            if ip_subnet_tuple:
                                ip_subnets.append(ip_subnet_tuple)
                        
                        # Process IPv6 ranges
                        for ip_range in networks.get('ipv6', []):
                            ip_subnet_tuple = self._parse_ip_subnet(ip_range)
                            if ip_subnet_tuple:
                                ip_subnets.append(ip_subnet_tuple)
                        
                        # Build one record per netroute with all IP subnets combined
                        if ip_subnets:
                            ip_record = self._build_ip_record(netroute_id, ip_subnets, rcsite_info, netsite_name)
                            if ip_record:
                                ip_records.append(ip_record)
                    
                    except Exception as e:
                        self.logger.error(f"Error processing netroute {netroute_id} in rcsite {rcsite_name}: {e}")
                        continue
                    
            except Exception as e:
                self.logger.error(f"Error processing rcsite {rcsite_name}: {e}")
                continue
        
        self.logger.info(f"Extracted {len(ip_records)} IP records from CRIC data")
        return ip_records
    
    def build_message(self, msg: dict, metadata: dict) -> list:
        """
        Build message from HTTPConsumer data.
        
        HTTPConsumer passes: {'url': url, 'status_code': ..., 'data': result.json()}
        where 'data' is the raw CRIC API response (dict of sites)
        """
        # Get the raw CRIC data from the message
        cric_data = msg.get('data', {})
        
        if not cric_data:
            self.logger.warning("No CRIC data in message")
            return []
        
        # Extract IP records from CRIC data
        ip_records = self._extract_ip_records(cric_data)
        
        if not ip_records:
            self.logger.warning("No IP records extracted from CRIC data")
            return []
        
        # Process each record through the parent's build_message
        # We need to format it as the parent expects: {'data': [...]}
        formatted_msg = {'data': ip_records}
        return super().build_message(formatted_msg, metadata)
    
    def build_metadata_fields(self, value: dict) -> dict:
        """Extract and format CRIC IP metadata fields"""
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)
        
        # Validate and format IP subnet
        ip_subnets = []
        for item in formatted_record.get('ip_subnet', []):
            if isinstance(item, (list, tuple)) and len(item) == 2:
                try:
                    ip_subnets.append((str(item[0]), int(item[1])))
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid ip_subnet item: {item}")
        formatted_record['ip_subnet'] = ip_subnets
        
        # Ensure required string field has default
        if not formatted_record.get('rcsite_name'):
            formatted_record['rcsite_name'] = 'Unknown'
        
        return formatted_record