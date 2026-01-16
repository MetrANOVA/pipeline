"""
Processor for Interface/ESDB Service Edge metadata
Handles Service Database interface/service edge information including organizations and peers.

Data model:
- One record per service edge (keyed by device::service_edge_name format)
- connection_name: Name of physical connection (for physical ports)
- svc_edge_id: ID of service edge (for service edge interfaces)
- org_short_name: Short name for the organization (e.g., LBNL)
- org_full_name: Full organization name (e.g., Lawrence Berkeley National Lab)
- org_hide: True if visibility is Hidden or tags contain 'hide'
- org_tag: Array of tags associated with organization
- org_tag_str: String version of tags (useful for aggregations)
- org_type: List of types for this organization (e.g., ESnet Site, Commercial Peer)
- org_funding_agency: Funding source (e.g., NSF)
- peer_as_id: AS number of peer organization
- peer_ipv4: IPv4 address of peer
- peer_ipv6: IPv6 address of peer
"""
import logging
import os
from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class MetaServiceEdgeProcessor(BaseMetadataProcessor):
    """Processor for meta_esdb_service_edge table - Interface/ESDB service edge metadata"""
    
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_ESDB_SERVICE_EDGE_TABLE', 'meta_esdb_service_edge')
        
        # Service Edge URL pattern for matching GraphQLConsumer messages
        self.service_edge_url_pattern = os.getenv('SERVICE_EDGE_URL_MATCH_PATTERN', 'esdb')
        
        # Type formatting
        self.int_fields = ['peer_as_id']
        self.bool_fields = ['org_hide']
        self.array_fields = ['org_tag', 'org_type']
        
        # ID and required fields
        self.val_id_field = ['id']
        self.required_fields = [['id']]
        
        # Column definitions
        self.column_defs.extend([
            # Connection/Service Edge identifiers
            ['connection_name', 'Nullable(String)', True],
            ['svc_edge_id', 'Nullable(String)', True],
            
            # Organization fields
            ['org_short_name', 'LowCardinality(Nullable(String))', True],
            ['org_full_name', 'Nullable(String)', True],
            ['org_hide', 'Bool', True],
            ['org_tag', 'Array(String)', True],
            ['org_tag_str', 'Nullable(String)', True],
            ['org_type', 'Array(String)', True],
            ['org_funding_agency', 'LowCardinality(Nullable(String))', True],
            
            # Peer fields
            ['peer_as_id', 'Nullable(UInt32)', True],
            ['peer_ipv4', 'Nullable(String)', True],
            ['peer_ipv6', 'Nullable(String)', True],
        ])

    def match_message(self, value):
        """
        Match messages for this processor.
        
        Matches messages that either:
        1. Have table field matching this processor's table (standard metadata pattern)
        2. Come from GraphQLConsumer with Service Edge URL and correct data structure
        """
        # Check for table match (standard metadata pattern)
        if value.get('table') == self.table:
            return super().match_message(value)
        
        # Check for GraphQLConsumer message with Service Edge URL
        url = value.get('url', '')
        if self.service_edge_url_pattern in url:
            # Additional check: verify this is actually service edge data
            data = value.get('data', {}).get('data', {})
            if 'serviceEdgeList' in data:
                return True
        
        return False
    
    def _build_service_edge_key(self, device_name, service_edge_name):
        """
        Build service edge key in format: device::service_edge_name
        
        Args:
            device_name: Router/device name
            service_edge_name: Service edge name from ESDB
        
        Returns:
            Service edge key string (e.g., "test-cr6::test_se-1267")
        """
        return f"{device_name}::{service_edge_name}"
    
    def _build_service_edge_record(self, interface_id, connection_name=None, svc_edge_id=None,
                                   org_info=None, peer_info=None):
        """Build a service edge record"""
        try:
            record = {
                'id': interface_id,
                'connection_name': connection_name,
                'svc_edge_id': svc_edge_id,
            }
            
            # Add organization info
            if org_info:
                record.update({
                    'org_short_name': org_info.get('short_name'),
                    'org_full_name': org_info.get('full_name'),
                    'org_hide': org_info.get('hide', False),
                    'org_tag': org_info.get('tags', []),
                    'org_tag_str': org_info.get('tags_str'),
                    'org_type': org_info.get('types', []),
                    'org_funding_agency': org_info.get('funding_agency'),
                })
            else:
                record.update({
                    'org_short_name': None,
                    'org_full_name': None,
                    'org_hide': False,
                    'org_tag': [],
                    'org_tag_str': None,
                    'org_type': [],
                    'org_funding_agency': None,
                })
            
            # Add peer info
            if peer_info:
                record.update({
                    'peer_as_id': peer_info.get('asn'),
                    'peer_ipv4': peer_info.get('ipv4'),
                    'peer_ipv6': peer_info.get('ipv6'),
                })
            else:
                record.update({
                    'peer_as_id': None,
                    'peer_ipv4': None,
                    'peer_ipv6': None,
                })
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error building service edge record for {interface_id}: {e}")
            return None

    def _extract_service_edge_records(self, service_edge_data):
        """
        Extract service edge records from GraphQL data.
        
        Expected structure from actual GraphQL query:
        {
            "data": {
                "serviceEdgeList": {
                    "list": [
                        {
                            "id": "78",
                            "name": "test_se-1267",
                            "physicalConnection": {
                                "id": "123",
                                "connectionName": "conn-b"
                            },
                            "equipmentInterface": {
                                "id": "2",
                                "device": {
                                    "id": "123",
                                    "name": "test-cr6"
                                },
                                "interface": "2/1/c1/3"
                            },
                            "serviceEdgeType": {
                                "id": "2",
                                "name": "Layer 3 Test Interface"
                            },
                            "vlan": 1000,
                            "organization": {
                                "id": "2",
                                "shortName": "TEST-ABC-AC",
                                "fullName": "TEST-ABC-AC Center",
                                "types": [
                                    {
                                        "id": "2",
                                        "name": "RESEARCH Site"
                                    }
                                ],
                                "fundingAgency": {
                                    "id": "2",
                                    "shortName": "TEST"
                                },
                                "visibility": "HIDDEN",
                                "tags": ["Transit", "hide"]
                            },
                            "bgpNeighbors": [
                                {
                                    "id": "2",
                                    "remoteIp": "192.0.2.1",
                                    "peerDetail": {
                                        "id": "4",
                                        "asn": 123
                                    }
                                }
                            ],
                            "equipment": {
                                "id": "2",
                                "model": {
                                    "id": "2",
                                    "manufacturer": {
                                        "id": "2",
                                        "shortName": "Nokia"
                                    }
                                },
                                "platform": {
                                    "id": "2",
                                    "manufacturer": {
                                        "id": "2",
                                        "shortName": "NOKIA"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }
        """
        service_edge_records = []
        
        if not service_edge_data:
            self.logger.warning("No service edge data to process")
            return service_edge_records
        
        # Navigate to service edges list
        service_edges = service_edge_data.get('data', {}).get('serviceEdgeList', {}).get('list', [])
        
        self.logger.debug(f"Found {len(service_edges)} service edges in response")
        
        if not service_edges:
            self.logger.warning("No service edges found in data")
            return service_edge_records
        
        for se in service_edges:
            try:
                # Get service edge name (NEW - this is now the primary identifier)
                service_edge_name = se.get('name')
                if not service_edge_name:
                    self.logger.warning(f"Skipping service edge without name (id: {se.get('id')})")
                    continue
                
                # Get equipment interface info to get device name
                equip_iface = se.get('equipmentInterface') or {}
                device = equip_iface.get('device') or {}
                device_name = device.get('name')
                
                if not device_name:
                    self.logger.warning(f"Skipping service edge without device name: {service_edge_name}")
                    continue
                
                # Build the interface ID using device::service_edge_name format
                interface_id = self._build_service_edge_key(device_name, service_edge_name)
                
                # Get service edge ID and connection name
                svc_edge_id = se.get('id')
                
                # Get connection name from physicalConnection
                physical_conn = se.get('physicalConnection') or {}
                connection_name = physical_conn.get('connectionName')
                
                # Extract organization info
                org = se.get('organization') or {}
                org_info = {
                    'short_name': org.get('shortName'),
                    'full_name': org.get('fullName'),
                    'types': [],
                    'funding_agency': None,
                    'hide': False,
                    'tags': [],
                    'tags_str': None,
                }
                
                # Get org types
                organization_types = org.get('types', []) or []
                for org_type in organization_types:
                    if isinstance(org_type, dict):
                        type_name = org_type.get('name', '')
                        if type_name:
                            org_info['types'].append(type_name)
                    elif isinstance(org_type, str):
                        org_info['types'].append(org_type)
                
                # Get funding agency
                funding_agency = org.get('fundingAgency') or {}
                if isinstance(funding_agency, dict):
                    org_info['funding_agency'] = funding_agency.get('shortName')
                elif isinstance(funding_agency, str):
                    org_info['funding_agency'] = funding_agency
                
                # Get visibility/hide status
                visibility = org.get('visibility', '')
                if visibility.upper() == 'HIDDEN':
                    org_info['hide'] = True
                
                # Process tags
                tags = org.get('tags', []) or []
                for tag in tags:
                    if tag:
                        org_info['tags'].append(tag)
                        if tag.lower() == 'hide':
                            org_info['hide'] = True
                
                # Build tags string
                if org_info['tags']:
                    org_info['tags_str'] = ','.join(org_info['tags'])
                
                # Extract peer info from BGP neighbors
                peer_info = None
                bgp_neighbors = se.get('bgpNeighbors', []) or []

                # Only process if 1 or 2 neighbors (direct connection, not exchange point)
                # 1 neighbor = single-stack (IPv4 OR IPv6 only)
                # 2 neighbors = dual-stack (IPv4 AND IPv6 to same peer)
                # 3+ neighbors = exchange point with multiple peers, skip
                if 0 < len(bgp_neighbors) <= 2:
                    peer_info = {}
                    peer_as_id = None
                    
                    for neighbor in bgp_neighbors:
                        if not isinstance(neighbor, dict):
                            continue
                        
                        # Get remote IP
                        remote_ip = neighbor.get('remoteIp')
                        if not remote_ip:
                            continue
                        
                        # Determine IP version
                        if ':' in remote_ip:
                            peer_info['ipv6'] = remote_ip
                        else:
                            peer_info['ipv4'] = remote_ip
                        
                        # Get ASN from peerDetail
                        peer_detail = neighbor.get('peerDetail') or {}
                        asn = peer_detail.get('asn')
                        
                        if asn is not None:
                            try:
                                asn = int(asn)
                            except (ValueError, TypeError):
                                self.logger.warning(f"Invalid ASN value: {asn}")
                                continue
                            
                            if peer_as_id is not None and peer_as_id != asn:
                                # Different ASNs, probably exchange point - skip
                                peer_info = None
                                break
                            peer_as_id = asn
                    
                    if peer_info and peer_as_id is not None:
                        peer_info['asn'] = peer_as_id
                    elif peer_info and ('ipv4' in peer_info or 'ipv6' in peer_info):
                        # Keep peer_info with just IPs if we have them but no ASN
                        pass
                    else:
                        peer_info = None
                else:
                    # Exchange point or no BGP neighbors - don't extract peer info
                    peer_info = None
                
                # Build the record
                record = self._build_service_edge_record(
                    interface_id=interface_id,
                    connection_name=connection_name,
                    svc_edge_id=svc_edge_id,
                    org_info=org_info,
                    peer_info=peer_info
                )
                
                if record:
                    service_edge_records.append(record)
                    
            except Exception as e:
                self.logger.error(f"Error processing service edge: {e}")
                continue
        
        self.logger.info(f"Extracted {len(service_edge_records)} service edge records")
        return service_edge_records
    
    def build_message(self, msg: dict, metadata: dict) -> list:
        """
        Build message from GraphQLConsumer data.
        
        GraphQLConsumer passes: {'url': url, 'status_code': ..., 'data': result.json()}
        where 'data' is the raw GraphQL API response
        """
        # Get the raw service edge data from the message
        service_edge_data = msg.get('data', {})
        
        if not service_edge_data:
            self.logger.warning("No service edge data in message")
            return []
        
        # Extract service edge records
        service_edge_records = self._extract_service_edge_records(service_edge_data)
        
        if not service_edge_records:
            self.logger.warning("No service edge records extracted")
            return []
        
        # Process each record through the parent's build_message
        formatted_msg = {'data': service_edge_records}
        return super().build_message(formatted_msg, metadata)
    
    def build_metadata_fields(self, value: dict) -> dict:
        """Extract and format service edge metadata fields"""
        formatted_record = super().build_metadata_fields(value)
        
        # Ensure arrays are lists
        for field in self.array_fields:
            if not isinstance(formatted_record.get(field), list):
                formatted_record[field] = []
        
        # Ensure bool field
        formatted_record['org_hide'] = bool(formatted_record.get('org_hide', False))
        
        # Validate peer ASN
        if formatted_record.get('peer_as_id') is not None:
            try:
                formatted_record['peer_as_id'] = int(formatted_record['peer_as_id'])
            except (ValueError, TypeError):
                formatted_record['peer_as_id'] = None
        
        return formatted_record