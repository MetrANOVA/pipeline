"""
Processor for CRIC IP metadata
Handles WLCG CRIC site network information including IP ranges, sites, and network routes
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
        
        # Type formatting - only the fields we need
        self.float_fields = ['latitude', 'longitude']
        self.int_fields = ['tier']
        self.array_fields = ['ip_subnet']
        
        # ID and required fields
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['ip_subnet']]
        
        # Extend column definitions - only 6 core fields
        self.column_defs.extend([
            # IP subnet
            ['ip_subnet', 'Array(Tuple(IPv6, UInt8))', True],
            
            # Core CRIC fields (matching the 6 fields from consumer)
            ['name', 'LowCardinality(String)', True],          # Site name
            ['latitude', 'Nullable(Float64)', True],            # Latitude
            ['longitude', 'Nullable(Float64)', True],           # Longitude
            ['country', 'LowCardinality(Nullable(String))', True],  # Country
            ['net_site', 'LowCardinality(Nullable(String))', True], # Network site
            ['tier', 'Nullable(UInt8)', True],                  # Tier level
        ])

        # Table settings
        self.table_engine = "ReplacingMergeTree"
        self.order_by = ['name', 'id', 'ref', 'insert_time']
        self.primary_keys = ['name', 'id'] # Primary Key must be a prefix of order_by
    
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
        if not formatted_record.get('name'):
            formatted_record['name'] = 'Unknown'
        
        return formatted_record