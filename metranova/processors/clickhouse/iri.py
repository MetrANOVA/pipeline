from datetime import datetime
import logging
import orjson
import os
from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor, BaseMetadataProcessor

logger = logging.getLogger(__name__)

class ResourceMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_IRI_RESOURCE_TABLE', 'meta_iri_resource')
        self.array_fields = ['capability_uris']
        self.column_defs.extend([
            ['uri', 'String', True],
            ['name', 'Nullable(String)', True],
            ['description', 'Nullable(String)', True],
            ['type', 'LowCardinality(String)', True],
            ['group', 'LowCardinality(Nullable(String))', True],
            ['capability_id', 'Array(LowCardinality(String))', True],
            ['site_id', 'LowCardinality(Nullable(String))', True],
            ['facility_id', 'LowCardinality(Nullable(String))', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['resource_type'], ['self_uri']]

    #Override match_message to not use table in url
    def match_message(self, value):
        return self.has_match_field(value)

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #add remaining fields to formatted_record
        formatted_record['uri'] = value.get('self_uri')
        formatted_record['name'] = value.get('name')
        formatted_record['description'] = value.get('description')
        formatted_record['type'] = value.get('resource_type')
        formatted_record['group'] = value.get('group')
        formatted_record['capability_id'] = value.get('capability_uris', [])
        formatted_record['site_id'] = None #todo: lookup
        formatted_record['facility_id'] = None #todo: lookup

        #build a hash with all the keys and values from value['data']
        return formatted_record
    
class ResourceDataProcessor(BaseDataGenericMetricProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [['id'], ['current_status']]
        self.collector_id = os.getenv('CLICKHOUSE_IRI_RESOURCE_COLLECTOR_ID', 'metranova_pipeline')
        self.metric_map = {
            'current_status': ('status', 'counter'),
            'last_modified': ('last_modified', 'counter')
        }
    
    def load_resource_types(self):
        return ['iri_resource']
    
    def format_value(self, metric_name, metric_value):
        #if none, return None
        if metric_value is None:
            return {},None
        
        #format value based on metric_name
        formtted_value = metric_value
        ext_obj = {}
        if metric_name == 'status':
            #convert status string to integer code
            status_map = {
                'up': 1,
                'degraded': 2,
                'down': 3,
                'unknown': 4
            }
            formtted_value = status_map.get(metric_value.lower(), 4)  # default to 4 if unknown status
            ext_obj = {'status_str': metric_value}
        elif metric_name == 'last_modified':
            #convert last_modified string to timestamp integer
            try:
                dt = datetime.fromisoformat(metric_value)
                formtted_value = int(dt.timestamp())
                ext_obj = {'last_modified_str': metric_value}
            except ValueError:
                self.logger.debug(f"Invalid date format for last_modified: {metric_value}")
                formtted_value = None
                ext_obj = {'last_modified_str': metric_value}
        return ext_obj,formtted_value    

    def build_message(self, value: dict, msg_metadata: dict) -> list:
        # Get a JSON list so need to iterate and then call super().build_message for each record
        if not value or not value.get("data", None):
            return []
        
        # check if value["data"] is a list
        if not isinstance(value["data"], list):
            self.logger.warning("Expected 'data' to be a list, got %s", type(value["data"]))
            return []
        
        #iterate over records in value["data"] and build formatted records
        formatted_records = []
        for record in value["data"]:
            formatted_records.extend(self.build_single_message(record, msg_metadata))
        return formatted_records
    
    def build_single_message(self, value, msg_metadata):
        # check required fields
        if not self.has_required_fields(value):
            return []
        
        formatted_records = []
        #iterate through metric_map and build formatted record for each metric
        for metric_src_field, (target_field, field_type) in self.metric_map.items():
            ext_obj, field_value = self.format_value(target_field, value.get(metric_src_field, None))
            if field_value is None:
                continue
            #get table name. resource type is fixed as 'iri_resource'
            table_name = self.get_table_name('iri_resource', field_type)
            #lookup resource ref from cacher
            resource_ref = self.pipeline.cacher("clickhouse").lookup_dict_key("meta_iri_resource", value.get('id'), 'ref')
            #build formatted record
            formatted_record = {
                "_clickhouse_table": table_name,
                "observation_time": datetime.now(),
                "collector_id": self.collector_id,
                "policy_originator": self.policy_originator,
                "policy_level": self.policy_level,
                "policy_scope": self.policy_scope,
                "ext": orjson.dumps(ext_obj).decode('utf-8'),
                "id": value.get('id'),
                "ref": resource_ref,
                "metric_name": target_field,
                "metric_value": field_value
            }
            formatted_records.append(formatted_record)
        
        return formatted_records


        
