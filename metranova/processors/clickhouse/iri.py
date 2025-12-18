from datetime import datetime
import logging
import re
import orjson
import os
from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor, BaseMetadataProcessor

logger = logging.getLogger(__name__)

class IRIBaseMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.column_defs.extend([
            ['uri', 'String', True],
            ['name', 'Nullable(String)', True],
            ['description', 'Nullable(String)', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['self_uri']]
        # override this with pattern to match in URL.
        # Example `/status/resources`, `status/inicidents/.+/events`
        self.url_match = None

    def match_message(self, value):
        if self.url_match is not None:
            #apply regex match on message url (value.get('url'))
            url = value.get('url', '')
            #remove trailing query params
            url = url.split('?', 1)[0]
            #remove trailing slash
            url = url.rstrip('/')
            if not url or not re.search(self.url_match, url):
                return False
        return self.has_match_field(value)

    def id_from_uri(self, uri):
        """Extract ID from a given URI"""
        if not uri:
            return None
        return uri.rstrip('/').split('/')[-1]

    def ids_and_refs_from_uri_list(self, uri_list, resource_type):
        """Extract IDs and refs from a list of URIs"""
        ids = []
        refs = []
        for uri in uri_list:
            resource_id = self.id_from_uri(uri)
            if resource_id is None:
                continue
            ids.append(resource_id)
            resource_ref = self.pipeline.cacher("clickhouse").lookup_dict_key(f"meta_{resource_type}", resource_id, 'ref')
            if resource_ref is not None:
                refs.append(resource_ref)
        return ids, refs

    def format_time_value(self, time_str):
        """Convert time string to datetime object"""
        if time_str is None:
            return None
        try:
            dt = datetime.fromisoformat(time_str)
            return dt
        except ValueError:
            self.logger.debug(f"Invalid date format: {time_str}")
            return None
    
    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #add remaining fields to formatted_record
        formatted_record['uri'] = value.get('self_uri')
        formatted_record['name'] = value.get('name', None)
        formatted_record['description'] = value.get('description', None)
        
        return formatted_record

class EventMetadataProcessor(IRIBaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.url_match = r'/status/incidents/.+/events$'
        self.table = os.getenv('CLICKHOUSE_IRI_EVENT_TABLE', 'meta_iri_event')
        self.column_defs.extend([
            ['last_modified', 'DateTime', True],
            ['status', 'LowCardinality(String)', True],
            ['occurred_at_time', 'DateTime', True],
            ['resource_id', 'LowCardinality(String)', True],
            ['resource_ref', 'Nullable(String)', True],
            ['incident_id', 'String', True],
            ['incident_ref', 'Nullable(String)', True]
        ])
        self.required_fields = [['id'], ['self_uri'], ['last_modified'], ['status'], ['occurred_at'], ['resource_uri'], ['incident_uri']]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #add remaining fields to formatted_record
        formatted_record['last_modified'] = self.format_time_value(value.get('last_modified'))
        formatted_record['status'] = value.get('status')
        formatted_record['occurred_at_time'] = self.format_time_value(value.get('occurred_at'))
        #resource id from URI
        formatted_record['resource_id'] = self.id_from_uri(value.get('resource_uri'))
        #lookup resource ref from cacher
        formatted_record['resource_ref'] = self.pipeline.cacher("clickhouse").lookup_dict_key("meta_iri_resource", formatted_record['resource_id'], 'ref')
        #incident id from URI
        formatted_record['incident_id'] = self.id_from_uri(value.get('incident_uri'))
        #lookup incident ref from cacher
        formatted_record['incident_ref'] = self.pipeline.cacher("clickhouse").lookup_dict_key("meta_iri_incident", formatted_record['incident_id'], 'ref')

        #build a hash with all the keys and values from value['data']
        return formatted_record
    
class IncidentMetadataProcessor(IRIBaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.url_match = r'/status/incidents$'
        self.table = os.getenv('CLICKHOUSE_IRI_INCIDENT_TABLE', 'meta_iri_incident')
        self.column_defs.extend([
            ['last_modified', 'DateTime', True],
            ['status', 'LowCardinality(String)', True],
            ['type', 'LowCardinality(String)', True],
            ['start_time', 'DateTime', True],
            ['end_time', 'Nullable(DateTime)', True],
            ['resolution', 'LowCardinality(String)', True],
            ['resource_id', 'Array(LowCardinality(String))', True],
            ['resource_ref', 'Array(String)', True]
        ])
        self.required_fields = [['id'], ['self_uri'], ['last_modified'], ['status'], ['type'], ['start'], ['resolution']]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #add remaining fields to formatted_record
        formatted_record['last_modified'] = self.format_time_value(value.get('last_modified'))
        formatted_record['status'] = value.get('status')
        formatted_record['type'] = value.get('type')
        formatted_record['start_time'] = self.format_time_value(value.get('start'))
        formatted_record['end_time'] = self.format_time_value(value.get('end', None))
        formatted_record['resolution'] = value.get('resolution')

        #Get ids from resource_uris which must be a list
        resource_uris = value.get('resource_uris', [])
        #verify it's a list
        if not isinstance(resource_uris, list):
            resource_uris = []
        formatted_record['resource_id'], formatted_record['resource_ref'] = self.ids_and_refs_from_uri_list(resource_uris, 'iri_resource')
        return formatted_record

class ResourceMetadataProcessor(IRIBaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.url_match = r'/status/resources$'
        self.table = os.getenv('CLICKHOUSE_IRI_RESOURCE_TABLE', 'meta_iri_resource')
        self.array_fields = ['capability_uris']
        self.column_defs.extend([
            ['type', 'LowCardinality(String)', True],
            ['group', 'LowCardinality(Nullable(String))', True],
            ['capability_id', 'Array(LowCardinality(String))', True],
            ['site_id', 'LowCardinality(Nullable(String))', True],
            ['facility_id', 'LowCardinality(Nullable(String))', True]
        ])
        self.required_fields = [['id'], ['resource_type'], ['self_uri']]

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #add remaining fields to formatted_record
        formatted_record['type'] = value.get('resource_type')
        formatted_record['group'] = value.get('group')
        formatted_record['capability_id'] = value.get('capability_uris', [])
        formatted_record['site_id'] = None #todo: lookup
        formatted_record['facility_id'] = None #todo: lookup

        #build a hash with all the keys and values from value['data']
        return formatted_record

class IRIBaseDataProcessor(BaseDataGenericMetricProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.resource_type = self.load_resource_types()[0]
        self.required_fields = [['id']]
        self.collector_id = os.getenv('CLICKHOUSE_IRI_COLLECTOR_ID', 'metranova_pipeline')
        self.metric_map = {
            'current_status': ('status', 'counter'),
            'status': ('status', 'counter'), #duplicate mapping for flexibility
            'last_modified': ('last_modified', 'counter')
        }
        self.url_match = None
    
    def match_message(self, value):
        if self.url_match is not None:
            #apply regex match on message url (value.get('url'))
            url = value.get('url', '')
            #remove trailing query params
            url = url.split('?', 1)[0]
            #remove trailing slash
            url = url.rstrip('/')
            if not url or not re.search(self.url_match, url):
                return False
        return self.has_match_field(value)
    
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
            #get table name
            table_name = self.get_table_name(f"{self.resource_type}", field_type)
            #lookup ref from cacher
            ref = self.pipeline.cacher("clickhouse").lookup_dict_key(f"meta_{self.resource_type}", value.get('id'), 'ref')
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
                "ref": ref,
                "metric_name": target_field,
                "metric_value": field_value
            }
            formatted_records.append(formatted_record)
        
        return formatted_records

class ResourceDataProcessor(IRIBaseDataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [['id'], ['current_status'], ['last_modified']]
        self.url_match = r'/status/resources$'
    
    def load_resource_types(self):
        return ['iri_resource']

        
