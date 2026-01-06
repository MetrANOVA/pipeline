from datetime import datetime, timedelta
import logging
import os
import re
from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor, BaseMetadataProcessor

logger = logging.getLogger(__name__)

class JobMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = os.getenv('CLICKHOUSE_ALCF_JOB_TABLE', 'meta_alcf_job')
        self.column_defs.extend([
            ['compute_resource_id', 'String', True],
            ['location', 'Array(String)', True],
            ['mode', 'LowCardinality(String)', True],
            ['node_count', 'UInt64', True],
            ['project', 'String', True],
            ['queue', 'String', True],
            ['schedule_select', 'Array(Tuple(at_queue Nullable(String), broken Nullable(Bool), debug Nullable(Bool), host Nullable(String), mem Nullable(String), mpiprocs Nullable(UInt32), nchunks Nullable(UInt32), ncpus Nullable(UInt32), ngpus Nullable(UInt32), system Nullable(String), validation Nullable(Bool)))', True],
            ['start_time', 'Nullable(DateTime)', True],
            ['end_time', 'Nullable(DateTime)', True],
            ['state', 'LowCardinality(String)', True],
            ['submit_time', 'DateTime', True],
            ['substate', 'UInt32', True],
            ['wall_time_secs', 'UInt64', True]
        ])
        self.val_id_field = ['jobid']
        self.array_fields = ['location', 'schedule_select']
        self.int_fields = ['node_count', 'substate', 'wall_time_secs'] #note due to name change, we format node_count and wall_time_secs in build_metadata_fields
        self.required_fields = [['jobid'], ['mode'], ['nodes'], ['project'], ['queue'], ['state'], ['submittime'], ['substate'], ['walltime']]

    def match_message(self, value):
        if value.get('type', None) != 'job':
            return False
        return self.has_match_field(value)

    def format_time_value(self, value):
        """Format time value from int timestamp to datetime object"""
        if value is None:
            return None
        try:
            int_value = int(value)
            if int_value < 0:
                return None
            return datetime.fromtimestamp(int_value)
        except ValueError:
            self.logger.debug(f"Invalid time value: {value}")
            return None

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #nodes -> node_count (int)
        formatted_record['node_count'] = int(value.get('nodes', 0))
       
        #schedselect -> schedule_select
        schedule_select_list = []
        for scheselect_obj in value.get('schedselect', []):
            #make sure we have a dict
            if not isinstance(scheselect_obj, dict):
                continue
            #build tuple in expected order from columnn definition
            scheselect_tuple = (
                scheselect_obj.get('at_queue', None),
                scheselect_obj.get('broken', None),
                scheselect_obj.get('debug', None),
                scheselect_obj.get('host', None),
                scheselect_obj.get('mem', None),
                scheselect_obj.get('mpiprocs', None),
                scheselect_obj.get('nchunks', None),
                scheselect_obj.get('ncpus', None),
                scheselect_obj.get('ngpus', None),
                scheselect_obj.get('system', None),
                scheselect_obj.get('validation', None)
            )
            schedule_select_list.append(scheselect_tuple)
        formatted_record['schedule_select'] = schedule_select_list

        #starttime -> start_time - grab int value and convert to datetime
        formatted_record['start_time'] = self.format_time_value(value.get('starttime', None))
         #walltime -> wall_time_secs (int)
        formatted_record['wall_time_secs'] = int(value.get('walltime', 0)) * 60  # convert minutes to seconds
        #calculate end_time if start_time and walltime are present
        if formatted_record.get('start_time') is not None and formatted_record.get('wall_time_secs') is not None:
            formatted_record['end_time'] = formatted_record['start_time'] + timedelta(seconds=formatted_record['wall_time_secs'])
        else:
            formatted_record['end_time'] = None
        
        #submittime -> submit_time - grab int value and convert to datetime
        formatted_record['submit_time'] = self.format_time_value(value.get('submittime', None))
        if formatted_record['submit_time'] is None:
            raise ValueError("submit_time is required but could not be parsed")

        #build a hash with all the keys and values from value['data']
        return formatted_record

class JobDataProcessor(BaseDataGenericMetricProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.resource_type = self.load_resource_types()[0]
        self.required_fields = [['jobid']]
        self.collector_id = os.getenv('CLICKHOUSE_ALCF_COLLECTOR_ID', 'metranova_pipeline')
        self.metric_map = {
            'runtime_secs': ('runtimef', 'gauge'),
            'queued_time_secs': ('queuedtimef', 'gauge'),
            'score': ('score', 'gauge')
        }
        self.url_match = None  

    def load_resource_types(self):
        return ['alcf_job']
    
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
        if not self.has_required_fields(value):
            return []
        
        formatted_records = []
        #iterate through metric_map and build formatted record for each metric
        for metric_src_field, (target_field, field_type) in self.metric_map.items():
            field_value = value.get(metric_src_field, None)
            if field_value is None:
                continue
            #get table name
            table_name = self.get_table_name(f"{self.resource_type}", field_type)
            #lookup ref from cacher
            ref = self.pipeline.cacher("clickhouse").lookup_dict_key(f"meta_{self.resource_type}", value.get('jobid'), 'ref')
            #build formatted record
            formatted_record = {
                "_clickhouse_table": table_name,
                "observation_time": datetime.now(),
                "collector_id": self.collector_id,
                "policy_originator": self.policy_originator,
                "policy_level": self.policy_level,
                "policy_scope": self.policy_scope,
                "ext": '{}',
                "id": value.get('jobid'),
                "ref": ref,
                "metric_name": target_field,
                "metric_value": field_value
            }
            formatted_records.append(formatted_record)
        
        return formatted_records

        
