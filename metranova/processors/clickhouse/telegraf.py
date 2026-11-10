import logging
import os
import re

import orjson

from metranova.processors.clickhouse.base import BaseDataGenericMetricProcessor

logger = logging.getLogger(__name__)

class DataGenericMetricProcessor(BaseDataGenericMetricProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.required_fields = [
            ["timestamp"], 
            ["fields"],
            ["tags"]
        ]
        # Get the resource type from the environment variable, default to "telegraf" if not set
        self.resource_type = os.getenv("TELEGRAF_RESOURCE_TYPE", "telegraf")
        # Optional regex applied to device_id to extract/transform it (e.g. strip a domain suffix).
        # Set TELEGRAF_DEVICE_ID_REGEX to a pattern with one capture group, e.g. "^([^.]+)\..*$"
        device_id_pattern = os.getenv("TELEGRAF_DEVICE_ID_REGEX", None)
        self.device_id_regex = re.compile(device_id_pattern) if device_id_pattern else None
        #tags to always skip - these are added to skip_tags in rules
        skip_tags_list = ["date", "routing_tag", "collector", "device"]
        self.skip_tags = {tag: True for tag in skip_tags_list}

    def build_message(self, value, msg_metadata):
        # check required fields
        if not self.has_required_fields(value):
            return []

        # get the device id and ref from the tags
        device_id = value.get("tags", {}).get("device", None)
        if device_id is None:
            self.logger.debug("Missing device id in tags")
            return []
        if self.device_id_regex:
            match = self.device_id_regex.search(device_id)
            device_id = match.group(1) if match and match.lastindex else device_id

        #lookup device ref in cache
        device_details = self.pipeline.cacher("clickhouse").lookup("meta_device", device_id)
        device_ref = device_details.get("ref", None) if device_details else None

        #grab the timestamp
        try:
            observation_time = int(value.get('timestamp', 0)) * 1000
        except (ValueError, TypeError):
            self.logger.error("Invalid timestamp format")
            return []

        #build ext from tags
        ext = {}
        for tag_key, tag_value in value.get("tags", {}).items():
            if self.skip_tags.get(tag_key, False):
                continue
            ext[tag_key] = tag_value
        # build json here to save from doing it multiple times
        ext_json = orjson.dumps(ext).decode('utf-8')

        # loop through fields and build messages
        formatted_records = []
        for field, field_value in value.get("fields", {}).items():
            #skip null values
            if field_value is None:
                continue
            # we may have to change this so create var local to loop
            tmp_ext_json = ext_json
            table_name = None
            # If it is an int, make it a counter, if it is a float, make it a gauge, otherwise make it a counter by setting value to 1 and adding value to ext
            if isinstance(field_value, float):
                table_name = self.get_table_name(self.resource_type, 'gauge')
            elif not isinstance(field_value, int):
                # For non-numeric, set to 1 and add original value to ext
                table_name = self.get_table_name(self.resource_type, 'counter')
                tmp_ext = ext.copy()
                tmp_ext["metric_value"] = field_value
                tmp_ext_json = orjson.dumps(tmp_ext).decode('utf-8')
                field_value = 1
            else:
                table_name = self.get_table_name(self.resource_type, 'counter')

            # build formatted record
            formatted_record = {
                "_clickhouse_table": table_name,
                "observation_time": observation_time,
                "collector_id": value.get("tags", {}).get("collector", "unknown"),
                "policy_originator": self.policy_originator,
                "policy_level": self.policy_level,
                "policy_scope": self.policy_scope,
                "ext": tmp_ext_json,
                "id": device_id,
                "ref": device_ref,
                "metric_name": field,
                "metric_value": field_value
            }
            formatted_records.append(formatted_record)

        return formatted_records