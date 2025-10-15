import logging
import os
import re
import hashlib
import orjson
from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BaseClickHouseProcessor(BaseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)

        # setup logger
        self.logger = logger

        #Override values in child class
        self.create_table_cmd = None
        self.column_names = []

    def message_to_columns(self, message: dict) -> list:
        cols = []
        for col in self.column_names:
            if col not in message.keys():
                raise ValueError(f"Missing column '{col}' in message")
            cols.append(message.get(col))
        return cols

class BaseMetadataProcessor(BaseClickHouseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.force_update = os.getenv('CLICKHOUSE_METADATA_FORCE_UPDATE', 'false').lower() in ['true', '1', 'yes']
        self.db_ref_field = 'ref'  # Field in the data to use as the versioned reference
        self.val_id_field = ['id']  # Field in the data to use as the identifier
        self.required_fields = []  # List of lists of required fields, any one of which must be present

    def build_message(self, value: dict, msg_metadata: dict) -> list:
        # check required fields
        if not self.has_required_fields(value):
            return None

        #convert record to json, sort keys, and get md5 hash
        value_json = orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_md5 = hashlib.md5(value_json.encode('utf-8')).hexdigest()
        #iterate through val_id_field, get the value if exists and the try next
        id = None
        for field in self.val_id_field:
            if id is None:
                id = value.get(field, None)
                continue
            elif isinstance(id, dict):
                id = id.get(field, None)
            else:
                id = None
                break
        if id is None:
            self.logger.error(f"Missing identifier field(s) {self.val_id_field} in message value")
            return None

        #determine ref and if we need new record
        ref = "{}__v1".format(id)
        cached_record = self.pipeline.cacher("clickhouse").lookup(self.table, id)
        if not self.force_update and cached_record and cached_record['hash'] == record_md5:
            self.logger.debug(f"Record {id} unchanged, skipping")
            return None
        elif cached_record:
            #change so update
            self.logger.info(f"Record {id} changed, updating")
            #get latest version number from end of existing ref suffix ov __v{version_num} which may be multiple digits
            latest_ref = cached_record.get(self.db_ref_field, '')
            #use regex to extract version number
            match = re.search(r'__v(\d+)$', latest_ref)
            if match:
                version_num = int(match.group(1)) + 1
                ref = "{}__v{}".format(id, version_num)
            else:
                # If no version found, log a warning and skip
                self.logger.warning(f"No version found in ref {latest_ref} for record {id}, skipping version increment")
                return None
    
        formatted_record = {
            'ref': ref,
            'hash': record_md5,
            'id': str(id)
        }
        # merge formatted_record with result of self.build_metadata_fields(value)
        formatted_record.update(self.build_metadata_fields(value))

        return [formatted_record]
    
    def build_metadata_fields(self, value: dict) -> dict:
        """Override in child class to extract additional fields from value"""
        return {}