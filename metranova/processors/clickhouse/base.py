from collections import defaultdict
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

        # Defaults that are relatively common but can be overridden
        self.table_engine = "MergeTree()"
        self.table_granularity = 8192  # default ClickHouse index granularity
        self.extension_columns = {"ext": True}  # default extension column"}

        # Override these in child classes
        # name of table
        self.table = ""
        # array of arrays in format [['col_name', 'col_definition', bool_include_in_insert], ...]
        # for extension columns, col_definition is ignored and can be set to None
        self.column_defs = []
        # dict where key is extension field name and value is array in same format as column_defs but only those columns that are extensions
        self.extension_defs = {"ext": []}
        #dictionary where key is extension field name and value is dict of options for that extension. Populated via get_extension_defs()
        self.extension_enabled = defaultdict(dict)
        self.partition_by = ""
        self.primary_keys = []
        self.order_by = []

    def create_table_command(self) -> str:
        """Return the ClickHouse table creation command"""
        # first check that we have everything we need
        if not self.table:
            raise ValueError("Table name is not set")
        if not self.column_defs:
            raise ValueError("Column definitions are not set")
        if not self.table_engine:
            raise ValueError("Table engine is not set")

        create_table_cmd =  "CREATE TABLE IF NOT EXISTS {} (".format(self.table)
        has_columns = False
        for col_def in self.column_defs:
            if has_columns:
                create_table_cmd += ","
            # handle extension columns. This code is somewhat redundant with wrapping code but keeps things clearer
            if col_def[0] in self.extension_columns and col_def[0] in self.extension_defs:
                #ignore column definition from main list, use extension_defs instead
                create_table_cmd += "\n    `{}` JSON(".format(col_def[0])
                ext_has_columns = False
                for ext_col_def in self.extension_defs[col_def[0]]:
                    if ext_has_columns:
                        create_table_cmd += ","
                    create_table_cmd += "\n        `{}` {}".format(ext_col_def[0], ext_col_def[1])
                    ext_has_columns = True
                create_table_cmd += "\n    )"
            elif col_def[0] in self.extension_columns:
                # if have an extension column but no definitions, just make a JSON column
                create_table_cmd += "\n    `{}` JSON".format(col_def[0])
            else:
                create_table_cmd += "\n    `{}` {}".format(col_def[0], col_def[1])
            has_columns = True
        create_table_cmd += "\n) \n"
        create_table_cmd += "ENGINE = {} \n".format(self.table_engine)
        if self.partition_by:
            create_table_cmd += "PARTITION BY {} \n".format(self.partition_by)
        if self.primary_keys:
            #format as `col1`,`col2`,...
            create_table_cmd += "PRIMARY KEY ({}) \n".format(",".join(["`{}`".format(col) for col in self.primary_keys]))
        if self.order_by:
            #format as `col1`,`col2`,...
            create_table_cmd += "ORDER BY ({}) \n".format(",".join(["`{}`".format(col) for col in self.order_by]))
        create_table_cmd += "SETTINGS index_granularity = {} \n".format(self.table_granularity)
        return create_table_cmd

    def get_extension_defs(self, env_var_name: str, extension_options: dict, json_column_name: str = "ext") -> list:
        """Get extension column definitions from environment variable and applies only those in extension_options which takes form {extension_name: [[col_name, col_definition], ...]}"""
        extension_str = os.getenv(env_var_name, None)
        if not extension_str:
            return []
        extension_str = extension_str.strip()
        extension_defs = []
        for ext in extension_str.split(','):
            ext = ext.strip()
            if not ext:
                continue
            if ext in extension_options:
                extension_defs.extend(extension_options[ext])
            # enable even if there are no type hints for this extension
            # processor may still want to store it without initial type hints
            self.extension_enabled[json_column_name][ext] = True
        return extension_defs

    def extension_is_enabled(self, extension_name: str, json_column_name: str = "ext") -> bool:
        return self.extension_enabled.get(json_column_name, {}).get(extension_name, False)

    def column_names(self) -> list:
        """Return list of column names for insertion into ClickHouse"""
        column_names = []
        for col_def in self.column_defs:
            include_in_insert = col_def[2]
            if include_in_insert:
                column_names.append(col_def[0])
        return column_names

    def message_to_columns(self, message: dict) -> list:
        """Convert a message dict to a list of column values for insertion into ClickHouse"""
        column_values = []
        for col_def in self.column_defs:
            col_name = col_def[0]
            include_in_insert = col_def[2]
            if not include_in_insert:
                continue
            if col_name not in message.keys():
                raise ValueError(f"Missing column '{col_name}' in message")
            column_values.append(message.get(col_name, None))
        return column_values

class BaseMetadataProcessor(BaseClickHouseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.force_update = os.getenv('CLICKHOUSE_METADATA_FORCE_UPDATE', 'false').lower() in ['true', '1', 'yes']
        self.db_ref_field = 'ref'  # Field in the data to use as the versioned reference
        self.val_id_field = ['id']  # Field in the data to use as the identifier
        self.match_fields = []  # Fields to match incoming messages against
        self.required_fields = []  # List of lists of required fields, any one of which must be present
        self.float_fields = []  # List of fields to format as floats
        self.int_fields = []  # List of fields to format as integers
        self.boolean_fields = []  # List of fields to format as booleans
        self.array_fields = []  # List of fields to format as arrays

        # array of arrays in format [['col_name', 'col_definition', bool_include_in_insert], ...]
        # for extension columns, col_definition is ignored and can be set to None
        self.column_defs = [
            ['id', 'String', True],
            ['ref', 'String', True],
            ['hash', 'String', True],
            ['insert_time', 'DateTime DEFAULT now()', False],
            ['ext', None, True],
            ['tag', 'Array(LowCardinality(String))', True]
        ]
        # dict where key is extension field name and value is array in same format as column_defs but only those columns that are extensions
        self.order_by = ['ref', 'id', 'insert_time']

    def match_message(self, value):
        if value.get("table", None) == self.table:
            return super().match_message(value)
        return False

    def build_message(self, value: dict, msg_metadata: dict) -> list:
        # check required fields
        if not self.has_required_fields(value):
            return None

        # Get ID
        # iterate through val_id_field, get the value if exists and the try next
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
        #make sure id is string
        id = str(id)

        # Build Initial Record which will be used to calculate hash
        formatted_record = { "id": id }
        # merge formatted_record with result of self.build_metadata_fields(value)
        formatted_record.update(self.build_metadata_fields(value))

        # Calculate hash - do after building full record so any refs or other changes in hash
        # convert record to json, sort keys, and get md5 hash
        record_json = orjson.dumps(formatted_record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        record_md5 = hashlib.md5(record_json.encode('utf-8')).hexdigest()

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
        
        #add hash and ref to record
        formatted_record['ref'] = ref
        formatted_record['hash'] = record_md5

        return [formatted_record]
    
    def format_float_fields(self, formatted_record: dict) -> dict:
        """Format specified fields in formatted_record as floats if possible"""
        for field in self.float_fields:
            if formatted_record.get(field, None) is not None:
                try:
                    formatted_record[field] = float(formatted_record[field])
                except (TypeError, ValueError):
                    formatted_record[field] = None

    def format_int_fields(self, formatted_record: dict) -> dict:
        """Format specified fields in formatted_record as integers if possible"""
        for field in self.int_fields:
            if formatted_record.get(field, None) is not None:
                try:
                    formatted_record[field] = int(formatted_record[field])
                except (TypeError, ValueError):
                    formatted_record[field] = None

    def format_boolean_fields(self, formatted_record: dict) -> dict:
        """Format specified fields in formatted_record as booleans"""
        for field in self.boolean_fields:
            formatted_record[field] = formatted_record.get(field, False) in [True, 'true', 'True', 1, '1']

    def format_array_fields(self, formatted_record: dict) -> dict:
        """Format specified fields in formatted_record as arrays"""
        for field in self.array_fields:
            if formatted_record[field] is None:
                formatted_record[field] = []
            elif isinstance(formatted_record[field], str):
                try:
                    formatted_record[field] = orjson.loads(formatted_record[field])
                except orjson.JSONDecodeError:
                    formatted_record[field] = []

    def build_metadata_fields(self, value: dict) -> dict:
        """Grabs values from column defs. Override in child class to extract additional fields from value"""
        value_data = value.get('data', {})

        #load values from data
        formatted_record = {}
        for field in self.column_defs:
            if not field[2] or field[0] in ['id', 'ref', 'hash']:
                continue
            formatted_record[field[0]] = value_data.get(field[0], None)

        # handle ext
        if formatted_record['ext'] is None:
            formatted_record['ext'] = '{}'
        elif isinstance(formatted_record['ext'], dict):
            formatted_record['ext'] = orjson.dumps(formatted_record['ext'], option=orjson.OPT_SORT_KEYS).decode('utf-8')
        
        #set tags default if none
        if formatted_record['tag'] is None:
            formatted_record['tag'] = []
        elif isinstance(formatted_record['tag'], str):
            try:
                formatted_record['tag'] = orjson.loads(formatted_record['tag'])
            except orjson.JSONDecodeError:
                formatted_record['tag'] = []

        #format data types
        self.format_float_fields(formatted_record)
        self.format_int_fields(formatted_record)
        self.format_boolean_fields(formatted_record)
        self.format_array_fields(formatted_record)

        return formatted_record

class BaseDataProcessor(BaseClickHouseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = []  # List of lists of required fields, any one of which must be present
        self.policy_originator = os.getenv('CLICKHOUSE_POLICY_ORIGINATOR', 'unknown')
        self.policy_level = os.getenv('CLICKHOUSE_POLICY_LEVEL', 'tlp:amber')
        self.policy_scope = os.getenv('CLICKHOUSE_POLICY_SCOPE', None)
        if self.policy_scope:
            self.policy_scope = [s.strip() for s in self.policy_scope.split(',')]
        else:
            self.policy_scope = []

        # array of arrays in format [['col_name', 'col_definition', bool_include_in_insert], ...]
        # for extension columns, col_definition is ignored and can be set to None
        self.column_defs = [
            ["insert_time", "DateTime64(3, 'UTC') DEFAULT now64() CODEC(Delta,ZSTD)", False],
            ["collector_id", "LowCardinality(String)", True],
            ["policy_originator", "LowCardinality(String)", True],
            ["policy_level", "LowCardinality(String)", True],
            ["policy_scope", "Array(LowCardinality(String))", True],
            ["ext", None, True]
        ]

class BaseDataGenericMetricProcessor(BaseDataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
                #get resource name from environment variable
        self.resource_name = os.getenv('CLICKHOUSE_METRIC_RESOURCE_NAME', None)
        if not self.resource_name:
            raise ValueError("CLICKHOUSE_METRIC_RESOURCE_NAME environment variable not set")
        #add standard columns
        id_field ="{}_id".format(self.resource_name)
        ref_field ="{}_ref".format(self.resource_name)
        self.column_defs.insert(0, ["observation_time", "DateTime64(3, 'UTC') CODEC(Delta,ZSTD)", True])
        self.column_defs.append([id_field, "LowCardinality(String)", True])
        self.column_defs.append([ref_field, "Nullable(String)", True])
        self.column_defs.append(["metric_name", "String", True])
        # adjust other table settings
        self.partition_by = "toYYYYMMDD(observation_time)"
        self.order_by = ("metric_name", id_field, "observation_time")

class DataCounterProcessor(BaseDataGenericMetricProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = "data_{}_counter".format(self.resource_name)
        self.column_defs.append(["metric_value", "UInt64 CODEC(Delta,ZSTD)", True])

class DataGaugeProcessor(BaseDataGenericMetricProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table = "data_{}_gauge".format(self.resource_name)
        self.column_defs.append(["metric_value", "Float64", True])