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
        self.table_ttl_column = "insert_time"

        # Override these in child classes
        # name of table
        self.table = ""
        self.table_ttl = None

        # array of arrays in format [['col_name', 'col_definition', bool_include_in_insert], ...]
        # for extension columns, col_definition is ignored and can be set to None
        self.column_defs = []
        # dict where key is extension field name and value is array in same format as column_defs but only those columns that are extensions
        self.extension_defs = {"ext": []}
        #dictionary where key is extension field name and value is dict of options for that extension. Populated via get_extension_defs()
        self.extension_enabled = defaultdict(dict)
        # list of reference fields that will be used if lookups to ip cacher and src dst IP then loaded in ext
        # if var name is <value> then assumes lookup table of meta_<value>, and creates fields like ext.src_<value>_ref and ext.dst_<value>_ref
        self.ip_ref_extensions = []
        self.partition_by = ""
        self.primary_keys = []
        self.order_by = []

    def get_table_names(self) -> list:
        """Return list of table names used by this processor. Override in child classes if multiple tables are used."""
        return [self.table]
    
    def create_table_command(self, table_name=None) -> str:
        """Return the ClickHouse table creation command"""
        # first check that we have everything we need
        if not table_name and not self.table:
            raise ValueError("Table name is not set")
        elif not table_name:
            table_name = self.table
        if not self.column_defs:
            raise ValueError("Column definitions are not set")
        if not self.table_engine:
            raise ValueError("Table engine is not set")

        table_settings = { "index_granularity": self.table_granularity }
        create_table_cmd =  "CREATE TABLE IF NOT EXISTS {} (".format(table_name)
        has_columns = False
        for col_def in self.column_defs:
            if len(col_def) > 3:
                # the 4th item checks suffix of table name and skips if not match
                if not table_name.endswith(f"_{col_def[3]}"):
                    continue
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
        if self.table_ttl and self.table_ttl_column:
            create_table_cmd += "TTL {} + INTERVAL {} \n".format(self.table_ttl_column, self.table_ttl)
            table_settings["ttl_only_drop_parts"] = "1"
        create_table_cmd += "SETTINGS {} \n".format(",".join(["{} = {}".format(k, v) for k, v in sorted(table_settings.items())]))
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

    def get_ip_ref_extensions(self, env_var_name: str) -> list:
        """Get list of extension names that should have IP reference fields from environment variable"""
        extension_str = os.getenv(env_var_name, None)
        ip_ref_extensions = []
        if not extension_str:
            return ip_ref_extensions
        extension_str = extension_str.strip()
        for ext in extension_str.split(','):
            ext = ext.strip()
            if not ext:
                continue
            ip_ref_extensions.append(ext)
        return ip_ref_extensions

    def lookup_ip_ref_extensions(self, ip_address: str, direction: str) -> dict:
        """Lookup IP reference extensions for a given IP address from the cacher"""
        ref_results = {}
        if not ip_address:
            return ref_results
        for ext in self.ip_ref_extensions:
            table_name = f"meta_ip_{ext}"
            ref = self.pipeline.cacher("ip").lookup(table_name, ip_address)
            if ref:
                ref_results[f"{direction}_ip_{ext}_ref"] = ref.get("ref", None)
        return ref_results

    def column_names(self) -> list:
        """Return list of column names for insertion into ClickHouse"""
        column_names = []
        columns_seen = {} #track columns seen so we don't add duplicates - can't use set since order matters
        for col_def in self.column_defs:
            if columns_seen.get(col_def[0], False):
                continue
            include_in_insert = col_def[2]
            if include_in_insert:
                column_names.append(col_def[0])
                columns_seen[col_def[0]] = True
        return column_names

    def message_to_columns(self, message: dict, table_name: str) -> list:
        """Convert a message dict to a list of column values for insertion into ClickHouse"""
        column_values = []
        columns_seen = {} #track columns seen so we don't add duplicates
        for col_def in self.column_defs:
            if columns_seen.get(col_def[0], False):
                continue
            col_name = col_def[0]
            include_in_insert = col_def[2]
            if not include_in_insert:
                continue
            if col_name not in message.keys():
                raise ValueError(f"Missing column '{col_name}' in message")
            column_values.append(message.get(col_name, None))
            columns_seen[col_name] = True
        # return as list
        return column_values

class BaseMetadataProcessor(BaseClickHouseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.table_ttl = os.getenv('CLICKHOUSE_METADATA_TTL', None)
        self.force_update = os.getenv('CLICKHOUSE_METADATA_FORCE_UPDATE', 'false').lower() in ['true', '1', 'yes']
        self.db_ref_field = 'ref'  # Field in the data to use as the versioned reference
        self.val_id_field = ['id']  # Field in the data to use as the identifier
        self.match_fields = []  # Fields to match incoming messages against
        self.required_fields = []  # List of lists of required fields, any one of which must be present
        self.float_fields = []  # List of fields to format as floats
        self.int_fields = []  # List of fields to format as integers
        self.boolean_fields = []  # List of fields to format as booleans
        self.array_fields = []  # List of fields to format as arrays
        self.self_ref_fields = []  # List of fields that have self references to update
        # If versioned then calculate hash and ref fields
        self.versioned  = True
        # array of arrays in format [['col_name', 'col_definition', bool_include_in_insert], ...]
        # for extension columns, col_definition is ignored and can be set to None
        # NOTE: if subclass is not versioned, then must override column_defs to remove hash and ref fields
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
            formatted_record = self.build_single_message(record, msg_metadata)
            if formatted_record:
                formatted_records.append(formatted_record)

        #update any self references in formatted records
        if self.self_ref_fields:
            #build a map of ids to refs from formatted records
            id_to_ref = {rec['id']: rec['ref'] for rec in formatted_records}
            # Update all the refs that point to same resource type
            new_formatted_records = []
            for rec in formatted_records:
                rec_updated = False
                #for self reference field we'll update the refs
                for field in self.self_ref_fields:
                    #build id and ref field names
                    id_field = f"{field}_id"
                    ref_field = f"{field}_ref"
                    # if we have a list of refs handle that case, otherwise single value
                    if isinstance(rec.get(id_field, None), list):
                        new_refs = []
                        for rid in rec[id_field]:
                            #Note: In this case we may have a mix of null and not null, but always add to keep indices aligned with id field
                            new_ref = id_to_ref.get(rid, None)
                            new_refs.append(new_ref)
                        rec[ref_field] = new_refs
                        rec_updated = True
                    else:
                        # Handle single value
                        new_ref = id_to_ref.get(rec[id_field], None)
                        if new_ref and id_field in rec:
                            rec[ref_field] = new_ref
                            rec_updated = True
                if rec_updated:
                    #recalculate hash since we changed the record
                    rec['hash'] = self.calculate_hash(rec)
                #now lookup in cache again and compare hash
                cached_record = self.pipeline.cacher("clickhouse").lookup(self.table, rec['id'])
                if self.force_update or not cached_record or cached_record['hash'] != rec['hash']:
                    new_formatted_records.append(rec)
                else:
                    self.logger.debug(f"Record {rec['id']} unchanged after self-ref update, skipping")
            # Finally set records - self-refs create a lot of work :) 
            formatted_records = new_formatted_records

        return formatted_records

    def calculate_hash(self, record: dict) -> str:
        #clear out existing hash if present
        if 'hash' in record:
            del record['hash']
        if 'ref' in record:
            #if already has ref, make a copy and remove to avoid modifying original
            record = record.copy()
            del record['ref']
        #convert to json, sort keys, and get md5 hash
        record_json = orjson.dumps(record, option=orjson.OPT_SORT_KEYS).decode('utf-8')
        return hashlib.md5(record_json.encode('utf-8')).hexdigest()

    def build_single_message(self, value: dict, msg_metadata: dict) -> dict | None:
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
        if not self.versioned:
            #if not versioned metadata, then we are done
            return formatted_record
        
        # Calculate hash - do after building full record so any refs or other changes in hash
        record_md5 = self.calculate_hash(formatted_record)

        #determine ref and if we need new record
        ref = "{}__v1".format(id)
        cached_record = self.pipeline.cacher("clickhouse").lookup(self.table, id)
        # if it has refs that point at itself, we need to build everything and then make decision later once we have updated all the refs
        if not self.force_update and cached_record and cached_record['hash'] == record_md5 and not self.self_ref_fields:
            self.logger.debug(f"Record {id} unchanged, skipping")
            return None
        elif (cached_record and cached_record['hash'] != record_md5) or self.force_update:
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
        elif self.self_ref_fields and cached_record and cached_record['hash'] == record_md5:
            # in this case we need to get the existing ref since we are not changing anything
            # but we still need to process the record to update self refs
            new_ref = cached_record.get(self.db_ref_field, '')
            if new_ref:
                ref = new_ref
        
        #add hash and ref to record
        formatted_record['ref'] = ref
        formatted_record['hash'] = record_md5

        return formatted_record
    
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
        #load values from data
        formatted_record = {}
        for field in self.column_defs:
            if not field[2] or field[0] in ['id', 'ref', 'hash']:
                continue
            formatted_record[field[0]] = value.get(field[0], None)

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
        self.metric_types = ['counter', 'gauge']  # list of metric types to create tables for
        #loads resource types from environment - this may be overriden by other methods in subclass
        self.resource_types = self.load_resource_types()
        #build map of table names
        self.table_name_map = {}
        for resource_type in self.resource_types:
            for metric_type in self.metric_types:
                table_name = self.get_table_name(resource_type, metric_type)
                self.table_name_map[table_name] = True

        #add standard columns
        self.column_defs.insert(0, ["observation_time", "DateTime64(3, 'UTC') CODEC(Delta,ZSTD)", True])
        self.column_defs.append(["id", "LowCardinality(String)", True])
        self.column_defs.append(["ref", "Nullable(String)", True])
        self.column_defs.append(["metric_name", "String", True])
        # last two are metric value types - the item at index 3 is a marker to indicate which type it is
        self.column_defs.append(["metric_value", "Float64", True, "gauge"])
        self.column_defs.append(["metric_value", "UInt64 CODEC(Delta,ZSTD)", True, "counter"])

        # adjust other table settings
        self.partition_by = "toYYYYMMDD(observation_time)"
        self.order_by = ("metric_name", "id", "observation_time")

    def load_resource_types(self) -> list:
        """Load resource names from CLICKHOUSE_METRIC_RESOURCE_NAME environment variable"""
        resource_str = os.getenv('CLICKHOUSE_METRIC_RESOURCE_NAME', None)
        if not resource_str:
            return []
        resource_types= []
        for res in resource_str.split(','):
            res = res.strip()
            if res:
                resource_types.append(res)
        if not resource_types:
            return []
        return resource_types

    def get_table_name(self, resource_type: str, metric_type: str) -> str:
        return f"data_{resource_type}_{metric_type}"

    def get_table_names(self):
        return self.table_name_map.keys()