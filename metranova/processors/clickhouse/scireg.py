import logging
import os
from datetime import datetime
from metranova.processors.clickhouse.base import BaseMetadataProcessor

logger = logging.getLogger(__name__)

class ScienceRegistryProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_SCIREG_METADATA_TABLE', 'meta_scireg')
        self.val_id_field = ['scireg_id']
        self.create_table_cmd = f"""
        CREATE TABLE IF NOT EXISTS {self.table} (
            ref String,
            hash String,
            id String,
            insert_ts DateTime DEFAULT now(),
            last_updated Date DEFAULT '1970-01-01',
            addresses Array(String),
            org_name Nullable(String),
            discipline Nullable(String),
            latitude Nullable(Float64),
            longitude Nullable(Float64),
            resource_name Nullable(String),
            project_name Nullable(String),
            contact_email Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY (ref)
        """
        self.column_names = [
            "ref",
            "hash",
            "id",
            "last_updated",
            "addresses",
            "org_name",
            "discipline",
            "latitude",
            "longitude",
            "resource_name",
            "project_name",
            "contact_email"
        ]

        self.required_fields = [
            ["scireg_id"],
            ["addresses"]
        ]

    def build_message(self, value: dict, msg_metadata: dict) -> list[dict]:
        # Get a JSON list so need to iterate and then call super().build_message for each record
        if not value or not value.get("data", None):
            return []
        
        # check if value["data"] is a list
        if not isinstance(value["data"], list):
            self.logger.warning("Expected 'data' to be a list, got %s", type(value["data"]))
            return []
        
        #iterate through each record in value["data"]
        records = []
        for record in value["data"]:
            formatted_records = super().build_message(record, msg_metadata)
            self.logger.debug(f"Formatted record: {formatted_records}")
            if formatted_records:
                records.extend(formatted_records)

        return records

    def build_metadata_fields(self, value: dict) -> dict | None:
        #init hash
        formatted_record = {
            'last_updated': value.get('last_updated', 'unknown'),
            'addresses': value['addresses'],
            'org_name': value.get('org_name', None),
            'discipline': value.get('discipline', None),
            'latitude': value.get('latitude', None),
            'longitude': value.get('longitude', None),
            'resource_name': value.get('resource_name', None),
            'project_name': value.get('project_name', None),
            'contact_email': value.get('contact_email', None)
        }

        #format last_updated if equals "unknown"
        if formatted_record['last_updated'] == "unknown":
            formatted_record['last_updated'] = '1970-01-01'
        #convert last_updated to a datetime object
        try:
            formatted_record['last_updated'] = datetime.strptime(formatted_record['last_updated'], '%Y-%m-%d').date()
        except ValueError:
            formatted_record['last_updated'] = datetime(1970, 1, 1).date()

        #cast latitude and longitude to a float, and set to None if exception when casting
        float_fields = ['latitude', 'longitude']
        for field in float_fields:
            if formatted_record[field] is not None:
                try:
                    formatted_record[field] = float(formatted_record[field])
                except ValueError:
                    formatted_record[field] = None

        return formatted_record