import logging
from typing import Any, Dict, Iterator
from metranova.processors.clickhouse.flow import BaseFlowProcessor

logger = logging.getLogger(__name__)

class SFAcctdFlowProcessor(BaseFlowProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        # Define required fields - these are examples, adjust as needed
        self.required_fields = [
            ['src_ip'], 
            ['dst_ip'], 
            ['toplevel', 'nested']
        ]

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        # check required fields
        if not self.has_required_fields(value):
            return None
        
        # Value should be a dict of the JSON object sent from sfacctd to Kafka
        self.logger.info( f"Processing sfacctd flow record: {value}" )
        
        # build the record here. The keys match the ClickHouse column names.
        # Printing column names for reference:
        self.logger.info(f"Column names: {self.column_names}")
        # Pull the relevant fields from the value dict as needed. 
        # you may also need to do some formattingto make sure theya re the right data type, etc
        # if you are not sure about something, just set it to None for now
        formatted_record = {}

        # expects a list returned (there are cases that aren;t this where you produce multiple records)
        # just wraps our one dict in an array
        return [formatted_record]