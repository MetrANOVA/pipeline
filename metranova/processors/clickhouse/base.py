
import logging

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
            self.logger.debug(f"Column '{col}': {message.get(col)}")
            cols.append(message.get(col))
        return cols