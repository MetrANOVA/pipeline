import logging
import os
import pytricia
from typing import Any, Dict, Iterator
from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

class BaseFileProcessor(BaseProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.required_fields = [
            ['table'],
            ['rows']
        ]

class IPTriePickleFileProcessor(BaseFileProcessor):
    """Expects input from ClickHouseConsumer and outputs to FileWriter"""
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.use_simple_table_name = os.getenv('IP_FILE_USE_SIMPLE_FILE_NAME', 'true').lower() in ('true', '1', 'yes')
        self.required_fields = [
            ['table'],
            ['rows'],
            ['column_names'],
        ]
    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        #check for required fields
        if not self.has_required_fields(value):
            return []

        #initialize ip trie
        ip_trie = pytricia.PyTricia(128)
        # Got through rows and add to trie
        for row in value.get('rows', []):
            id, ref, ip_subnet, *fields = row
            if id is None or ref is None or ip_subnet is None:
                self.logger.debug(f"Skipping row with null values: id={id}, ref={ref}, ip_subnet={ip_subnet}")
                continue
            # check ip_subnet is a list
            if not isinstance(ip_subnet, list):
                self.logger.warning(f"Expected 'ip_subnet' to be a list, got {type(ip_subnet)}")
                continue
            #build trie entry
            lookup_value = {'ref': ref}
            #for other fields, get key from value['column_names'] and value from fields (zip joins them into one iterator)
            for field, col_name in zip(fields, value['column_names'][3:]):
                #remove latest_ prefix if present since column names come from ClickHouse query
                if col_name.startswith('latest_'):
                    col_name = col_name[len('latest_'):]
                lookup_value[col_name] = field

            #iterate over list of tuples and assign to trie
            for ip_subnet_tuple in ip_subnet:
                if not ip_subnet_tuple or not isinstance(ip_subnet_tuple, (list, tuple)) or len(ip_subnet_tuple) != 2:
                    self.logger.warning(f"Invalid subnet format: {ip_subnet_tuple}")
                    continue
                address, prefix = ip_subnet_tuple
                if address is None or prefix is None:
                    self.logger.debug(f"Skipping invalid subnet with null values: {ip_subnet_tuple}")
                    continue
                #Might be an IP type object so convert to string
                address = str(address)
                if address.startswith('::ffff:'):
                    # Convert IPv4-mapped IPv6 address to IPv4
                    address = address.split('::ffff:')[1]
                #Clickhouse makes IPv4 addresses look like IPv6 so convert back
                ip_trie["{}/{}".format(address, prefix)] = lookup_value

        #freeze the trie to make it read-only
        ip_trie.freeze()

        #generate file name
        file_table_name = None
        if self.use_simple_table_name:
            file_table_name = value['table'].split(':', 1)[0]
        else:
            file_table_name = value['table'].replace(':', '.')

        return [{
            'name': f"ip_trie_{file_table_name}.pickle",
            'data': ip_trie
        }]

   