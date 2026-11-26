import logging
import os
from typing import Any, Dict, Iterator
from metranova.processors.base import BaseProcessor
from metranova.utils.ip_trie import IPTrie

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

        #initialize ip trie. IPTrie rather than one pytricia.PyTricia(128) for both
        #families: in a shared trie an IPv4 address matches IPv6 prefixes that start
        #with the same bytes and vice versa (38.0.31.24 matches 2600:1f18::/32)
        ip_trie = IPTrie()
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
            #build trie entry as tuple (ref, *fields)
            lookup_value = (ref, *fields)

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
                    # An ip_subnet column that backs an IP_TRIE() dictionary stores an
                    # IPv4 prefix length offset by 96, because the dictionary compares
                    # the IPv4-mapped (::ffff:a.b.c.d) form of the address. Undo that
                    # along with the address: pytricia does not reject an IPv4 prefix
                    # longer than 32, it stores it as a /32, which would turn every IPv4
                    # block into a single-host entry that matches nothing else inside it.
                    if isinstance(prefix, int) and prefix >= 96:
                        prefix -= 96
                    elif isinstance(prefix, int) and prefix > 32:
                        self.logger.warning(f"Invalid prefix length for IPv4 subnet: {ip_subnet_tuple}")
                        continue
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

   