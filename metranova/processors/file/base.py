import logging
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
    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        #initialize ip trie
        ip_trie = pytricia.PyTricia(128)

        # Got through rows and add to trie
        for row in value.get('rows', []):
            id, ref, ip_subnet = row
            if id is None or ref is None or ip_subnet is None:
                self.logger.debug(f"Skipping row with null values: id={id}, ref={ref}, ip_subnet={ip_subnet}")
                continue
            # check ip_subnet is a list
            if not isinstance(ip_subnet, list):
                self.logger.warning(f"Expected 'ip_subnet' to be a list, got {type(ip_subnet)}")
                continue
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
                ip_trie["{}/{}".format(address, prefix)] = ref

        #freeze the trie to make it read-only
        ip_trie.freeze()

        return [{
            'name': f"ip_trie_{value['table']}.pickle",
            'data': ip_trie
        }]

   