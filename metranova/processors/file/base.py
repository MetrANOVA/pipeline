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
            for addr in ip_subnet:
                ip_trie["{}/{}".format(addr[0], addr[1])] = ref

        #freeze the trie to make it read-only
        ip_trie.freeze()

        return [{
            'name': f"ip_trie_{value['table']}.pickle",
            'data': ip_trie
        }]

   