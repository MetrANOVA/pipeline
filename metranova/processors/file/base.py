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
            id, ref, addresses = row
            if id is None or ref is None or addresses is None:
                self.logger.debug(f"Skipping row with null values: id={id}, ref={ref}, addresses={addresses}")
                continue
            # check addresses is a list
            if not isinstance(addresses, list):
                self.logger.warning(f"Expected 'addresses' to be a list, got {type(addresses)}")
                continue
            for addr in addresses:
                ip_trie[addr] = ref

        #freeze the trie to make it read-only
        ip_trie.freeze()

        return [{
            'name': f"ip_trie_{value['table']}.pickle",
            'data': ip_trie
        }]

   