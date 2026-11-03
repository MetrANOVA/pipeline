"""
Processor that converts rows streamed from data_bmp_route_latest into the
rootbeer JSON route-report schema (docs/rootbeer_schema.json).

Query strategy
--------------
data_bmp_route_latest is an AggregatingMergeTree so every select must merge
the aggregate states with *Merge combiners.  We issue two queries:

  1. "accepted" – rows where is_post = true AND action = 'update'
  2. "rejected"  – rows where is_post = false AND action = 'update'
                   whose (device_id, peer_ip, prefix) key is NOT present
                   in the accepted set.

To keep memory bounded for hundreds-of-millions-of-row tables the processor
receives pre-aggregated per-(device_id, peer_ip) dicts that are built by the
consumer in a streaming fashion (see metranova/consumers/clickhouse.py
RootbeerClickHouseConsumer).  Each call to build_message() receives one such
dict and emits exactly one route-report document.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List

from metranova.processors.base import BaseProcessor

logger = logging.getLogger(__name__)

# Sentinel used by the consumer to signal that all data has been collected and
# the processor should emit the final document.
ROOTBEER_FLUSH_KEY = "__rootbeer_flush__"


class RootbeerProcessor(BaseProcessor):
    """
    Expects messages in one of two forms:

    Incremental chunk (emitted per device/peer block by the consumer):
        {
            "rootbeer_chunk": True,
            "device_id":  str,
            "peer_ip":    str,       # peer IP string
            "remote_as":  str,       # ASN as string (may be empty)
            "accepted":   [ {"prefix": "x/y", "as-path": ["asn", ...]}, ... ],
            "rejected":   [ {"prefix": "x/y", "as-path": ["asn", ...]}, ... ],
        }

    Flush signal (emitted once by the consumer after all chunks are done):
        { "rootbeer_flush": True, "timestamp": "<ISO-8601>" }
    """

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.output_filename = os.getenv('ROOTBEER_OUTPUT_FILENAME', 'rootbeer_route_report.json')
        # match_fields left empty – we match manually in match_message
        self._state: Dict[str, Dict[str, Any]] = {}   # device_id -> router output

    # ------------------------------------------------------------------
    # BaseProcessor interface
    # ------------------------------------------------------------------

    def match_message(self, value: dict) -> bool:
        return bool(value.get("rootbeer_chunk") or value.get("rootbeer_flush"))

    def build_message(self, value: dict, msg_metadata: dict) -> Iterator[Dict[str, Any]]:
        if value.get("rootbeer_chunk"):
            self._accumulate(value)
            return []

        if value.get("rootbeer_flush"):
            doc = self._build_document(value.get("timestamp", ""))
            self._state.clear()
            return [doc]

        return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _accumulate(self, chunk: dict) -> None:
        device_id = chunk["device_id"]
        peer_ip = chunk["peer_ip"]

        if device_id not in self._state:
            self._state[device_id] = {
                "re:output": {
                    "neighbor-result": []
                }
            }

        neighbor = {
            "address": peer_ip,
            "remote-as": chunk.get("remote_as", ""),
            "accepted": chunk.get("accepted", []),
            "rejected": chunk.get("rejected", []),
        }
        self._state[device_id]["re:output"]["neighbor-result"].append(neighbor)

    def _build_document(self, timestamp: str) -> dict:
        if not timestamp:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        route_report = list(self._state.values())
        self.logger.info(
            "Rootbeer: building document with %d routers, %d neighbor sessions",
            len(route_report),
            sum(
                len(r["re:output"]["neighbor-result"]) for r in route_report
            ),
        )
        return {
            "name": self.output_filename,
            "data": {
                "timestamp": timestamp,
                "route-report": route_report,
            },
        }
