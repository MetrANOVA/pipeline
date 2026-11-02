import logging
import os
import socket
from typing import Any, List, Optional

import dns.rdatatype
import dns.resolver
import dns.reversename
from cachetools import TTLCache

from metranova.cachers.base import BaseCacher

logger = logging.getLogger(__name__)


class DNSCacher(BaseCacher):
    """Cacher that performs DNS lookups and stores results in a local TTLCache.

    Cache keys are namespaced as ``"<rdtype>:<query>"`` so forward, reverse,
    and generic lookups never collide.

    Environment variables
    ---------------------
    DNS_CACHER_MAX_SIZE  : int, default 1000  — maximum number of cached entries.
    DNS_CACHER_MAX_TTL   : int, default 3600  — TTL of each cached entry in seconds.
    """

    def __init__(self):
        super().__init__()
        self.logger = logger
        self.cache_max_size = int(os.getenv("DNS_CACHER_MAX_SIZE", "1000"))
        self.cache_max_ttl = int(os.getenv("DNS_CACHER_MAX_TTL", "3600"))
        self.local_cache: TTLCache = TTLCache(
            maxsize=self.cache_max_size, ttl=self.cache_max_ttl
        )
        self.resolver = dns.resolver.Resolver()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cache_key(self, rdtype: str, query: str) -> str:
        return f"{rdtype.upper()}:{query}"

    def _cached(self, rdtype: str, query: str) -> Any:
        """Return the cached value for *rdtype*/*query*, or a sentinel miss."""
        return self.local_cache.get(self._cache_key(rdtype, query), _MISS)

    def _store(self, rdtype: str, query: str, value: Any) -> None:
        self.local_cache[self._cache_key(rdtype, query)] = value

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup_addresses(self, hostname: str) -> List[str]:
        """Return all IPv4 and IPv6 addresses for *hostname*.

        Results are cached. Returns an empty list when the host is not found.
        """
        cached = self._cached("ADDR", hostname)
        if cached is not _MISS:
            return cached

        addresses: List[str] = []
        for rdtype in ("A", "AAAA"):
            try:
                answers = self.resolver.resolve(hostname, rdtype)
                addresses.extend(str(r.address) for r in answers)
            except (
                dns.resolver.NoAnswer,
                dns.resolver.NXDOMAIN,
                dns.resolver.NoNameservers,
                dns.exception.Timeout,
            ):
                pass

        self._store("ADDR", hostname, addresses)
        return addresses

    def lookup_hostname(self, ip: str) -> Optional[str]:
        """Reverse-DNS lookup for *ip* (IPv4 or IPv6 string).

        Returns the first PTR name (trailing dot stripped) or ``None`` if no
        record exists or the lookup fails.
        """
        cached = self._cached("PTR", ip)
        if cached is not _MISS:
            return cached

        result: Optional[str] = None
        try:
            rev_name = dns.reversename.from_address(ip)
            answers = self.resolver.resolve(rev_name, "PTR")
            hostname = str(next(iter(answers))).rstrip(".")
            result = hostname or None
        except (
            dns.resolver.NoAnswer,
            dns.resolver.NXDOMAIN,
            dns.resolver.NoNameservers,
            dns.exception.Timeout,
            dns.exception.SyntaxError,  # invalid IP string passed to from_address
            ValueError,
        ):
            pass

        self._store("PTR", ip, result)
        return result

    def lookup_records(self, name: str, rdtype: str) -> List[str]:
        """Generic DNS lookup for *name* and record type *rdtype* (e.g. ``"MX"``).

        Returns a list of string representations of the answer RRs, or an
        empty list when the record does not exist or the lookup fails.
        """
        rdtype_upper = rdtype.upper()
        cached = self._cached(rdtype_upper, name)
        if cached is not _MISS:
            return cached

        results: List[str] = []
        try:
            answers = self.resolver.resolve(name, rdtype_upper)
            results = [str(r) for r in answers]
        except (
            dns.resolver.NoAnswer,
            dns.resolver.NXDOMAIN,
            dns.resolver.NoNameservers,
            dns.exception.Timeout,
        ):
            pass

        self._store(rdtype_upper, name, results)
        return results

    # ------------------------------------------------------------------
    # BaseCacher interface
    # ------------------------------------------------------------------

    def lookup(self, table: str, key: str) -> Optional[str]:
        """Generic lookup — delegates to :meth:`lookup_records`.

        *table* is treated as the DNS record type (e.g. ``"A"``, ``"MX"``).
        Returns the first result string or ``None``.
        """
        results = self.lookup_records(key, table)
        return results[0] if results else None

    def prime(self) -> None:
        # DNS cache is populated lazily; nothing to prime.
        pass


# Sentinel object used to distinguish a cached ``None`` from a cache miss.
_MISS = object()
