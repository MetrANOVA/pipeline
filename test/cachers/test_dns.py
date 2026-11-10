#!/usr/bin/env python3

import sys
import os
import unittest
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import dns.resolver
import dns.reversename
import dns.exception

from metranova.cachers.dns import DNSCacher, _MISS


def _make_answer(strings):
    """Build a minimal mock dns.resolver.Answer whose records stringify to *strings*."""
    records = [MagicMock(__str__=lambda self, s=s: s) for s in strings]
    answer = MagicMock()
    answer.__iter__ = MagicMock(return_value=iter(records))
    answer.__next__ = MagicMock(side_effect=iter(records).__next__)
    return answer


def _make_address_answer(addresses):
    """Build an answer where each record has an .address attribute."""
    records = []
    for addr in addresses:
        r = MagicMock()
        r.address = addr
        records.append(r)
    answer = MagicMock()
    answer.__iter__ = MagicMock(return_value=iter(records))
    return answer


class TestDNSCacherInit(unittest.TestCase):
    def test_default_cache_size_and_ttl(self):
        cacher = DNSCacher()
        self.assertEqual(cacher.cache_max_size, 1000)
        self.assertEqual(cacher.cache_max_ttl, 3600)

    def test_env_override_size(self):
        with patch.dict(os.environ, {"DNS_CACHER_MAX_SIZE": "500", "DNS_CACHER_MAX_TTL": "120"}):
            cacher = DNSCacher()
        self.assertEqual(cacher.cache_max_size, 500)
        self.assertEqual(cacher.cache_max_ttl, 120)

    def test_local_cache_is_ttlcache(self):
        from cachetools import TTLCache
        cacher = DNSCacher()
        self.assertIsInstance(cacher.local_cache, TTLCache)

    def test_prime_is_noop(self):
        cacher = DNSCacher()
        cacher.prime()  # should not raise


class TestLookupAddresses(unittest.TestCase):
    def setUp(self):
        self.cacher = DNSCacher()

    def _resolve_side_effect(self, a_addrs, aaaa_addrs):
        def side_effect(name, rdtype):
            if rdtype == "A":
                if not a_addrs:
                    raise dns.resolver.NoAnswer
                return _make_address_answer(a_addrs)
            elif rdtype == "AAAA":
                if not aaaa_addrs:
                    raise dns.resolver.NoAnswer
                return _make_address_answer(aaaa_addrs)
        return side_effect

    def test_returns_ipv4_addresses(self):
        self.cacher.resolver.resolve = MagicMock(
            side_effect=self._resolve_side_effect(["1.2.3.4"], [])
        )
        result = self.cacher.lookup_addresses("example.com")
        self.assertIn("1.2.3.4", result)

    def test_returns_ipv6_addresses(self):
        self.cacher.resolver.resolve = MagicMock(
            side_effect=self._resolve_side_effect([], ["2001:db8::1"])
        )
        result = self.cacher.lookup_addresses("example.com")
        self.assertIn("2001:db8::1", result)

    def test_returns_both_address_families(self):
        self.cacher.resolver.resolve = MagicMock(
            side_effect=self._resolve_side_effect(["1.2.3.4"], ["2001:db8::1"])
        )
        result = self.cacher.lookup_addresses("example.com")
        self.assertIn("1.2.3.4", result)
        self.assertIn("2001:db8::1", result)

    def test_returns_empty_list_on_nxdomain(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NXDOMAIN)
        result = self.cacher.lookup_addresses("nonexistent.invalid")
        self.assertEqual(result, [])

    def test_returns_empty_list_on_timeout(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.exception.Timeout)
        result = self.cacher.lookup_addresses("example.com")
        self.assertEqual(result, [])

    def test_result_is_cached(self):
        self.cacher.resolver.resolve = MagicMock(
            side_effect=self._resolve_side_effect(["1.2.3.4"], [])
        )
        self.cacher.lookup_addresses("example.com")
        self.cacher.lookup_addresses("example.com")
        # resolver.resolve should only have been called twice (once per rdtype on the first call)
        self.assertEqual(self.cacher.resolver.resolve.call_count, 2)

    def test_cache_hit_returns_same_result(self):
        self.cacher.resolver.resolve = MagicMock(
            side_effect=self._resolve_side_effect(["1.2.3.4"], [])
        )
        first = self.cacher.lookup_addresses("example.com")
        second = self.cacher.lookup_addresses("example.com")
        self.assertEqual(first, second)

    def test_empty_list_is_cached(self):
        """A negative result (empty list) should itself be cached."""
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NXDOMAIN)
        self.cacher.lookup_addresses("nonexistent.invalid")
        self.cacher.lookup_addresses("nonexistent.invalid")
        self.assertEqual(self.cacher.resolver.resolve.call_count, 2)

    def test_multiple_ipv4(self):
        self.cacher.resolver.resolve = MagicMock(
            side_effect=self._resolve_side_effect(["1.1.1.1", "1.0.0.1"], [])
        )
        result = self.cacher.lookup_addresses("one.one.one.one")
        self.assertEqual(sorted(result), ["1.0.0.1", "1.1.1.1"])


class TestLookupHostname(unittest.TestCase):
    def setUp(self):
        self.cacher = DNSCacher()

    def test_returns_hostname_for_ipv4(self):
        answer = _make_answer(["host.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        result = self.cacher.lookup_hostname("1.2.3.4")
        self.assertEqual(result, "host.example.com")

    def test_trailing_dot_stripped(self):
        answer = _make_answer(["ptr.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        result = self.cacher.lookup_hostname("1.2.3.4")
        self.assertFalse(result.endswith("."))

    def test_returns_hostname_for_ipv6(self):
        answer = _make_answer(["ipv6host.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        result = self.cacher.lookup_hostname("2001:db8::1")
        self.assertEqual(result, "ipv6host.example.com")

    def test_returns_none_on_nxdomain(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NXDOMAIN)
        result = self.cacher.lookup_hostname("192.0.2.1")
        self.assertIsNone(result)

    def test_returns_none_on_no_answer(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NoAnswer)
        result = self.cacher.lookup_hostname("192.0.2.1")
        self.assertIsNone(result)

    def test_returns_none_on_timeout(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.exception.Timeout)
        result = self.cacher.lookup_hostname("192.0.2.1")
        self.assertIsNone(result)

    def test_returns_none_on_invalid_ip(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=ValueError)
        result = self.cacher.lookup_hostname("not-an-ip")
        self.assertIsNone(result)

    def test_result_is_cached(self):
        answer = _make_answer(["host.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        self.cacher.lookup_hostname("1.2.3.4")
        self.cacher.lookup_hostname("1.2.3.4")
        self.assertEqual(self.cacher.resolver.resolve.call_count, 1)

    def test_none_is_cached(self):
        """A None (no PTR) result should itself be cached to avoid repeat lookups."""
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NXDOMAIN)
        self.cacher.lookup_hostname("192.0.2.1")
        self.cacher.lookup_hostname("192.0.2.1")
        self.assertEqual(self.cacher.resolver.resolve.call_count, 1)

    def test_different_ips_cached_separately(self):
        def side_effect(name, rdtype):
            if "4.3.2.1" in str(name):
                return _make_answer(["host-a.example.com."])
            return _make_answer(["host-b.example.com."])
        self.cacher.resolver.resolve = MagicMock(side_effect=side_effect)
        self.assertEqual(self.cacher.lookup_hostname("1.2.3.4"), "host-a.example.com")
        self.assertEqual(self.cacher.lookup_hostname("4.3.2.1"), "host-b.example.com")


class TestLookupRecords(unittest.TestCase):
    def setUp(self):
        self.cacher = DNSCacher()

    def test_mx_records_returned(self):
        answer = _make_answer(["10 mail.example.com.", "20 mail2.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        result = self.cacher.lookup_records("example.com", "MX")
        self.assertEqual(len(result), 2)

    def test_txt_records_returned(self):
        answer = _make_answer(['"v=spf1 include:example.com ~all"'])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        result = self.cacher.lookup_records("example.com", "TXT")
        self.assertIn('"v=spf1 include:example.com ~all"', result)

    def test_rdtype_is_case_insensitive(self):
        answer = _make_answer(["10 mail.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        self.cacher.lookup_records("example.com", "mx")
        self.cacher.resolver.resolve.assert_called_once_with("example.com", "MX")

    def test_returns_empty_list_on_no_answer(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NoAnswer)
        result = self.cacher.lookup_records("example.com", "MX")
        self.assertEqual(result, [])

    def test_returns_empty_list_on_nxdomain(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NXDOMAIN)
        result = self.cacher.lookup_records("nonexistent.invalid", "A")
        self.assertEqual(result, [])

    def test_result_is_cached(self):
        answer = _make_answer(["10 mail.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        self.cacher.lookup_records("example.com", "MX")
        self.cacher.lookup_records("example.com", "MX")
        self.assertEqual(self.cacher.resolver.resolve.call_count, 1)

    def test_different_rdtypes_cached_independently(self):
        def side_effect(name, rdtype):
            if rdtype == "A":
                return _make_address_answer(["1.2.3.4"])
            return _make_answer(["10 mail.example.com."])
        self.cacher.resolver.resolve = MagicMock(side_effect=side_effect)
        self.cacher.lookup_records("example.com", "A")
        self.cacher.lookup_records("example.com", "MX")
        # Both should have triggered a real lookup, not served each other's cache
        self.assertEqual(self.cacher.resolver.resolve.call_count, 2)


class TestLookupGenericInterface(unittest.TestCase):
    """Tests for the BaseCacher-compatible lookup() method."""

    def setUp(self):
        self.cacher = DNSCacher()

    def test_lookup_returns_first_result(self):
        answer = _make_answer(["ns1.example.com.", "ns2.example.com."])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        result = self.cacher.lookup("NS", "example.com")
        self.assertEqual(result, "ns1.example.com.")

    def test_lookup_returns_none_on_nxdomain(self):
        self.cacher.resolver.resolve = MagicMock(side_effect=dns.resolver.NXDOMAIN)
        result = self.cacher.lookup("A", "nonexistent.invalid")
        self.assertIsNone(result)

    def test_lookup_uses_cache(self):
        answer = _make_answer(["1.2.3.4"])
        self.cacher.resolver.resolve = MagicMock(return_value=answer)
        self.cacher.lookup("A", "example.com")
        self.cacher.lookup("A", "example.com")
        self.assertEqual(self.cacher.resolver.resolve.call_count, 1)


class TestCacheIsolation(unittest.TestCase):
    """Verify that different lookup types never share cache entries."""

    def setUp(self):
        self.cacher = DNSCacher()

    def test_address_and_record_caches_are_independent(self):
        addr_answer = _make_address_answer(["1.2.3.4"])
        rec_answer = _make_answer(["10 mail.example.com."])

        call_counts = {"A_addr": 0, "A_rec": 0}

        def side_effect(name, rdtype):
            if rdtype == "A":
                return addr_answer
            raise dns.resolver.NoAnswer

        self.cacher.resolver.resolve = MagicMock(side_effect=side_effect)

        self.cacher.lookup_addresses("example.com")
        self.cacher.lookup_records("example.com", "A")
        # lookup_addresses and lookup_records use different cache namespaces
        # so lookup_records must perform its own DNS call
        self.assertEqual(self.cacher.resolver.resolve.call_count, 3)  # A+AAAA for addresses, A for records


if __name__ == "__main__":
    unittest.main(verbosity=2)
