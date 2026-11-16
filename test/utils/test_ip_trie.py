import pickle
import unittest

import pytricia

from metranova.utils.ip_trie import IPTrie


class TestIPTrie(unittest.TestCase):

    def test_mixed_pytricia_matches_across_families(self):
        """Documents the pytricia behaviour IPTrie exists to avoid: one shared
        128-bit trie matches IPv4 prefixes against IPv6 addresses (and the
        reverse) whenever their leading bytes are the same."""
        mixed = pytricia.PyTricia(128)
        mixed['38.0.0.0/8'] = 'v4'
        self.assertEqual(mixed.get('2600::1'), 'v4')

    def test_ipv4_prefix_never_matches_ipv6_address(self):
        trie = IPTrie()
        trie['38.0.0.0/8'] = 'v4'
        trie['32.1.0.0/18'] = 'v4-att'
        self.assertIsNone(trie.get('2600::1'))
        self.assertIsNone(trie.get('2001:208::/32'))
        self.assertNotIn('2600::1', trie)
        self.assertEqual(trie.get('38.1.2.3'), 'v4')

    def test_ipv6_prefix_never_matches_ipv4_address(self):
        trie = IPTrie()
        trie['2600::/8'] = 'v6'
        trie['::/0'] = 'v6-default'
        self.assertIsNone(trie.get('38.1.2.3'))
        self.assertIsNone(trie.get('38.1.0.0/16'))
        self.assertEqual(trie.get('2600::1'), 'v6')
        self.assertEqual(trie.get('2a00::1'), 'v6-default')

    def test_same_leading_bits_do_not_overwrite_each_other(self):
        """38.0.31.0/24 and 2600:1f00::/24 are the same key in a mixed trie."""
        trie = IPTrie()
        trie['38.0.31.0/24'] = 'v4'
        trie['2600:1f00::/24'] = 'v6'
        self.assertEqual(len(trie), 2)
        self.assertEqual(trie.get('38.0.31.9'), 'v4')
        self.assertEqual(trie.get('2600:1f00::9'), 'v6')

    def test_longest_prefix_match_and_exact_match(self):
        trie = IPTrie()
        trie['128.182.0.0/16'] = 'city'
        trie['128.182.64.0/18'] = 'asn'
        # longest-prefix match for addresses and for narrower networks
        self.assertEqual(trie.get('128.182.64.1'), 'asn')
        self.assertEqual(trie.get('128.182.200.1'), 'city')
        self.assertEqual(trie.get('128.182.65.0/24'), 'asn')
        self.assertEqual(trie['128.182.1.0/24'], 'city')
        self.assertIn('128.182.64.0/20', trie)
        # a broader network is not "in" a narrower entry
        self.assertIsNone(trie.get('128.0.0.0/8'))
        self.assertNotIn('128.0.0.0/8', trie)
        # has_key is exact
        self.assertTrue(trie.has_key('128.182.64.0/18'))
        self.assertFalse(trie.has_key('128.182.64.0/20'))
        self.assertFalse(trie.has_key('128.182.64.1'))

    def test_get_default_and_missing_key(self):
        trie = IPTrie()
        trie['10.0.0.0/8'] = 'x'
        self.assertEqual(trie.get('11.0.0.1', 'missing'), 'missing')
        self.assertIsNone(trie.get('2001:db8::1'))
        with self.assertRaises(KeyError):
            _ = trie['11.0.0.1']

    def test_ipv4_mapped_keys_are_treated_as_ipv4(self):
        trie = IPTrie()
        trie['::ffff:192.168.1.0/120'] = 'mapped'   # = 192.168.1.0/24
        self.assertEqual(trie.get('192.168.1.7'), 'mapped')
        self.assertEqual(trie.get('::ffff:192.168.1.7'), 'mapped')
        self.assertIsNone(trie.get('192.168.2.7'))
        self.assertEqual(trie.keys(), ['192.168.1.0/24'])

    def test_len_and_keys_cover_both_families(self):
        trie = IPTrie()
        self.assertEqual(len(trie), 0)
        trie['10.0.0.0/8'] = 1
        trie['2001:db8::/32'] = 2
        self.assertEqual(len(trie), 2)
        self.assertEqual(sorted(trie.keys()), ['10.0.0.0/8', '2001:db8::/32'])

    def test_freeze_and_pickle_round_trip(self):
        trie = IPTrie()
        trie['10.0.0.0/8'] = ('ref-1', 65001)
        trie['2001:db8::/32'] = ('ref-2', 65002)
        trie.freeze()
        with self.assertRaises(Exception):
            trie['11.0.0.0/8'] = 'nope'
        loaded = pickle.loads(pickle.dumps(trie))
        self.assertIsInstance(loaded, IPTrie)
        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded.get('10.9.9.9'), ('ref-1', 65001))
        self.assertEqual(loaded.get('2001:db8::1'), ('ref-2', 65002))
        self.assertIsNone(loaded.get('2600::1'))


if __name__ == '__main__':
    unittest.main()
