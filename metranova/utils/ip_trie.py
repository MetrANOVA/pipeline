"""Longest-prefix-match IP trie that keeps IPv4 and IPv6 apart."""
import pytricia

IPV4_MAPPED_PREFIX = '::ffff:'
IPV4_MAPPED_OFFSET = 96


class IPTrie:
    """Drop-in replacement for a single mixed-family ``pytricia.PyTricia(128)``.

    pytricia compares the raw leading bits of a key and has no notion of address
    family, so in one shared 128-bit trie an IPv4 prefix and an IPv6 prefix that
    start with the same bytes are the same branch: 38.0.0.0/8 (0x26) matches
    2600::1, 32.1.0.0/18 matches 2001:208::, and an IPv4 /24 and an IPv6 /24 with
    the same leading three bytes are one key that silently overwrite each other.
    Here every key is routed to a 32-bit trie or a 128-bit trie by its family, so
    a lookup can only ever match an entry of its own family.

    IPv4-mapped IPv6 keys in dotted form (``::ffff:a.b.c.d``, optionally with a
    prefix length of 96 or more) are treated as the IPv4 address they represent.

    Lookups (``get``, ``[]``, ``in``) are longest-prefix matches, exactly as in
    pytricia; ``has_key`` is an exact match.
    """

    def __init__(self):
        self._v4 = pytricia.PyTricia(32)
        self._v6 = pytricia.PyTricia(128)

    def _route(self, key):
        """Return (trie, key) for the family the key belongs to."""
        key = str(key)
        if ':' not in key:
            return self._v4, key
        if key.lower().startswith(IPV4_MAPPED_PREFIX) and '.' in key:
            address, has_length, length = key[len(IPV4_MAPPED_PREFIX):].partition('/')
            if not has_length:
                return self._v4, address
            if length.isdigit() and int(length) >= IPV4_MAPPED_OFFSET:
                return self._v4, f"{address}/{int(length) - IPV4_MAPPED_OFFSET}"
        return self._v6, key

    def __setitem__(self, key, value):
        trie, key = self._route(key)
        trie[key] = value

    def __getitem__(self, key):
        trie, key = self._route(key)
        return trie[key]

    def __contains__(self, key):
        trie, key = self._route(key)
        return key in trie

    def __len__(self):
        return len(self._v4) + len(self._v6)

    def get(self, key, default=None):
        trie, key = self._route(key)
        return trie.get(key, default)

    def has_key(self, key):
        trie, key = self._route(key)
        return trie.has_key(key)

    def keys(self):
        return list(self._v4.keys()) + list(self._v6.keys())

    def freeze(self):
        """Make the trie read-only (required by pytricia before pickling)."""
        self._v4.freeze()
        self._v6.freeze()
