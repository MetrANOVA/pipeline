import unittest
from unittest.mock import Mock, patch

from metranova.processors.clickhouse.rootbeer import RootbeerProcessor


def _make_pipeline():
    pipeline = Mock()
    pipeline.process_message = Mock()
    return pipeline


def _make_processor(env=None):
    env = env or {}
    with patch.dict('os.environ', env, clear=False):
        return RootbeerProcessor(_make_pipeline())


class TestRootbeerProcessorInit(unittest.TestCase):
    def test_default_output_filename(self):
        proc = _make_processor()
        self.assertEqual(proc.output_filename, 'rootbeer_route_report.json')

    @patch.dict('os.environ', {'ROOTBEER_OUTPUT_FILENAME': 'custom_report.json'})
    def test_custom_output_filename(self):
        proc = RootbeerProcessor(_make_pipeline())
        self.assertEqual(proc.output_filename, 'custom_report.json')

    def test_state_starts_empty(self):
        proc = _make_processor()
        self.assertEqual(proc._state, {})


class TestRootbeerProcessorMatchMessage(unittest.TestCase):
    def setUp(self):
        self.proc = _make_processor()

    def test_matches_chunk(self):
        self.assertTrue(self.proc.match_message({'rootbeer_chunk': True}))

    def test_matches_flush(self):
        self.assertTrue(self.proc.match_message({'rootbeer_flush': True}))

    def test_rejects_unrelated(self):
        self.assertFalse(self.proc.match_message({'some_other_key': True}))

    def test_rejects_empty(self):
        self.assertFalse(self.proc.match_message({}))


class TestRootbeerProcessorAccumulate(unittest.TestCase):
    def setUp(self):
        self.proc = _make_processor()

    def _chunk(self, device_id, peer_ip, remote_as='65001', accepted=None, rejected=None):
        return {
            'rootbeer_chunk': True,
            'device_id': device_id,
            'peer_ip': peer_ip,
            'remote_as': remote_as,
            'accepted': accepted or [],
            'rejected': rejected or [],
        }

    def test_single_chunk_creates_router_entry(self):
        chunk = self._chunk('router1', '192.0.2.1')
        self.proc.build_message(chunk, {})

        self.assertIn('router1', self.proc._state)
        neighbors = self.proc._state['router1']['re:output']['neighbor-result']
        self.assertEqual(len(neighbors), 1)
        self.assertEqual(neighbors[0]['address'], '192.0.2.1')
        self.assertEqual(neighbors[0]['remote-as'], '65001')

    def test_multiple_peers_same_router(self):
        self.proc.build_message(self._chunk('router1', '192.0.2.1'), {})
        self.proc.build_message(self._chunk('router1', '192.0.2.2'), {})

        neighbors = self.proc._state['router1']['re:output']['neighbor-result']
        self.assertEqual(len(neighbors), 2)
        self.assertEqual({n['address'] for n in neighbors}, {'192.0.2.1', '192.0.2.2'})

    def test_multiple_routers(self):
        self.proc.build_message(self._chunk('router1', '192.0.2.1'), {})
        self.proc.build_message(self._chunk('router2', '192.0.2.3'), {})

        self.assertIn('router1', self.proc._state)
        self.assertIn('router2', self.proc._state)

    def test_accepted_and_rejected_routes_stored(self):
        accepted = [{'prefix': '10.0.0.0/24', 'as-path': ['65001', '65002']}]
        rejected = [{'prefix': '10.1.0.0/24', 'as-path': ['65001']}]
        chunk = self._chunk('router1', '192.0.2.1', accepted=accepted, rejected=rejected)
        self.proc.build_message(chunk, {})

        neighbor = self.proc._state['router1']['re:output']['neighbor-result'][0]
        self.assertEqual(neighbor['accepted'], accepted)
        self.assertEqual(neighbor['rejected'], rejected)

    def test_empty_remote_as_stored_as_empty_string(self):
        chunk = self._chunk('router1', '192.0.2.1', remote_as='')
        self.proc.build_message(chunk, {})

        neighbor = self.proc._state['router1']['re:output']['neighbor-result'][0]
        self.assertEqual(neighbor['remote-as'], '')

    def test_chunk_returns_empty_iterator(self):
        chunk = self._chunk('router1', '192.0.2.1')
        result = list(self.proc.build_message(chunk, {}))
        self.assertEqual(result, [])


class TestRootbeerProcessorFlush(unittest.TestCase):
    def setUp(self):
        self.proc = _make_processor()

    def _load_chunks(self):
        self.proc.build_message({
            'rootbeer_chunk': True,
            'device_id': 'router1',
            'peer_ip': '192.0.2.1',
            'remote_as': '65001',
            'accepted': [{'prefix': '10.0.0.0/8', 'as-path': ['65001']}],
            'rejected': [],
        }, {})
        self.proc.build_message({
            'rootbeer_chunk': True,
            'device_id': 'router1',
            'peer_ip': '192.0.2.2',
            'remote_as': '65002',
            'accepted': [],
            'rejected': [{'prefix': '172.16.0.0/12', 'as-path': ['65002', '65003']}],
        }, {})

    def test_flush_emits_one_document(self):
        self._load_chunks()
        docs = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))
        self.assertEqual(len(docs), 1)

    def test_flush_document_has_correct_top_level_keys(self):
        self._load_chunks()
        doc = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))[0]
        self.assertIn('name', doc)
        self.assertIn('data', doc)
        self.assertIn('timestamp', doc['data'])
        self.assertIn('route-report', doc['data'])

    def test_flush_uses_provided_timestamp(self):
        self._load_chunks()
        doc = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-06-15T12:00:00Z'}, {}))[0]
        self.assertEqual(doc['data']['timestamp'], '2026-06-15T12:00:00Z')

    def test_flush_generates_timestamp_if_missing(self):
        self._load_chunks()
        doc = list(self.proc.build_message({'rootbeer_flush': True}, {}))[0]
        self.assertRegex(doc['data']['timestamp'], r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')

    def test_flush_uses_output_filename(self):
        self._load_chunks()
        doc = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))[0]
        self.assertEqual(doc['name'], 'rootbeer_route_report.json')

    def test_flush_route_report_contains_router(self):
        self._load_chunks()
        doc = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))[0]
        report = doc['data']['route-report']
        self.assertEqual(len(report), 1)  # one router
        neighbors = report[0]['re:output']['neighbor-result']
        self.assertEqual(len(neighbors), 2)

    def test_flush_clears_state(self):
        self._load_chunks()
        self.assertNotEqual(self.proc._state, {})
        list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))
        self.assertEqual(self.proc._state, {})

    def test_flush_on_empty_state_returns_document_with_empty_report(self):
        docs = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]['data']['route-report'], [])

    def test_multiple_routers_in_report(self):
        for router in ('router1', 'router2', 'router3'):
            self.proc.build_message({
                'rootbeer_chunk': True,
                'device_id': router,
                'peer_ip': '192.0.2.1',
                'remote_as': '65001',
                'accepted': [],
                'rejected': [],
            }, {})
        doc = list(self.proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))[0]
        self.assertEqual(len(doc['data']['route-report']), 3)


class TestRootbeerProcessorRoundTrip(unittest.TestCase):
    """Full chunk→flush cycle covering the accepted/rejected route shape."""

    def test_route_shape(self):
        proc = _make_processor()
        proc.build_message({
            'rootbeer_chunk': True,
            'device_id': 'rtr',
            'peer_ip': '2001:db8::1',
            'remote_as': '64496',
            'accepted': [
                {'prefix': '192.0.2.0/24', 'as-path': ['64496', '64497']},
                {'prefix': '198.51.100.0/24', 'as-path': ['64496']},
            ],
            'rejected': [
                {'prefix': '203.0.113.0/24', 'as-path': ['64496', '64500']},
            ],
        }, {})

        doc = list(proc.build_message({'rootbeer_flush': True, 'timestamp': '2026-01-01T00:00:00Z'}, {}))[0]
        neighbor = doc['data']['route-report'][0]['re:output']['neighbor-result'][0]

        self.assertEqual(neighbor['address'], '2001:db8::1')
        self.assertEqual(neighbor['remote-as'], '64496')
        self.assertEqual(len(neighbor['accepted']), 2)
        self.assertEqual(len(neighbor['rejected']), 1)
        self.assertEqual(neighbor['accepted'][0]['prefix'], '192.0.2.0/24')
        self.assertEqual(neighbor['accepted'][0]['as-path'], ['64496', '64497'])
        self.assertEqual(neighbor['rejected'][0]['prefix'], '203.0.113.0/24')
