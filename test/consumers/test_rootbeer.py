import unittest
from unittest.mock import Mock, MagicMock, patch, call


def _make_pipeline():
    pipeline = Mock()
    pipeline.process_message = Mock()
    return pipeline


def _make_consumer(pipeline=None, env=None):
    env = env or {}
    pipeline = pipeline or _make_pipeline()
    with patch('metranova.consumers.rootbeer.ClickHouseConnector'):
        with patch.dict('os.environ', env, clear=False):
            from metranova.consumers.rootbeer import RootbeerClickHouseConsumer
            return RootbeerClickHouseConsumer(pipeline), pipeline


class TestRootbeerClickHouseConsumerInit(unittest.TestCase):
    def test_default_env_values(self):
        consumer, _ = _make_consumer()
        self.assertEqual(consumer.source_table, 'data_bmp_route_latest')
        self.assertEqual(consumer.ip_metadata_table, 'meta_ip')
        self.assertEqual(consumer.update_interval, -1)
        self.assertEqual(consumer.block_size, 100000)

    def test_custom_env_values(self):
        consumer, _ = _make_consumer(env={
            'ROOTBEER_SOURCE_TABLE': 'custom_table',
            'ROOTBEER_IP_METADATA_TABLE': 'custom_meta',
            'ROOTBEER_UPDATE_INTERVAL': '300',
            'ROOTBEER_QUERY_BLOCK_SIZE': '50000',
        })
        self.assertEqual(consumer.source_table, 'custom_table')
        self.assertEqual(consumer.ip_metadata_table, 'custom_meta')
        self.assertEqual(consumer.update_interval, 300)
        self.assertEqual(consumer.block_size, 50000)

    def test_auto_prime_cachers_disabled(self):
        consumer, _ = _make_consumer()
        self.assertFalse(consumer.auto_prime_cachers)


class TestRootbeerBuildQuery(unittest.TestCase):
    def setUp(self):
        self.consumer, _ = _make_consumer()

    def test_accepted_query_uses_is_post_true(self):
        q = self.consumer._build_query(is_post=True)
        self.assertIn('is_post = true', q)
        self.assertNotIn('is_post = false', q)

    def test_pre_policy_query_uses_is_post_false(self):
        q = self.consumer._build_query(is_post=False)
        self.assertIn('is_post = false', q)
        self.assertNotIn('is_post = true', q)

    def test_query_references_source_table(self):
        q = self.consumer._build_query(is_post=True)
        self.assertIn('data_bmp_route_latest', q)

    def test_query_references_meta_ip_table(self):
        q = self.consumer._build_query(is_post=True)
        self.assertIn('meta_ip', q)

    def test_query_joins_on_peer_ip_ref(self):
        q = self.consumer._build_query(is_post=True)
        self.assertIn('peer_ip_ref = m.ref', q)

    def test_query_includes_as_id_select(self):
        q = self.consumer._build_query(is_post=True)
        self.assertIn('m.as_id', q)

    def test_query_orders_by_device_peer(self):
        q = self.consumer._build_query(is_post=True)
        self.assertIn('ORDER BY device_id, peer_ip', q)

    def test_custom_table_names_in_query(self):
        consumer, _ = _make_consumer(env={
            'ROOTBEER_SOURCE_TABLE': 'my_bmp_table',
            'ROOTBEER_IP_METADATA_TABLE': 'my_meta',
        })
        q = consumer._build_query(is_post=True)
        self.assertIn('my_bmp_table', q)
        self.assertIn('my_meta', q)


class TestRootbeerConsumeMessages(unittest.TestCase):
    """Tests for consume_messages using mocked ClickHouse streaming."""

    def _make_stream(self, rows):
        """Return a context manager mock that yields rows as a single block."""
        block = rows
        stream_cm = MagicMock()
        stream_cm.__enter__ = Mock(return_value=iter([block]))
        stream_cm.__exit__ = Mock(return_value=False)
        return stream_cm

    def _run(self, accepted_rows, pre_rows, env=None):
        pipeline = _make_pipeline()
        consumer, _ = _make_consumer(pipeline=pipeline, env=env or {})

        accepted_stream = self._make_stream(accepted_rows)
        pre_stream = self._make_stream(pre_rows)

        consumer.datasource.client.query_row_block_stream = Mock(
            side_effect=[accepted_stream, pre_stream]
        )
        consumer.consume_messages()
        return pipeline.process_message.call_args_list

    def test_no_data_emits_only_flush(self):
        calls = self._run([], [])
        # Only the flush signal
        self.assertEqual(len(calls), 1)
        flush_msg = calls[0][0][0]
        self.assertTrue(flush_msg.get('rootbeer_flush'))

    def test_accepted_row_emits_chunk_with_accepted_route(self):
        row = ('router1', '192.0.2.1', '65001', '10.0.0.0', 24, 0, [65001, 65002])
        calls = self._run([row], [])

        chunk_calls = [c for c in calls if c[0][0].get('rootbeer_chunk')]
        self.assertEqual(len(chunk_calls), 1)
        msg = chunk_calls[0][0][0]
        self.assertEqual(msg['device_id'], 'router1')
        self.assertEqual(msg['peer_ip'], '192.0.2.1')
        self.assertEqual(msg['remote_as'], '65001')
        self.assertEqual(len(msg['accepted']), 1)
        self.assertEqual(msg['accepted'][0]['prefix'], '10.0.0.0/24')
        self.assertEqual(msg['accepted'][0]['as-path'], ['65001', '65002'])
        self.assertEqual(msg['rejected'], [])

    def test_pre_policy_only_row_emits_rejected(self):
        # A prefix not in accepted becomes rejected
        pre_row = ('router1', '192.0.2.1', '65001', '172.16.0.0', 12, 0, [65001])
        calls = self._run([], [pre_row])

        chunk_calls = [c for c in calls if c[0][0].get('rootbeer_chunk')]
        self.assertEqual(len(chunk_calls), 1)
        msg = chunk_calls[0][0][0]
        self.assertEqual(len(msg['rejected']), 1)
        self.assertEqual(msg['rejected'][0]['prefix'], '172.16.0.0/12')
        self.assertEqual(msg['accepted'], [])

    def test_accepted_prefix_not_in_rejected(self):
        # Prefix accepted → should NOT appear in rejected even if present in pre-policy
        row = ('router1', '192.0.2.1', '65001', '10.0.0.0', 24, 0, [65001])
        calls = self._run([row], [row])

        chunk_calls = [c for c in calls if c[0][0].get('rootbeer_chunk')]
        # The accepted chunk from pass 1
        accepted_chunks = [c[0][0] for c in chunk_calls if c[0][0].get('accepted')]
        rejected_chunks = [c[0][0] for c in chunk_calls if c[0][0].get('rejected')]

        # Accepted list should have the route; rejected should be empty for that prefix
        self.assertTrue(any(r['prefix'] == '10.0.0.0/24' for c in accepted_chunks for r in c['accepted']))
        for c in rejected_chunks:
            self.assertFalse(any(r['prefix'] == '10.0.0.0/24' for r in c['rejected']))

    def test_multiple_peers_emit_separate_chunks(self):
        rows = [
            ('router1', '192.0.2.1', '65001', '10.0.0.0', 8, 0, [65001]),
            ('router1', '192.0.2.2', '65002', '172.16.0.0', 12, 0, [65002]),
        ]
        calls = self._run(rows, [])
        chunk_calls = [c[0][0] for c in calls if c[0][0].get('rootbeer_chunk')]
        peer_ips = {c['peer_ip'] for c in chunk_calls}
        self.assertEqual(peer_ips, {'192.0.2.1', '192.0.2.2'})

    def test_flush_signal_sent_after_all_chunks(self):
        row = ('router1', '192.0.2.1', '65001', '10.0.0.0', 24, 0, [65001])
        calls = self._run([row], [])
        last_msg = calls[-1][0][0]
        self.assertTrue(last_msg.get('rootbeer_flush'))

    def test_flush_signal_has_timestamp(self):
        calls = self._run([], [])
        flush_msg = calls[-1][0][0]
        self.assertIn('timestamp', flush_msg)
        self.assertRegex(flush_msg['timestamp'], r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')

    def test_stream_error_in_pass1_aborts(self):
        pipeline = _make_pipeline()
        consumer, _ = _make_consumer(pipeline=pipeline)
        consumer.datasource.client.query_row_block_stream = Mock(
            side_effect=Exception("CH connection failed")
        )
        consumer.consume_messages()
        # Nothing should be emitted — no flush, no chunks
        pipeline.process_message.assert_not_called()

    def test_empty_as_path_handled(self):
        row = ('router1', '192.0.2.1', '65001', '10.0.0.0', 24, 0, [])
        calls = self._run([row], [])
        chunk_calls = [c[0][0] for c in calls if c[0][0].get('rootbeer_chunk')]
        self.assertEqual(chunk_calls[0]['accepted'][0]['as-path'], [])

    def test_null_remote_as_becomes_empty_string(self):
        row = ('router1', '192.0.2.1', None, '10.0.0.0', 24, 0, [65001])
        calls = self._run([row], [])
        chunk_calls = [c[0][0] for c in calls if c[0][0].get('rootbeer_chunk')]
        self.assertEqual(chunk_calls[0]['remote_as'], '')
