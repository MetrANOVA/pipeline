import unittest
from unittest.mock import Mock, patch

from metranova.processors.clickhouse.bmp import (
    BaseBMPPeerDownProcessor,
    BaseBMPPeerUpProcessor,
    BaseBMPRouteProcessor,
    BaseBMPStatProcessor,
    MaterializedViewBMPRouteLatest,
    ViewBMPRouteLatestAdjRIBInPost,
    ViewBMPRouteLatestAdjRIBInPre,
)


class TestMaterializedViewBMPRouteLatest(unittest.TestCase):
    def test_base_route_table_uses_export_time_ordering_ttl_and_partition(self):
        processor = BaseBMPRouteProcessor(Mock())

        result = processor.create_table_command()

        self.assertLess(result.index('`export_time`'), result.index('`observation_time`'))
        self.assertIn(
            'PRIMARY KEY (`device_id`,`action`,`peer_ip`,`ip_prefix_range_start`,`ip_prefix_range_end`,`add_path_id`,`export_time`)',
            result,
        )
        self.assertIn(
            'ORDER BY (`device_id`,`action`,`peer_ip`,`ip_prefix_range_start`,`ip_prefix_range_end`,`add_path_id`,`export_time`)',
            result,
        )
        self.assertIn('PARTITION BY toYYYYMMDD(export_time)', result)
        self.assertIn('TTL export_time + INTERVAL 1 DAY', result)

    def test_create_table_command(self):
        materialized_view = MaterializedViewBMPRouteLatest(source_table_name='data_bmp_route')

        result = materialized_view.create_table_command()

        self.assertIn('CREATE TABLE IF NOT EXISTS data_bmp_route_latest', result)
        self.assertIn('ENGINE = AggregatingMergeTree()', result)
        self.assertIn('`device_id` LowCardinality(String)', result)
        self.assertIn('`peer_ip` IPv6', result)
        self.assertIn('`peer_ip_ref` Nullable(String)', result)
        self.assertIn('`ip_prefix_ref` Nullable(String)', result)
        self.assertIn('`is_post` Bool', result)
        self.assertIn(
            "`action_state` AggregateFunction(argMax, LowCardinality(String), DateTime64(3, 'UTC'))",
            result,
        )
        self.assertIn(
            "`export_time_state` AggregateFunction(max, DateTime64(3, 'UTC'))",
            result,
        )
        self.assertIn(
            "`observation_time_state` AggregateFunction(argMax, DateTime64(3, 'UTC'), DateTime64(3, 'UTC'))",
            result,
        )
        self.assertIn(
            'PRIMARY KEY (`device_id`,`peer_ip`,`peer_ip_ref`,`ip_prefix_range_start`,`ip_prefix_range_end`,`ip_prefix_ref`,`add_path_id`,`is_post`)',
            result,
        )
        self.assertIn(
            'ORDER BY (`device_id`,`peer_ip`,`peer_ip_ref`,`ip_prefix_range_start`,`ip_prefix_range_end`,`ip_prefix_ref`,`add_path_id`,`is_post`)',
            result,
        )

    def test_create_table_command_allows_nullable_key(self):
        materialized_view = MaterializedViewBMPRouteLatest(source_table_name='data_bmp_route')

        result = materialized_view.create_table_command()

        self.assertIn('allow_nullable_key = 1', result)

    def test_create_mv_command(self):
        materialized_view = MaterializedViewBMPRouteLatest(source_table_name='data_bmp_route')

        result = materialized_view.create_mv_command()

        self.assertIn('CREATE MATERIALIZED VIEW IF NOT EXISTS data_bmp_route_latest_mv', result)
        self.assertIn('TO data_bmp_route_latest', result)
        self.assertIn('argMaxState(action, export_time) AS action_state', result)
        self.assertIn('maxState(export_time) AS export_time_state', result)
        self.assertIn('argMaxState(observation_time, export_time) AS observation_time_state', result)
        self.assertIn('argMaxState(mpls_label, export_time) AS mpls_label_state', result)
        self.assertIn('FROM data_bmp_route', result)
        self.assertIn('WHERE is_in = true', result)
        self.assertIn('GROUP BY', result)
        self.assertIn('peer_ip_ref', result)
        self.assertIn('ip_prefix_ref', result)
        self.assertIn('add_path_id,\n                is_post', result)

    @patch.dict('os.environ', {
        'CLICKHOUSE_BMP_ROUTE_TABLE': 'custom_bmp_route',
        'CLICKHOUSE_BMP_ROUTE_VIEWS': 'adj_rib_in_pre,adj_rib_in_post',
    }, clear=True)
    def test_processor_registers_latest_materialized_view(self):
        processor = BaseBMPRouteProcessor(Mock())

        self.assertEqual(len(processor.get_materialized_views()), 1)
        materialized_view = processor.get_materialized_views()[0]
        self.assertIsInstance(materialized_view, MaterializedViewBMPRouteLatest)
        self.assertEqual(materialized_view.source_table_name, 'custom_bmp_route')
        self.assertEqual(materialized_view.table, 'data_bmp_route_latest')


class TestBMPRouteLatestViews(unittest.TestCase):
    def test_pre_policy_view_command(self):
        view = ViewBMPRouteLatestAdjRIBInPre()

        result = view.create_view_command()

        self.assertIn('CREATE VIEW IF NOT EXISTS data_bmp_route_latest_adj_rib_in_pre_view', result)
        self.assertIn('FROM data_bmp_route_latest', result)
        self.assertIn('WHERE is_post = false', result)
        self.assertIn("HAVING argMaxMerge(action_state) = 'update'", result)
        self.assertLess(result.index('AS export_time'), result.index('AS observation_time'))
        self.assertIn('maxMerge(export_time_state) AS export_time', result)
        self.assertIn('argMaxMerge(observation_time_state) AS observation_time', result)
        self.assertIn('argMaxMerge(bgp_nexthop_state) AS bgp_nexthop', result)
        self.assertIn('peer_ip_ref', result)
        self.assertIn('ip_prefix_ref', result)

    def test_post_policy_view_command(self):
        view = ViewBMPRouteLatestAdjRIBInPost()

        result = view.create_view_command()

        self.assertIn('CREATE VIEW IF NOT EXISTS data_bmp_route_latest_adj_rib_in_post_view', result)
        self.assertIn('WHERE is_post = true', result)
        self.assertLess(result.index('AS export_time'), result.index('AS observation_time'))
        self.assertIn('GROUP BY', result)
        self.assertIn('peer_ip_ref', result)
        self.assertIn('ip_prefix_ref', result)

    @patch.dict('os.environ', {'CLICKHOUSE_BMP_ROUTE_VIEWS': 'adj_rib_in_pre'})
    def test_processor_loads_configured_adj_rib_views(self):
        processor = BaseBMPRouteProcessor(Mock())

        self.assertEqual(
            [view.view_name for view in processor.get_views()],
            ['data_bmp_route_latest_adj_rib_in_pre_view'],
        )

    @patch.dict('os.environ', {
        'CLICKHOUSE_BMP_ROUTE_MATERIALIZED_VIEWS': '',
        'CLICKHOUSE_BMP_ROUTE_VIEWS': '',
    })
    def test_processor_can_disable_latest_views(self):
        processor = BaseBMPRouteProcessor(Mock())

        self.assertEqual(processor.get_materialized_views(), [])
        self.assertEqual(processor.get_views(), [])


class TestBMPStatProcessor(unittest.TestCase):
    """Tests for BaseBMPStatProcessor (BMP Statistics Report, message type 1)."""

    def _make(self, env=None):
        with patch.dict('os.environ', env or {}, clear=False):
            return BaseBMPStatProcessor(Mock())

    # ------------------------------------------------------------------
    # Table name / env override
    # ------------------------------------------------------------------

    def test_default_table_name(self):
        self.assertEqual(self._make().table, 'data_bmp_stat')

    def test_table_name_env_override(self):
        p = self._make({'CLICKHOUSE_BMP_STAT_TABLE': 'custom_bmp_stat'})
        self.assertEqual(p.table, 'custom_bmp_stat')

    # ------------------------------------------------------------------
    # DDL content
    # ------------------------------------------------------------------

    def test_create_table_command_starts_with_create_table(self):
        ddl = self._make().create_table_command()
        self.assertIn('CREATE TABLE IF NOT EXISTS data_bmp_stat', ddl)

    def test_observation_time_is_first_column(self):
        ddl = self._make().create_table_command()
        self.assertLess(ddl.index('`observation_time`'), ddl.index('`insert_time`'))

    def test_export_time_not_present(self):
        ddl = self._make().create_table_command()
        self.assertNotIn('export_time', ddl)

    def test_expected_columns_present(self):
        ddl = self._make().create_table_command()
        for col in [
            'observation_time', 'insert_time',
            'collector_id', 'policy_originator', 'policy_level', 'policy_scope',
            'device_id', 'device_ref',
            'peer_ip', 'peer_ip_ref', 'peer_as_id', 'peer_as_ref', 'peer_type',
            'bgp_id', 'rd',
            'is_loc', 'is_filtered',
            'metric_type', 'metric_type_str', 'metric_value',
        ]:
            self.assertIn(f'`{col}`', ddl, msg=f'Missing column: {col}')

    def test_metric_value_is_uint64(self):
        ddl = self._make().create_table_command()
        self.assertIn('`metric_value` UInt64', ddl)

    def test_metric_type_is_uint16(self):
        ddl = self._make().create_table_command()
        self.assertIn('`metric_type` UInt16', ddl)

    def test_peer_as_id_is_nullable_uint32(self):
        ddl = self._make().create_table_command()
        self.assertIn('`peer_as_id` Nullable(UInt32)', ddl)

    def test_peer_as_ref_is_nullable_string(self):
        ddl = self._make().create_table_command()
        self.assertIn('`peer_as_ref` Nullable(String)', ddl)

    def test_rd_origin_not_present(self):
        ddl = self._make().create_table_command()
        self.assertNotIn('rd_origin', ddl)

    def test_peer_ip_is_ipv6(self):
        ddl = self._make().create_table_command()
        self.assertIn('`peer_ip` IPv6', ddl)

    def test_peer_ip_ref_is_nullable_string(self):
        ddl = self._make().create_table_command()
        self.assertIn('`peer_ip_ref` Nullable(String)', ddl)

    def test_bgp_id_is_nullable_ipv6(self):
        ddl = self._make().create_table_command()
        self.assertIn('`bgp_id` Nullable(IPv6)', ddl)

    # ------------------------------------------------------------------
    # Primary key / ordering
    # ------------------------------------------------------------------

    def test_primary_key(self):
        ddl = self._make().create_table_command()
        self.assertIn(
            'PRIMARY KEY (`device_id`,`peer_ip`,`metric_type`,`observation_time`)',
            ddl,
        )

    def test_order_by(self):
        ddl = self._make().create_table_command()
        self.assertIn(
            'ORDER BY (`device_id`,`peer_ip`,`metric_type`,`observation_time`)',
            ddl,
        )

    def test_primary_keys_list(self):
        p = self._make()
        self.assertEqual(p.primary_keys, ['device_id', 'peer_ip', 'metric_type', 'observation_time'])

    # ------------------------------------------------------------------
    # TTL / partition
    # ------------------------------------------------------------------

    def test_default_ttl(self):
        ddl = self._make().create_table_command()
        self.assertIn('TTL observation_time + INTERVAL 7 DAY', ddl)

    def test_ttl_env_override(self):
        ddl = self._make({'CLICKHOUSE_BMP_STAT_TTL': '30 DAY'}).create_table_command()
        self.assertIn('TTL observation_time + INTERVAL 30 DAY', ddl)

    def test_default_partition_by(self):
        ddl = self._make().create_table_command()
        self.assertIn('PARTITION BY toYYYYMMDD(observation_time)', ddl)

    def test_partition_by_env_override(self):
        ddl = self._make(
            {'CLICKHOUSE_BMP_STAT_PARTITION_BY': 'toYYYYMM(observation_time)'}
        ).create_table_command()
        self.assertIn('PARTITION BY toYYYYMM(observation_time)', ddl)

    # ------------------------------------------------------------------
    # No materialized views by default
    # ------------------------------------------------------------------

    def test_no_materialized_views_by_default(self):
        self.assertEqual(self._make().get_materialized_views(), [])

    def test_no_views_by_default(self):
        self.assertEqual(self._make().get_views(), [])


class TestBMPPeerUpProcessor(unittest.TestCase):
    """Unit tests for BaseBMPPeerUpProcessor."""

    def _make(self, **env):
        with patch.dict('os.environ', env):
            return BaseBMPPeerUpProcessor(Mock())

    def _cols(self):
        return {c[0]: c[1] for c in self._make().column_defs}

    def _col_names(self):
        return [c[0] for c in self._make().column_defs]

    def test_table_name_default(self):
        self.assertEqual(self._make().table, 'data_bmp_peer_up')

    def test_table_name_env_override(self):
        self.assertEqual(
            self._make(CLICKHOUSE_BMP_PEER_UP_TABLE='custom_peer_up').table,
            'custom_peer_up',
        )

    def test_ttl_default(self):
        self.assertEqual(self._make().table_ttl, '90 DAY')

    def test_ttl_column_default(self):
        self.assertEqual(self._make().table_ttl_column, 'observation_time')

    def test_partition_by_default(self):
        self.assertEqual(self._make().partition_by, 'toYYYYMMDD(observation_time)')

    def test_observation_time_is_first_column(self):
        self.assertEqual(self._col_names()[0], 'observation_time')

    def test_expected_columns_present(self):
        cols = self._col_names()
        for name in [
            'observation_time', 'insert_time',
            'device_id', 'device_ref',
            'peer_ip', 'peer_ip_ref',
            'peer_as_id', 'peer_as_ref',
            'peer_type', 'bgp_id', 'rd',
            'local_as', 'local_port', 'remote_port', 'local_ip',
        ]:
            self.assertIn(name, cols, msg=f"Column '{name}' missing")

    def test_peer_down_columns_absent(self):
        cols = self._col_names()
        for name in ['reason_type', 'reason_str']:
            self.assertNotIn(name, cols, msg=f"Column '{name}' should not be present")

    def test_local_ip_is_nullable_ipv6(self):
        self.assertEqual(self._cols()['local_ip'], 'Nullable(IPv6)')

    def test_local_port_is_nullable_uint16(self):
        self.assertEqual(self._cols()['local_port'], 'Nullable(UInt16)')

    def test_remote_port_is_nullable_uint16(self):
        self.assertEqual(self._cols()['remote_port'], 'Nullable(UInt16)')

    def test_bgp_id_is_nullable_ipv6(self):
        self.assertEqual(self._cols()['bgp_id'], 'Nullable(IPv6)')

    def test_primary_keys(self):
        self.assertEqual(
            self._make().primary_keys,
            ['device_id', 'peer_ip', 'observation_time'],
        )

    def test_order_by_matches_primary_keys(self):
        p = self._make()
        self.assertEqual(p.order_by, p.primary_keys)

    def test_no_materialized_views(self):
        self.assertEqual(self._make().get_materialized_views(), [])

    def test_no_views(self):
        self.assertEqual(self._make().get_views(), [])


class TestBMPPeerDownProcessor(unittest.TestCase):
    """Unit tests for BaseBMPPeerDownProcessor."""

    def _make(self, **env):
        with patch.dict('os.environ', env):
            return BaseBMPPeerDownProcessor(Mock())

    def _cols(self):
        return {c[0]: c[1] for c in self._make().column_defs}

    def _col_names(self):
        return [c[0] for c in self._make().column_defs]

    def test_table_name_default(self):
        self.assertEqual(self._make().table, 'data_bmp_peer_down')

    def test_table_name_env_override(self):
        self.assertEqual(
            self._make(CLICKHOUSE_BMP_PEER_DOWN_TABLE='custom_peer_down').table,
            'custom_peer_down',
        )

    def test_ttl_default(self):
        self.assertEqual(self._make().table_ttl, '90 DAY')

    def test_ttl_column_default(self):
        self.assertEqual(self._make().table_ttl_column, 'observation_time')

    def test_partition_by_default(self):
        self.assertEqual(self._make().partition_by, 'toYYYYMMDD(observation_time)')

    def test_observation_time_is_first_column(self):
        self.assertEqual(self._col_names()[0], 'observation_time')

    def test_expected_columns_present(self):
        cols = self._col_names()
        for name in [
            'observation_time', 'insert_time',
            'device_id', 'device_ref',
            'peer_ip', 'peer_ip_ref',
            'peer_as_id', 'peer_as_ref',
            'peer_type', 'rd',
            'reason_type', 'reason_str',
        ]:
            self.assertIn(name, cols, msg=f"Column '{name}' missing")

    def test_peer_up_columns_absent(self):
        cols = self._col_names()
        for name in ['local_as', 'local_port', 'remote_port', 'local_ip', 'bgp_id']:
            self.assertNotIn(name, cols, msg=f"Column '{name}' should not be present")

    def test_reason_type_is_nullable_uint8(self):
        self.assertEqual(self._cols()['reason_type'], 'Nullable(UInt8)')

    def test_reason_str_is_nullable_string(self):
        self.assertEqual(self._cols()['reason_str'], 'Nullable(String)')

    def test_primary_keys(self):
        self.assertEqual(
            self._make().primary_keys,
            ['device_id', 'peer_ip', 'observation_time'],
        )

    def test_order_by_matches_primary_keys(self):
        p = self._make()
        self.assertEqual(p.order_by, p.primary_keys)

    def test_no_materialized_views(self):
        self.assertEqual(self._make().get_materialized_views(), [])

    def test_no_views(self):
        self.assertEqual(self._make().get_views(), [])


if __name__ == '__main__':
    unittest.main()