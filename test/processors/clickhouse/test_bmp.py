import unittest
from unittest.mock import Mock, patch

from metranova.processors.clickhouse.bmp import (
    BaseBMPRouteProcessor,
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
            'PRIMARY KEY (`device_id`,`peer_ip`,`ip_prefix_range_start`,`ip_prefix_range_end`,`add_path_id`,`is_post`)',
            result,
        )
        self.assertIn(
            'ORDER BY (`device_id`,`peer_ip`,`ip_prefix_range_start`,`ip_prefix_range_end`,`add_path_id`,`is_post`)',
            result,
        )

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

    def test_post_policy_view_command(self):
        view = ViewBMPRouteLatestAdjRIBInPost()

        result = view.create_view_command()

        self.assertIn('CREATE VIEW IF NOT EXISTS data_bmp_route_latest_adj_rib_in_post_view', result)
        self.assertIn('WHERE is_post = true', result)
        self.assertLess(result.index('AS export_time'), result.index('AS observation_time'))
        self.assertIn('GROUP BY', result)

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


if __name__ == '__main__':
    unittest.main()