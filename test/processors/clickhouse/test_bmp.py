import unittest
from unittest.mock import Mock, patch

from metranova.processors.clickhouse.bmp import (
    BaseBMPRouteProcessor,
    MaterializedViewBMPRouteLatest,
)


class TestMaterializedViewBMPRouteLatest(unittest.TestCase):
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
            "`observation_time_state` AggregateFunction(max, DateTime64(3, 'UTC'))",
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
        self.assertIn('argMaxState(action, observation_time) AS action_state', result)
        self.assertIn('maxState(observation_time) AS observation_time_state', result)
        self.assertIn('argMaxState(mpls_label, observation_time) AS mpls_label_state', result)
        self.assertIn('FROM data_bmp_route', result)
        self.assertIn('WHERE is_in = true', result)
        self.assertIn('GROUP BY', result)
        self.assertIn('add_path_id,\n                is_post', result)

    @patch.dict('os.environ', {'CLICKHOUSE_BMP_ROUTE_TABLE': 'custom_bmp_route'})
    def test_processor_registers_latest_materialized_view(self):
        processor = BaseBMPRouteProcessor(Mock())

        self.assertEqual(len(processor.get_materialized_views()), 1)
        materialized_view = processor.get_materialized_views()[0]
        self.assertIsInstance(materialized_view, MaterializedViewBMPRouteLatest)
        self.assertEqual(materialized_view.source_table_name, 'custom_bmp_route')
        self.assertEqual(materialized_view.table, 'data_bmp_route_latest')


if __name__ == '__main__':
    unittest.main()