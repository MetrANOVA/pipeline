#!/usr/bin/env python3

import unittest
import sys
import os
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from metranova.processors.clickhouse.alcf import JobMetadataProcessor, JobDataProcessor


class TestJobMetadataProcessor(unittest.TestCase):
    """Unit tests for JobMetadataProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        mock_cacher = Mock()
        mock_cacher.lookup.return_value = None
        self.mock_pipeline.cacher.return_value = mock_cacher
        
        self.env_patcher = patch.dict(os.environ, {
            'CLICKHOUSE_ALCF_JOB_TABLE': 'meta_alcf_job'
        })
        self.env_patcher.start()
        
        self.processor = JobMetadataProcessor(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that JobMetadataProcessor initializes correctly."""
        self.assertEqual(self.processor.table, 'meta_alcf_job')
        self.assertIn(['compute_resource_id', 'String', True], self.processor.column_defs)
        self.assertIn(['location', 'Array(String)', True], self.processor.column_defs)
        self.assertIn(['mode', 'LowCardinality(String)', True], self.processor.column_defs)
        self.assertEqual(self.processor.val_id_field, ['jobid'])
        self.assertIn('location', self.processor.array_fields)
        self.assertIn('schedule_select', self.processor.array_fields)
        self.assertIn('node_count', self.processor.int_fields)
        self.assertIn('substate', self.processor.int_fields)
        self.assertIn('wall_time_secs', self.processor.int_fields)

    def test_match_message_valid(self):
        """Test match_message with valid job message."""
        value = {
            'type': 'job',
            'jobid': 'test_job_123'
        }
        self.assertTrue(self.processor.match_message(value))

    def test_match_message_invalid_type(self):
        """Test match_message with invalid type."""
        value = {
            'type': 'not_a_job',
            'jobid': 'test_job_123'
        }
        self.assertFalse(self.processor.match_message(value))

    def test_match_message_missing_type(self):
        """Test match_message with missing type field."""
        value = {
            'jobid': 'test_job_123'
        }
        self.assertFalse(self.processor.match_message(value))

    def test_format_time_value_valid(self):
        """Test format_time_value with valid timestamp."""
        timestamp = 1609459200  # 2021-01-01 00:00:00 UTC
        result = self.processor.format_time_value(timestamp)
        self.assertIsInstance(result, datetime)
        self.assertEqual(result, datetime.fromtimestamp(timestamp))

    def test_format_time_value_none(self):
        """Test format_time_value with None."""
        result = self.processor.format_time_value(None)
        self.assertIsNone(result)

    def test_format_time_value_negative(self):
        """Test format_time_value with negative timestamp."""
        result = self.processor.format_time_value(-1)
        self.assertIsNone(result)

    def test_format_time_value_invalid(self):
        """Test format_time_value with invalid string."""
        result = self.processor.format_time_value('invalid')
        self.assertIsNone(result)

    def test_build_metadata_fields_basic(self):
        """Test build_metadata_fields with basic job data."""
        value = {
            'jobid': 'job123',
            'mode': 'batch',
            'nodes': 10,
            'project': 'test_project',
            'queue': 'default',
            'state': 'running',
            'submittime': 1609459200,
            'substate': 1,
            'walltime': 60  # in minutes
        }
        
        result = self.processor.build_metadata_fields(value)
        
        self.assertEqual(result['node_count'], 10)
        self.assertEqual(result['wall_time_secs'], 3600)  # 60 minutes * 60 seconds
        self.assertIsInstance(result['submit_time'], datetime)
        self.assertEqual(result['submit_time'], datetime.fromtimestamp(1609459200))

    def test_build_metadata_fields_with_schedselect(self):
        """Test build_metadata_fields with schedselect data."""
        value = {
            'jobid': 'job123',
            'mode': 'batch',
            'nodes': 10,
            'project': 'test_project',
            'queue': 'default',
            'state': 'running',
            'submittime': 1609459200,
            'substate': 1,
            'walltime': 60,
            'schedselect': [
                {
                    'at_queue': 'default',
                    'broken': False,
                    'debug': False,
                    'host': 'node001',
                    'mem': '128gb',
                    'mpiprocs': 32,
                    'nchunks': 1,
                    'ncpus': 64,
                    'ngpus': 4,
                    'system': 'linux',
                    'validation': True
                },
                {
                    'at_queue': 'gpu',
                    'ncpus': 32,
                    'ngpus': 8
                }
            ]
        }
        
        result = self.processor.build_metadata_fields(value)
        
        self.assertIn('schedule_select', result)
        self.assertIsInstance(result['schedule_select'], list)
        self.assertEqual(len(result['schedule_select']), 2)
        
        # Check first tuple structure
        first_tuple = result['schedule_select'][0]
        self.assertEqual(first_tuple[0], 'default')  # at_queue
        self.assertEqual(first_tuple[1], False)  # broken
        self.assertEqual(first_tuple[4], '128gb')  # mem
        self.assertEqual(first_tuple[7], 64)  # ncpus

    def test_build_metadata_fields_with_start_and_end_time(self):
        """Test build_metadata_fields calculates end_time correctly."""
        start_timestamp = 1609459200
        walltime_minutes = 120
        
        value = {
            'jobid': 'job123',
            'mode': 'batch',
            'nodes': 10,
            'project': 'test_project',
            'queue': 'default',
            'state': 'running',
            'submittime': 1609459200,
            'substate': 1,
            'walltime': walltime_minutes,
            'starttime': start_timestamp
        }
        
        result = self.processor.build_metadata_fields(value)
        
        self.assertIsInstance(result['start_time'], datetime)
        self.assertEqual(result['start_time'], datetime.fromtimestamp(start_timestamp))
        
        self.assertIsInstance(result['end_time'], datetime)
        expected_end_time = datetime.fromtimestamp(start_timestamp) + timedelta(seconds=walltime_minutes * 60)
        self.assertEqual(result['end_time'], expected_end_time)

    def test_build_metadata_fields_missing_start_time(self):
        """Test build_metadata_fields with missing start_time."""
        value = {
            'jobid': 'job123',
            'mode': 'batch',
            'nodes': 10,
            'project': 'test_project',
            'queue': 'default',
            'state': 'queued',
            'submittime': 1609459200,
            'substate': 1,
            'walltime': 60
        }
        
        result = self.processor.build_metadata_fields(value)
        
        self.assertIsNone(result['start_time'])
        self.assertIsNone(result['end_time'])

    def test_build_metadata_fields_missing_submit_time_raises_error(self):
        """Test build_metadata_fields raises error when submit_time cannot be parsed."""
        value = {
            'jobid': 'job123',
            'mode': 'batch',
            'nodes': 10,
            'project': 'test_project',
            'queue': 'default',
            'state': 'running',
            'substate': 1,
            'walltime': 60
        }
        
        with self.assertRaises(ValueError) as context:
            self.processor.build_metadata_fields(value)
        
        self.assertIn("submit_time is required", str(context.exception))

    def test_build_metadata_fields_invalid_schedselect(self):
        """Test build_metadata_fields with invalid schedselect (not a dict)."""
        value = {
            'jobid': 'job123',
            'mode': 'batch',
            'nodes': 10,
            'project': 'test_project',
            'queue': 'default',
            'state': 'running',
            'submittime': 1609459200,
            'substate': 1,
            'walltime': 60,
            'schedselect': [
                "invalid_string",
                {'at_queue': 'default'}
            ]
        }
        
        result = self.processor.build_metadata_fields(value)
        
        # Should only include the valid dict entry
        self.assertEqual(len(result['schedule_select']), 1)


class TestJobDataProcessor(unittest.TestCase):
    """Unit tests for JobDataProcessor class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_pipeline = Mock()
        mock_cacher = Mock()
        mock_cacher.lookup_dict_key.return_value = 'test_ref_123'
        self.mock_pipeline.cacher.return_value = mock_cacher
        
        self.env_patcher = patch.dict(os.environ, {
            'CLICKHOUSE_ALCF_COLLECTOR_ID': 'test_collector',
            'POLICY_ORIGINATOR': 'test_originator',
            'POLICY_LEVEL': '1',
            'POLICY_SCOPE': 'test_scope'
        })
        self.env_patcher.start()
        
        self.processor = JobDataProcessor(self.mock_pipeline)

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()

    def test_initialization(self):
        """Test that JobDataProcessor initializes correctly."""
        self.assertEqual(self.processor.resource_type, 'alcf_job')
        self.assertEqual(self.processor.collector_id, 'test_collector')
        self.assertEqual(self.processor.required_fields, [['jobid']])
        self.assertIn('runtimef', self.processor.metric_map)
        self.assertIn('queuedtimef', self.processor.metric_map)
        self.assertIn('score', self.processor.metric_map)

    def test_load_resource_types(self):
        """Test load_resource_types returns correct resource type."""
        result = self.processor.load_resource_types()
        self.assertEqual(result, ['alcf_job'])

    def test_build_message_empty_value(self):
        """Test build_message with empty value."""
        result = self.processor.build_message({}, {})
        self.assertEqual(result, [])

    def test_build_message_no_data(self):
        """Test build_message with missing data field."""
        value = {'jobid': 'job123'}
        result = self.processor.build_message(value, {})
        self.assertEqual(result, [])

    def test_build_message_data_not_list(self):
        """Test build_message with data field not being a list."""
        value = {'data': 'not_a_list'}
        result = self.processor.build_message(value, {})
        self.assertEqual(result, [])

    def test_build_message_single_record(self):
        """Test build_message with single record."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600,
                    'queuedtimef': 600,
                    'score': 0.95
                }
            ]
        }
        
        result = self.processor.build_message(value, {})
        
        # Should have 3 records, one for each metric
        self.assertEqual(len(result), 3)
        
        # Check runtime_secs metric (target name)
        runtime_record = next((r for r in result if r['metric_name'] == 'runtime_secs'), None)
        self.assertIsNotNone(runtime_record)
        self.assertEqual(runtime_record['metric_value'], 3600)
        self.assertEqual(runtime_record['id'], 'job123')
        self.assertEqual(runtime_record['ref'], 'test_ref_123')
        self.assertEqual(runtime_record['collector_id'], 'test_collector')
        self.assertIn('data_alcf_job_gauge', runtime_record['_clickhouse_table'])

    def test_build_message_multiple_records(self):
        """Test build_message with multiple records."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600,
                    'score': 0.95
                },
                {
                    'jobid': 'job456',
                    'queuedtimef': 600,
                    'score': 0.85
                }
            ]
        }
        
        result = self.processor.build_message(value, {})
        
        # First record has 2 metrics, second record has 2 metrics = 4 total
        self.assertEqual(len(result), 4)
        
        # Check that we have records for both jobs
        job_ids = {r['id'] for r in result}
        self.assertIn('job123', job_ids)
        self.assertIn('job456', job_ids)

    def test_build_message_missing_required_field(self):
        """Test build_message with missing required field (jobid)."""
        value = {
            'data': [
                {
                    'runtimef': 3600,
                    'score': 0.95
                }
            ]
        }
        
        result = self.processor.build_message(value, {})
        
        # Should return empty list as jobid is missing
        self.assertEqual(result, [])

    def test_build_message_missing_metrics(self):
        """Test build_message with some metrics missing."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600
                    # queuedtimef and score are missing
                }
            ]
        }
        
        result = self.processor.build_message(value, {})
        
        # Should only have 1 record (for runtimef -> runtime_secs)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['metric_name'], 'runtime_secs')
        self.assertEqual(result[0]['metric_value'], 3600)

    def test_build_message_metric_types(self):
        """Test that metrics are assigned correct type (gauge)."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600,
                    'queuedtimef': 600,
                    'score': 0.95
                }
            ]
        }
        
        result = self.processor.build_message(value, {})
        
        # All metrics should be gauge type
        for record in result:
            self.assertIn('gauge', record['_clickhouse_table'])

    def test_build_message_observation_time(self):
        """Test that observation_time is set correctly."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600
                }
            ]
        }
        
        before_time = datetime.now()
        result = self.processor.build_message(value, {})
        after_time = datetime.now()
        
        self.assertEqual(len(result), 1)
        observation_time = result[0]['observation_time']
        self.assertIsInstance(observation_time, datetime)
        self.assertGreaterEqual(observation_time, before_time)
        self.assertLessEqual(observation_time, after_time)

    def test_build_message_cacher_lookup(self):
        """Test that cacher lookup is called correctly."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600
                }
            ]
        }
        
        self.processor.build_message(value, {})
        
        # Verify cacher was called
        self.mock_pipeline.cacher.assert_called_with("clickhouse")
        mock_cacher = self.mock_pipeline.cacher.return_value
        mock_cacher.lookup_dict_key.assert_called_with('meta_alcf_job', 'job123', 'ref')

    def test_build_message_all_fields_present(self):
        """Test that all expected fields are present in output."""
        value = {
            'data': [
                {
                    'jobid': 'job123',
                    'runtimef': 3600
                }
            ]
        }
        
        result = self.processor.build_message(value, {})
        
        self.assertEqual(len(result), 1)
        record = result[0]
        
        expected_fields = [
            '_clickhouse_table',
            'observation_time',
            'collector_id',
            'policy_originator',
            'policy_level',
            'policy_scope',
            'ext',
            'id',
            'ref',
            'metric_name',
            'metric_value'
        ]
        
        for field in expected_fields:
            self.assertIn(field, record)

    def test_build_single_message_directly(self):
        """Test build_single_message method directly."""
        value = {
            'jobid': 'job789',
            'runtimef': 7200,
            'score': 0.88
        }
        
        result = self.processor.build_single_message(value, {})
        
        self.assertEqual(len(result), 2)
        
        # Check both metrics are present (target metric names)
        metric_names = {r['metric_name'] for r in result}
        self.assertIn('runtime_secs', metric_names)
        self.assertIn('score', metric_names)


if __name__ == '__main__':
    unittest.main()
