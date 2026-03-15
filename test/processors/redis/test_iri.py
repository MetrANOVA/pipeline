#!/usr/bin/env python3

import unittest
from unittest.mock import MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.redis.iri import (
    IRIBaseRedisProcessor,
    SiteToFacilityProcessor,
    ResourceToSiteProcessor,
)


class TestIRIBaseRedisProcessor(unittest.TestCase):
    def setUp(self):
        self.mock_pipeline = MagicMock()

    def test_init_defaults(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)

        self.assertEqual(processor.required_fields, [['id'], ['self_uri']])
        self.assertEqual(processor.expires, 86400)
        self.assertEqual(processor.val_id_field, 'id')
        self.assertIsNone(processor.url_match)
        self.assertIsNone(processor.table_name)
        self.assertIsNone(processor.ref_field)

    def test_match_message_with_url_match(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)
        processor.url_match = r'/facility/sites$'

        self.assertTrue(processor.match_message({'url': 'https://iri/facility/sites/?a=1'}))
        self.assertFalse(processor.match_message({'url': 'https://iri/facility'}))

    def test_build_message_missing_data_returns_empty(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)
        processor.table_name = 'iri_site_to_facility'
        processor.ref_field = 'site_uris'

        self.assertEqual(processor.build_message({}, {}), [])
        self.assertEqual(processor.build_message({'data': None}, {}), [])

    def test_build_message_single_dict_data_coerced_to_list(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)
        processor.table_name = 'iri_site_to_facility'
        processor.ref_field = 'site_uris'

        value = {
            'data': {
                'id': 'facility-1',
                'self_uri': 'https://iri/facility/facility-1',
                'site_uris': [
                    'https://iri/facility/sites/123e4567-e89b-12d3-a456-426614174000'
                ]
            }
        }

        result = processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['table'], 'iri_site_to_facility')
        self.assertEqual(result[0]['key'], '123e4567-e89b-12d3-a456-426614174000')
        self.assertEqual(result[0]['value'], 'facility-1')
        self.assertEqual(result[0]['expires'], 86400)

    def test_build_message_skips_invalid_reference_ids(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)
        processor.table_name = 'iri_site_to_facility'
        processor.ref_field = 'site_uris'

        value = {
            'data': [{
                'id': 'facility-1',
                'self_uri': 'https://iri/facility/facility-1',
                'site_uris': [
                    'https://iri/facility/sites/not-a-uuid',
                    'https://iri/facility/sites/123e4567-e89b-12d3-a456-426614174000/'
                ]
            }]
        }

        result = processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['key'], '123e4567-e89b-12d3-a456-426614174000')

    def test_build_message_missing_required_fields_returns_empty(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)
        processor.table_name = 'iri_site_to_facility'
        processor.ref_field = 'site_uris'

        value = {
            'data': [{
                'id': 'facility-1',
                # missing self_uri
                'site_uris': ['https://iri/facility/sites/123e4567-e89b-12d3-a456-426614174000']
            }]
        }

        self.assertEqual(processor.build_message(value, {}), [])

    def test_build_message_ref_field_not_set(self):
        processor = IRIBaseRedisProcessor(self.mock_pipeline)
        processor.table_name = 'iri_generic'
        processor.ref_field = None

        value = {
            'data': [{
                'id': 'resource-1',
                'self_uri': 'https://iri/resource/resource-1'
            }]
        }

        self.assertEqual(processor.build_message(value, {}), [])


class TestSpecificIRIRedisProcessors(unittest.TestCase):
    def setUp(self):
        self.mock_pipeline = MagicMock()

    def test_site_to_facility_processor_settings(self):
        processor = SiteToFacilityProcessor(self.mock_pipeline)

        self.assertEqual(processor.url_match, r'/facility$')
        self.assertEqual(processor.table_name, 'iri_site_to_facility')
        self.assertEqual(processor.ref_field, 'site_uris')

    def test_resource_to_site_processor_settings(self):
        processor = ResourceToSiteProcessor(self.mock_pipeline)

        self.assertEqual(processor.url_match, r'/facility/sites$')
        self.assertEqual(processor.table_name, 'iri_resource_to_site')
        self.assertEqual(processor.ref_field, 'resource_uris')

    def test_site_to_facility_build_message(self):
        processor = SiteToFacilityProcessor(self.mock_pipeline)

        value = {
            'url': 'https://iri/facility',
            'data': [{
                'id': 'facility-1',
                'self_uri': 'https://iri/facility/facility-1',
                'site_uris': [
                    'https://iri/facility/sites/123e4567-e89b-12d3-a456-426614174000',
                    'https://iri/facility/sites/223e4567-e89b-12d3-a456-426614174001'
                ]
            }]
        }

        result = processor.build_message(value, {})
        self.assertEqual(len(result), 2)
        keys = [row['key'] for row in result]
        self.assertIn('123e4567-e89b-12d3-a456-426614174000', keys)
        self.assertIn('223e4567-e89b-12d3-a456-426614174001', keys)

    def test_resource_to_site_build_message(self):
        processor = ResourceToSiteProcessor(self.mock_pipeline)

        value = {
            'url': 'https://iri/facility/sites',
            'data': [{
                'id': 'site-1',
                'self_uri': 'https://iri/facility/sites/site-1',
                'resource_uris': [
                    'https://iri/status/resources/323e4567-e89b-12d3-a456-426614174002'
                ]
            }]
        }

        result = processor.build_message(value, {})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['table'], 'iri_resource_to_site')
        self.assertEqual(result[0]['value'], 'site-1')
        self.assertEqual(result[0]['key'], '323e4567-e89b-12d3-a456-426614174002')


if __name__ == '__main__':
    unittest.main(verbosity=2)
