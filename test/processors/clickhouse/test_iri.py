#!/usr/bin/env python3

import unittest
from datetime import datetime
from unittest.mock import MagicMock

# Add the project root to Python path for imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from metranova.processors.clickhouse.iri import (
    IRIBaseMetadataProcessor,
    FacilityMetadataProcessor,
    SiteMetadataProcessor,
    EventMetadataProcessor,
    IncidentMetadataProcessor,
    ResourceMetadataProcessor,
    ResourceDataProcessor,
)


class TestIRIProcessors(unittest.TestCase):
    def setUp(self):
        self.mock_pipeline = MagicMock()

        self.mock_clickhouse_cacher = MagicMock()
        self.mock_redis_cacher = MagicMock()

        self.mock_clickhouse_cacher.lookup.return_value = None
        self.mock_clickhouse_cacher.lookup_dict_key.return_value = None
        self.mock_redis_cacher.lookup.return_value = None

        def cacher_side_effect(name):
            if name == "clickhouse":
                return self.mock_clickhouse_cacher
            if name == "redis":
                return self.mock_redis_cacher
            return MagicMock()

        self.mock_pipeline.cacher.side_effect = cacher_side_effect

    def test_iri_base_match_message_url_pattern(self):
        processor = IRIBaseMetadataProcessor(self.mock_pipeline)
        processor.url_match = r"/status/resources$"

        self.assertTrue(processor.match_message({"url": "https://example/status/resources/?page=1"}))
        self.assertFalse(processor.match_message({"url": "https://example/status/incidents"}))

    def test_id_from_uri(self):
        processor = IRIBaseMetadataProcessor(self.mock_pipeline)

        self.assertEqual(processor.id_from_uri("https://x/y/z/abc123/"), "abc123")
        self.assertEqual(processor.id_from_uri("https://x/y/z/abc123"), "abc123")
        self.assertIsNone(processor.id_from_uri(None))

    def test_ids_and_refs_from_uri_list(self):
        processor = IRIBaseMetadataProcessor(self.mock_pipeline)

        def lookup_dict_key_side_effect(table, key, dict_key):
            if table == "meta_iri_resource" and key == "r1" and dict_key == "ref":
                return "r1__v1"
            return None

        self.mock_clickhouse_cacher.lookup_dict_key.side_effect = lookup_dict_key_side_effect

        ids, refs = processor.ids_and_refs_from_uri_list(
            ["https://iri/status/resources/r1", "https://iri/status/resources/r2", None],
            "iri_resource",
        )

        self.assertEqual(ids, ["r1", "r2"])
        self.assertEqual(refs, ["r1__v1"])

    def test_format_time_value(self):
        processor = IRIBaseMetadataProcessor(self.mock_pipeline)

        dt = processor.format_time_value("2025-01-02T03:04:05")
        self.assertEqual(dt, datetime(2025, 1, 2, 3, 4, 5))
        self.assertIsNone(processor.format_time_value("not-a-date"))
        self.assertIsNone(processor.format_time_value(None))

    def test_facility_build_metadata_fields(self):
        processor = FacilityMetadataProcessor(self.mock_pipeline)

        record = processor.build_metadata_fields({
            "self_uri": "https://iri/facility/f1",
            "name": "Facility 1",
            "description": "desc",
            "last_modified": "2025-01-02T03:04:05",
            "short_name": "F1",
            "organization_name": "Org",
            "support_uri": "mailto:support@example.org",
        })

        self.assertEqual(record["uri"], "https://iri/facility/f1")
        self.assertEqual(record["short_name"], "F1")
        self.assertEqual(record["organization_name"], "Org")
        self.assertEqual(record["support_uri"], "mailto:support@example.org")
        self.assertEqual(record["last_modified"], datetime(2025, 1, 2, 3, 4, 5))

    def test_site_build_metadata_fields_with_lookup(self):
        processor = SiteMetadataProcessor(self.mock_pipeline)

        self.mock_redis_cacher.lookup.return_value = "facility-1"
        self.mock_clickhouse_cacher.lookup_dict_key.return_value = "facility-1__v2"

        record = processor.build_metadata_fields({
            "id": "site-1",
            "self_uri": "https://iri/facility/sites/site-1",
            "last_modified": "2025-02-02T10:20:30",
            "state_or_province_name": "CA",
        })

        self.assertEqual(record["facility_id"], "facility-1")
        self.assertEqual(record["facility_ref"], "facility-1__v2")
        self.assertEqual(record["country_sub_name"], "CA")

    def test_event_build_metadata_fields(self):
        processor = EventMetadataProcessor(self.mock_pipeline)

        def lookup_side_effect(table, key, dict_key):
            if table == "meta_iri_resource":
                return "resource-ref"
            if table == "meta_iri_incident":
                return "incident-ref"
            return None

        self.mock_clickhouse_cacher.lookup_dict_key.side_effect = lookup_side_effect

        record = processor.build_metadata_fields({
            "id": "ev-1",
            "self_uri": "https://iri/status/events/ev-1",
            "last_modified": "2025-03-01T00:00:00",
            "status": "up",
            "occurred_at": "2025-03-01T01:00:00",
            "resource_uri": "https://iri/status/resources/res-1",
            "incident_uri": "https://iri/status/incidents/inc-1",
        })

        self.assertEqual(record["resource_id"], "res-1")
        self.assertEqual(record["resource_ref"], "resource-ref")
        self.assertEqual(record["incident_id"], "inc-1")
        self.assertEqual(record["incident_ref"], "incident-ref")

    def test_incident_build_metadata_fields_resource_uris_not_list(self):
        processor = IncidentMetadataProcessor(self.mock_pipeline)

        record = processor.build_metadata_fields({
            "id": "inc-1",
            "self_uri": "https://iri/status/incidents/inc-1",
            "last_modified": "2025-04-01T00:00:00",
            "status": "active",
            "type": "network",
            "start": "2025-04-01T00:01:00",
            "resolution": "open",
            "resource_uris": "not-a-list",
        })

        self.assertEqual(record["resource_id"], [])
        self.assertEqual(record["resource_ref"], [])

    def test_resource_build_metadata_fields(self):
        processor = ResourceMetadataProcessor(self.mock_pipeline)

        def redis_lookup_side_effect(table, key):
            if table == "iri_resource_to_site":
                return "site-1"
            if table == "iri_site_to_facility":
                return "facility-1"
            return None

        def clickhouse_lookup_side_effect(table, key, dict_key):
            if table == "meta_iri_site":
                return "site-1__v1"
            if table == "meta_iri_facility":
                return "facility-1__v1"
            return None

        self.mock_redis_cacher.lookup.side_effect = redis_lookup_side_effect
        self.mock_clickhouse_cacher.lookup_dict_key.side_effect = clickhouse_lookup_side_effect

        record = processor.build_metadata_fields({
            "id": "resource-1",
            "self_uri": "https://iri/status/resources/resource-1",
            "resource_type": "router",
            "capability_uris": ["c1", "c2"],
        })

        self.assertEqual(record["type"], "router")
        self.assertEqual(record["capability_id"], ["c1", "c2"])
        self.assertEqual(record["site_id"], "site-1")
        self.assertEqual(record["site_ref"], "site-1__v1")
        self.assertEqual(record["facility_id"], "facility-1")
        self.assertEqual(record["facility_ref"], "facility-1__v1")

    def test_resource_data_format_value(self):
        processor = ResourceDataProcessor(self.mock_pipeline)

        ext, value = processor.format_value("status", "degraded")
        self.assertEqual(value, 2)
        self.assertEqual(ext, {"status_str": "degraded"})

        ext, value = processor.format_value("status", "unexpected")
        self.assertEqual(value, 4)
        self.assertEqual(ext, {"status_str": "unexpected"})

        ext, value = processor.format_value("last_modified", "not-a-date")
        self.assertIsNone(value)
        self.assertEqual(ext, {"last_modified_str": "not-a-date"})

    def test_resource_data_match_message(self):
        processor = ResourceDataProcessor(self.mock_pipeline)

        self.assertTrue(processor.match_message({"url": "https://iri/status/resources"}))
        self.assertTrue(processor.match_message({"url": "https://iri/status/resources/?x=1"}))
        self.assertFalse(processor.match_message({"url": "https://iri/status/incidents"}))

    def test_resource_data_build_message_requires_list_data(self):
        processor = ResourceDataProcessor(self.mock_pipeline)

        result = processor.build_message({"data": {"id": "r1"}}, {})
        self.assertEqual(result, [])

    def test_resource_data_build_message_success(self):
        processor = ResourceDataProcessor(self.mock_pipeline)
        self.mock_clickhouse_cacher.lookup_dict_key.return_value = "resource-ref"

        result = processor.build_message({
            "data": [{
                "id": "resource-1",
                "current_status": "up",
                "last_modified": "2025-05-01T10:00:00",
            }]
        }, {})

        # current_status + last_modified => two metric rows
        self.assertEqual(len(result), 2)
        metric_names = sorted([row["metric_name"] for row in result])
        self.assertEqual(metric_names, ["last_modified", "status"])
        self.assertEqual(result[0]["id"], "resource-1")
        self.assertEqual(result[0]["ref"], "resource-ref")


if __name__ == '__main__':
    unittest.main(verbosity=2)
