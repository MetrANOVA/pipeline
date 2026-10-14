import logging
import os
import ipaddress
from datetime import datetime
from metranova.processors.clickhouse.base import (
    BaseMetadataProcessor,
    BaseClickHouseDictionaryMixin,
    BaseClickHouseMaterializedViewMixin,
)

logger = logging.getLogger(__name__)


def _trie_prefix_length(ip_str: str, prefix_length: int) -> int:
    """ip_subnet is stored as Array(Tuple(IPv6, UInt8)) -- ClickHouse maps an
    IPv4 address into that 128-bit space as ::ffff:a.b.c.d, a fixed 96-bit
    prefix, so an IPv4 CIDR's prefix length has to be offset by 96 to stay
    correct once embedded there (e.g. 129.114.63.128/27 -> /123). Without
    this, the IP_TRIE() dictionary built from this data only ever compares
    the leading 96 always-zero/::ffff: bits shared by every IPv4-mapped
    address, so every IPv4 lookup spuriously "matches" every stored prefix.
    Native IPv6 addresses need no adjustment.
    """
    try:
        if ipaddress.ip_address(ip_str).version == 4:
            return prefix_length + 96
    except ValueError:
        pass
    return prefix_length


class ScienceRegistryProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_SCIREG_METADATA_TABLE', 'meta_ip_scireg')
        self.val_id_field = ['scireg_id']
        self.column_defs.extend([
            ['scireg_update_time', 'Date', True],
            ['ip_subnet', 'Array(Tuple(IPv6,UInt8))', True],
            ['organization_name', 'Nullable(String)', True],
            ['organization_id', 'LowCardinality(Nullable(String))', True],
            ['organization_ref', 'Nullable(String)', True],
            ['discipline', 'Nullable(String)', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True],
            ['resource_name', 'Nullable(String)', True],
            ['project_name', 'Nullable(String)', True],
            ['contact_email', 'Nullable(String)', True]
        ])
        self.required_fields = [
            ["scireg_id"],
            ["addresses"]
        ]

        # Build a ClickHouse dictionary for IP -> organization/resource lookups,
        # same idea as ASDictionary in as.py, except an IP_TRIE() dictionary can't
        # be sourced directly from this table since ip_subnet is an
        # Array(Tuple(IPv6,UInt8)) column -- IP_TRIE dictionaries need one row per
        # prefix. So a materialized view first flattens ip_subnet (one row per
        # prefix, ARRAY JOIN) into a plain table, and the dictionary is sourced
        # from that flattened table instead. Both are declared here and created
        # automatically by ClickHouseBatcher at startup -- no manual SQL script
        # required, and the dictionary's LIFETIME keeps it refreshed as new scireg
        # records flow in.
        self.dictionary_enabled = os.getenv('CLICKHOUSE_SCIREG_DICTIONARY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        if self.dictionary_enabled:
            prefix_mv = SciregPrefixMaterializedView(source_table_name=self.table)
            self.materialized_views.append(prefix_mv)
            self.ch_dictionaries.append(SciregDictionary(prefix_mv.table))

    def match_message(self, value):
        #override base since don't set table in url
        return self.has_match_field(value)

    def build_metadata_fields(self, value: dict) -> dict | None:
        #iterate over strings in value['addresses'] and build a new list of tuples where first element is IP address and second is prefix length.
        ip_subnets = []
        for addr in value['addresses']:
            #if has slash use otherwise default to a full-length host prefix
            if '/' in addr:
                ip, prefix = addr.split('/', 1)
                try:
                    prefix = int(prefix)
                except ValueError:
                    self.logger.warning(f"Invalid IP address format: {addr}")
                    continue
                ip_subnets.append((ip, _trie_prefix_length(ip, prefix)))
            else:
                try:
                    ipaddress.ip_address(addr)
                    # a bare host address is always a full-length /128 once
                    # embedded in the IPv6 storage column, whether it started
                    # as IPv4 (32 + the 96-bit offset) or native IPv6 -- see
                    # _trie_prefix_length
                    ip_subnets.append((addr, 128))
                except (ipaddress.AddressValueError, ValueError):
                    self.logger.warning(f"Invalid IP address format: {addr}")
                    continue

        org_name = value.get('org_name', None)

        #init record
        formatted_record = {
            'scireg_update_time': value.get('last_updated', 'unknown'),
            'ip_subnet': ip_subnets,
            'organization_name': org_name,
            'discipline': value.get('discipline', None),
            'latitude': value.get('latitude', None),
            'longitude': value.get('longitude', None),
            'resource_name': value.get('resource_name', None),
            'project_name': value.get('project_name', None),
            'contact_email': value.get('contact_email', None),
            'ext': '{}',
            'tag': []
        }

        # Resolve organization_id/organization_ref via the clickhouse cacher, same
        # pattern ASMetadataProcessor uses for its own organization lookup -- except
        # keyed by name (via the cacher's "table:field" composite-key form) since
        # the science registry only gives us an org name, not a meta_organization id.
        # NOTE: this requires 'meta_organization:name' to be listed in the
        # CLICKHOUSE_CACHER_TABLES env var (in addition to 'meta_organization' if
        # something else, e.g. ASMetadataProcessor, still needs the id-keyed form) --
        # otherwise this lookup always returns None and organization_id/ref fall
        # back to the same behavior as before.
        cached_org_info = self.pipeline.cacher("clickhouse").lookup("meta_organization:name", org_name)
        if cached_org_info:
            formatted_record["organization_id"] = cached_org_info.get("id", org_name)
            formatted_record["organization_ref"] = cached_org_info.get(self.db_ref_field, None)
        else:
            formatted_record["organization_id"] = org_name
            formatted_record["organization_ref"] = None

        #format scireg_update_time if equals "unknown"
        if formatted_record['scireg_update_time'] == "unknown":
            formatted_record['scireg_update_time'] = '1970-01-01'
        #convert scireg_update_time to a datetime object
        try:
            formatted_record['scireg_update_time'] = datetime.strptime(formatted_record['scireg_update_time'], '%Y-%m-%d').date()
        except ValueError:
            formatted_record['scireg_update_time'] = datetime(1970, 1, 1).date()

        #cast latitude and longitude to a float, and set to None if exception when casting
        float_fields = ['latitude', 'longitude']
        for field in float_fields:
            if formatted_record[field] is not None:
                try:
                    formatted_record[field] = float(formatted_record[field])
                except ValueError:
                    formatted_record[field] = None

        return formatted_record


class SciregPrefixMaterializedView(BaseClickHouseMaterializedViewMixin):
    """Flattens meta_ip_scireg.ip_subnet (one array per org record) into one row
    per prefix, so it can be used as the SOURCE table for an IP_TRIE() dictionary.
    """

    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        super().__init__(source_table_name, agg_window)
        self.table = os.getenv('CLICKHOUSE_SCIREG_PREFIX_TABLE', 'meta_ip_scireg_prefix')
        #plain (non-nullable) String columns: ClickHouse's IP_TRIE dictionary
        #layout doesn't support Nullable attributes at all (UNSUPPORTED_METHOD:
        #"array or nullable attributes not supported for dictionary of type
        #Trie"), so NULLs are coalesced to '' below in mv_select_query instead
        #of being passed through as Nullable
        self.column_defs = [
            ['prefix', 'String', True],
            ['organization_name', 'String', True],
            ['organization_id', 'String', True],
            ['resource_name', 'String', True],
            ['id', 'String', True],
            ['insert_time', 'DateTime DEFAULT now()', False],
        ]
        self.table_engine = 'ReplacingMergeTree'
        self.table_engine_opts = 'insert_time'
        self.primary_keys = ['prefix']
        self.order_by = ['prefix', 'id']
        self.mv_name = self.table + "_mv"
        self.mv_select_query = f"""
            SELECT
                concat(IPv6NumToString(ip_subnet_entry.1), '/', toString(ip_subnet_entry.2)) AS prefix,
                coalesce(organization_name, '') AS organization_name,
                coalesce(organization_id, '') AS organization_id,
                coalesce(resource_name, '') AS resource_name,
                id
            FROM {self.source_table_name}
            ARRAY JOIN ip_subnet AS ip_subnet_entry
        """


class SciregDictionary(BaseClickHouseDictionaryMixin):
    """IP_TRIE() dictionary for longest-prefix-match lookups of organization and
    resource info by IP, sourced from the flattened SciregPrefixMaterializedView
    table (not the raw meta_ip_scireg table -- see that class for why).
    """

    def __init__(self, source_table_name: str):
        super().__init__(source_table_name)
        self.dictionary_name = os.getenv('CLICKHOUSE_SCIREG_DICTIONARY_NAME', 'meta_ip_scireg_dict')
        #plain String, not Nullable(String) -- IP_TRIE doesn't support nullable
        #attributes, so unset values arrive as '' (coalesced upstream in
        #SciregPrefixMaterializedView.mv_select_query) rather than NULL
        self.column_defs = [
            ['prefix', 'String'],
            ['organization_name', "String DEFAULT ''"],
            ['organization_id', "String DEFAULT ''"],
            ['resource_name', "String DEFAULT ''"],
        ]
        self.primary_keys = ['prefix']
        #miniumum and maximum lifetime in seconds
        self.lifetime_min = os.getenv('CLICKHOUSE_SCIREG_DICTIONARY_LIFETIME_MIN', "600")
        self.lifetime_max = os.getenv('CLICKHOUSE_SCIREG_DICTIONARY_LIFETIME_MAX', "3600")
        #set the layout, will be the full layout definition
        self.layout = "IP_TRIE()"