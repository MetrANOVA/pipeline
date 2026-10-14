import logging
import os
import ipaddress
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


class CommunityRegistryProcessor(BaseMetadataProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_COMMUNITY_METADATA_TABLE', 'meta_ip_community')
        self.val_id_field = ['community_id']
        self.column_defs.extend([
            ['ip_subnet', 'Array(Tuple(IPv6,UInt8))', True],
            ['organization_name', 'Nullable(String)', True],
            ['organization_id', 'LowCardinality(Nullable(String))', True],
            ['organization_ref', 'Nullable(String)', True],
            ['community', 'LowCardinality(Nullable(String))', True],
            ['as_id', 'Nullable(UInt32)', True],
            ['as_ref', 'Nullable(String)', True],
            ['notes', 'Nullable(String)', True]
        ])
        self.required_fields = [
            ["community_id"],
            ["addresses"]
        ]

        # Same idea as ScienceRegistryProcessor/ASMetadataProcessor: build an
        # IP_TRIE() dictionary for organization/community lookups by IP. Since
        # ip_subnet is an Array(Tuple(IPv6,UInt8)) column, a materialized view
        # first flattens it (one row per prefix) into a plain table that the
        # dictionary is sourced from. Both are created automatically by
        # ClickHouseBatcher at startup.
        self.dictionary_enabled = os.getenv('CLICKHOUSE_COMMUNITY_DICTIONARY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        if self.dictionary_enabled:
            prefix_mv = CommunityPrefixMaterializedView(source_table_name=self.table)
            self.materialized_views.append(prefix_mv)
            self.ch_dictionaries.append(CommunityDictionary(prefix_mv.table))

    def match_message(self, value):
        #override base since don't set table in url
        return self.has_match_field(value)

    def build_metadata_fields(self, value: dict) -> dict | None:
        #iterate over strings in value['addresses'] and build a new list of tuples where first element is IP address and second is prefix length.
        ip_subnets = []
        for addr in value['addresses']:
            #every address must include a '/' prefix length; anything else is an error
            if '/' not in addr:
                self.logger.warning(f"Missing prefix length (no '/'): {addr}")
                continue
            ip, prefix_str = addr.split('/', 1)
            try:
                prefix = int(prefix_str)
            except ValueError:
                self.logger.warning(f"Invalid IP address format: {addr}")
                continue

            ip_subnets.append((ip, _trie_prefix_length(ip, prefix)))

            #subnets smaller than /24 (IPv4) or /64 (IPv6) are often the result of
            #de-identification elsewhere, where the host portion of the address is
            #replaced with the value 1 (e.g. .1 for IPv4, ::1 for IPv6). Add that
            #address too so we can still match against data anonymized that way.
            #NOTE: this comparison and ipaddress.ip_network() call below both need
            #the original, un-offset prefix length (real-world IPv4/IPv6 semantics),
            #not the trie-storage-adjusted one from _trie_prefix_length above.
            try:
                version = ipaddress.ip_address(ip).version
            except ValueError:
                self.logger.warning(f"Invalid IP address format: {addr}")
                continue
            boundary_prefix = 24 if version == 4 else 64
            if prefix > boundary_prefix:
                network = ipaddress.ip_network(f"{ip}/{boundary_prefix}", strict=False)
                deidentified_ip = network.network_address + 1
                # a single de-identified host is always a full-length /128
                # once embedded in the IPv6 storage column -- see
                # _trie_prefix_length
                ip_subnets.append((str(deidentified_ip), 128))

        #de-duplicate while preserving order (multiple narrow subnets can share
        #the same enclosing /24 or /64, which would otherwise generate the same
        #de-identified address more than once)
        ip_subnets = list(dict.fromkeys(ip_subnets))

        org_name = value.get('org_name', None)

        #init record
        formatted_record = {
            'ip_subnet': ip_subnets,
            'organization_name': org_name,
            'community': value.get('community', None),
            'as_id': value.get('asn', None),
            'notes': value.get('notes', None),
            'ext': '{}',
            'tag': []
        }

        # Resolve organization_id/organization_ref via the clickhouse cacher, same
        # pattern as ASMetadataProcessor and ScienceRegistryProcessor -- keyed by
        # name (via the cacher's "table:field" composite-key form) since this
        # source only gives us an org name, not a meta_organization id.
        # NOTE: this requires 'meta_organization:name' to be listed in the
        # CLICKHOUSE_CACHER_TABLES env var for this pipeline.
        cached_org_info = self.pipeline.cacher("clickhouse").lookup("meta_organization:name", org_name)
        if cached_org_info:
            formatted_record["organization_id"] = cached_org_info.get("id", org_name)
            formatted_record["organization_ref"] = cached_org_info.get(self.db_ref_field, None)
        else:
            formatted_record["organization_id"] = org_name
            formatted_record["organization_ref"] = None

        #cast as_id to an int, and set to None if exception when casting
        if formatted_record['as_id'] is not None:
            try:
                formatted_record['as_id'] = int(formatted_record['as_id'])
            except ValueError:
                formatted_record['as_id'] = None

        # Resolve as_ref via the clickhouse cacher, same pattern as
        # IPMetadataProcessor -- meta_as is keyed directly by AS number, unlike
        # meta_organization above which needs the 'table:field' composite-key
        # form since it's keyed by name.
        # NOTE: this requires 'meta_as' to be listed in the CLICKHOUSE_CACHER_TABLES
        # env var for this pipeline.
        cached_as_info = self.pipeline.cacher("clickhouse").lookup("meta_as", formatted_record['as_id'])
        if cached_as_info:
            formatted_record["as_ref"] = cached_as_info.get(self.db_ref_field, None)
        else:
            formatted_record["as_ref"] = None

        return formatted_record


class CommunityPrefixMaterializedView(BaseClickHouseMaterializedViewMixin):
    """Flattens meta_ip_community.ip_subnet (one array per community record)
    into one row per prefix, so it can be used as the SOURCE table for an
    IP_TRIE() dictionary.
    """

    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        super().__init__(source_table_name, agg_window)
        self.table = os.getenv('CLICKHOUSE_COMMUNITY_PREFIX_TABLE', 'meta_ip_community_prefix')
        #plain (non-nullable) String columns: ClickHouse's IP_TRIE dictionary
        #layout doesn't support Nullable attributes at all (UNSUPPORTED_METHOD:
        #"array or nullable attributes not supported for dictionary of type
        #Trie"), so NULLs are coalesced to '' below in mv_select_query instead
        #of being passed through as Nullable
        self.column_defs = [
            ['prefix', 'String', True],
            ['organization_name', 'String', True],
            ['organization_id', 'String', True],
            ['community', 'String', True],
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
                coalesce(community, '') AS community,
                id
            FROM {self.source_table_name}
            ARRAY JOIN ip_subnet AS ip_subnet_entry
        """


class CommunityDictionary(BaseClickHouseDictionaryMixin):
    """IP_TRIE() dictionary for longest-prefix-match lookups of organization and
    community info by IP, sourced from the flattened
    CommunityPrefixMaterializedView table (not the raw meta_ip_community table).
    """

    def __init__(self, source_table_name: str):
        super().__init__(source_table_name)
        self.dictionary_name = os.getenv('CLICKHOUSE_COMMUNITY_DICTIONARY_NAME', 'meta_ip_community_dict')
        #plain String, not Nullable(String) -- IP_TRIE doesn't support nullable
        #attributes, so unset values arrive as '' (coalesced upstream in
        #CommunityPrefixMaterializedView.mv_select_query) rather than NULL
        self.column_defs = [
            ['prefix', 'String'],
            ['organization_name', "String DEFAULT ''"],
            ['organization_id', "String DEFAULT ''"],
            ['community', "String DEFAULT ''"],
        ]
        self.primary_keys = ['prefix']
        #miniumum and maximum lifetime in seconds
        self.lifetime_min = os.getenv('CLICKHOUSE_COMMUNITY_DICTIONARY_LIFETIME_MIN', "600")
        self.lifetime_max = os.getenv('CLICKHOUSE_COMMUNITY_DICTIONARY_LIFETIME_MAX', "3600")
        #set the layout, will be the full layout definition
        self.layout = "IP_TRIE()"

