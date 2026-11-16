import logging
import os

from metranova.processors.clickhouse.base import (
    BaseMetadataProcessor,
    BaseClickHouseDictionaryMixin,
    BaseClickHouseMaterializedViewMixin,
    trie_prefix_length,
)

logger = logging.getLogger(__name__)

class IPMetadataProcessor(BaseMetadataProcessor):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.table = os.getenv('CLICKHOUSE_IP_METADATA_TABLE', 'meta_ip')
        self.float_fields = ['latitude', 'longitude']
        self.array_fields = ['ip_subnet']
        self.int_fields = ['as_id']
        self.column_defs.extend([
            ['ip_subnet', 'Array(Tuple(IPv6,UInt8))', True],
            ['city_name', 'LowCardinality(Nullable(String))', True],
            ['continent_name', 'LowCardinality(Nullable(String))', True],
            ['country_name', 'LowCardinality(Nullable(String))', True],
            ['country_code', 'LowCardinality(Nullable(String))', True],
            ['country_sub_name', 'LowCardinality(Nullable(String))', True],
            ['country_sub_code', 'LowCardinality(Nullable(String))', True],
            ['latitude', 'Nullable(Float64)', True],
            ['longitude', 'Nullable(Float64)', True],
            ['as_id', 'Nullable(UInt32)', True],
            ['as_ref', 'Nullable(String)', True]
        ])
        self.val_id_field = ['id']
        self.required_fields = [['id'], ['ip_subnet']]

        # Build a ClickHouse dictionary for IP -> geolocation/AS lookups. An
        # IP_TRIE() dictionary can't be sourced directly from this table since
        # ip_subnet is an
        # Array(Tuple(IPv6,UInt8)) column -- IP_TRIE dictionaries need one row per
        # prefix. So a materialized view first flattens ip_subnet (one row per
        # prefix, ARRAY JOIN) into a plain table, and the dictionary is sourced from
        # that flattened table instead. Both are declared here and created
        # automatically by ClickHouseBatcher at startup -- no manual SQL script
        # required, and the dictionary's LIFETIME keeps it refreshed as new geo
        # records flow in.
        self.dictionary_enabled = os.getenv('CLICKHOUSE_IP_GEO_DICTIONARY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        if self.dictionary_enabled:
            prefix_mv = IPGeoPrefixMaterializedView(source_table_name=self.table)
            self.materialized_views.append(prefix_mv)
            self.ch_dictionaries.append(IPGeoDictionary(prefix_mv.table))

    def build_metadata_fields(self, value: dict) -> dict:
        # Call parent to build initial record
        formatted_record = super().build_metadata_fields(value)

        #make sure that each item in ip_subnet is a tuple of (ip, prefix length) where
        #first element is string and second is int. The prefix length is offset for
        #IP_TRIE storage via trie_prefix_length() (see base.py) -- an IPv4 CIDR's
        #real-world prefix length has to be shifted by 96 to stay correct once
        #embedded in this column's IPv6 space, or every IPv4 lookup against the
        #dictionary built from this data spuriously matches every stored prefix.
        ip_subnets = []
        for item in formatted_record.get('ip_subnet', []):
            if isinstance(item, (list, tuple)) and len(item) == 2:
                try:
                    ip = str(item[0])
                    ip_subnets.append((ip, trie_prefix_length(ip, int(item[1]))))
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid ip_subnet item: {item}")
        formatted_record['ip_subnet'] = ip_subnets

        # lookup ref fields from cacher
        cached_as_info = self.pipeline.cacher("clickhouse").lookup("meta_as", formatted_record.get('as_id', None))
        if cached_as_info:
            formatted_record["as_ref"] = cached_as_info.get(self.db_ref_field, None)
        else:
            formatted_record["as_ref"] = None

        return formatted_record


class IPGeoPrefixMaterializedView(BaseClickHouseMaterializedViewMixin):
    """Flattens meta_ip.ip_subnet (one array per geo/AS block record) into one row per
    prefix, so it can be used as the SOURCE table for an IP_TRIE() dictionary.
    """

    def __init__(self, source_table_name: str = "", agg_window: str = ""):
        super().__init__(source_table_name, agg_window)
        self.table = os.getenv('CLICKHOUSE_IP_GEO_PREFIX_TABLE', 'meta_ip_geo_prefix')
        #plain (non-nullable) columns: ClickHouse's IP_TRIE dictionary layout doesn't
        #support Nullable attributes at all (UNSUPPORTED_METHOD: "array or nullable
        #attributes not supported for dictionary of type Trie"), so NULLs are
        #coalesced below in mv_select_query instead of being passed through as
        #Nullable. latitude/longitude coalesce to NaN rather than 0 -- (0, 0) is a
        #real point in the Gulf of Guinea, so it would silently misrepresent an
        #unknown location as a specific one; NaN is unambiguous.
        self.column_defs = [
            ['prefix', 'String', True],
            ['city_name', 'String', True],
            ['continent_name', 'String', True],
            ['country_name', 'String', True],
            ['country_code', 'String', True],
            ['country_sub_name', 'String', True],
            ['country_sub_code', 'String', True],
            ['latitude', 'Float64', True],
            ['longitude', 'Float64', True],
            ['as_id', 'UInt32', True],
            ['as_ref', 'String', True],
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
                coalesce(city_name, '') AS city_name,
                coalesce(continent_name, '') AS continent_name,
                coalesce(country_name, '') AS country_name,
                coalesce(country_code, '') AS country_code,
                coalesce(country_sub_name, '') AS country_sub_name,
                coalesce(country_sub_code, '') AS country_sub_code,
                coalesce(latitude, nan) AS latitude,
                coalesce(longitude, nan) AS longitude,
                coalesce(as_id, 0) AS as_id,
                coalesce(as_ref, '') AS as_ref,
                id
            FROM {self.source_table_name}
            ARRAY JOIN ip_subnet AS ip_subnet_entry
        """


class IPGeoDictionary(BaseClickHouseDictionaryMixin):
    """IP_TRIE() dictionary for longest-prefix-match lookups of geolocation/AS info by
    IP, sourced from the flattened IPGeoPrefixMaterializedView table (not the raw
    meta_ip table -- see that class for why).
    """

    def __init__(self, source_table_name: str):
        super().__init__(source_table_name)
        self.dictionary_name = os.getenv('CLICKHOUSE_IP_GEO_DICTIONARY_NAME', 'meta_ip_geo_dict')
        #plain types, not Nullable -- IP_TRIE doesn't support nullable attributes, so
        #unset values arrive already coalesced upstream in
        #IPGeoPrefixMaterializedView.mv_select_query rather than as NULL
        self.column_defs = [
            ['prefix', 'String'],
            ['city_name', "String DEFAULT ''"],
            ['continent_name', "String DEFAULT ''"],
            ['country_name', "String DEFAULT ''"],
            ['country_code', "String DEFAULT ''"],
            ['country_sub_name', "String DEFAULT ''"],
            ['country_sub_code', "String DEFAULT ''"],
            ['latitude', "Float64 DEFAULT nan"],
            ['longitude', "Float64 DEFAULT nan"],
            ['as_id', "UInt32 DEFAULT 0"],
            ['as_ref', "String DEFAULT ''"],
        ]
        self.primary_keys = ['prefix']
        #miniumum and maximum lifetime in seconds
        self.lifetime_min = os.getenv('CLICKHOUSE_IP_GEO_DICTIONARY_LIFETIME_MIN', "600")
        self.lifetime_max = os.getenv('CLICKHOUSE_IP_GEO_DICTIONARY_LIFETIME_MAX', "3600")
        #set the layout, will be the full layout definition
        self.layout = "IP_TRIE()"
