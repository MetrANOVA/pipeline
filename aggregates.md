# Dictionaries and Materialized Views

This document records the commands used to create dictionaries and materialized view aggregates.

Note that any dimensions used must also be in the PRIMARY KEY or ORDER BY to working correctly for SummingMergeTree. For this reason, its nice to split the PRIMARY KEY and sorting keys so its easier to make changes over time without having to touch the primary key. See the document here for details: https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#choosing-a-primary-key-that-differs-from-the-sorting-key 

## Dictionary: meta_application_dict
This is a dictionary for looking up application names from meta_application based on protocol and port.
```
CREATE DICTIONARY meta_application_dict (
    `id` String,
    `protocol` String,
    `port_range_min` UInt16,
    `port_range_max` UInt16
)
PRIMARY KEY protocol
SOURCE(CLICKHOUSE(TABLE 'meta_application' USER 'default' PASSWORD '<password>' DB 'metranova'))
LIFETIME(MIN 600 MAX 3600)
LAYOUT(RANGE_HASHED(range_lookup_strategy 'min'))
RANGE(MIN port_range_min MAX port_range_max)
```

## Materialized View: 5m_edge_asns

### Create Table
```
CREATE TABLE IF NOT EXISTS flow_edge_5m_asns_v2 (
            `start_ts` DateTime,
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `app_name` LowCardinality(Nullable(String)),
            `bgp_as_path` Array(Nullable(UInt32)),
            `bgp_comms` Array(LowCardinality(Nullable(String))),
            `bgp_ecomms` Array(LowCardinality(Nullable(String))),
            `bgp_lcomms` Array(LowCardinality(Nullable(String))),
            `bgp_local_pref` Nullable(UInt32),
            `bgp_med` Nullable(UInt32),
            `bgp_next_hop` Nullable(IPv6),
            `bgp_peer_as_dst` Nullable(UInt32),
            `device_name` LowCardinality(Nullable(String)),
            `dst_asn` UInt32,
            `dst_scireg_ref` Nullable(String),
            `ifin_ref` Nullable(String),
            `ifout_ref` Nullable(String),
            `ip_version`  Nullable(UInt8),
            `protocol` LowCardinality(Nullable(String)),
            `src_asn` UInt32,
            `src_scireg_ref` Nullable(String),
            `flow_count` UInt32,
            `bit_count` UInt32,
            `packet_count` UInt32
        )
        ENGINE = SummingMergeTree((flow_count, num_bits, num_pkts))
        PARTITION BY toYYYYMMDD(`start_ts`)
        PRIMARY KEY (
            src_asn,
            dst_asn,
            start_ts
        )
        ORDER BY (
            src_asn,
            dst_asn,
            start_ts,
            src_as_name, 
            dst_as_name,
            ifin_ref,
            ifout_ref, 
            policy_level,
            policy_scopes,
            policy_originator,
            app_name,
            device_name,
            bgp_next_hop,  
            bgp_peer_as_dst,
            bgp_peer_as_dst_name,
            bgp_as_path,
            bgp_as_path_name,
            bgp_comms,
            bgp_lcomms,
            bgp_ecomms,
            bgp_local_pref,
            bgp_med,
            dst_pub_asn,
            dst_continent,
            dst_country_name,
            dst_esdb_ipsvc_ref,
            dst_region_name,
            dst_region_iso_code,
            dst_pref_org,
            dst_scireg_ref,
            ip_version,
            protocol, 
            src_pub_asn,
            src_continent,
            src_country_name,
            src_esdb_ipsvc_ref,
            src_region_name,
            src_region_iso_code,
            src_pref_org,
            src_scireg_ref,
            traffic_class
        )
        TTL start_ts + INTERVAL 5 YEAR
        SETTINGS index_granularity = 8192, allow_nullable_key = 1
```

### Materialized View
```
CREATE MATERIALIZED VIEW flow_edge_5m_asns_v2_mv TO flow_edge_5m_asns_v2 AS
SELECT 
    toStartOfInterval(start_ts, INTERVAL 5 MINUTE) AS start_ts, 
    policy_originator,
    policy_level,
    policy_scopes,
    app_name, 
    bgp_next_hop,  
    bgp_peer_as_dst,
    bgp_peer_as_dst_name,
    bgp_as_path,
    bgp_as_path_name,
    bgp_comms,
    bgp_lcomms,
    bgp_ecomms,
    bgp_local_pref,
    bgp_med,
    device_name,
    dst_as_name,
    dst_asn,
    dst_pub_asn,
    dst_continent,
    dst_country_name,
    dst_esdb_ipsvc_ref,
    dst_region_name,
    dst_region_iso_code,
    dst_pref_org,
    dst_scireg_ref,
    ifin_ref, 
    ifout_ref, 
    ip_version, 
    protocol,
    src_as_name,
    src_asn,
    src_pub_asn,
    src_continent,
    src_country_name,
    src_esdb_ipsvc_ref,
    src_region_name,
    src_region_iso_code,
    src_pref_org,
    src_scireg_ref,
    traffic_class,
    1 AS flow_count, 
    num_bits, 
    num_pkts
FROM flow_edge_v2 
```

## Materialized View: 5m_ip_version 

### Create Table
```
CREATE TABLE IF NOT EXISTS flow_edge_5m_ip_version (
            `start_ts` DateTime,
            `ip_version`  UInt8,
            `flow_count` UInt32,
            `num_bits` Float64,
            `num_pkts` Float64
        )
        ENGINE = SummingMergeTree((flow_count, num_bits, num_pkts))
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`ip_version`,`start_ts`)
        SETTINGS index_granularity = 8192
```

### Materialized View
```
CREATE MATERIALIZED VIEW flow_edge_5m_ip_version_mv TO flow_edge_5m_ip_version AS
SELECT 
    toStartOfInterval(start_ts, INTERVAL 5 MINUTE) AS start_ts, ip_version, 
    1 AS flow_count, 
    num_bits, 
    num_pkts
FROM flow_edge_v2
WHERE ip_version IS NOT NULL
```

## Materialized View: 5m_ifaces

### Create Table
```
CREATE TABLE IF NOT EXISTS flow_edge_5m_if (
            `start_ts` DateTime,
            `ifin_ref` String,
            `ifout_ref` String,
            `device_name` LowCardinality(Nullable(String)),
            `device_ip` Nullable(IPv6),
            `flow_count` UInt32,
            `num_bits` Float64,
            `num_pkts` Float64
        )
        ENGINE = SummingMergeTree((flow_count, num_bits, num_pkts))
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`ifin_ref`,`ifout_ref`, `device_name`, `device_ip`, `start_ts`)
        SETTINGS index_granularity = 8192, allow_nullable_key = 1
```

### Materialized View
```
CREATE MATERIALIZED VIEW flow_edge_5m_if_mv TO flow_edge_5m_if AS
SELECT 
    toStartOfInterval(start_ts, INTERVAL 5 MINUTE) AS start_ts, 
    ifin_ref,
    ifout_ref
    device_name,
    device_ip,
    1 AS flow_count, 
    num_bits, 
    num_pkts 
FROM flow_edge_v2 
WHERE ifin_ref IS NOT NULL AND ifout_ref is NOT NULL
```