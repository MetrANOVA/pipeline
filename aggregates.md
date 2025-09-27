# Aggregates

This document records the commands used toi create materialized view aggregates.

## 5m_edge_asns

### Create Table
```
CREATE TABLE IF NOT EXISTS flow_edge_5m_asns (
            `start_ts` DateTime,
            `src_as_name` LowCardinality(Nullable(String)),
            `dst_as_name` LowCardinality(Nullable(String)),
            `policy_originator` LowCardinality(Nullable(String)),
            `policy_level` LowCardinality(Nullable(String)),
            `policy_scopes` Array(LowCardinality(String)),
            `app_name` LowCardinality(Nullable(String)),
            `bgp_as_path_name` Array(Nullable(String)),
            `bgp_comms` Array(LowCardinality(Nullable(String))),
            `bgp_ecomms` Array(LowCardinality(Nullable(String))),
            `bgp_lcomms` Array(LowCardinality(Nullable(String))),
            `bgp_local_pref` Nullable(UInt32),
            `bgp_med` Nullable(UInt32),
            `bgp_next_hop` Nullable(IPv6),
            `bgp_peer_as_dst` Nullable(UInt32),
            `bgp_peer_as_dst_name` LowCardinality(Nullable(String)),
            `device_name` LowCardinality(Nullable(String)),
            `dst_asn` UInt32,
            `dst_continent` LowCardinality(Nullable(String)),
            `dst_country_name` LowCardinality(Nullable(String)),
            `dst_esdb_ipsvc_ref` Array(Nullable(String)),
            `dst_pub_asn` Nullable(UInt32),
            `dst_region_iso_code` LowCardinality(Nullable(String)),
            `dst_region_name` LowCardinality(Nullable(String)),
            `dst_pref_org` LowCardinality(Nullable(String)),
            `dst_scireg_ref` Nullable(String),
            `ifin_ref` Nullable(String),
            `ifout_ref` Nullable(String),
            `ip_version`  Nullable(UInt8),
            `protocol` LowCardinality(Nullable(String)),
            `src_asn` UInt32,
            `src_continent` LowCardinality(Nullable(String)),
            `src_country_name` LowCardinality(Nullable(String)),
            `src_esdb_ipsvc_ref` Array(Nullable(String)),
            `src_pub_asn` Nullable(UInt32),
            `src_region_iso_code` LowCardinality(Nullable(String)),
            `src_region_name` LowCardinality(Nullable(String)),
            `src_pref_org` LowCardinality(Nullable(String)),
            `src_scireg_ref` Nullable(String),
            `traffic_class` LowCardinality(Nullable(String)),
            `flow_count` UInt32,
            `num_bits` Float64,
            `num_pkts` Float64
        )
        ENGINE = SummingMergeTree((flow_count, num_bits, num_pkts))
        PARTITION BY toYYYYMMDD(`start_ts`)
        ORDER BY (`src_asn`,`dst_asn`,`start_ts`)
        TTL start_ts + INTERVAL 5 YEAR
        SETTINGS index_granularity = 8192
```

### Materialized View
```
CREATE MATERIALIZED VIEW flow_edge_5m_asns_mv TO flow_edge_5m_asns AS
SELECT 
    toStartOfInterval(start_ts, INTERVAL 5 MINUTE) AS start_ts, 
    src_as_name, 
    dst_as_name, 
    policy_originator,
    policy_level,
    policy_scopes,
    app_name, 
    bgp_next_hop,  
    bgp_peer_as_dst,
    bgp_peer_as_dst_name,
    bgp_as_path_name,
    bgp_comms,
    bgp_lcomms,
    bgp_ecomms,
    bgp_local_pref,
    bgp_med,
    device_name, 
    dst_asn,
    dst_pub_asn,
    dst_continent,
    dst_country_name,
    dst_esdb_ipsvc_ref
    dst_region_name,
    dst_region_iso_code,
    dst_pref_org,
    dst_scireg_ref,
    ifin_ref, 
    ifout_ref, 
    ip_version, 
    protocol,
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
    count(*) AS flow_count, 
    SUM(num_bits) AS num_bits, 
    SUM(num_pkts) AS  num_pkts
FROM flow_edge_v2 
GROUP BY 
    start_ts, 
    src_as_name, 
    dst_as_name, 
    policy_originator,
    policy_level,
    policy_scopes,
    app_name,
    device_name,
    bgp_next_hop,  
    bgp_peer_as_dst,
    bgp_peer_as_dst_name,
    bgp_as_path_name,
    bgp_comms,
    bgp_lcomms,
    bgp_ecomms,
    bgp_local_pref,
    bgp_med,
    dst_asn,
    dst_pub_asn,
    dst_as_name,
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
    src_asn,
    src_pub_asn,
    src_continent,
    src_country_name,
    src_esdb_ipsvc_ref,
    src_region_name,
    src_region_iso_code,
    src_pref_org,
    src_scireg_ref,
    traffic_class
```

## 5m_ip_version 

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
    count(*) AS flow_count, 
    SUM(num_bits) AS num_bits, 
    SUM(num_pkts) AS num_pkts 
FROM flow_edge_v2 
WHERE ip_version IS NOT NULL
GROUP BY start_ts, ip_version
```

## 5m_ifaces

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
        ORDER BY (`ifin_ref`,`ifout_ref`, `start_ts`)
        SETTINGS index_granularity = 8192
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
    count(*) AS flow_count, 
    SUM(num_bits) AS num_bits, 
    SUM(num_pkts) AS num_pkts 
FROM flow_edge_v2 
WHERE ifin_ref IS NOT NULL AND ifout_ref is NOT NULL
GROUP BY start_ts, ifin_ref, ifout_ref, device_name, device_ip
```