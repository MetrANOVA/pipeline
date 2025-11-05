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
CREATE TABLE IF NOT EXISTS data_flow_by_edge_as_5m (
    `start_time` DateTime,
    `policy_originator` LowCardinality(Nullable(String)),
    `policy_level` LowCardinality(Nullable(String)),
    `policy_scope` Array(LowCardinality(String)),
    `ext` JSON(
        `bgp_as_path_id` Array(UInt32)
    ),
    `src_as_id` UInt32,
    `dst_as_id` UInt32,
    `device_id` LowCardinality(String),
    `application_id` LowCardinality(Nullable(String)),
    `in_interface_id` LowCardinality(Nullable(String)),
    `in_interface_ref` Nullable(String),
    `out_interface_id` LowCardinality(Nullable(String)),
    `out_interface_ref` Nullable(String),
    `ip_version` UInt8,
    `flow_count` UInt64,
    `bit_count` UInt64,
    `packet_count` UInt64
)
ENGINE = SummingMergeTree((flow_count, bit_count, packet_count))
PARTITION BY toYYYYMMDD(`start_time`)
PRIMARY KEY (
    src_as_id,
    dst_as_id,
    start_time
)
ORDER BY (
    src_as_id,
    dst_as_id,
    start_time,
    policy_originator,
    policy_level,
    policy_scope,
    ext.bgp_as_path_id,
    device_id,
    application_id,
    in_interface_id,
    in_interface_ref,
    out_interface_id,
    out_interface_ref,
    ip_version
)
TTL start_time + INTERVAL 5 YEAR
SETTINGS index_granularity = 8192, allow_nullable_key = 1
```

### Materialized View
```
CREATE MATERIALIZED VIEW data_flow_by_edge_as_5m_mv TO data_flow_by_edge_as_5m AS
SELECT
    toStartOfInterval(start_time, INTERVAL 5 MINUTE) AS start_time, 
    policy_originator,
    policy_level,
    policy_scope,
    toJSONString(
        map(
            'bgp_as_path_id', ext.bgp_as_path_id
        )
    ) as ext,
    src_as_id,
    dst_as_id,
    device_id,
    dictGetOrNull('meta_application_dict', 'id', protocol, application_port) AS application_id,
    in_interface_id,
    in_interface_ref,
    out_interface_id,
    out_interface_ref,
    ip_version,
    1 AS flow_count,
    bit_count,
    packet_count
FROM data_flow
WHERE 
    true = (SELECT edge FROM meta_interface WHERE ref=data_flow.in_interface_ref)
    OR true = (SELECT edge FROM meta_interface WHERE ref=data_flow.out_interface_ref)
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