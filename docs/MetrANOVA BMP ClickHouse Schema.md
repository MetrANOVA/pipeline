# Data Tables

## BMP Route

### Table: `data_bmp_route`

| Column Name | Data Type | Description |
| :---- | :---- | :---- |
| `export_time` | DateTime64(3, 'UTC') | Timestamp representing the time the router exported the data |
| `observation_time` | DateTime64(3, 'UTC') | Timestamp representing the time the data was generated on the router. Note this may be further in past and is not reliable for TTL.  |
| `insert_time` | DateTime64(3, 'UTC') | The time the row was inserted into the database |
| `collector_id` | LowCardinality(String) | Identifies the collector that received the flow message |
| `policy_originator` | LowCardinality(String) | Identifies the entity that originally shared the flow record |
| `policy_level` | LowCardinality(String) | The TLP level of the data |
| `policy_scope` | Array(LowCardinality(String)) | The scope indicating with whom this record can be shared |
| `ext` | JSON(`<initial data extension definition>`) | Standard extension field for additional attributes and metrics |
| `device_id` | LowCardinality(String) | An identifier for a device in the `meta_device` table |
| `device_ref` | Nullable(String) | A versioned reference to a device in the `meta_device` table |
| `action` | LowCardinality(String) | Indicates the type of route message: `update` \- Router advertised a route (route added/replaced in RIB); `withdraw` \- Router explicitly withdrew a route (route removed from RIB); `delete` \- Peer session dropped (all routes for peer removed from RIB) |
| `peer_ip` | IPv6 | The IP address of the BMP peer. IPv4 peers are stored as IPv4-mapped IPv6 addresses. |
| `peer_ip_ref` | Nullable(String) | Reference to `meta_ip` for the peer IP address |
| `peer_hostname` | String | Reverse-DNS hostname of the peer IP. Falls back to the IP string if no PTR record is found. |
| `peer_as_id` | Nullable(UInt32) | BGP peer AS number (from `peer_asn` in the BMP message) |
| `peer_as_ref` | Nullable(String) | Reference to `meta_as` for the peer AS |
| `ip_prefix_address` | IPv6 | The network address of the IP prefix (e.g. `'10.1.1.0'` for `10.1.1.0/24`). IPv4 prefixes are stored as IPv4-mapped IPv6 addresses. |
| `ip_prefix_length` | UInt8 | The prefix length (0–128). Used when sorting results by specificity (longest-prefix-match ordering). |
| `ip_prefix_range_start` | IPv6 | The first IP address in the prefix range. Pre-computed by the ingestion pipeline. Used as a primary-key index column for efficient range and exact-prefix lookups. |
| `ip_prefix_range_end` | IPv6 | The last IP address in the prefix range. Pre-computed by the ingestion pipeline. Used together with `ip_prefix_range_start` to support range queries without per-query computation. |
| `ip_prefix_ref` | Nullable(String) | A pointer to `meta_ip` table that maps to the ip\_prefix |
| `add_path_id` | UInt32 DEFAULT 0 | BGP ADD-Path Path Identifier ([https\://tools.ietf.org/html/rfc7911\#section-3](https://tools.ietf.org/html/rfc7911#section-3)). RFC 7911 defines this as a 4-octet unsigned integer; valid path IDs start at 1, so `0` is used as a sentinel meaning "no add-path". Routers not using add-path will always have `add_path_id = 0`. |
| `is_in` | Bool | If true, indicates data from Local-RIB **NOTE: Is it from Adj-RIB-In possibly too?** |
| `is_out` | Bool | If true, indicates data from Adj-RIB-Out |
| `is_post` | Bool | If true, indicates post-policy data |
| `afi` | Nullable(UInt8) | BGP Address Family Indicator (RFC 4760 \- Multiprotocol Extensions for BGP-4) |
| `safi` | Nullable(UInt8) | BGP Subsequent Address Family Identifier (RFC 4760 \- Multiprotocol Extensions for BGP-4) |
| `ip_version` | Nullable(UInt8) | The IP version of the route. Valid values are: `4`, `6` |
| `as_path_id` | Array(UInt32) | A list of AS numbers in the path |
| `as_path_ref` | Array(Nullable(String)) | A list of pointers to the `meta_as` table for each entry in the `as_path_id` list |
| `bgp_nexthop` | Nullable(IPv6) | The IP address of the BGP next hop |
| `bgp_nexthop_ref` | Nullable(String) | A pointer to `meta_ip` table that maps to the `bgp_nexthop` |
| `community` | Array(LowCardinality(String)) | A list of standard BGP communities |
| `ext_community` | Array(LowCardinality(String)) | A list of extended BGP communities |
| `origin` | LowCardinality(Nullable(String)) | The BGP origin attribute |
| `local_pref` | Nullable(UInt32) | The BGP local preference value |
| `med` | Nullable(UInt32) | The BGP Multi-Exit Discriminator (MED) value |
| `rd` | Nullable(String) | The BGP route distinguisher |
| `mpls_label` | Nullable(UInt32) | The BGP MPLS VPN label |
| `aigp` | Nullable(UInt64) | BGP AIGP attribute ([https\://tools.ietf.org/html/rfc7311\#section-3](https://tools.ietf.org/html/rfc7311#section-3)) |
| `psid_li` | Nullable(UInt64) | BGP Prefix-SID Label Index attribute ([https\://tools.ietf.org/html/rfc8669\#section-3.1](https://tools.ietf.org/html/rfc8669#section-3.1)) |
| `path_status` | Array(LowCardinality(String)) | BMP path-marking status [https\://tools.ietf.org/html/draft-cppy-grow-bmp-path-marking-tlv\#section-2.1](https://tools.ietf.org/html/draft-cppy-grow-bmp-path-marking-tlv#section-2.1) |
| `reason_code` | Array(LowCardinality(String)) | BMP path-marking reason\_code [https\://tools.ietf.org/html/draft-cppy-grow-bmp-path-marking-tlv\#section-2.1](https://tools.ietf.org/html/draft-cppy-grow-bmp-path-marking-tlv#section-2.1) |

**Primary Key/Ordering:** `(device_id, action, peer_ip, ip_prefix_range_start, ip_prefix_range_end, add_path_id, export_time)`

**Partitioning:** `toYYYYMMDD(export_time)`

**TTL:** `export_time + INTERVAL 1 DAY` by default. `export_time` is used because router `observation_time` may be significantly delayed.

**Engine:** ReplicatedMergeTree or MergeTree (depending on deployment strategy)

### Table: `data_bmp_route_latest`

Stores partial aggregation states produced by `data_bmp_route_latest_mv`. The grouping key includes `is_post` so pre- and post-policy routes are tracked independently and can be queries via the views below.

| Column Name | Data Type | Description |
| :---- | :---- | :---- |
| `device_id` | LowCardinality(String) | Grouping key — identifies the router |
| `peer_ip` | IPv6 | Grouping key — IP address of the BMP peer |
| `peer_ip_ref` | Nullable(String) | Grouping key — reference to `meta_ip` for the peer IP address; used to join `meta_ip` for peer ASN lookups |
| `peer_hostname` | String | Grouping key — reverse-DNS hostname of the peer IP; falls back to the IP string if no PTR record found |
| `peer_as_id` | Nullable(UInt32) | Grouping key — BGP peer AS number |
| `peer_as_ref` | Nullable(String) | Grouping key — reference to `meta_as` for the peer AS |
| `ip_prefix_address` | IPv6 | Grouping key — network address of the prefix |
| `ip_prefix_length` | UInt8 | Grouping key — prefix length (0–128) |
| `ip_prefix_range_start` | IPv6 | Grouping key — first IP in the prefix range |
| `ip_prefix_range_end` | IPv6 | Grouping key — last IP in the prefix range |
| `ip_prefix_ref` | Nullable(String) | Grouping key — reference to `meta_ip` for the prefix |
| `add_path_id` | UInt32 | Grouping key — BGP ADD-Path identifier; `0` = no add-path |
| `is_post` | Bool | Grouping key — `false` = pre-policy, `true` = post-policy |
| `action_state` | AggregateFunction(argMax, LowCardinality(String), DateTime64(3, 'UTC')) | Most recent `action` value (`update`, `withdraw`, or `delete`) |
| `export_time_state` | AggregateFunction(max, DateTime64(3, 'UTC')) | Most recent export timestamp |
| `observation_time_state` | AggregateFunction(argMax, DateTime64(3, 'UTC'), DateTime64(3, 'UTC')) | Router observation timestamp belonging to the most recently exported route |
| `bgp_nexthop_state` | AggregateFunction(argMax, Nullable(IPv6), DateTime64(3, 'UTC')) | Most recent BGP next hop |
| `as_path_id_state` | AggregateFunction(argMax, Array(UInt32), DateTime64(3, 'UTC')) | Most recent AS path |
| `community_state` | AggregateFunction(argMax, Array(LowCardinality(String)), DateTime64(3, 'UTC')) | Most recent standard BGP communities |
| `ext_community_state` | AggregateFunction(argMax, Array(LowCardinality(String)), DateTime64(3, 'UTC')) | Most recent extended BGP communities |
| `origin_state` | AggregateFunction(argMax, LowCardinality(Nullable(String)), DateTime64(3, 'UTC')) | Most recent BGP origin attribute |
| `local_pref_state` | AggregateFunction(argMax, Nullable(UInt32), DateTime64(3, 'UTC')) | Most recent local preference |
| `med_state` | AggregateFunction(argMax, Nullable(UInt32), DateTime64(3, 'UTC')) | Most recent MED value |
| `rd_state` | AggregateFunction(argMax, Nullable(String), DateTime64(3, 'UTC')) | Most recent route distinguisher |
| `afi_state` | AggregateFunction(argMax, Nullable(UInt8), DateTime64(3, 'UTC')) | Most recent Address Family Indicator |
| `safi_state` | AggregateFunction(argMax, Nullable(UInt8), DateTime64(3, 'UTC')) | Most recent Subsequent Address Family Identifier |
| `ip_version_state` | AggregateFunction(argMax, Nullable(UInt8), DateTime64(3, 'UTC')) | Most recent IP version (`4` or `6`) |
| `mpls_label_state` | AggregateFunction(argMax, Nullable(UInt32), DateTime64(3, 'UTC')) | Most recent MPLS VPN label |

**Primary Key/Ordering:** `(device_id, peer_ip, peer_ip_ref, peer_hostname, peer_as_id, peer_as_ref, ip_prefix_range_start, ip_prefix_range_end, ip_prefix_ref, add_path_id, is_post)`

**Engine:** ReplicatedAggregatingMergeTree — ZooKeeper path `/clickhouse/tables/{shard}/data_bmp_route_latest`

**Settings:** `allow_nullable_key = 1` — required because `peer_ip_ref` and `ip_prefix_ref` are `Nullable(String)` columns in the primary key.

---

### Materialized View: `data_bmp_route_latest_mv`

Fires on every insert into `data_bmp_route`. Filters to all Adj-RIB-In rows (`is_in = true`, both pre- and post-policy) and writes `*State` binary blobs into `data_bmp_route_latest`. The MV fires locally on each shard; replication propagates the state blobs to other replicas.

**Source table:** `data_bmp_route`

**Target table:** `data_bmp_route_latest`

**Filter:** `WHERE is_in = true`

**Aggregation:** `argMaxState(column, export_time)` for route attributes; `maxState(export_time)` for the export timestamp; `argMaxState(observation_time, export_time)` preserves the router observation timestamp belonging to that export.

---

### View: `data_bmp_route_latest_adj_rib_in_pre_view`

Finalizes aggregation states from `data_bmp_route_latest` and exposes the current **pre-policy** Adj-RIB-In state. Only active routes are returned — withdrawn and deleted routes are excluded via `HAVING action = 'update'`.

**Source table:** `data_bmp_route_latest`

**Filter:** `WHERE is_post = false`

| Column Name | Data Type | Description |
| :---- | :---- | :---- |
| `device_id` | LowCardinality(String) | Identifies the router |
| `peer_ip` | IPv6 | IP address of the BMP peer |
| `peer_ip_ref` | Nullable(String) | Reference to `meta_ip` for the peer IP address |
| `peer_hostname` | String | Reverse-DNS hostname of the peer IP; falls back to the IP string |
| `peer_as_id` | Nullable(UInt32) | BGP peer AS number |
| `peer_as_ref` | Nullable(String) | Reference to `meta_as` for the peer AS |
| `ip_prefix_address` | IPv6 | Network address of the prefix |
| `ip_prefix_length` | UInt8 | Prefix length (0–128) |
| `ip_prefix_range_start` | IPv6 | First IP in the prefix range |
| `ip_prefix_range_end` | IPv6 | Last IP in the prefix range |
| `ip_prefix_ref` | Nullable(String) | Reference to `meta_ip` for the prefix |
| `add_path_id` | UInt32 | BGP ADD-Path identifier; `0` = no add-path |
| `action` | LowCardinality(String) | Most recent action (always `update` — withdrawn routes are excluded) |
| `export_time` | DateTime64(3, 'UTC') | Timestamp when the router exported the most recent route update |
| `observation_time` | DateTime64(3, 'UTC') | Timestamp of the most recent route update |
| `bgp_nexthop` | Nullable(IPv6) | BGP next hop at time of most recent update |
| `as_path_id` | Array(UInt32) | AS path at time of most recent update |
| `community` | Array(LowCardinality(String)) | Standard BGP communities at time of most recent update |
| `ext_community` | Array(LowCardinality(String)) | Extended BGP communities at time of most recent update |
| `origin` | LowCardinality(Nullable(String)) | BGP origin attribute at time of most recent update |
| `local_pref` | Nullable(UInt32) | Local preference at time of most recent update |
| `med` | Nullable(UInt32) | MED value at time of most recent update |
| `rd` | Nullable(String) | Route distinguisher at time of most recent update |
| `afi` | Nullable(UInt8) | Address Family Indicator |
| `safi` | Nullable(UInt8) | Subsequent Address Family Identifier |
| `ip_version` | Nullable(UInt8) | IP version (`4` or `6`) |
| `mpls_label` | Nullable(UInt32) | MPLS VPN label at time of most recent update |

---

### View: `data_bmp_route_latest_adj_rib_in_post_view`

Identical in structure to `data_bmp_route_latest_adj_rib_in_pre_view` but exposes the current **post-policy** Adj-RIB-In state. A route present here was accepted by inbound policy; a route present pre-policy but absent post-policy was rejected.

**Source table:** `data_bmp_route_latest`

**Filter:** `WHERE is_post = true`

| Column Name | Data Type | Description |
| :---- | :---- | :---- |
| `device_id` | LowCardinality(String) | Identifies the router |
| `peer_ip` | IPv6 | IP address of the BMP peer |
| `peer_ip_ref` | Nullable(String) | Reference to `meta_ip` for the peer IP address |
| `peer_hostname` | String | Reverse-DNS hostname of the peer IP; falls back to the IP string |
| `peer_as_id` | Nullable(UInt32) | BGP peer AS number |
| `peer_as_ref` | Nullable(String) | Reference to `meta_as` for the peer AS |
| `ip_prefix_address` | IPv6 | Network address of the prefix |
| `ip_prefix_length` | UInt8 | Prefix length (0–128) |
| `ip_prefix_range_start` | IPv6 | First IP in the prefix range |
| `ip_prefix_range_end` | IPv6 | Last IP in the prefix range |
| `ip_prefix_ref` | Nullable(String) | Reference to `meta_ip` for the prefix |
| `add_path_id` | UInt32 | BGP ADD-Path identifier; `0` = no add-path |
| `action` | LowCardinality(String) | Most recent action (always `update` — withdrawn routes are excluded) |
| `export_time` | DateTime64(3, 'UTC') | Timestamp when the router exported the most recent route update |
| `observation_time` | DateTime64(3, 'UTC') | Timestamp of the most recent route update |
| `bgp_nexthop` | Nullable(IPv6) | BGP next hop at time of most recent update |
| `as_path_id` | Array(UInt32) | AS path at time of most recent update |
| `community` | Array(LowCardinality(String)) | Standard BGP communities at time of most recent update |
| `ext_community` | Array(LowCardinality(String)) | Extended BGP communities at time of most recent update |
| `origin` | LowCardinality(Nullable(String)) | BGP origin attribute at time of most recent update |
| `local_pref` | Nullable(UInt32) | Local preference at time of most recent update |
| `med` | Nullable(UInt32) | MED value at time of most recent update |
| `rd` | Nullable(String) | Route distinguisher at time of most recent update |
| `afi` | Nullable(UInt8) | Address Family Indicator |
| `safi` | Nullable(UInt8) | Subsequent Address Family Identifier |
| `ip_version` | Nullable(UInt8) | IP version (`4` or `6`) |
| `mpls_label` | Nullable(UInt32) | MPLS VPN label at time of most recent update |
---

## `data_bmp_stat`

Stores individual counter observations from **BMP Statistics Report** messages (BMP message type 1) emitted by routers and collected by pmacct/pbmpd.  A single statistics report message from a router contains multiple counters; each counter becomes one row in this table.

**Source**: pmacct/pbmpd `bmp_msg_type: "stats"` JSON messages.

### Primary Key & Ordering

```
(device_id, peer_ip, metric_type, observation_time)
```

One row per (device, peer, IANA metric type, report timestamp).  The primary key allows efficient range scans for a single metric type across time for any device/peer combination.

### Default Table Settings

| Setting | Default | Environment variable |
|---|---|---|
| Table name | `data_bmp_stat` | `CLICKHOUSE_BMP_STAT_TABLE` |
| Partition by | `toYYYYMMDD(observation_time)` | `CLICKHOUSE_BMP_STAT_PARTITION_BY` |
| TTL | `observation_time + INTERVAL 7 DAY` | `CLICKHOUSE_BMP_STAT_TTL` / `CLICKHOUSE_BMP_STAT_TTL_COLUMN` |

### Columns

| Column | Type | Description |
|---|---|---|
| `observation_time` | DateTime64(3, 'UTC') | Timestamp when the BMP statistics message was generated on the router (maps to pbmpd `timestamp`) |
| `insert_time` | DateTime64(3, 'UTC') | Timestamp when the row was inserted into ClickHouse |
| `collector_id` | LowCardinality(String) | Identifier of the MetrANOVA collector that processed the message |
| `policy_originator` | LowCardinality(String) | Policy originator label |
| `policy_level` | LowCardinality(String) | TLP/policy level |
| `policy_scope` | Array(LowCardinality(String)) | Policy scope |
| `ext` | JSON | Extension fields |
| `device_id` | LowCardinality(String) | IP address of the BMP router peering to pmacct (maps to pbmpd `bmp_router`) |
| `device_ref` | Nullable(String) | Reference to `meta_device` for the router |
| `peer_ip` | IPv6 | BGP peer IP address (IPv4-mapped to IPv6 where needed; maps to pbmpd `peer_ip`) |
| `peer_ip_ref` | Nullable(String) | Reference to `meta_ip` for the peer IP |
| `peer_hostname` | String | Reverse-DNS hostname of the peer IP; falls back to the IP string if no PTR record found |
| `peer_as_id` | Nullable(UInt32) | BGP peer AS number |
| `peer_as_ref` | Nullable(String) | Reference to `meta_as` for the peer AS |
| `peer_type` | Nullable(UInt8) | IANA BMP peer type code |
| `bgp_id` | Nullable(IPv6) | BGP router ID of the remote peer (maps to pbmpd `bgp_id`) |
| `rd` | Nullable(String) | BGP Route Distinguisher for the peer session |
| `is_loc` | Bool | `true` if this metric is for Loc-RIB data |
| `is_filtered` | Bool | `true` if this metric is for filtered Loc-RIB data (used together with `is_loc`) |
| `metric_type` | UInt16 | IANA BMP statistics type code (see [IANA BMP Parameters](https://www.iana.org/assignments/bmp-parameters/bmp-parameters.xhtml#statistics-types) and RFC 7854 §4.8) |
| `metric_type_str` | LowCardinality(String) | Human-readable description of the metric type (as reported by pmacct) |
| `metric_value` | UInt64 | Raw metric value; accommodates both 32-bit counters and 64-bit gauges per RFC 7854 |

### Common `metric_type` Values (RFC 7854 §4.8)

| Type | Description |
|---|---|
| 0 | Prefixes rejected by inbound policy |
| 1 | Duplicate prefix advertisements (withdrawn) |
| 2 | Duplicate withdrawals |
| 3 | Updates invalidated — CLUSTER_LIST loop |
| 4 | Updates invalidated — AS_PATH loop |
| 5 | Updates invalidated — ORIGINATOR_ID |
| 6 | Updates invalidated — AS_CONFED loop |
| 7 | Routes in Adj-RIBs-In |
| 8 | Routes in Loc-RIB |
| 9–10 | Per-AFI/SAFI Adj-RIBs-In / Loc-RIB counts |
| 11 | Updates subjected to treatment as withdrawal |
| 12 | Prefixes subjected to treatment as withdrawal |
| 13 | Duplicate update messages |
| 14 | Routes in Adj-RIBs-Out |
| 15 | Routes in per-AFI/SAFI Adj-RIBs-Out |

---

## `data_bmp_peer_up`

Records **BMP Peer Up** (message type 3) events reported by pmacct/pbmpd.  Each row captures one peer session establishment, including the local side of the BGP session from the BMP peer-up TLV.

### Primary Key

```
(device_id, peer_ip, observation_time)
```

### Table Settings

| Setting      | Default                        | Env Variable                            |
|--------------|--------------------------------|-----------------------------------------|
| Table name   | `data_bmp_peer_up`             | `CLICKHOUSE_BMP_PEER_UP_TABLE`          |
| TTL          | `90 DAY`                       | `CLICKHOUSE_BMP_PEER_UP_TTL`            |
| TTL column   | `observation_time`             | `CLICKHOUSE_BMP_PEER_UP_TTL_COLUMN`     |
| Partition by | `toYYYYMMDD(observation_time)` | `CLICKHOUSE_BMP_PEER_UP_PARTITION_BY`   |

### Columns

| Column             | Type                     | Description                                                                                                              |
|--------------------|--------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `observation_time` | `DateTime64(3, 'UTC')`   | Timestamp from pbmpd `timestamp` — when the peer came up on the router.                                                  |
| `insert_time`      | `DateTime64(3, 'UTC')`   | Time the row was inserted into ClickHouse.                                                                               |
| `collector_id`     | `LowCardinality(String)` | Identifies the pmacct/pbmpd collector (from `label`).                                                                    |
| `policy_originator`| `Nullable(String)`       | Policy originator (from pipeline metadata).                                                                              |
| `policy_level`     | `Nullable(String)`       | Policy level (from pipeline metadata).                                                                                   |
| `policy_scope`     | `Nullable(String)`       | Policy scope (from pipeline metadata).                                                                                   |
| `ext`              | `Nullable(String)`       | Extension / extra metadata (JSON).                                                                                       |
| `device_id`        | `LowCardinality(String)` | Router IP address or resolved device identifier.                                                                         |
| `device_ref`       | `Nullable(String)`       | Resolved reference string for the device (from MetrANOVA device cache).                                                  |
| `peer_ip`          | `IPv6`                   | BGP peer IP address (IPv4-mapped to IPv6 where applicable).                                                              |
| `peer_ip_ref`      | `Nullable(String)`       | Resolved reference string for the peer IP.                                                                               |
| `peer_hostname`    | `String`                 | Reverse-DNS hostname of the peer IP; falls back to the IP string if no PTR record found.                                 |
| `peer_as_id`       | `Nullable(UInt32)`       | BGP peer AS number (from `peer_asn`).                                                                                    |
| `peer_as_ref`      | `Nullable(String)`       | Resolved reference string for the peer AS.                                                                               |
| `peer_type`        | `Nullable(UInt8)`        | BMP peer type code ([IANA](https://www.iana.org/assignments/bmp-parameters/bmp-parameters.xhtml#peer-types)).            |
| `bgp_id`           | `Nullable(IPv6)`         | BGP router ID of the remote peer from the BGP OPEN message.                                                              |
| `rd`               | `Nullable(String)`       | Route Distinguisher ([RFC 7854 §4.2](https://tools.ietf.org/html/rfc7854#section-4.2)).                                  |
| `local_as`         | `Nullable(UInt32)`       | Local BGP AS number.                                                                                                     |
| `local_port`       | `Nullable(UInt16)`       | Local TCP port of the BGP session.                                                                                       |
| `remote_port`      | `Nullable(UInt16)`       | Remote TCP port of the BGP session.                                                                                      |
| `local_ip`         | `Nullable(IPv6)`         | Local IP address the BGP session was established from.                                                                   |

---

## `data_bmp_peer_down`

Records **BMP Peer Down** (message type 2) events reported by pmacct/pbmpd.  Each row captures one peer session termination, including the reason code from the BMP message.

### Primary Key

```
(device_id, peer_ip, observation_time)
```

### Table Settings

| Setting      | Default                        | Env Variable                              |
|--------------|--------------------------------|-------------------------------------------|
| Table name   | `data_bmp_peer_down`           | `CLICKHOUSE_BMP_PEER_DOWN_TABLE`          |
| TTL          | `90 DAY`                       | `CLICKHOUSE_BMP_PEER_DOWN_TTL`            |
| TTL column   | `observation_time`             | `CLICKHOUSE_BMP_PEER_DOWN_TTL_COLUMN`     |
| Partition by | `toYYYYMMDD(observation_time)` | `CLICKHOUSE_BMP_PEER_DOWN_PARTITION_BY`   |

### Columns

| Column             | Type                     | Description                                                                                                                              |
|--------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `observation_time` | `DateTime64(3, 'UTC')`   | Timestamp from pbmpd `timestamp` — when the peer went down on the router.                                                                |
| `insert_time`      | `DateTime64(3, 'UTC')`   | Time the row was inserted into ClickHouse.                                                                                               |
| `collector_id`     | `LowCardinality(String)` | Identifies the pmacct/pbmpd collector (from `label`).                                                                                    |
| `policy_originator`| `Nullable(String)`       | Policy originator (from pipeline metadata).                                                                                              |
| `policy_level`     | `Nullable(String)`       | Policy level (from pipeline metadata).                                                                                                   |
| `policy_scope`     | `Nullable(String)`       | Policy scope (from pipeline metadata).                                                                                                   |
| `ext`              | `Nullable(String)`       | Extension / extra metadata (JSON).                                                                                                       |
| `device_id`        | `LowCardinality(String)` | Router IP address or resolved device identifier.                                                                                         |
| `device_ref`       | `Nullable(String)`       | Resolved reference string for the device (from MetrANOVA device cache).                                                                  |
| `peer_ip`          | `IPv6`                   | BGP peer IP address (IPv4-mapped to IPv6 where applicable).                                                                              |
| `peer_ip_ref`      | `Nullable(String)`       | Resolved reference string for the peer IP.                                                                                               |
| `peer_hostname`    | `String`                 | Reverse-DNS hostname of the peer IP; falls back to the IP string if no PTR record found.                                                 |
| `peer_as_id`       | `Nullable(UInt32)`       | BGP peer AS number (from `peer_asn`).                                                                                                    |
| `peer_as_ref`      | `Nullable(String)`       | Resolved reference string for the peer AS.                                                                                               |
| `peer_type`        | `Nullable(UInt8)`        | BMP peer type code ([IANA](https://www.iana.org/assignments/bmp-parameters/bmp-parameters.xhtml#peer-types)).                            |
| `rd`               | `Nullable(String)`       | Route Distinguisher ([RFC 7854 §4.2](https://tools.ietf.org/html/rfc7854#section-4.2)).                                                  |
| `reason_type`      | `Nullable(UInt8)`        | IANA peer-down reason code ([IANA](https://www.iana.org/assignments/bmp-parameters/bmp-parameters.xhtml#peer-down-reason-codes)).         |
| `reason_str`       | `Nullable(String)`       | Human-readable description of the peer-down reason.                                                                                      |
