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

---

## Design Notes

### Partitioning and TTL timestamp choice

BMP messages carry two timestamps: `export_time` (when the router emitted the BMP message) and `observation_time` (when the event was originally observed on the router). For route monitoring messages, `observation_time` can be arbitrarily far in the past — a router may replay its full Adj-RIB-In when a BMP session is established, meaning freshly ingested rows can carry `observation_time` values days or weeks old.

Using `observation_time` for partitioning or TTL on `data_bmp_route` would therefore cause two problems: rows with old observation times would be placed into partitions that have already been TTL-expired or compacted away, leading to data loss or unexpected behaviour; and range queries filtered on `observation_time` would fan out across many partitions rather than landing in the expected recent ones.

`export_time` is used for both the partition key (`toYYYYMMDD(export_time)`) and the TTL expression on `data_bmp_route` and `data_bmp_route_latest` because it reflects when the data arrived, which is always recent and predictable.

This concern does not apply to `data_bmp_stat`, `data_bmp_peer_up`, or `data_bmp_peer_down`. Statistics and peer-event messages are always generated in real time — a router does not replay historical stat reports or peer-up events when a BMP session is (re-)established — so `observation_time` is reliable for partitioning and TTL on those tables.

### `data_bmp_route` — primary key and ordering

The primary key for `data_bmp_route` is `(device_id, action, peer_ip, ip_prefix_range_start, ip_prefix_range_end, add_path_id, export_time)`. The ordering is chosen to make the most common access pattern — filtering by device, then peer, then prefix — index-assisted at each level:

- `device_id` first narrows to a single router, eliminating the vast majority of granules immediately.
- `peer_ip` second allows per-peer queries to skip all other peers' granules without scanning them.
- `ip_prefix_range_start` / `ip_prefix_range_end` rather than a single CIDR string supports efficient range queries (e.g. "which routes cover this address?") via primary key comparisons without per-row parsing.

`ip_prefix_length` is stored as a plain column alongside the range columns so that longest-prefix-match ordering (`ORDER BY ip_prefix_length DESC`) is cheap without recomputing it from the address pair.

### IP prefix representation

Prefixes are stored as four columns — `ip_prefix_address`, `ip_prefix_length`, `ip_prefix_range_start`, and `ip_prefix_range_end` — all pre-computed by the ingestion pipeline before insert. This redundancy is intentional:

- `(ip_prefix_range_start, ip_prefix_range_end)` in the `ORDER BY` makes both exact-match and range-based lookups index-assisted without per-query computation.
- `ip_prefix_address` and `ip_prefix_length` allow easy CIDR string reconstruction and cheap length-based sorting.
- IPv4 prefixes are stored as IPv4-mapped IPv6 addresses so both address families share a single column type and index, avoiding schema branching.

An `ip_trie` dictionary was considered for O(1) longest-prefix-match lookups. At scale — many routers each with a full internet routing table (~900 K IPv6 prefixes) — loading the entire RIB per dictionary instance is impractical: each instance consumes roughly 200–400 MB, and the total across all routers and replicas grows into tens of gigabytes. The range-column approach avoids this memory pressure entirely.

### `data_bmp_route_latest` — `AggregatingMergeTree` engine choice

`data_bmp_route_latest` uses `AggregatingMergeTree` rather than `ReplacingMergeTree`. Both engines maintain a deduplicated current-state view from an append-only log, but they have meaningfully different query-time behaviour:

- **`ReplacingMergeTree`** stores plain column values. The MV is a simple `SELECT *`. During background merges the engine keeps only the row with the highest version column per key. The problem is that at any given moment, with continuous BMP ingestion, there will be multiple unmerged parts containing duplicate rows. Every query must add `FINAL` to force deduplication at read time — a sort-merge across unmerged parts that adds latency proportional to the number of parts.

- **`AggregatingMergeTree`** stores `*State` binary blobs computed by the MV (e.g. `argMaxState(action, export_time)`). Background merges combine blobs using the matching `*Merge` combinator. Queries finalise values with `argMaxMerge(action_state)`. Crucially, **`*Merge` aggregates correctly across unmerged parts without `FINAL`**, so query correctness is guaranteed even when many parts exist.

The primary driver for choosing `AggregatingMergeTree` is query correctness without `FINAL`. The known trade-offs are:

- Attribute columns are stored as opaque binary blobs, so skip indexes (e.g. a bloom filter on `as_path_id`) and conditional TTL expressions (e.g. purging withdrawn routes by TTL) cannot be applied to them in `data_bmp_route_latest`.
- The MV and both query views must stay in sync: every attribute column requires a matched `*State` / `*Merge` pair, so adding a column means updating the MV, the target table, and both views together.

### Skip index for `as_path_id` queries

Queries filtering on AS path membership — `has(as_path_id, 513)`, `hasAny(as_path_id, [513, 1234])` — benefit from a `bloom_filter` skip index on `as_path_id`, which allows ClickHouse to skip granules that provably do not contain the target AS.

Because `data_bmp_route_latest` stores `as_path_id` as an `AggregateFunction` blob, a skip index cannot be applied there. 

### `add_path_id` in the unique key

BGP ADD-PATH (RFC 7911) allows a peer to advertise multiple paths for the same prefix simultaneously, each distinguished by an `add_path_id`. This field is included as a grouping key in `data_bmp_route_latest` so each path is tracked independently. Routers not using ADD-PATH have `add_path_id = 0` for all rows — RFC 7911 path IDs start at 1, making `0` a natural sentinel — so the schema is backwards-compatible with non-ADD-PATH deployments.

### Post-policy withdraw behaviour and stale entries

RFC 7854 does not prescribe consistent withdraw behaviour for the post-policy Adj-RIB-In, and router implementations vary significantly in practice. For the **pre-policy** RIB, BMP faithfully mirrors the raw BGP UPDATE stream: every peer withdrawal results in a corresponding BMP withdraw message, so the pre-policy view reliably tracks the full route lifecycle. For the **post-policy** RIB, the behaviour is implementation-defined:

- Some routers send post-policy withdraws only when a route is removed by a *policy change* on the router itself, not when the peer withdraws it.
- Some implementations never send post-policy withdraws at all, treating the pre-policy withdraw as sufficient.
- During BMP session establishment, routers replay their Adj-RIB-In. The pre-policy replay typically completes before the post-policy replay, leaving a window where post-policy entries exist in `data_bmp_route_latest` with no corresponding pre-policy entry.

The practical consequence is that **the post-policy view can contain stale entries that have no pre-policy counterpart** — routes that were withdrawn at the peer level but whose post-policy state was never explicitly cleared by the router. This has been observed in production.

**The correct way to query accepted and rejected routes is therefore always to compare the two views**, not to query the post-policy view in isolation:

- **Accepted routes** = routes present in both pre- and post-policy views (the pre-policy view is the authority on what the peer is currently advertising).
- **Rejected routes** = routes present in the pre-policy view but absent from the post-policy view.

Querying the post-policy view alone risks including stale routes that the peer has already withdrawn. The `LEFT SEMI JOIN` and `LEFT ANTI JOIN` patterns in the [example queries](#example-queries) below enforce this correctly.

---

## Example Queries

### Current routing table for a device

```sql
-- All active pre-policy Adj-RIB-In routes for a device
SELECT peer_ip, ip_prefix_address, ip_prefix_length, bgp_nexthop, as_path_id
FROM data_bmp_route_latest_adj_rib_in_pre_view
WHERE device_id = 'my-router'
ORDER BY peer_ip, ip_prefix_address, ip_prefix_length DESC;

-- All active post-policy Adj-RIB-In routes for a device
SELECT peer_ip, ip_prefix_address, ip_prefix_length, bgp_nexthop, as_path_id
FROM data_bmp_route_latest_adj_rib_in_post_view
WHERE device_id = 'my-router'
ORDER BY peer_ip, ip_prefix_address, ip_prefix_length DESC;
```

### Accepted routes

A route is accepted if it appears in the post-policy view — it was received pre-policy and survived inbound policy. The simplest query is therefore just the post-policy view itself:

```sql
-- Accepted routes
SELECT peer_ip, ip_prefix_address, ip_prefix_length, bgp_nexthop, as_path_id
FROM data_bmp_route_latest_adj_rib_in_post_view
WHERE device_id = 'my-router';
```

To see accepted routes with their **pre-policy attributes** (e.g. communities or next-hops before policy transformed them), use a `LEFT SEMI JOIN`. This keeps only pre-policy rows that have a matching post-policy entry, without adding any post-policy columns:

```sql
-- Accepted routes with pre-policy attributes
SELECT pre.peer_ip, pre.ip_prefix_address, pre.ip_prefix_length,
       pre.bgp_nexthop, pre.as_path_id, pre.community
FROM data_bmp_route_latest_adj_rib_in_pre_view AS pre
LEFT SEMI JOIN data_bmp_route_latest_adj_rib_in_post_view AS post
    USING (device_id, peer_ip, ip_prefix_range_start, ip_prefix_range_end, add_path_id)
WHERE pre.device_id = 'my-router';
```

### Rejected routes

A route is rejected if it exists pre-policy but has no corresponding post-policy entry. Use a `LEFT ANTI JOIN`:

```sql
-- Rejected routes (received pre-policy, dropped by inbound policy)
SELECT peer_ip, ip_prefix_address, ip_prefix_length, bgp_nexthop, as_path_id, community
FROM data_bmp_route_latest_adj_rib_in_pre_view AS pre
LEFT ANTI JOIN data_bmp_route_latest_adj_rib_in_post_view AS post
    USING (device_id, peer_ip, ip_prefix_range_start, ip_prefix_range_end, add_path_id)
WHERE pre.device_id = 'my-router';
```

> `LEFT SEMI JOIN` and `LEFT ANTI JOIN` use the right table purely as a filter — no columns from it appear in the result — and short-circuit on the first match found, making them more efficient than a `LEFT JOIN … WHERE post.col IS NULL` pattern.

### Accepted and rejected counts per peer

```sql
SELECT
    peer_ip,
    peer_hostname,
    countIf(post.ip_prefix_range_start IS NOT NULL) AS accepted,
    countIf(post.ip_prefix_range_start IS NULL)     AS rejected
FROM data_bmp_route_latest_adj_rib_in_pre_view AS pre
LEFT JOIN data_bmp_route_latest_adj_rib_in_post_view AS post
    USING (device_id, peer_ip, ip_prefix_range_start, ip_prefix_range_end, add_path_id)
WHERE pre.device_id = 'my-router'
GROUP BY peer_ip, peer_hostname
ORDER BY peer_ip;
```

### Routes containing a specific AS in the path

```sql
-- All active pre-policy routes that traverse AS 513
SELECT peer_ip, ip_prefix_address, ip_prefix_length, as_path_id
FROM data_bmp_route_latest_adj_rib_in_pre_view
WHERE device_id = 'my-router'
  AND has(as_path_id, 513);
```

### Recent peer up/down events

```sql
-- Last 24 hours of peer events for a device
SELECT observation_time, peer_ip, peer_hostname, peer_as_id, 'up' AS event
FROM data_bmp_peer_up
WHERE device_id = 'my-router'
  AND observation_time >= now() - INTERVAL 1 DAY

UNION ALL

SELECT observation_time, peer_ip, peer_hostname, peer_as_id, 'down' AS event
FROM data_bmp_peer_down
WHERE device_id = 'my-router'
  AND observation_time >= now() - INTERVAL 1 DAY

ORDER BY observation_time DESC;
```

### Route count statistics from BMP stats messages

```sql
-- Latest Adj-RIBs-In route count (metric type 7) per peer
SELECT peer_ip, peer_hostname, argMax(metric_value, observation_time) AS routes_in_adj_rib_in
FROM data_bmp_stat
WHERE device_id = 'my-router'
  AND metric_type = 7
GROUP BY peer_ip, peer_hostname
ORDER BY peer_ip;
```
