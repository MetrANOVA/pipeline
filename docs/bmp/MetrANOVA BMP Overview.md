# BMP Pipeline Overview

## Use Cases

The BMP pipeline enables a range of routing and operational analytics:

- Query prefix matches against pre- and post-policy Adj-RIB-In to determine which routes are accepted or rejected
- Compute per-peer statistics such as total route counts and rejection rates
- Enumerate the full list of accepted vs. rejected routes, not just aggregate counts
- Visualise AS paths containing specific autonomous systems
- Monitor peer up/down events for session health and availability tracking
- Export routing data to external route analysis tools such as ROOTBEER

## Pipeline Architecture

![BMP Pipeline Overview](../images/MetrANOVA%20BMP%20Pipeline-Overview.png)

BGP Monitoring Protocol (BMP) data originates from **pmbmpd**, which forwards three categories of events to Kafka:

- **Route Monitoring** — Adj-RIB-In prefix advertisements and withdrawals
- **Peer Statistics** — periodic counters such as route counts and update rates
- **Peer Up / Down** — session establishment and teardown notifications

The **data_bmp** pipeline consumes these Kafka topics and writes each event type to its own set of ClickHouse tables, providing durable, queryable storage for all three message categories.

Once the data is in ClickHouse, the **cache_rootbeer** pipeline periodically queries the latest routing state and produces a JSON file in the format expected by [ROOTBEER](https://github.com/morrowc/bgp-infrastructure), enabling external route analysis without direct ClickHouse access.

### Adj-RIB-In Tables

![BMP Route Tables](../images/MetrANOVA%20BMP%20Pipeline-Route%20Tables.png)

The Adj-RIB-In tables are built using a combination of a **Materialized View** and **ClickHouse views**:

- The **Materialized View** (`data_bmp_route_latest`) maintains the current state of the routing table by continuously aggregating route updates as they arrive, keeping only the most recent state for each prefix per peer. Because it stores intermediate aggregation state rather than full rows, queries against it are fast and never need a deduplication pass regardless of ingestion rate.
- **Two ClickHouse views** — `data_bmp_route_latest_adj_rib_in_pre_view` and `data_bmp_route_latest_adj_rib_in_post_view` — sit on top of the materialized view and finalise the aggregation state into plain queryable columns. They expose the current pre- and post-policy Adj-RIB-In respectively, filtering out any withdrawn or deleted routes so only active routes are returned. Comparing the two views directly gives accepted routes (present post-policy) and rejected routes (present pre-policy but absent post-policy).

Together these structures give a clean, always-current view of what each peer is advertising and what inbound policy is accepting, without duplicating storage or requiring full-table scans.

For full table definitions, column descriptions, and schema details see the [BMP ClickHouse Schema](./MetrANOVA%20BMP%20ClickHouse%20Schema.md).

## Future Work

A routing table "time machine" would be achievable for the Adj-RIB-In by either:

- **Periodic snapshots** — capturing the full state of `data_bmp_route_latest` at regular intervals, or
- **Compressed update history** — storing a compact log of all updates and withdrawals and replaying them to reconstruct the table at any point in time

Either approach would allow the routing state at any past moment to be recovered, enabling more sophisticated analysis of routing changes, convergence events, and long-term trends.