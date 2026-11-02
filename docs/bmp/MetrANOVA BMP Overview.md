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

- The **Materialized View** (`data_bmp_route_latest`) maintains the current state of the routing table by keeping only the most recent update for each prefix per peer. This provides a fast, low-latency snapshot of what each peer is currently advertising.
- **Views** built on top of the raw route table expose a historical record of all updates and withdrawals, enabling trend analysis and investigation of routing changes over time.

Together these structures support both real-time prefix lookups and historical analysis without duplicating storage.

For full table definitions, column descriptions, and schema details see the [BMP ClickHouse Schema](./MetrANOVA%20BMP%20ClickHouse%20Schema.md).

## Future Work

A routing table "time machine" would be achievable for the Adj-RIB-In by either:

- **Periodic snapshots** — capturing the full state of `data_bmp_route_latest` at regular intervals, or
- **Compressed update history** — storing a compact log of all updates and withdrawals and replaying them to reconstruct the table at any point in time

Either approach would allow the routing state at any past moment to be recovered, enabling more sophisticated analysis of routing changes, convergence events, and long-term trends.