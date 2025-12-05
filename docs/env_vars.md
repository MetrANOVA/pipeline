# Environment Variables

This document describes all environment variables used by the MetrANOVA Pipeline system. Variables are organized by component.

## General Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DEBUG` | `false` | Enable debug logging. Set to `true` or `1` to enable |
| `PIPELINE_YAML` | (none) | Path to YAML pipeline configuration file. Required if not passed via `--pipeline` argument |

## Docker Compose Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `PIPELINE_ENV_FILE` | `.env` | Path to environment file to load for the generic `pipeline` service |
| `PIPELINE_REPLICAS` | `1` | Number of pipeline replicas to deploy |

## ClickHouse Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse server hostname |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse server port (8123 for HTTP, 9440 for HTTPS) |
| `CLICKHOUSE_DATABASE` | `default` | ClickHouse database name |
| `CLICKHOUSE_USERNAME` | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | (empty) | ClickHouse password |
| `CLICKHOUSE_SECURE` | `false` | Enable HTTPS connection |
| `CLICKHOUSE_CLUSTER_NAME` | (none) | ClickHouse cluster name for distributed operations |
| `CLICKHOUSE_SKIP_DB_CREATE` | `false` | Skip automatic database creation on startup |

## ClickHouse Table Names

### Data Tables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_FLOW_TABLE` | `data_flow` | Network flow data table |
| `CLICKHOUSE_IF_TRAFFIC_TABLE` | `data_interface_traffic` | Interface traffic statistics table |
| `CLICKHOUSE_RAW_KAFKA_TABLE` | `data_kafka_message` | Raw Kafka messages table |

### Metadata Tables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_IF_METADATA_TABLE` | `meta_interface` | Interface metadata table |
| `CLICKHOUSE_DEVICE_METADATA_TABLE` | `meta_device` | Device metadata table |
| `CLICKHOUSE_ORGANIZATION_METADATA_TABLE` | `meta_organization` | Organization metadata table |
| `CLICKHOUSE_CIRCUIT_METADATA_TABLE` | `meta_circuit` | Circuit metadata table |
| `CLICKHOUSE_AS_METADATA_TABLE` | `meta_as` | Autonomous System metadata table |
| `CLICKHOUSE_IP_METADATA_TABLE` | `meta_ip` | IP address metadata table |
| `CLICKHOUSE_APPLICATION_METADATA_TABLE` | `meta_application` | Application metadata table |
| `CLICKHOUSE_SCIREG_METADATA_TABLE` | `meta_ip_scireg` | Science Registry IP metadata table |
| `CLICKHOUSE_SCINET_METADATA_TABLE` | `meta_ip_scinet` | SCINet booth metadata table |

## ClickHouse Writer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_BATCH_SIZE` | `1000` | Number of records to batch before inserting |
| `CLICKHOUSE_BATCH_TIMEOUT` | `30.0` | Maximum seconds to wait before flushing batch |
| `CLICKHOUSE_FLUSH_INTERVAL` | `0.1` | Interval in seconds between flush checks |

## ClickHouse Table Configuration

### Replication Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_REPLICATION` | `false` | Enable table replication |
| `CLICKHOUSE_REPLICA_PATH` | `/clickhouse/tables/{shard}/{database}/{table}` | ZooKeeper path for replicated tables |
| `CLICKHOUSE_REPLICA_NAME` | `{replica}` | Replica name identifier |

### Table TTL Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_FLOW_TTL` | `30 DAY` | TTL for flow data table |
| `CLICKHOUSE_FLOW_TTL_COLUMN` | `start_time` | Column to use for flow TTL |
| `CLICKHOUSE_IF_TRAFFIC_TTL` | `180 DAY` | TTL for interface traffic table |
| `CLICKHOUSE_IF_TRAFFIC_TTL_COLUMN` | `start_time` | Column to use for interface traffic TTL |
| `CLICKHOUSE_METADATA_TTL` | (none) | TTL for metadata tables |

### Table Partitioning

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_FLOW_PARTITION_BY` | `toYYYYMMDD(start_time)` | Partition expression for flow table |
| `CLICKHOUSE_IF_TRAFFIC_PARTITION_BY` | `toYYYYMMDD(start_time)` | Partition expression for interface traffic table |

## ClickHouse Flow Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_FLOW_TYPE` | `unknown` | Flow data type identifier |
| `CLICKHOUSE_FLOW_EXTENSIONS` | (none) | Comma-separated list of flow extensions to enable (e.g., `bgp,ipv4,ipv6,mpls`) |
| `CLICKHOUSE_FLOW_IP_REF_EXTENSIONS` | (none) | Comma-separated list of IP reference extensions (e.g., `scireg`) |
| `CLICKHOUSE_FLOW_POLICY_AUTO_SCOPES` | `true` | Automatically determine policy scopes from BGP communities |
| `CLICKHOUSE_FLOW_POLICY_COMMUNITY_SCOPE_MAP` | (none) | Map BGP communities to policy scopes (format: `community:scope,community:scope`) |

## ClickHouse Metadata Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_METADATA_FORCE_UPDATE` | `false` | Force metadata updates even if no changes detected |
| `CLICKHOUSE_AS_METADATA_EXTENSIONS` | (none) | Comma-separated list of AS metadata extensions (e.g., `peeringdb`) |

## ClickHouse Processor Policy Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_POLICY_ORIGINATOR` | `unknown` | Default policy originator identifier |
| `CLICKHOUSE_POLICY_LEVEL` | `tlp:amber` | Default Traffic Light Protocol (TLP) level |
| `CLICKHOUSE_POLICY_SCOPE` | (none) | Default policy scope |

## ClickHouse Metric Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_METRIC_RESOURCE_NAME` | (none) | Resource name for metric identification |
| `METRANOVA_IF_TRAFFIC_INTERVAL` | `30` | Interface traffic collection interval in seconds |

## ClickHouse Consumer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_CONSUMER_UPDATE_INTERVAL` | `-1` | Seconds between consumer updates (-1 to disable periodic updates) |
| `CLICKHOUSE_CONSUMER_TABLES` | (empty) | Comma-separated list of tables to consume. Supports key specifications (e.g., `table:key1:key2` or `table:@special_key`) |

## ClickHouse Cacher Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_CACHER_TABLES` | (empty) | Comma-separated list of tables to cache. Supports key specifications (e.g., `table:key1:key2:key3`) |
| `CLICKHOUSE_CACHER_MAX_SIZE` | `100000000` | Maximum number of cache entries (100 million) |
| `CLICKHOUSE_CACHER_MAX_TTL` | `86400` | Cache entry TTL in seconds (1 day) |
| `CLICKHOUSE_CACHER_REFRESH_INTERVAL` | `600` | Seconds between cache refresh operations |

## Kafka Consumer Settings

### Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated list of Kafka broker addresses |
| `KAFKA_TOPIC` | `metranova_flow` | Kafka topic to consume |
| `KAFKA_CONSUMER_GROUP` | `ch-writer-group` | Consumer group ID |

### SSL/TLS Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SSL_CA_LOCATION` | `/app/conf/certificates/ca-cert` | Path to CA certificate file |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | `/app/conf/certificates/client-cert` | Path to client certificate file |
| `KAFKA_SSL_KEY_LOCATION` | `/app/conf/certificates/client-key` | Path to client private key file |
| `KAFKA_SSL_KEY_PASSWORD` | (none) | Password for client private key |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | `https` | SSL endpoint verification algorithm |

### Consumer Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Offset reset policy (`earliest` or `latest`) |
| `KAFKA_ENABLE_AUTO_COMMIT` | `true` | Enable automatic offset commits |
| `KAFKA_AUTO_COMMIT_INTERVAL_MS` | `5000` | Auto commit interval in milliseconds |
| `KAFKA_SESSION_TIMEOUT_MS` | `30000` | Session timeout in milliseconds |
| `KAFKA_HEARTBEAT_INTERVAL_MS` | `10000` | Heartbeat interval in milliseconds |
| `KAFKA_MAX_POLL_INTERVAL_MS` | `300000` | Maximum poll interval in milliseconds |
| `KAFKA_FETCH_MIN_BYTES` | `1` | Minimum bytes to fetch per request |
| `KAFKA_FETCH_MAX_BYTES` | `52428800` | Maximum bytes to fetch per request (50MB) |

## Redis Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_DB` | `0` | Redis database number |
| `REDIS_PASSWORD` | (none) | Redis password |

## Redis Consumer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_CONSUMER_UPDATE_INTERVAL` | `-1` | Seconds between consumer updates (-1 to disable) |
| `REDIS_CONSUMER_TABLES` | (empty) | Comma-separated list of Redis hash tables to consume |

## Redis Processor Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_IF_METADATA_TABLE` | `meta_interface_cache` | Interface metadata cache table name |
| `REDIS_IF_METADATA_EXPIRES` | `86400` | Interface metadata cache expiration in seconds (1 day) |
| `REDIS_TELEGRAF_LOOKUP_TABLE_EXPIRES` | `86400` | Telegraf lookup table expiration in seconds (1 day) |

## HTTP Consumer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_CONSUMER_ENV_PREFIX` | (empty) | Environment variable prefix for HTTP consumer (allows multiple HTTP consumers) |
| `HTTP_CONSUMER_UPDATE_INTERVAL` | `-1` | Seconds between HTTP fetch operations (-1 to disable) |
| `HTTP_CONSUMER_URLS` | (empty) | Comma-separated list of URLs to fetch |
| `HTTP_TIMEOUT` | `30` | HTTP request timeout in seconds |
| `HTTP_SSL_VERIFY` | `false` | Enable SSL certificate verification |
| `HTTP_HEADERS` | (empty) | Additional HTTP headers (format: `Header1:Value1,Header2:Value2`) |

## File Consumer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `FILE_CONSUMER_ENV_PREFIX` | (empty) | Environment variable prefix for file consumer (allows multiple file consumers) |
| `FILE_CONSUMER_UPDATE_INTERVAL` | `-1` | Seconds between file reads (-1 to disable periodic reading) |
| `FILE_CONSUMER_PATHS` | (empty) | Comma-separated list of file paths to read |
| `FILE_PROCESSORS` | (empty) | Comma-separated list of file processor classes |

## File Writer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `PICKLE_FILE_DIRECTORY` | `caches` | Directory for pickle file output |
| `IP_FILE_USE_SIMPLE_FILE_NAME` | `true` | Use simplified file naming for IP cache files |

## IP Cacher Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `IP_CACHER_TABLES` | (empty) | Comma-separated list of tables to load into IP trie cache |
| `IP_CACHER_DIR` | `caches` | Directory containing IP trie cache files |
| `IP_CACHER_REFRESH_INTERVAL` | `600` | Seconds between IP cache refresh operations |

## CAIDA Organization/AS Metadata Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `CAIDA_ORG_AS_CONSUMER_UPDATE_INTERVAL` | `-1` | Seconds between CAIDA data updates |
| `CAIDA_ORG_AS_CONSUMER_AS_TABLE` | `meta_as` | AS metadata table name |
| `CAIDA_ORG_AS_CONSUMER_ORG_TABLE` | `meta_organization` | Organization metadata table name |
| `CAIDA_ORG_AS_CONSUMER_AS2ORG_FILE` | `/app/caches/caida_as_org2info.jsonl` | CAIDA AS-to-Org mapping file |
| `CAIDA_ORG_AS_CONSUMER_PEERINGDB_FILE` | `/app/caches/caida_peeringdb.json` | PeeringDB data file |
| `CAIDA_ORG_AS_CONSUMER_CUSTOM_AS_FILE` | (none) | Custom AS additions YAML file |
| `CAIDA_ORG_AS_CONSUMER_CUSTOM_ORG_FILE` | (none) | Custom organization additions YAML file |

## IP Geolocation Metadata Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `IP_GEO_CSV_CONSUMER_UPDATE_INTERVAL` | `-1` | Seconds between IP geolocation data updates |
| `IP_GEO_CSV_CONSUMER_TABLE` | `meta_ip` | IP metadata table name |
| `IP_GEO_CSV_CONSUMER_ASN_FILES` | `/app/caches/ip_geo_asn.csv` | Comma-separated list of ASN block CSV files |
| `IP_GEO_CSV_CONSUMER_LOCATION_FILES` | `/app/caches/ip_geo_location.csv` | Comma-separated list of location CSV files |
| `IP_GEO_CSV_CONSUMER_IP_BLOCK_FILES` | `/app/caches/ip_geo_ip_blocks.csv` | Comma-separated list of IP block CSV files |
| `IP_GEO_CSV_CONSUMER_CUSTOM_IP_FILE` | (none) | Custom IP additions YAML file |

## Telegraf Processor Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `TELEGRAF_IFMIB_NAME` | `snmp_if` | Telegraf measurement name for IF-MIB data |
| `TELEGRAF_IFMIB_IFNAME_LOOKUP_TABLE` | `ifindex_to_ifname` | Redis lookup table for ifIndex to ifName mapping |
| `TELEGRAF_IFMIB_USE_SHORT_DEVICE_NAME` | `true` | Use short hostname for device names |
| `TELEGRAF_MAPPINGS_PATH` | /app/conf/metranova_pipeline/telegraf_mappings.yml` | Path to Telegraf field mappings YAML file |

## Environment Variable Prefixes

Several consumers support environment variable prefixes to allow multiple instances with different configurations:

- **`HTTP_CONSUMER_ENV_PREFIX`**: Prepended to HTTP consumer variables (e.g., `SCIREG_HTTP_CONSUMER_URLS`)
- **`FILE_CONSUMER_ENV_PREFIX`**: Prepended to file consumer variables (e.g., `SCINET_FILE_CONSUMER_PATHS`)

## Example Usage

### Running a Flow Data Pipeline

```bash
export PIPELINE_YAML=/app/pipelines/data_flow.yml
export KAFKA_TOPIC=stardust_flow
export CLICKHOUSE_FLOW_TABLE=data_flow
export CLICKHOUSE_CACHER_TABLES=meta_as,meta_device:@loopback_ip
python bin/run.py
```

### Running with Docker Compose

```bash
PIPELINE_ENV_FILE=envs/data_flow.env docker compose run --rm pipeline
```

### Running Multiple Pipeline Replicas

```bash
PIPELINE_ENV_FILE=envs/data_flow.env PIPELINE_REPLICAS=10 docker compose up -d pipeline
```

## Table Specification Format

Several environment variables support special table specification formats for advanced key-based lookups:

### Basic Format

```
TABLE_NAME
```

Simple table name without any key specifications.

### Multi-Key Format

```
TABLE_NAME:key1:key2:key3
```

Specifies a composite key for lookups. The cacher/consumer will build keys by concatenating the specified field values with colons as separators.

**Example:**
```bash
CLICKHOUSE_CACHER_TABLES=meta_interface:device_id:flow_index:edge
```

This creates cache keys like `device123:5:true` for lookups.

### Array Key Prefix: `@`

```
TABLE_NAME:@array_field
```

The `@` prefix indicates key should be created for each item in an an array field.

**Example:**
```bash
CLICKHOUSE_CACHER_TABLES=meta_device:@loopback_ip
```

Grabs the array field loopback_ip and creates a key for each items in the array.

### Combining Multiple Table Specifications

You can specify multiple tables with different key formats in a comma-separated list:

```bash
CLICKHOUSE_CACHER_TABLES=meta_as,meta_device:@loopback_ip,meta_interface:device_id:flow_index:edge
```

This example:
1. Caches `meta_as` with default primary key
2. Caches `meta_device` with special `@loopback_ip` key
3. Caches `meta_interface` with composite key of `device_id:flow_index:edge`

### Applicable Variables

This table specification format is supported by:

- `CLICKHOUSE_CONSUMER_TABLES`
- `CLICKHOUSE_CACHER_TABLES`
- `IP_CACHER_TABLES`
