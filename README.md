# MetrANOVA Pipeline

A data pipeline for processing and storing network measurement data.

## Architecture Overview

The MetrANOVA Pipeline implements a modular pipeline architecture that processes network telemetry data:

![General Pipeline Architecture](docs/images/general_pipeline_architecture.png)

The system consists of several key components:

- **Consumers**: Read data from various sources (e.g. Kafka, Files, Redis, ClickHouse, etc)
- **Processors**: Transform and validate incoming data
- **Cachers**: Provide efficient lookup mechanisms for metadata during the processing phase
- **Writers**: Writes processed data to target database our other output channel
- **Connectors**: Provide standardized interfaces to underlying resources and can be used by any other component

A **pipeline** is a specific configuration of these components to access and process data between the various resources.

### Resource Types

The architecture supports three types of resources:

- **Data Sources**: Systems that provide streaming or batch data input (e.g. Files, Kafka topics)
- **Data Stores**: Persistent storage systems for long-term data retention and analytics (e.g. ClickHouse databases)
- **Caches**: High-performance, temporary storage for frequently accessed data and metadata lookups (e.g., Redis)

Each resource type is accessed through dedicated connectors that provide consistent interfaces and handle connection management, authentication, and error handling across the pipeline components. 

## Quick Start

### Prerequisites

1. Docker and Docker Compose
2. SSL certificates for Kafka authentication (if using Kafka connectors)
3. Access to ClickHouse (if using ClickHouse connectors)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd pipeline
   ```

2. **Configure environment variables**
   ```bash
   cp -r conf.example/ conf/
   # Edit conf/envs/base.env with clickhouse and kafka credentials if applicable
   ```

3. **Set up SSL certificates** (for Kafka pipelines)
   ```bash
   mkdir -p certificates/
   # Place your Kafka SSL certificates:
   # - certificates/kafka_ca.crt (CA certificate)
   # - certificates/kafka_user.crt (User certificate) 
   # - certificates/kafka_user.key (User private key)
   ```
4. Decide which pipeline you want to run and find its conifguration file in in `conf/envs/`. For example to run the flow pipeline we will use `conf/envs/data_flow.env`. Copy this to `.env` and make any edits:
  ```bash
  cp conf/envs/data_flow.env .env #replace data_flow.env with chosen pipeline.  
  ```
  *NOTE: You can alternatively set the PIPELINE_ENV_FILE environment variable to the path of the chosen file instead of copying to .env. EXAMPLE: PIPELINE_ENV_FILE=conf/envs/data_flow.env* 

4. **Build and run the pipelines**
   ```bash
   docker compose up --build pipeline
   ```

## Running Pipelines in Development

You can run individual pipelines using `docker compose run` for testing, debugging, or one-time operations. You can also leverage the `PIPELINE_ENV_FILE` environment variable to easily switch between pipelines without changing .env. Example that runs the flow pipeline:
```
PIPELINE_ENV_FILE=conf/envs/data_flow.env docker compose run --rm --build pipeline
```

### Custom Pipeline Configurations

You can also override environment variables for custom pipeline configurations:

```bash
# Run with debug logging enabled
docker compose run -e DEBUG=true pipeline

# Run interface pipeline with specific processors only
docker compose run -e CLICKHOUSE_PROCESSORS=metranova.processors.clickhouse.interface.InterfaceTrafficProcessor pipeline

# Run with a different Kafka topic
docker compose run -e KAFKA_TOPIC=custom_topic pipeline
```

## Environment Variables

### General Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `DEBUG` | `false` | Enable debug logging |

### ClickHouse Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse server hostname |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse server port |
| `CLICKHOUSE_DATABASE` | `default` | ClickHouse database name |
| `CLICKHOUSE_USERNAME` | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | `` | ClickHouse password |
| `CLICKHOUSE_SECURE` | `false` | Enable HTTPS connection |

### ClickHouse Table Names
| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_FLOW_TABLE` | `flow_edge` | Flow data table name |
| `CLICKHOUSE_IF_TRAFFIC_TABLE` | `if_traffic` | Interface traffic table name |
| `CLICKHOUSE_IF_SAP_QUEUE_TABLE` | `if_sap_queue` | SAP queue table name |
| `CLICKHOUSE_IF_PORT_QUEUE_TABLE` | `if_port_queue` | Port queue table name |
| `CLICKHOUSE_RAW_KAFKA_TABLE` | `raw_kafka_messages` | Raw Kafka messages table |
| `CLICKHOUSE_IF_METADATA_TABLE` | `meta_if` | Interface metadata table |

### ClickHouse Processing Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_BATCH_SIZE` | `1000` | Batch size for inserts |
| `CLICKHOUSE_BATCH_TIMEOUT` | `30.0` | Batch timeout in seconds |
| `CLICKHOUSE_FLUSH_INTERVAL` | `0.1` | Flush check interval |

### ClickHouse Consumer Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_CONSUMER_UPDATE_INTERVAL` | `-1` | Update interval in seconds |
| `CLICKHOUSE_CONSUMER_TABLES` | `` | Comma-separated table list |
| `CLICKHOUSE_CACHER_TABLES` | `` | Tables for caching |
| `CLICKHOUSE_METADATA_FORCE_UPDATE` | `false` | Force metadata updates |

### Interface Queue Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_IF_PORT_QUEUE_MIN` | `1` | Minimum port queue number |
| `CLICKHOUSE_IF_PORT_QUEUE_MAX` | `16` | Maximum port queue number |
| `CLICKHOUSE_IF_SAP_QUEUE_MIN` | `1` | Minimum SAP queue number |
| `CLICKHOUSE_IF_SAP_QUEUE_MAX` | `16` | Maximum SAP queue number |

### Kafka Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `metranova_flow` | Kafka topic to consume |
| `KAFKA_CONSUMER_GROUP` | `ch-writer-group` | Consumer group ID |

### Kafka SSL Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SSL_CA_LOCATION` | `/app/certificates/ca-cert` | CA certificate path |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | `/app/certificates/client-cert` | Client certificate path |
| `KAFKA_SSL_KEY_LOCATION` | `/app/certificates/client-key` | Client key path |
| `KAFKA_SSL_KEY_PASSWORD` | | Client key password |

### Kafka Consumer Tuning
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_AUTO_OFFSET_RESET` | `latest` | Offset reset policy |
| `KAFKA_ENABLE_AUTO_COMMIT` | `true` | Enable auto commit |
| `KAFKA_AUTO_COMMIT_INTERVAL_MS` | `5000` | Auto commit interval |
| `KAFKA_SESSION_TIMEOUT_MS` | `30000` | Session timeout |
| `KAFKA_HEARTBEAT_INTERVAL_MS` | `10000` | Heartbeat interval |
| `KAFKA_MAX_POLL_INTERVAL_MS` | `300000` | Max poll interval |
| `KAFKA_FETCH_MIN_BYTES` | `1` | Minimum fetch bytes |
| `KAFKA_FETCH_MAX_BYTES` | `52428800` | Maximum fetch bytes |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | `https` | SSL endpoint verification |

### Redis Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `REDIS_DB` | `0` | Redis database number |
| `REDIS_PASSWORD` | | Redis password |

### Redis Consumer Settings
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_CONSUMER_UPDATE_INTERVAL` | `-1` | Update interval in seconds |
| `REDIS_CONSUMER_TABLES` | `` | Comma-separated table list |

### Redis Interface Metadata
| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_IF_METADATA_TABLE` | `meta_if_cache` | Interface metadata cache table |
| `REDIS_IF_METADATA_EXPIRES` | `86400` | Cache expiration in seconds |

### Pipeline Configuration (Docker Compose)
These variables are typically set in `docker-compose.yml` rather than `.env`:

| Variable | Description |
|----------|-------------|
| `PIPELINE_TYPE` | Pipeline type: `clickhouse`, `metadata_import`, `metadata_export`, `json` |
| `CLICKHOUSE_PROCESSORS` | Comma-separated list of ClickHouse processor classes |
| `REDIS_PROCESSORS` | Comma-separated list of Redis processor classes |
| `REDIS_HASH_PROCESSORS` | Comma-separated list of Redis hash processor classes |

## Development

### Adding New Pipelines

1. Define a new YAML file in `pipelines/` with the desired consumers, cacher, processors and writers. Should be named in format `pipelines/<pipeline_name>.yml` where `<pipeline_name>` is name of pipeline.
2. Define a `conf/envs/<pipeline_name>.env` with a `PIPELINE_YAML=pipelines/<pipeline_name>.yml` environment variable and any other pipeline specific settings
3. Run the pipeline by running `cp conf/envs/<pipeline_name>.env .env` or by setting the PIPELINE_ENV_FILE environment variable as follows:
```bash
PIPELINE_ENV_FILE=conf/envs/<pipeline_name>.env docker compose run pipeline
```

### Adding New Processors

1. Create processor class inheriting from a base processor for the target output. Example: `BaseClickHouseProcessor`
2. Implement required methods
3. Add processor to appropriate file in `pipelines/` directory

## Performance Notes

- ClickHouse batch sizes can be tuned based on available memory and throughput requirements
- Multiple pipeline replicas can be run for horizontal scaling

## Monitoring

The system provides comprehensive logging at INFO and DEBUG levels. Key metrics to monitor:

- Batch insert rates and sizes
- Consumer lag on Kafka topics
- ClickHouse query performance
- Pipeline processing throughput

For production deployments, integrate with your monitoring stack to track these metrics. We plan to add OpenTelemetry support for enhanced observability and distributed tracing.