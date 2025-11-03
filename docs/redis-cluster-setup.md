# Redis Cluster Setup for MetrANOVA Pipeline

## Overview

This setup provides a **6-node Redis cluster** (3 masters + 3 replicas) with automatic data sharding and multi-core utilization to solve your single-threaded CPU bottleneck.

## Architecture

```
Your Pipeline Services (Redis Client)
         ↓
    Direct Cluster Connection
         ↓
┌─────────────────────────────────────────┐
│            Redis Cluster                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │Master 1 │ │Master 2 │ │Master 3 │   │
│  │Port 7001│ │Port 7002│ │Port 7003│   │
│  └─────────┘ └─────────┘ └─────────┘   │
│       │           │           │       │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐   │
│  │Replica 1│ │Replica 2│ │Replica 3│   │
│  │Port 7004│ │Port 7005│ │Port 7006│   │
│  └─────────┘ └─────────┘ └─────────┘   │
└─────────────────────────────────────────┘
```

## Benefits

✅ **Data Sharding**: Automatically distributes data across 3 masters  
✅ **Multi-Core Utilization**: Each node runs independently  
✅ **High Availability**: Automatic failover with replicas  
✅ **Horizontal Scaling**: Easy to add more nodes  
✅ **No Proxy Overhead**: Direct cluster connections for maximum performance  

## Quick Start

1. **Setup the cluster:**
   ```bash
   ./redis-cluster.sh setup
   ```

2. **Check status:**
   ```bash
   ./redis-cluster.sh status
   ```

3. **Start your pipeline:**
   ```bash
   docker compose up data_pipeline_flow data_pipeline_interface
   ```

## Configuration Changes Made

### 1. Environment Variables (`.env`)
```properties
# Redis Cluster Configuration (Direct Connection)
REDIS_HOST=redis-cluster-1
REDIS_PORT=6379
REDIS_DB=0
REDIS_CLUSTER_ENABLED=true
REDIS_CLUSTER_NODES=redis-cluster-1:6379,redis-cluster-2:6379,redis-cluster-3:6379,redis-cluster-4:6379,redis-cluster-5:6379,redis-cluster-6:6379
```

### 2. Redis Connector Updated
- Added Redis Cluster support to `metranova/connectors/redis.py`
- Automatic detection of cluster vs standalone mode
- Backwards compatible with existing standalone Redis

### 3. Docker Compose Services
- 6 Redis cluster nodes (redis-cluster-1 through redis-cluster-6)
- Direct client connections to cluster nodes
- Persistent volumes for each node
- Automatic cluster initialization

## Management Commands

```bash
# Setup cluster
./redis-cluster.sh setup

# Show cluster status
./redis-cluster.sh status

# Show configuration info
./redis-cluster.sh info

# Test cluster performance
./redis-cluster.sh test

# View cluster logs
./redis-cluster.sh logs

# Reset cluster (deletes all data)
./redis-cluster.sh reset
```

## Performance Benefits

### Before (Single Redis):
- Single-threaded bottleneck
- All CPU load on one core
- Limited by single node performance

### After (Redis Cluster):
- **Data distributed across 3 masters**
- **Parallel processing on multiple cores**
- **Automatic load balancing**
- **Linear performance scaling**
- **No proxy overhead**

## Data Distribution

Redis Cluster automatically distributes your keys across 3 master nodes using consistent hashing:
- **Node 1**: Hash slots 0-5460 (~33% of data)
- **Node 2**: Hash slots 5461-10922 (~33% of data)  
- **Node 3**: Hash slots 10923-16383 (~34% of data)

## Monitoring

1. **Cluster Status:**
   ```bash
   docker compose exec redis-cluster-1 redis-cli cluster nodes
   ```

2. **Performance Monitoring:**
   ```bash
   docker compose exec redis-cluster-1 redis-cli info stats
   ```

3. **Memory Usage:**
   ```bash
   docker compose exec redis-cluster-1 redis-cli info memory
   ```

## Troubleshooting

### Cluster Not Forming
```bash
# Check if all nodes are running
docker compose ps | grep redis-cluster

# Reinitialize cluster
docker compose up redis-cluster-init
```

### Connection Issues
```bash
# Test direct node connection
docker compose exec redis-cluster-1 redis-cli ping

# Test cluster connectivity
docker compose exec redis-cluster-1 redis-cli cluster nodes
```

### Performance Issues
```bash
# Check cluster balance
./redis-cluster.sh status

# Monitor real-time stats
docker compose exec redis-cluster-1 redis-cli --latency-history
```

## Production Considerations

1. **Resource Allocation**: Each node should have adequate CPU/memory
2. **Network Latency**: Keep nodes in same datacenter/region
3. **Backup Strategy**: Regular cluster snapshots
4. **Monitoring**: Use Redis monitoring tools (RedisInsight, etc.)
5. **Scaling**: Add nodes in pairs (master + replica)

This setup will significantly improve your Redis performance by utilizing multiple CPU cores and distributing the lookup workload across the cluster!