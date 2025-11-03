#!/bin/bash

echo "🚀 Starting Redis Cluster Setup"

# Start Redis cluster nodes
echo "📡 Starting Redis cluster nodes..."
docker compose up -d redis-cluster-1 redis-cluster-2 redis-cluster-3 redis-cluster-4 redis-cluster-5 redis-cluster-6

# Wait for nodes to be ready
echo "⏳ Waiting for Redis nodes to be ready..."
sleep 15

# Initialize cluster
echo "🔧 Initializing Redis cluster..."
docker compose up redis-cluster-init

# Test cluster
echo "🧪 Testing Redis cluster connectivity..."
docker compose exec redis-cluster-1 redis-cli ping

echo "✅ Redis Cluster Setup Complete!"
echo ""
echo "📊 Cluster Information:"
echo "   • Cluster Nodes: 6 (3 masters + 3 replicas)"
echo "   • Client Connection: Direct to cluster nodes"
echo "   • Node Ports: 7001-7006"
echo ""
echo "🔍 Useful Commands:"
echo "   • Check cluster status: docker compose exec redis-cluster-1 redis-cli cluster nodes"
echo "   • Monitor cluster: docker compose exec redis-cluster-1 redis-cli cluster info"
echo "   • Connect to node: docker compose exec redis-cluster-1 redis-cli"