#!/bin/bash

# Redis Cluster Management Script

show_help() {
    echo "Redis Cluster Management"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  setup     - Initialize and start Redis cluster"
    echo "  status    - Show cluster status and node information"
    echo "  info      - Show cluster configuration info"
    echo "  test      - Test cluster connectivity and performance"
    echo "  reset     - Reset cluster (WARNING: deletes all data)"
    echo "  logs      - Show cluster logs"
    echo "  help      - Show this help message"
}

setup_cluster() {
    echo "🚀 Setting up Redis Cluster..."
    ./scripts/redis-cluster-setup.sh
}

show_status() {
    echo "📊 Redis Cluster Status:"
    echo ""
    echo "Node Status:"
    docker compose exec redis-cluster-1 redis-cli cluster nodes
    echo ""
    echo "Cluster Info:"
    docker compose exec redis-cluster-1 redis-cli cluster info
}

show_info() {
    echo "ℹ️  Redis Cluster Configuration:"
    echo ""
    echo "Nodes:"
    for i in {1..6}; do
        echo "  redis-cluster-$i: localhost:700$i"
    done
    echo ""
    echo "Connection: Direct to cluster nodes (no proxy)"
    echo "Sharding: Automatic across 3 master nodes"
    echo "Replication: 1 replica per master"
    echo "Total Slots: 16384"
}

test_cluster() {
    echo "🧪 Testing Redis Cluster Performance..."
    echo ""
    echo "Basic connectivity test:"
    docker compose exec redis-cluster-1 redis-cli ping
    echo ""
    echo "Setting test keys across cluster:"
    for i in {1..10}; do
        docker compose exec redis-cluster-1 redis-cli set "test:key:$i" "value$i"
    done
    echo ""
    echo "Reading test keys (may redirect to different nodes):"
    for i in {1..10}; do
        result=$(docker compose exec redis-cluster-1 redis-cli get "test:key:$i")
        echo "  test:key:$i = $result"
    done
    echo ""
    echo "Cleaning up test keys:"
    for i in {1..10}; do
        docker compose exec redis-cluster-1 redis-cli del "test:key:$i"
    done
    echo "✅ Cluster test complete!"
}

reset_cluster() {
    echo "⚠️  WARNING: This will delete ALL data in the Redis cluster!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Resetting Redis cluster..."
        docker compose down
        docker volume rm $(docker volume ls -q | grep redis-cluster) 2>/dev/null || true
        echo "✅ Cluster reset complete. Run 'setup' to reinitialize."
    else
        echo "❌ Reset cancelled."
    fi
}

show_logs() {
    echo "📋 Redis Cluster Logs:"
    docker compose logs -f redis-cluster-1 redis-cluster-2 redis-cluster-3 redis-cluster-4 redis-cluster-5 redis-cluster-6
}

# Main command handling
case "${1:-help}" in
    setup)
        setup_cluster
        ;;
    status)
        show_status
        ;;
    info)
        show_info
        ;;
    test)
        test_cluster
        ;;
    reset)
        reset_cluster
        ;;
    logs)
        show_logs
        ;;
    help|*)
        show_help
        ;;
esac