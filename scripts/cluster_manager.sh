#!/bin/bash

# Kafka Cluster Manager
# Manage multiple Kafka clusters

set -e

show_usage() {
    echo "🎯 Kafka Cluster Manager"
    echo "========================"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  create <name> [kafka-port] [zk-port]  - Create a new cluster"
    echo "  list                                   - List all running clusters"
    echo "  status <name>                          - Check cluster status"
    echo "  stop <name>                            - Stop a cluster"
    echo "  start <name>                           - Start a stopped cluster"
    echo "  remove <name>                          - Remove cluster completely"
    echo "  connect <name>                         - Show connection details"
    echo ""
    echo "Examples:"
    echo "  $0 create dev-cluster 9093 2182"
    echo "  $0 list"
    echo "  $0 status dev-cluster"
    echo "  $0 stop dev-cluster"
    echo "  $0 remove dev-cluster"
}

create_cluster() {
    local name=$1
    local kafka_port=${2:-9093}
    local zk_port=${3:-2182}
    
    if [[ -z "$name" ]]; then
        echo "❌ Cluster name is required"
        echo "Usage: $0 create <cluster-name> [kafka-port] [zk-port]"
        exit 1
    fi
    
    if [[ -f "docker-compose-${name}.yml" ]]; then
        echo "❌ Cluster '$name' already exists"
        exit 1
    fi
    
    echo "🚀 Creating cluster: $name"
    ./scripts/create_new_cluster.sh "$name" "$kafka_port" "$zk_port"
}

list_clusters() {
    echo "📋 Running Kafka Clusters:"
    echo "=========================="
    
    local found=false
    for compose_file in docker-compose-*.yml; do
        if [[ -f "$compose_file" && "$compose_file" != "docker-compose.yml" ]]; then
            local cluster_name=$(echo "$compose_file" | sed 's/docker-compose-\(.*\)\.yml/\1/')
            local status=$(docker compose -f "$compose_file" ps --format "table {{.Service}}\t{{.Status}}" 2>/dev/null | grep -v "SERVICE" | head -1 | awk '{print $2}' || echo "stopped")
            
            if [[ "$status" == "Up" ]]; then
                echo "✅ $cluster_name - Running"
            else
                echo "❌ $cluster_name - Stopped"
            fi
            found=true
        fi
    done
    
    if [[ "$found" == false ]]; then
        echo "No clusters found. Create one with: $0 create <cluster-name>"
    fi
}

cluster_status() {
    local name=$1
    
    if [[ -z "$name" ]]; then
        echo "❌ Cluster name is required"
        echo "Usage: $0 status <cluster-name>"
        exit 1
    fi
    
    local compose_file="docker-compose-${name}.yml"
    if [[ ! -f "$compose_file" ]]; then
        echo "❌ Cluster '$name' not found"
        exit 1
    fi
    
    echo "📊 Cluster Status: $name"
    echo "======================="
    docker compose -f "$compose_file" ps
    
    echo ""
    echo "🔍 Health Check:"
    if docker compose -f "$compose_file" exec "kafka-${name}" kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo "✅ Kafka is healthy"
    else
        echo "❌ Kafka is not responding"
    fi
}

stop_cluster() {
    local name=$1
    
    if [[ -z "$name" ]]; then
        echo "❌ Cluster name is required"
        echo "Usage: $0 stop <cluster-name>"
        exit 1
    fi
    
    local compose_file="docker-compose-${name}.yml"
    if [[ ! -f "$compose_file" ]]; then
        echo "❌ Cluster '$name' not found"
        exit 1
    fi
    
    echo "🛑 Stopping cluster: $name"
    docker compose -f "$compose_file" down
    echo "✅ Cluster '$name' stopped"
}

start_cluster() {
    local name=$1
    
    if [[ -z "$name" ]]; then
        echo "❌ Cluster name is required"
        echo "Usage: $0 start <cluster-name>"
        exit 1
    fi
    
    local compose_file="docker-compose-${name}.yml"
    if [[ ! -f "$compose_file" ]]; then
        echo "❌ Cluster '$name' not found"
        exit 1
    fi
    
    echo "🔄 Starting cluster: $name"
    docker compose -f "$compose_file" up -d
    echo "✅ Cluster '$name' started"
}

remove_cluster() {
    local name=$1
    
    if [[ -z "$name" ]]; then
        echo "❌ Cluster name is required"
        echo "Usage: $0 remove <cluster-name>"
        exit 1
    fi
    
    local compose_file="docker-compose-${name}.yml"
    if [[ ! -f "$compose_file" ]]; then
        echo "❌ Cluster '$name' not found"
        exit 1
    fi
    
    echo "🗑️  Removing cluster: $name"
    echo "This will delete all data. Are you sure? (y/N)"
    read -r confirmation
    
    if [[ "$confirmation" =~ ^[Yy]$ ]]; then
        docker compose -f "$compose_file" down -v
        rm "$compose_file"
        echo "✅ Cluster '$name' removed completely"
    else
        echo "❌ Operation cancelled"
    fi
}

show_connection() {
    local name=$1
    
    if [[ -z "$name" ]]; then
        echo "❌ Cluster name is required"
        echo "Usage: $0 connect <cluster-name>"
        exit 1
    fi
    
    local compose_file="docker-compose-${name}.yml"
    if [[ ! -f "$compose_file" ]]; then
        echo "❌ Cluster '$name' not found"
        exit 1
    fi
    
    # Extract ports from compose file
    local kafka_port=$(grep -A 10 "kafka-${name}:" "$compose_file" | grep -E "^\s*-\s*\"[0-9]+:9092\"" | sed 's/.*"\([0-9]*\):9092".*/\1/')
    local zk_port=$(grep -A 10 "zookeeper-${name}:" "$compose_file" | grep -E "^\s*-\s*\"[0-9]+:2181\"" | sed 's/.*"\([0-9]*\):2181".*/\1/')
    
    echo "🔌 Connection Details: $name"
    echo "============================"
    echo "Kafka Bootstrap Servers: localhost:${kafka_port}"
    echo "Zookeeper Connect: localhost:${zk_port}"
    echo ""
    echo "🛠️ Quick Commands:"
    echo "List topics:"
    echo "  docker compose -f $compose_file exec kafka-${name} kafka-topics --bootstrap-server localhost:9092 --list"
    echo ""
    echo "Create topic:"
    echo "  docker compose -f $compose_file exec kafka-${name} kafka-topics --bootstrap-server localhost:9092 --create --topic my-topic --partitions 3 --replication-factor 1"
    echo ""
    echo "Producer:"
    echo "  docker compose -f $compose_file exec kafka-${name} kafka-console-producer --bootstrap-server localhost:9092 --topic my-topic"
    echo ""
    echo "Consumer:"
    echo "  docker compose -f $compose_file exec kafka-${name} kafka-console-consumer --bootstrap-server localhost:9092 --topic my-topic --from-beginning"
}

# Main script logic
if [[ $# -eq 0 ]]; then
    show_usage
    exit 1
fi

case "$1" in
    "create")
        create_cluster "$2" "$3" "$4"
        ;;
    "list")
        list_clusters
        ;;
    "status")
        cluster_status "$2"
        ;;
    "stop")
        stop_cluster "$2"
        ;;
    "start")
        start_cluster "$2"
        ;;
    "remove")
        remove_cluster "$2"
        ;;
    "connect")
        show_connection "$2"
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac