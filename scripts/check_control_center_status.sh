#!/bin/bash

# Check status of Confluent Platform services

echo "🔍 Checking Confluent Platform Services Status..."
echo ""

# Function to check service health
check_service() {
    local service_name=$1
    local url=$2
    local description=$3
    
    if curl -f "$url" > /dev/null 2>&1; then
        echo "✅ $description: $url"
    else
        echo "❌ $description: $url (not responding)"
    fi
}

# Check Docker containers
echo "📦 Docker Containers:"
docker compose ps

echo ""
echo "🌐 Service Health Checks:"

# Check each service
check_service "kafka" "http://localhost:9092" "Kafka Broker"
check_service "schema-registry" "http://localhost:8081/subjects" "Schema Registry"
check_service "connect" "http://localhost:8083/connectors" "Kafka Connect"
check_service "control-center" "http://localhost:9021/health" "Control Center"
check_service "kafka-ops-api" "http://localhost:8000" "Kafka Ops API"
check_service "kafka-ops-monitoring" "http://localhost:8080/health" "Kafka Ops Monitoring"

echo ""
echo "📊 Quick Access URLs:"
echo "  • Control Center:  http://localhost:9021"
echo "  • Schema Registry: http://localhost:8081"
echo "  • Kafka Connect:   http://localhost:8083"
echo "  • Kafka Ops API:   http://localhost:8000"

echo ""
echo "🔧 Test Kafka Connection:"
echo "docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list"