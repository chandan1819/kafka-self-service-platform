#!/bin/bash

# Kafka Self-Service Platform with Confluent Control Center
# This script starts the full Confluent Platform including Control Center

set -e

echo "🚀 Starting Kafka Self-Service Platform with Confluent Control Center..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Stop any existing containers
echo "🧹 Cleaning up existing containers..."
docker compose down -v 2>/dev/null || true

# Start the services
echo "🔄 Starting services..."
docker compose up -d

echo "⏳ Waiting for services to be healthy..."

# Wait for Kafka to be ready
echo "  - Waiting for Kafka..."
timeout=120
counter=0
while ! docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "❌ Kafka failed to start within $timeout seconds"
        docker compose logs kafka
        exit 1
    fi
    sleep 2
    counter=$((counter + 2))
    echo -n "."
done
echo " ✅ Kafka is ready!"

# Wait for Schema Registry
echo "  - Waiting for Schema Registry..."
counter=0
while ! curl -f http://localhost:8081/subjects > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "❌ Schema Registry failed to start within $timeout seconds"
        docker compose logs schema-registry
        exit 1
    fi
    sleep 2
    counter=$((counter + 2))
    echo -n "."
done
echo " ✅ Schema Registry is ready!"

# Wait for Connect
echo "  - Waiting for Kafka Connect..."
counter=0
while ! curl -f http://localhost:8083/connectors > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "❌ Kafka Connect failed to start within $timeout seconds"
        docker compose logs connect
        exit 1
    fi
    sleep 2
    counter=$((counter + 2))
    echo -n "."
done
echo " ✅ Kafka Connect is ready!"

# Wait for Control Center
echo "  - Waiting for Control Center..."
counter=0
while ! curl -f http://localhost:9021/health > /dev/null 2>&1; do
    if [ $counter -ge 180 ]; then  # Control Center takes longer to start
        echo "❌ Control Center failed to start within 180 seconds"
        docker compose logs control-center
        exit 1
    fi
    sleep 3
    counter=$((counter + 3))
    echo -n "."
done
echo " ✅ Control Center is ready!"

# Wait for your Kafka Ops API
echo "  - Waiting for Kafka Ops API..."
counter=0
while ! curl -f http://localhost:8080/health > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        echo "⚠️  Kafka Ops API not ready yet (this is optional)"
        break
    fi
    sleep 2
    counter=$((counter + 2))
    echo -n "."
done
if curl -f http://localhost:8080/health > /dev/null 2>&1; then
    echo " ✅ Kafka Ops API is ready!"
fi

echo ""
echo "🎉 All services are running!"
echo ""
echo "📊 Access URLs:"
echo "  • Confluent Control Center: http://localhost:9021"
echo "  • Schema Registry:          http://localhost:8081"
echo "  • Kafka Connect:            http://localhost:8083"
echo "  • Kafka Ops API:            http://localhost:8000"
echo "  • Kafka Ops Monitoring:     http://localhost:8080"
echo ""
echo "🔧 Kafka Connection:"
echo "  • Bootstrap Servers: localhost:9092"
echo "  • Zookeeper:        localhost:2181"
echo ""
echo "💡 Quick Commands:"
echo "  • View logs:        docker compose logs -f [service-name]"
echo "  • Stop services:    docker compose down"
echo "  • Stop & cleanup:   docker compose down -v"
echo ""
echo "🌐 Open Control Center in your browser: http://localhost:9021"