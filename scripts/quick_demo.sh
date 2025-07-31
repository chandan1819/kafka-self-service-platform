#!/bin/bash

# Quick Demo Script - Complete Kafka Workflow
# This script demonstrates the entire process from start to finish

set -e

echo "🚀 Kafka Self-Service Platform - Quick Demo"
echo "============================================"
echo ""

# Step 1: Check if Docker is running
echo "🔍 Step 1: Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi
echo "✅ Docker is running"

# Step 2: Start services
echo ""
echo "🔄 Step 2: Starting Kafka services..."
echo "This may take 2-3 minutes..."
./scripts/start_with_control_center.sh

# Step 3: Create a demo topic
echo ""
echo "📝 Step 3: Creating demo topic..."
DEMO_TOPIC="quick-demo-$(date +%s)"
docker compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$DEMO_TOPIC" \
    --partitions 3 \
    --replication-factor 1
echo "✅ Created topic: $DEMO_TOPIC"

# Step 4: Produce messages
echo ""
echo "📤 Step 4: Producing sample messages..."
MESSAGES=(
    "Welcome to Kafka!"
    "This is a demo message"
    "Kafka is working perfectly"
    "Message number 4"
    "Final demo message"
)

for i in "${!MESSAGES[@]}"; do
    echo "  Sending: ${MESSAGES[$i]}"
    echo "${MESSAGES[$i]}" | docker compose exec -T kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic "$DEMO_TOPIC"
done
echo "✅ Produced ${#MESSAGES[@]} messages"

# Step 5: Consume messages
echo ""
echo "📥 Step 5: Consuming messages..."
echo "Messages received:"
echo "=================="
timeout 8s docker compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$DEMO_TOPIC" \
    --from-beginning \
    --timeout-ms 6000 || true

# Step 6: Test API
echo ""
echo "🔌 Step 6: Testing API..."
API_TOPIC="api-test-topic"
echo "Creating topic via API..."
curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "X-API-Key: admin-secret-key" \
    -d "{\"name\":\"$API_TOPIC\",\"partitions\":2,\"replication_factor\":1}" \
    http://localhost:8000/api/v1/topics > /dev/null

echo "✅ API topic created successfully"

# Step 7: Show results
echo ""
echo "🎉 Demo Complete! Here's what you can do now:"
echo "=============================================="
echo ""
echo "🌐 Access Control Center:"
echo "   http://localhost:9021"
echo "   → View topics: $DEMO_TOPIC, $API_TOPIC"
echo "   → Monitor consumer groups and message flow"
echo ""
echo "🔌 Your API is running at:"
echo "   http://localhost:8000"
echo "   → Try: curl -H 'X-API-Key: admin-secret-key' http://localhost:8000/api/v1/topics"
echo ""
echo "📊 Monitoring dashboard:"
echo "   http://localhost:8080"
echo ""
echo "🛠️  Interactive Tools:"
echo "   ./scripts/topic_operations.sh list"
echo "   ./scripts/interactive_producer.sh $DEMO_TOPIC"
echo "   ./scripts/interactive_consumer.sh $DEMO_TOPIC"
echo ""
echo "🧹 Cleanup when done:"
echo "   docker compose down"
echo ""
echo "📚 Full documentation:"
echo "   docs/COMPLETE_KAFKA_GUIDE.md"