#!/bin/bash

# Demo script for Kafka producer and consumer operations
# This script demonstrates end-to-end message flow

set -e

echo "🎯 Kafka Producer/Consumer Demo"
echo "================================"

# Check if services are running
if ! docker compose ps | grep -q "kafka.*Up"; then
    echo "❌ Kafka services are not running. Please start them first:"
    echo "   ./scripts/start_with_control_center.sh"
    exit 1
fi

# Create demo topic
TOPIC_NAME="demo-topic-$(date +%s)"
echo "📝 Creating demo topic: $TOPIC_NAME"

docker compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions 3 \
    --replication-factor 1

echo "✅ Topic '$TOPIC_NAME' created successfully"

# Produce sample messages
echo ""
echo "📤 Producing sample messages..."

# Create sample messages
MESSAGES=(
    "Hello Kafka World!"
    "Message from producer demo"
    "Testing message flow"
    "Kafka is working great!"
    "End-to-end test successful"
)

for i in "${!MESSAGES[@]}"; do
    echo "Producing: ${MESSAGES[$i]}"
    echo "${MESSAGES[$i]}" | docker compose exec -T kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic "$TOPIC_NAME"
    sleep 1
done

echo "✅ Produced ${#MESSAGES[@]} messages"

# Show topic details
echo ""
echo "📊 Topic Details:"
docker compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic "$TOPIC_NAME"

# Consume messages
echo ""
echo "📥 Consuming messages (will timeout after 10 seconds)..."
echo "Messages received:"
echo "==================="

timeout 10s docker compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --timeout-ms 8000 || true

echo ""
echo "✅ Demo completed successfully!"
echo ""
echo "🌐 View in Control Center:"
echo "   http://localhost:9021"
echo "   Go to Topics → $TOPIC_NAME"
echo ""
echo "🧹 Cleanup (optional):"
echo "   docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic $TOPIC_NAME"