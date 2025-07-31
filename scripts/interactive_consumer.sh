#!/bin/bash

# Interactive Kafka Consumer Script
# Allows you to consume messages from a topic

set -e

TOPIC_NAME=${1:-"interactive-topic"}
CONSUMER_GROUP=${2:-"interactive-group"}

echo "🎯 Interactive Kafka Consumer"
echo "============================="
echo "Topic: $TOPIC_NAME"
echo "Consumer Group: $CONSUMER_GROUP"
echo ""

# Check if services are running
if ! docker compose ps | grep -q "kafka.*Up"; then
    echo "❌ Kafka services are not running. Please start them first:"
    echo "   ./scripts/start_with_control_center.sh"
    exit 1
fi

# Check if topic exists
if ! docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q "^$TOPIC_NAME$"; then
    echo "❌ Topic '$TOPIC_NAME' doesn't exist."
    echo "💡 Create it first or use the producer script:"
    echo "   ./scripts/interactive_producer.sh $TOPIC_NAME"
    exit 1
fi

echo "📥 Starting interactive consumer..."
echo "💡 You'll see messages as they arrive"
echo "💡 Press Ctrl+C to exit"
echo "💡 Monitor consumer group in Control Center: http://localhost:9021"
echo ""
echo "Listening for messages from '$TOPIC_NAME':"
echo "=========================================="

# Start interactive consumer
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC_NAME" \
    --group "$CONSUMER_GROUP" \
    --from-beginning