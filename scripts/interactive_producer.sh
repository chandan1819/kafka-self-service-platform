#!/bin/bash

# Interactive Kafka Producer Script
# Allows you to send messages interactively

set -e

TOPIC_NAME=${1:-"interactive-topic"}

echo "🎯 Interactive Kafka Producer"
echo "============================="
echo "Topic: $TOPIC_NAME"
echo ""

# Check if services are running
if ! docker compose ps | grep -q "kafka.*Up"; then
    echo "❌ Kafka services are not running. Please start them first:"
    echo "   ./scripts/start_with_control_center.sh"
    exit 1
fi

# Check if topic exists, create if not
if ! docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q "^$TOPIC_NAME$"; then
    echo "📝 Topic '$TOPIC_NAME' doesn't exist. Creating it..."
    docker compose exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$TOPIC_NAME" \
        --partitions 3 \
        --replication-factor 1
    echo "✅ Topic created successfully"
fi

echo ""
echo "📤 Starting interactive producer..."
echo "💡 Type your messages and press Enter to send"
echo "💡 Press Ctrl+C to exit"
echo "💡 View messages in Control Center: http://localhost:9021"
echo ""
echo "Ready to send messages to '$TOPIC_NAME':"
echo "========================================"

# Start interactive producer
docker compose exec kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC_NAME"