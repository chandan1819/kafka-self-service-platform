#!/bin/bash

# Topic Operations Script
# Provides easy commands for topic management

set -e

show_usage() {
    echo "🎯 Kafka Topic Operations"
    echo "========================="
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  list                          - List all topics"
    echo "  create <topic> [partitions]   - Create a topic (default: 3 partitions)"
    echo "  delete <topic>                - Delete a topic"
    echo "  describe <topic>              - Show topic details"
    echo "  config <topic>                - Show topic configuration"
    echo "  produce <topic>               - Start producer for topic"
    echo "  consume <topic> [group]       - Start consumer for topic"
    echo ""
    echo "Examples:"
    echo "  $0 list"
    echo "  $0 create my-topic 5"
    echo "  $0 describe my-topic"
    echo "  $0 produce my-topic"
    echo "  $0 consume my-topic my-group"
}

check_kafka() {
    if ! docker compose ps | grep -q "kafka.*Up"; then
        echo "❌ Kafka services are not running. Please start them first:"
        echo "   ./scripts/start_with_control_center.sh"
        exit 1
    fi
}

list_topics() {
    echo "📋 Available Topics:"
    echo "==================="
    docker compose exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --list
}

create_topic() {
    local topic=$1
    local partitions=${2:-3}
    
    if [[ -z "$topic" ]]; then
        echo "❌ Topic name is required"
        echo "Usage: $0 create <topic-name> [partitions]"
        exit 1
    fi
    
    echo "📝 Creating topic: $topic (partitions: $partitions)"
    docker compose exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1
    echo "✅ Topic '$topic' created successfully"
}

delete_topic() {
    local topic=$1
    
    if [[ -z "$topic" ]]; then
        echo "❌ Topic name is required"
        echo "Usage: $0 delete <topic-name>"
        exit 1
    fi
    
    echo "🗑️  Deleting topic: $topic"
    docker compose exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --delete \
        --topic "$topic"
    echo "✅ Topic '$topic' deleted successfully"
}

describe_topic() {
    local topic=$1
    
    if [[ -z "$topic" ]]; then
        echo "❌ Topic name is required"
        echo "Usage: $0 describe <topic-name>"
        exit 1
    fi
    
    echo "📊 Topic Details: $topic"
    echo "======================="
    docker compose exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --describe \
        --topic "$topic"
}

show_config() {
    local topic=$1
    
    if [[ -z "$topic" ]]; then
        echo "❌ Topic name is required"
        echo "Usage: $0 config <topic-name>"
        exit 1
    fi
    
    echo "⚙️  Topic Configuration: $topic"
    echo "==============================="
    docker compose exec kafka kafka-configs \
        --bootstrap-server localhost:9092 \
        --describe \
        --entity-type topics \
        --entity-name "$topic"
}

start_producer() {
    local topic=$1
    
    if [[ -z "$topic" ]]; then
        echo "❌ Topic name is required"
        echo "Usage: $0 produce <topic-name>"
        exit 1
    fi
    
    echo "📤 Starting producer for topic: $topic"
    echo "💡 Type messages and press Enter to send"
    echo "💡 Press Ctrl+C to exit"
    echo ""
    
    docker compose exec kafka kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic "$topic"
}

start_consumer() {
    local topic=$1
    local group=${2:-"cli-consumer-group"}
    
    if [[ -z "$topic" ]]; then
        echo "❌ Topic name is required"
        echo "Usage: $0 consume <topic-name> [consumer-group]"
        exit 1
    fi
    
    echo "📥 Starting consumer for topic: $topic"
    echo "Consumer Group: $group"
    echo "💡 Press Ctrl+C to exit"
    echo ""
    
    docker compose exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --group "$group" \
        --from-beginning
}

# Main script logic
if [[ $# -eq 0 ]]; then
    show_usage
    exit 1
fi

check_kafka

case "$1" in
    "list")
        list_topics
        ;;
    "create")
        create_topic "$2" "$3"
        ;;
    "delete")
        delete_topic "$2"
        ;;
    "describe")
        describe_topic "$2"
        ;;
    "config")
        show_config "$2"
        ;;
    "produce")
        start_producer "$2"
        ;;
    "consume")
        start_consumer "$2" "$3"
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac