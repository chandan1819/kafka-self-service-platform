#!/bin/bash

# Create New Kafka Cluster Script
# This script creates additional Kafka clusters

set -e

CLUSTER_NAME=${1:-"kafka-cluster-$(date +%s)"}
KAFKA_PORT=${2:-9093}
ZOOKEEPER_PORT=${3:-2182}

echo "🚀 Creating new Kafka cluster: $CLUSTER_NAME"
echo "Kafka Port: $KAFKA_PORT"
echo "Zookeeper Port: $ZOOKEEPER_PORT"

# Create cluster-specific docker-compose file
cat > "docker-compose-${CLUSTER_NAME}.yml" << EOF
version: '3.8'

services:
  zookeeper-${CLUSTER_NAME}:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper-${CLUSTER_NAME}
    container_name: zookeeper-${CLUSTER_NAME}
    ports:
      - "${ZOOKEEPER_PORT}:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-${CLUSTER_NAME}-data:/var/lib/zookeeper/data
      - zookeeper-${CLUSTER_NAME}-logs:/var/lib/zookeeper/log
    networks:
      - ${CLUSTER_NAME}-network

  kafka-${CLUSTER_NAME}:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka-${CLUSTER_NAME}
    container_name: kafka-${CLUSTER_NAME}
    depends_on:
      - zookeeper-${CLUSTER_NAME}
    ports:
      - "${KAFKA_PORT}:9092"
      - "$((KAFKA_PORT + 1000)):9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper-${CLUSTER_NAME}:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-${CLUSTER_NAME}:29092,PLAINTEXT_HOST://localhost:${KAFKA_PORT}
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    volumes:
      - kafka-${CLUSTER_NAME}-data:/var/lib/kafka/data
    networks:
      - ${CLUSTER_NAME}-network
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 10

volumes:
  zookeeper-${CLUSTER_NAME}-data:
  zookeeper-${CLUSTER_NAME}-logs:
  kafka-${CLUSTER_NAME}-data:

networks:
  ${CLUSTER_NAME}-network:
    name: ${CLUSTER_NAME}-network
EOF

echo "📝 Created docker-compose-${CLUSTER_NAME}.yml"

# Start the cluster
echo "🔄 Starting cluster..."
docker compose -f "docker-compose-${CLUSTER_NAME}.yml" up -d

# Wait for cluster to be ready
echo "⏳ Waiting for cluster to be ready..."
sleep 30

# Test cluster
echo "🧪 Testing cluster connectivity..."
if docker compose -f "docker-compose-${CLUSTER_NAME}.yml" exec kafka-${CLUSTER_NAME} kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "✅ Cluster $CLUSTER_NAME is ready!"
    echo ""
    echo "📊 Cluster Details:"
    echo "  Name: $CLUSTER_NAME"
    echo "  Kafka: localhost:$KAFKA_PORT"
    echo "  Zookeeper: localhost:$ZOOKEEPER_PORT"
    echo ""
    echo "🛠️ Management Commands:"
    echo "  List topics: docker compose -f docker-compose-${CLUSTER_NAME}.yml exec kafka-${CLUSTER_NAME} kafka-topics --bootstrap-server localhost:9092 --list"
    echo "  Stop cluster: docker compose -f docker-compose-${CLUSTER_NAME}.yml down"
    echo "  Remove cluster: docker compose -f docker-compose-${CLUSTER_NAME}.yml down -v && rm docker-compose-${CLUSTER_NAME}.yml"
else
    echo "❌ Cluster failed to start properly"
    echo "Check logs: docker compose -f docker-compose-${CLUSTER_NAME}.yml logs"
    exit 1
fi