#!/bin/bash

# Start Confluent Platform locally without Docker
# This uses Confluent CLI to run services natively

set -e

echo "🚀 Starting Confluent Platform locally (no Docker)..."

# Check if Confluent CLI is installed
if ! command -v confluent &> /dev/null; then
    echo "❌ Confluent CLI not found. Installing..."
    
    # Install Confluent CLI
    curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest
    
    # Add to PATH
    export PATH=$(pwd)/bin:$PATH
    
    echo "✅ Confluent CLI installed"
fi

# Stop any existing services
echo "🧹 Stopping existing services..."
confluent local services stop 2>/dev/null || true

# Start Confluent Platform services
echo "🔄 Starting Confluent Platform services..."
confluent local services start

echo ""
echo "🎉 Confluent Platform is running!"
echo ""
echo "📊 Access URLs:"
echo "  • Control Center:    http://localhost:9021"
echo "  • Kafka:            localhost:9092"
echo "  • Schema Registry:   http://localhost:8081"
echo "  • Kafka Connect:     http://localhost:8083"
echo "  • KSQL:             http://localhost:8088"
echo ""
echo "🔧 Useful Commands:"
echo "  • Status:           confluent local services status"
echo "  • Stop services:    confluent local services stop"
echo "  • View logs:        confluent local services log [service]"
echo ""
echo "🌐 Open Control Center: http://localhost:9021"