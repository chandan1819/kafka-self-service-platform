#!/bin/bash

# Day 1 Interactive Testing Script
API_URL="http://localhost:8000"
API_KEY="admin-secret-key"

echo "🚀 Kafka Self-Service Platform - Day 1 Testing"
echo "=============================================="

# Function to make API calls
api_call() {
    local method=$1
    local endpoint=$2
    local data=$3
    
    if [ -n "$data" ]; then
        curl -s -X $method \
            -H "Content-Type: application/json" \
            -H "X-API-Key: $API_KEY" \
            -d "$data" \
            "$API_URL$endpoint"
    else
        curl -s -X $method \
            -H "X-API-Key: $API_KEY" \
            "$API_URL$endpoint"
    fi
}

# Test 1: Health Check
echo "🏥 Testing health check..."
health_response=$(curl -s "$API_URL/health")
echo "Response: $health_response"
echo

# Test 2: Service Catalog
echo "📋 Testing service catalog..."
catalog_response=$(api_call GET "/v2/catalog")
echo "Response: $catalog_response"
echo

# Test 3: Create Topic
echo "📝 Creating test topic..."
topic_data='{
    "name": "day1-test-topic",
    "partitions": 3,
    "replication_factor": 1,
    "config": {
        "retention.ms": "604800000"
    }
}'

create_response=$(api_call POST "/api/v1/topics" "$topic_data")
echo "Response: $create_response"
echo

# Test 4: List Topics
echo "📋 Listing topics..."
topics_response=$(api_call GET "/api/v1/topics")
echo "Response: $topics_response"
echo

echo "✅ Day 1 testing completed!"
echo
echo "Next steps:"
echo "1. Try creating more topics with different configurations"
echo "2. Explore the monitoring endpoint: curl $API_URL/health"
echo "3. Check the API documentation for more endpoints"