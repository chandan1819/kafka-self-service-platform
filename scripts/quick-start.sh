#!/bin/bash

# Kafka Self-Service Platform - Quick Start Script
# This script provisions your first Kafka cluster in under 5 minutes

set -e

# Configuration
API_URL="${API_URL:-http://localhost:8000}"
MONITORING_URL="${MONITORING_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-admin-secret-key}"
PROVIDER="${PROVIDER:-docker}"
CLUSTER_NAME="${CLUSTER_NAME:-my-first-kafka}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if curl is available
    if ! command -v curl &> /dev/null; then
        log_error "curl is required but not installed"
        exit 1
    fi
    
    # Check if jq is available (optional but helpful)
    if ! command -v jq &> /dev/null; then
        log_warning "jq is not installed - output will be less formatted"
        JQ_AVAILABLE=false
    else
        JQ_AVAILABLE=true
    fi
    
    log_success "Prerequisites check completed"
}

# Check if the platform is running
check_platform() {
    log_info "Checking if Kafka Ops Platform is running..."
    
    if curl -s -f "$MONITORING_URL/health" > /dev/null; then
        log_success "Platform is running and healthy"
    else
        log_error "Platform is not running or not healthy"
        log_info "Please start the platform first:"
        log_info "  docker-compose up -d"
        log_info "  OR"
        log_info "  python -m kafka_ops_agent.api"
        exit 1
    fi
}

# Get service catalog
get_catalog() {
    log_info "Getting service catalog..."
    
    CATALOG_RESPONSE=$(curl -s -H "X-API-Key: $API_KEY" "$API_URL/v2/catalog")
    
    if [ $? -eq 0 ]; then
        if [ "$JQ_AVAILABLE" = true ]; then
            SERVICE_COUNT=$(echo "$CATALOG_RESPONSE" | jq '.services | length')
            log_success "Found $SERVICE_COUNT available services"
        else
            log_success "Service catalog retrieved"
        fi
    else
        log_error "Failed to get service catalog"
        exit 1
    fi
}

# Provision cluster
provision_cluster() {
    log_info "Provisioning $PROVIDER cluster: $CLUSTER_NAME"
    
    # Generate unique instance ID
    INSTANCE_ID="${PROVIDER}-${CLUSTER_NAME}-$(date +%s)"
    
    # Create service configuration based on provider
    case $PROVIDER in
        docker)
            SERVICE_CONFIG='{
                "service_id": "kafka-cluster",
                "plan_id": "docker-small",
                "parameters": {
                    "cluster_name": "'$CLUSTER_NAME'",
                    "broker_count": 1,
                    "zookeeper_count": 1,
                    "storage_size": "1Gi",
                    "network_name": "kafka-network"
                }
            }'
            ;;
        kubernetes)
            SERVICE_CONFIG='{
                "service_id": "kafka-cluster",
                "plan_id": "kubernetes-medium",
                "parameters": {
                    "cluster_name": "'$CLUSTER_NAME'",
                    "namespace": "kafka-clusters",
                    "broker_count": 3,
                    "zookeeper_count": 3,
                    "storage_class": "standard",
                    "storage_size": "5Gi"
                }
            }'
            ;;
        terraform)
            SERVICE_CONFIG='{
                "service_id": "kafka-cluster",
                "plan_id": "terraform-aws-medium",
                "parameters": {
                    "cluster_name": "'$CLUSTER_NAME'",
                    "cloud_provider": "aws",
                    "region": "us-west-2",
                    "instance_type": "t3.medium",
                    "broker_count": 3,
                    "zookeeper_count": 3,
                    "storage_size": "50"
                }
            }'
            ;;
        *)
            log_error "Unsupported provider: $PROVIDER"
            exit 1
            ;;
    esac
    
    # Start provisioning
    PROVISION_RESPONSE=$(curl -s -X PUT \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d "$SERVICE_CONFIG" \
        "$API_URL/v2/service_instances/$INSTANCE_ID")
    
    if [ $? -eq 0 ]; then
        log_success "Provisioning started for instance: $INSTANCE_ID"
        if [ "$JQ_AVAILABLE" = true ]; then
            OPERATION_ID=$(echo "$PROVISION_RESPONSE" | jq -r '.operation // "N/A"')
            log_info "Operation ID: $OPERATION_ID"
        fi
    else
        log_error "Failed to start provisioning"
        exit 1
    fi
}

# Wait for provisioning to complete
wait_for_provisioning() {
    log_info "Waiting for provisioning to complete..."
    
    # Set timeout based on provider
    case $PROVIDER in
        docker) TIMEOUT=300 ;;      # 5 minutes
        kubernetes) TIMEOUT=600 ;;  # 10 minutes
        terraform) TIMEOUT=1200 ;;  # 20 minutes
    esac
    
    START_TIME=$(date +%s)
    
    while true; do
        CURRENT_TIME=$(date +%s)
        ELAPSED=$((CURRENT_TIME - START_TIME))
        
        if [ $ELAPSED -gt $TIMEOUT ]; then
            log_error "Provisioning timed out after $TIMEOUT seconds"
            exit 1
        fi
        
        # Check operation status
        STATUS_RESPONSE=$(curl -s -H "X-API-Key: $API_KEY" \
            "$API_URL/v2/service_instances/$INSTANCE_ID/last_operation")
        
        if [ $? -eq 0 ]; then
            if [ "$JQ_AVAILABLE" = true ]; then
                STATE=$(echo "$STATUS_RESPONSE" | jq -r '.state // "unknown"')
                DESCRIPTION=$(echo "$STATUS_RESPONSE" | jq -r '.description // ""')
                
                case $STATE in
                    succeeded)
                        log_success "Provisioning completed successfully!"
                        
                        # Extract connection info
                        BOOTSTRAP_SERVERS=$(echo "$STATUS_RESPONSE" | jq -r '.connection_info.bootstrap_servers // "N/A"')
                        if [ "$BOOTSTRAP_SERVERS" != "N/A" ]; then
                            log_info "Bootstrap servers: $BOOTSTRAP_SERVERS"
                        fi
                        
                        return 0
                        ;;
                    failed)
                        log_error "Provisioning failed: $DESCRIPTION"
                        exit 1
                        ;;
                    *)
                        log_info "Status: $STATE - $DESCRIPTION"
                        ;;
                esac
            else
                # Without jq, just check if we can parse basic status
                if echo "$STATUS_RESPONSE" | grep -q '"state":"succeeded"'; then
                    log_success "Provisioning completed successfully!"
                    return 0
                elif echo "$STATUS_RESPONSE" | grep -q '"state":"failed"'; then
                    log_error "Provisioning failed"
                    exit 1
                else
                    log_info "Provisioning in progress..."
                fi
            fi
        else
            log_warning "Failed to check status, retrying..."
        fi
        
        sleep 10
    done
}

# Create sample topics
create_sample_topics() {
    log_info "Creating sample topics..."
    
    TOPICS=(
        '{"name":"user-events","partitions":3,"replication_factor":1,"config":{"retention.ms":"604800000"}}'
        '{"name":"order-events","partitions":6,"replication_factor":1,"config":{"retention.ms":"2592000000","compression.type":"snappy"}}'
        '{"name":"audit-logs","partitions":1,"replication_factor":1,"config":{"retention.ms":"31536000000","compression.type":"gzip"}}'
    )
    
    CREATED_TOPICS=()
    
    for TOPIC_CONFIG in "${TOPICS[@]}"; do
        TOPIC_RESPONSE=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            -H "X-API-Key: $API_KEY" \
            -d "$TOPIC_CONFIG" \
            "$API_URL/api/v1/topics")
        
        if [ $? -eq 0 ]; then
            if [ "$JQ_AVAILABLE" = true ]; then
                TOPIC_NAME=$(echo "$TOPIC_CONFIG" | jq -r '.name')
                STATUS=$(echo "$TOPIC_RESPONSE" | jq -r '.status // "unknown"')
                
                if [ "$STATUS" = "success" ]; then
                    log_success "Created topic: $TOPIC_NAME"
                    CREATED_TOPICS+=("$TOPIC_NAME")
                else
                    log_warning "Failed to create topic: $TOPIC_NAME"
                fi
            else
                if echo "$TOPIC_RESPONSE" | grep -q '"status":"success"'; then
                    log_success "Topic created successfully"
                else
                    log_warning "Failed to create topic"
                fi
            fi
        else
            log_warning "Failed to create topic"
        fi
    done
    
    log_info "Created ${#CREATED_TOPICS[@]} topics"
}

# Show final status
show_final_status() {
    echo
    echo "🎉 Day 1 Kafka cluster provisioning completed!"
    echo "================================================"
    echo "Cluster ID: $INSTANCE_ID"
    echo "Provider: $PROVIDER"
    echo "Cluster Name: $CLUSTER_NAME"
    echo
    echo "Next steps:"
    echo "1. Test connectivity to your cluster"
    echo "2. Create additional topics as needed"
    echo "3. Configure your applications to use the cluster"
    echo "4. Set up monitoring and alerting"
    echo
    echo "Useful commands:"
    echo "  # List topics"
    echo "  curl -H 'X-API-Key: $API_KEY' $API_URL/api/v1/topics"
    echo
    echo "  # Check cluster health"
    echo "  curl $MONITORING_URL/health"
    echo
    echo "  # View metrics"
    echo "  curl $MONITORING_URL/metrics"
    echo
    echo "  # Deprovision cluster (when done)"
    echo "  curl -X DELETE -H 'X-API-Key: $API_KEY' $API_URL/v2/service_instances/$INSTANCE_ID"
    echo
}

# Cleanup function
cleanup() {
    if [ -n "$INSTANCE_ID" ]; then
        log_info "Cleaning up resources..."
        curl -s -X DELETE -H "X-API-Key: $API_KEY" "$API_URL/v2/service_instances/$INSTANCE_ID" > /dev/null
        log_success "Cleanup initiated"
    fi
}

# Main execution
main() {
    echo "🚀 Kafka Self-Service Platform - Quick Start"
    echo "============================================="
    echo "Provider: $PROVIDER"
    echo "Cluster Name: $CLUSTER_NAME"
    echo "API URL: $API_URL"
    echo
    
    # Set up cleanup trap
    trap cleanup EXIT
    
    check_prerequisites
    check_platform
    get_catalog
    provision_cluster
    wait_for_provisioning
    create_sample_topics
    show_final_status
    
    # Disable cleanup trap since we want to keep resources
    trap - EXIT
}

# Help function
show_help() {
    echo "Kafka Self-Service Platform - Quick Start Script"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -p, --provider PROVIDER    Provisioning provider (docker, kubernetes, terraform)"
    echo "  -n, --name NAME           Cluster name"
    echo "  -u, --api-url URL         API base URL"
    echo "  -k, --api-key KEY         API key"
    echo "  -h, --help                Show this help message"
    echo
    echo "Environment Variables:"
    echo "  API_URL                   API base URL (default: http://localhost:8000)"
    echo "  MONITORING_URL            Monitoring URL (default: http://localhost:8080)"
    echo "  API_KEY                   API key (default: admin-secret-key)"
    echo "  PROVIDER                  Provider (default: docker)"
    echo "  CLUSTER_NAME              Cluster name (default: my-first-kafka)"
    echo
    echo "Examples:"
    echo "  $0                                    # Use defaults (Docker)"
    echo "  $0 -p kubernetes -n prod-kafka       # Kubernetes cluster"
    echo "  $0 -p terraform -n cloud-kafka       # Terraform cluster"
    echo "  PROVIDER=kubernetes $0                # Use environment variable"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--provider)
            PROVIDER="$2"
            shift 2
            ;;
        -n|--name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        -u|--api-url)
            API_URL="$2"
            shift 2
            ;;
        -k|--api-key)
            API_KEY="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate provider
case $PROVIDER in
    docker|kubernetes|terraform)
        ;;
    *)
        log_error "Invalid provider: $PROVIDER"
        log_info "Valid providers: docker, kubernetes, terraform"
        exit 1
        ;;
esac

# Run main function
main