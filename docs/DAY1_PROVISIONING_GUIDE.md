# Day 1 Kafka Cluster Provisioning Guide

This guide walks you through provisioning your first Kafka cluster using the Kafka Self-Service Platform. We'll cover all three deployment methods: Docker, Kubernetes, and Terraform.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Method 1: Docker Provisioning](#method-1-docker-provisioning)
- [Method 2: Kubernetes Provisioning](#method-2-kubernetes-provisioning)
- [Method 3: Terraform Provisioning](#method-3-terraform-provisioning)
- [Using the CLI](#using-the-cli)
- [Using the REST API](#using-the-rest-api)
- [Monitoring Your Cluster](#monitoring-your-cluster)
- [Managing Topics](#managing-topics)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Software

- **Docker**: 20.10+ (for Docker provisioning)
- **Kubernetes**: 1.24+ with kubectl configured (for K8s provisioning)
- **Terraform**: 1.0+ (for Terraform provisioning)
- **Python**: 3.11+ (for CLI usage)
- **curl**: For API testing

### Platform Setup

First, ensure the Kafka Ops Agent is running:

```bash
# Option 1: Run with Docker Compose
git clone <repository>
cd kafka-ops-agent
docker-compose up -d

# Option 2: Run locally
pip install -r requirements.txt
pip install -e .
python -m kafka_ops_agent.api
```

Verify the platform is running:

```bash
curl http://localhost:8080/health
```

## Quick Start

### 1. Get Service Catalog

First, see what services are available:

```bash
curl -H "X-API-Key: admin-secret-key" \
     http://localhost:8000/v2/catalog | jq
```

### 2. Provision Your First Cluster

Choose your preferred method and follow the detailed instructions below.

## Method 1: Docker Provisioning

Docker provisioning is perfect for development, testing, and local environments.

### Step 1: Provision Docker Cluster

```bash
# Create a unique instance ID
INSTANCE_ID="my-first-kafka-$(date +%s)"

# Provision Docker cluster
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "service_id": "kafka-cluster",
    "plan_id": "docker-small",
    "parameters": {
      "cluster_name": "my-first-kafka",
      "broker_count": 1,
      "zookeeper_count": 1,
      "storage_size": "1Gi",
      "network_name": "kafka-network"
    }
  }' \
  "http://localhost:8000/v2/service_instances/${INSTANCE_ID}"
```

### Step 2: Monitor Provisioning Progress

```bash
# Check provisioning status
curl -H "X-API-Key: admin-secret-key" \
     "http://localhost:8000/v2/service_instances/${INSTANCE_ID}/last_operation"
```

Wait for the status to show `"state": "succeeded"`.

### Step 3: Get Connection Information

```bash
# Get cluster connection details
curl -H "X-API-Key: admin-secret-key" \
     "http://localhost:8000/v2/service_instances/${INSTANCE_ID}/last_operation" | \
     jq '.connection_info'
```

### Step 4: Verify Docker Cluster

```bash
# Check running containers
docker ps | grep kafka

# Test Kafka connectivity
docker exec -it $(docker ps -q --filter "name=kafka") \
  kafka-topics --bootstrap-server localhost:9092 --list
```

## Method 2: Kubernetes Provisioning

Kubernetes provisioning creates production-ready clusters with StatefulSets, persistent storage, and proper resource management.

### Step 1: Prepare Kubernetes Environment

```bash
# Ensure kubectl is configured
kubectl cluster-info

# Create namespace (optional)
kubectl create namespace kafka-clusters
```

### Step 2: Provision Kubernetes Cluster

```bash
# Create instance ID
INSTANCE_ID="k8s-kafka-$(date +%s)"

# Provision Kubernetes cluster
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "service_id": "kafka-cluster",
    "plan_id": "kubernetes-medium",
    "parameters": {
      "cluster_name": "production-kafka",
      "namespace": "kafka-clusters",
      "broker_count": 3,
      "zookeeper_count": 3,
      "storage_class": "fast-ssd",
      "storage_size": "10Gi",
      "resource_limits": {
        "memory": "2Gi",
        "cpu": "1000m"
      },
      "resource_requests": {
        "memory": "1Gi",
        "cpu": "500m"
      }
    }
  }' \
  "http://localhost:8000/v2/service_instances/${INSTANCE_ID}"
```

### Step 3: Monitor Kubernetes Provisioning

```bash
# Check provisioning status
curl -H "X-API-Key: admin-secret-key" \
     "http://localhost:8000/v2/service_instances/${INSTANCE_ID}/last_operation"

# Watch Kubernetes resources
kubectl get pods -n kafka-clusters -w
kubectl get statefulsets -n kafka-clusters
kubectl get services -n kafka-clusters
```

### Step 4: Access Kubernetes Cluster

```bash
# Port forward to access Kafka
kubectl port-forward -n kafka-clusters svc/production-kafka 9092:9092

# Test connectivity (in another terminal)
kafka-topics --bootstrap-server localhost:9092 --list
```

## Method 3: Terraform Provisioning

Terraform provisioning creates cloud-native Kafka clusters on AWS, GCP, or Azure with proper networking, security, and monitoring.

### Step 1: Configure Cloud Credentials

```bash
# For AWS
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-west-2"

# For GCP
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_PROJECT="your-project-id"

# For Azure
export ARM_CLIENT_ID="your-client-id"
export ARM_CLIENT_SECRET="your-client-secret"
export ARM_SUBSCRIPTION_ID="your-subscription-id"
export ARM_TENANT_ID="your-tenant-id"
```

### Step 2: Provision Cloud Cluster

```bash
# Create instance ID
INSTANCE_ID="cloud-kafka-$(date +%s)"

# Provision AWS cluster
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "service_id": "kafka-cluster",
    "plan_id": "terraform-aws-large",
    "parameters": {
      "cluster_name": "production-kafka-aws",
      "cloud_provider": "aws",
      "region": "us-west-2",
      "instance_type": "m5.large",
      "broker_count": 3,
      "zookeeper_count": 3,
      "storage_size": "100",
      "storage_type": "gp3",
      "vpc_cidr": "10.0.0.0/16",
      "enable_monitoring": true,
      "enable_encryption": true,
      "backup_retention_days": 7
    }
  }' \
  "http://localhost:8000/v2/service_instances/${INSTANCE_ID}"
```

### Step 3: Monitor Terraform Provisioning

```bash
# Check provisioning status (this may take 10-15 minutes)
curl -H "X-API-Key: admin-secret-key" \
     "http://localhost:8000/v2/service_instances/${INSTANCE_ID}/last_operation"

# View Terraform outputs when complete
curl -H "X-API-Key: admin-secret-key" \
     "http://localhost:8000/v2/service_instances/${INSTANCE_ID}/last_operation" | \
     jq '.terraform_outputs'
```

### Step 4: Access Cloud Cluster

```bash
# Get connection information
BOOTSTRAP_SERVERS=$(curl -s -H "X-API-Key: admin-secret-key" \
  "http://localhost:8000/v2/service_instances/${INSTANCE_ID}/last_operation" | \
  jq -r '.connection_info.bootstrap_servers')

echo "Kafka Bootstrap Servers: $BOOTSTRAP_SERVERS"

# Test connectivity
kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --list
```

## Using the CLI

The platform includes a powerful CLI for cluster management:

### Install CLI

```bash
pip install -e .
```

### Configure CLI

```bash
# Create CLI configuration
mkdir -p ~/.kafka-ops
cat > ~/.kafka-ops/config.yaml << EOF
api:
  url: http://localhost:8000
  api_key: admin-secret-key

default_provider: docker
EOF
```

### CLI Commands

```bash
# List available service plans
kafka-ops cluster plans

# Provision cluster via CLI
kafka-ops cluster create my-dev-cluster \
  --provider docker \
  --plan small \
  --brokers 1 \
  --storage 1Gi

# List clusters
kafka-ops cluster list

# Get cluster status
kafka-ops cluster status my-dev-cluster

# Get cluster connection info
kafka-ops cluster info my-dev-cluster

# Delete cluster
kafka-ops cluster delete my-dev-cluster
```

## Using the REST API

### Authentication

All API calls require authentication:

```bash
# Set API key header
API_KEY="admin-secret-key"
BASE_URL="http://localhost:8000"
```

### Service Broker API Endpoints

```bash
# Get service catalog
curl -H "X-API-Key: $API_KEY" "$BASE_URL/v2/catalog"

# Provision service instance
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"service_id": "kafka-cluster", "plan_id": "docker-small", "parameters": {...}}' \
  "$BASE_URL/v2/service_instances/my-instance-id"

# Get last operation status
curl -H "X-API-Key: $API_KEY" \
     "$BASE_URL/v2/service_instances/my-instance-id/last_operation"

# Deprovision service instance
curl -X DELETE \
  -H "X-API-Key: $API_KEY" \
  "$BASE_URL/v2/service_instances/my-instance-id"
```

### Topic Management API

```bash
# Create topic
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "name": "my-first-topic",
    "partitions": 3,
    "replication_factor": 1,
    "config": {
      "retention.ms": "604800000"
    }
  }' \
  "$BASE_URL/api/v1/topics"

# List topics
curl -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/topics"

# Get topic details
curl -H "X-API-Key: $API_KEY" "$BASE_URL/api/v1/topics/my-first-topic"

# Update topic configuration
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "config": {
      "retention.ms": "1209600000"
    }
  }' \
  "$BASE_URL/api/v1/topics/my-first-topic"

# Delete topic
curl -X DELETE \
  -H "X-API-Key: $API_KEY" \
  "$BASE_URL/api/v1/topics/my-first-topic"
```

## Monitoring Your Cluster

### Health Checks

```bash
# Check overall system health
curl http://localhost:8080/health

# Check specific health checks
curl http://localhost:8080/health/checks

# Check Kafka connectivity
curl http://localhost:8080/health/checks/kafka
```

### Metrics

```bash
# Get metrics in JSON format
curl http://localhost:8080/metrics

# Get Prometheus format metrics
curl http://localhost:8080/metrics/prometheus

# Check system status
curl http://localhost:8080/status
```

### Monitoring Dashboard

Access the monitoring dashboard at: http://localhost:8080

Key metrics to monitor:
- `kafka_ops_clusters_total`: Number of managed clusters
- `kafka_ops_topics_total`: Number of managed topics
- `kafka_ops_requests_total`: Total API requests
- `kafka_ops_errors_total`: Total errors

## Managing Topics

### Create Your First Topic

```bash
# Using CLI
kafka-ops topic create my-app-events \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000

# Using API
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "name": "my-app-events",
    "partitions": 6,
    "replication_factor": 3,
    "config": {
      "retention.ms": "604800000",
      "cleanup.policy": "delete",
      "compression.type": "snappy"
    }
  }' \
  "http://localhost:8000/api/v1/topics"
```

### Topic Best Practices

```bash
# High-throughput topic
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "name": "high-throughput-events",
    "partitions": 12,
    "replication_factor": 3,
    "config": {
      "retention.ms": "86400000",
      "segment.ms": "3600000",
      "compression.type": "lz4",
      "batch.size": "65536"
    }
  }' \
  "http://localhost:8000/api/v1/topics"

# Long-term storage topic
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-secret-key" \
  -d '{
    "name": "audit-logs",
    "partitions": 3,
    "replication_factor": 3,
    "config": {
      "retention.ms": "31536000000",
      "cleanup.policy": "delete",
      "compression.type": "gzip",
      "segment.ms": "86400000"
    }
  }' \
  "http://localhost:8000/api/v1/topics"
```

## Troubleshooting

### Common Issues

#### 1. Provisioning Stuck in Progress

```bash
# Check detailed operation status
curl -H "X-API-Key: admin-secret-key" \
     "http://localhost:8000/v2/service_instances/YOUR_INSTANCE_ID/last_operation" | jq

# Check system logs
curl http://localhost:8080/health/checks

# For Docker: Check container logs
docker logs $(docker ps -q --filter "name=kafka-ops")

# For Kubernetes: Check pod logs
kubectl logs -l app.kubernetes.io/name=kafka-ops-agent -n kafka-ops
```

#### 2. Connection Issues

```bash
# Test API connectivity
curl -v http://localhost:8000/v2/catalog

# Check authentication
curl -H "X-API-Key: wrong-key" http://localhost:8000/v2/catalog

# Verify cluster connectivity
curl http://localhost:8080/health/checks/kafka
```

#### 3. Resource Issues

```bash
# Check system resources
curl http://localhost:8080/health/checks/memory
curl http://localhost:8080/health/checks/disk_space

# Check metrics
curl http://localhost:8080/metrics | grep kafka_ops
```

### Debug Commands

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Check configuration
kafka-ops config show

# Validate provider setup
kafka-ops cluster validate-provider docker
kafka-ops cluster validate-provider kubernetes
kafka-ops cluster validate-provider terraform

# Test cluster connectivity
kafka-ops cluster test-connection my-cluster-id
```

### Getting Help

```bash
# CLI help
kafka-ops --help
kafka-ops cluster --help
kafka-ops topic --help

# API documentation
curl http://localhost:8000/docs  # If Swagger UI is enabled
```

## Next Steps

After successfully provisioning your first cluster:

1. **Set up monitoring**: Configure Prometheus and Grafana dashboards
2. **Create topics**: Set up topics for your applications
3. **Configure security**: Set up authentication and authorization
4. **Automate operations**: Use the API for automated provisioning
5. **Scale clusters**: Add more brokers or create additional clusters
6. **Set up backups**: Configure automated backup procedures

## Example Workflows

### Development Workflow

```bash
# 1. Create development cluster
kafka-ops cluster create dev-cluster --provider docker --plan small

# 2. Create development topics
kafka-ops topic create user-events --partitions 1 --replication-factor 1
kafka-ops topic create order-events --partitions 1 --replication-factor 1

# 3. Test your application
# ... run your application tests ...

# 4. Clean up
kafka-ops topic delete user-events
kafka-ops topic delete order-events
kafka-ops cluster delete dev-cluster
```

### Production Workflow

```bash
# 1. Create production cluster
kafka-ops cluster create prod-cluster \
  --provider kubernetes \
  --plan large \
  --brokers 5 \
  --storage 100Gi \
  --storage-class fast-ssd

# 2. Create production topics with proper configuration
kafka-ops topic create user-events \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config compression.type=snappy

# 3. Monitor cluster health
kafka-ops cluster status prod-cluster
curl http://localhost:8080/health

# 4. Set up alerts and monitoring
# ... configure Prometheus alerts ...
```

This guide provides everything you need to get started with Day 1 Kafka cluster provisioning using the Kafka Self-Service Platform. Choose the method that best fits your environment and requirements!