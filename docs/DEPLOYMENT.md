# Deployment Guide

This guide covers deploying the Kafka Operations Agent in various environments, from development to production.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Helm Chart Deployment](#helm-chart-deployment)
- [Database Setup](#database-setup)
- [Monitoring Setup](#monitoring-setup)
- [Security Configuration](#security-configuration)
- [Scaling and High Availability](#scaling-and-high-availability)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required Software

- **Docker**: 20.10+ for containerized deployment
- **Kubernetes**: 1.24+ for Kubernetes deployment
- **Helm**: 3.8+ for Helm chart deployment
- **PostgreSQL**: 12+ for metadata storage
- **Apache Kafka**: 2.8+ for message streaming

### Optional Software

- **Redis**: 6+ for caching and session storage
- **Prometheus**: For metrics collection
- **Grafana**: For monitoring dashboards
- **Nginx Ingress Controller**: For Kubernetes ingress

## Configuration

### Environment Variables

The application uses environment variables for configuration:

```bash
# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Monitoring Configuration
MONITORING_HOST=0.0.0.0
MONITORING_PORT=8080

# Database Configuration
DATABASE_URL=postgresql://user:password@host:5432/database
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=json

# Security Configuration
API_KEY=your-secret-api-key
JWT_SECRET=your-jwt-secret-key

# Provider Configuration
DOCKER_ENABLED=true
KUBERNETES_ENABLED=true
TERRAFORM_ENABLED=true

# Feature Flags
AUDIT_LOGGING=true
METRICS_COLLECTION=true
HEALTH_CHECKS=true
CLEANUP_SCHEDULER=true
```

### Configuration File

Alternatively, use a YAML configuration file:

```yaml
# config/config.yaml
api:
  host: "0.0.0.0"
  port: 8000
  workers: 4
  timeout: 300

monitoring:
  host: "0.0.0.0"
  port: 8080
  enabled: true

database:
  url: "postgresql://user:password@host:5432/database"
  pool_size: 10
  max_overflow: 20

kafka:
  bootstrap_servers: "kafka:9092"
  security_protocol: "PLAINTEXT"

logging:
  level: "INFO"
  format: "json"

providers:
  docker:
    enabled: true
    socket_path: "/var/run/docker.sock"
  kubernetes:
    enabled: true
    in_cluster: true
  terraform:
    enabled: true
    binary_path: "/usr/local/bin/terraform"

features:
  audit_logging: true
  metrics_collection: true
  health_checks: true
  cleanup_scheduler: true

security:
  api_key_required: true
  rate_limiting: true
  cors_enabled: true

cleanup:
  default_retention_days: 7
  orphaned_resource_check_interval: 3600

alerts:
  enabled: true
  webhook_url: ""
```

## Docker Deployment

### Build Docker Image

```bash
# Build the image
docker build -t kafka-ops-agent:latest .

# Build with specific version
docker build -t kafka-ops-agent:1.0.0 \
  --build-arg VERSION=1.0.0 \
  --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') \
  --build-arg VCS_REF=$(git rev-parse HEAD) \
  .
```

### Run with Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: kafka_ops
      POSTGRES_USER: kafka_ops
      POSTGRES_PASSWORD: kafka_ops_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U kafka_ops"]
      interval: 5s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka-ops-agent:
    image: kafka-ops-agent:latest
    environment:
      DATABASE_URL: postgresql://kafka_ops:kafka_ops_password@postgres:5432/kafka_ops
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      API_KEY: admin-secret-key
      JWT_SECRET: jwt-secret-key
    ports:
      - "8000:8000"
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_started
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  postgres_data:
```

### Run the Stack

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f kafka-ops-agent

# Stop all services
docker-compose down
```

## Kubernetes Deployment

### Apply Kubernetes Manifests

```bash
# Create namespace
kubectl apply -f k8s/namespace.yaml

# Apply configuration
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml

# Apply RBAC
kubectl apply -f k8s/rbac.yaml

# Deploy application
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

# Apply ingress (optional)
kubectl apply -f k8s/ingress.yaml

# Apply autoscaling (optional)
kubectl apply -f k8s/hpa.yaml
```

### Verify Deployment

```bash
# Check pods
kubectl get pods -n kafka-ops

# Check services
kubectl get services -n kafka-ops

# Check ingress
kubectl get ingress -n kafka-ops

# View logs
kubectl logs -f deployment/kafka-ops-agent -n kafka-ops

# Check health
kubectl port-forward service/kafka-ops-agent 8080:8080 -n kafka-ops
curl http://localhost:8080/health
```

## Helm Chart Deployment

### Install with Helm

```bash
# Add Helm repository (if published)
helm repo add kafka-ops https://charts.example.com/kafka-ops
helm repo update

# Install from local chart
helm install kafka-ops-agent helm/kafka-ops-agent/ \
  --namespace kafka-ops \
  --create-namespace \
  --values values-production.yaml

# Install with custom values
helm install kafka-ops-agent helm/kafka-ops-agent/ \
  --namespace kafka-ops \
  --create-namespace \
  --set image.tag=1.0.0 \
  --set ingress.hosts[0].host=kafka-ops.example.com \
  --set postgresql.auth.password=secure-password
```

### Custom Values File

Create `values-production.yaml`:

```yaml
# Production values
replicaCount: 3

image:
  repository: your-registry/kafka-ops-agent
  tag: "1.0.0"
  pullPolicy: IfNotPresent

resources:
  limits:
    cpu: 1000m
    memory: 1Gi
  requests:
    cpu: 500m
    memory: 512Mi

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

ingress:
  enabled: true
  className: "nginx"
  hosts:
    - host: kafka-ops.example.com
      paths:
        - path: /
          pathType: Prefix
          service: api
  tls:
    - secretName: kafka-ops-tls
      hosts:
        - kafka-ops.example.com

postgresql:
  enabled: true
  auth:
    password: "secure-database-password"
  primary:
    persistence:
      enabled: true
      size: 20Gi
    resources:
      requests:
        memory: 512Mi
        cpu: 500m
      limits:
        memory: 1Gi
        cpu: 1000m

monitoring:
  serviceMonitor:
    enabled: true
  prometheusRule:
    enabled: true

networkPolicy:
  enabled: true
```

### Upgrade Deployment

```bash
# Upgrade with new values
helm upgrade kafka-ops-agent helm/kafka-ops-agent/ \
  --namespace kafka-ops \
  --values values-production.yaml

# Rollback if needed
helm rollback kafka-ops-agent 1 --namespace kafka-ops
```

## Database Setup

### Run Migrations

```bash
# Run all pending migrations
python scripts/run_migrations.py migrate

# Run migrations to specific version
python scripts/run_migrations.py migrate --target 002_add_indexes

# Check migration status
python scripts/run_migrations.py status

# Rollback to specific version
python scripts/run_migrations.py rollback --target 001_initial_schema
```

### Database Backup

```bash
# Create backup
pg_dump -h localhost -U kafka_ops kafka_ops > backup.sql

# Restore backup
psql -h localhost -U kafka_ops kafka_ops < backup.sql
```

### Database Monitoring

```bash
# Check database connections
SELECT count(*) FROM pg_stat_activity WHERE datname = 'kafka_ops';

# Check table sizes
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables WHERE schemaname = 'public' ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

# Check migration status
SELECT * FROM schema_migrations ORDER BY version;
```

## Monitoring Setup

### Prometheus Configuration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'kafka-ops-agent'
    static_configs:
      - targets: ['kafka-ops-agent:8080']
    metrics_path: '/metrics/prometheus'
    scrape_interval: 30s
```

### Grafana Dashboard

Import the provided Grafana dashboard or create custom dashboards with these metrics:

- `kafka_ops_requests_total` - Total requests
- `kafka_ops_request_duration_seconds` - Request duration
- `kafka_ops_active_connections` - Active connections
- `kafka_ops_topics_total` - Total topics
- `kafka_ops_clusters_total` - Total clusters
- `kafka_ops_errors_total` - Total errors

### Health Check Monitoring

Set up external monitoring for health endpoints:

```bash
# Basic health check
curl -f http://kafka-ops.example.com/health

# Readiness check (for load balancers)
curl -f http://kafka-ops.example.com/health/ready

# Liveness check
curl -f http://kafka-ops.example.com/health/live
```

## Security Configuration

### TLS/SSL Setup

#### Generate Certificates

```bash
# Generate private key
openssl genrsa -out kafka-ops.key 2048

# Generate certificate signing request
openssl req -new -key kafka-ops.key -out kafka-ops.csr

# Generate self-signed certificate (for testing)
openssl x509 -req -days 365 -in kafka-ops.csr -signkey kafka-ops.key -out kafka-ops.crt

# Create Kubernetes secret
kubectl create secret tls kafka-ops-tls \
  --cert=kafka-ops.crt \
  --key=kafka-ops.key \
  --namespace kafka-ops
```

#### Configure TLS in Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: kafka-ops-agent
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  tls:
    - hosts:
        - kafka-ops.example.com
      secretName: kafka-ops-tls
  rules:
    - host: kafka-ops.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: kafka-ops-agent
                port:
                  number: 8000
```

### Authentication Setup

#### API Key Authentication

```bash
# Generate API key
API_KEY=$(openssl rand -hex 32)

# Create secret
kubectl create secret generic kafka-ops-secrets \
  --from-literal=api-key=$API_KEY \
  --namespace kafka-ops
```

#### JWT Configuration

```bash
# Generate JWT secret
JWT_SECRET=$(openssl rand -hex 64)

# Update secret
kubectl patch secret kafka-ops-secrets \
  --patch='{"data":{"jwt-secret":"'$(echo -n $JWT_SECRET | base64)'"}}' \
  --namespace kafka-ops
```

### Network Security

#### Network Policies

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-ops-agent-netpol
  namespace: kafka-ops
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: kafka-ops-agent
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: ingress-nginx
      ports:
        - protocol: TCP
          port: 8000
        - protocol: TCP
          port: 8080
  egress:
    - to: []
      ports:
        - protocol: TCP
          port: 53
        - protocol: UDP
          port: 53
    - to:
        - namespaceSelector:
            matchLabels:
              name: kafka
      ports:
        - protocol: TCP
          port: 9092
```

## Scaling and High Availability

### Horizontal Pod Autoscaling

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: kafka-ops-agent
  namespace: kafka-ops
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: kafka-ops-agent
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
```

### Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: kafka-ops-agent
  namespace: kafka-ops
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: kafka-ops-agent
```

### Database High Availability

For production, use a managed PostgreSQL service or set up PostgreSQL with:

- **Primary-Replica Setup**: For read scaling
- **Connection Pooling**: Using PgBouncer
- **Backup Strategy**: Regular automated backups
- **Monitoring**: Database performance monitoring

### Load Balancing

Configure load balancer health checks:

```yaml
# AWS Application Load Balancer
apiVersion: v1
kind: Service
metadata:
  name: kafka-ops-agent-external
  namespace: kafka-ops
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    service.beta.kubernetes.io/aws-load-balancer-healthcheck-path: "/health/ready"
    service.beta.kubernetes.io/aws-load-balancer-healthcheck-port: "8080"
spec:
  type: LoadBalancer
  ports:
    - name: api
      port: 80
      targetPort: 8000
    - name: api-tls
      port: 443
      targetPort: 8000
  selector:
    app.kubernetes.io/name: kafka-ops-agent
```

## Troubleshooting

### Common Issues

#### Pod Startup Issues

```bash
# Check pod status
kubectl get pods -n kafka-ops

# Check pod events
kubectl describe pod <pod-name> -n kafka-ops

# Check logs
kubectl logs <pod-name> -n kafka-ops

# Check resource usage
kubectl top pods -n kafka-ops
```

#### Database Connection Issues

```bash
# Test database connection
kubectl exec -it <pod-name> -n kafka-ops -- \
  psql postgresql://user:password@host:5432/database -c "SELECT 1"

# Check database logs
kubectl logs <postgres-pod> -n kafka-ops

# Check network connectivity
kubectl exec -it <pod-name> -n kafka-ops -- \
  nc -zv postgres 5432
```

#### Performance Issues

```bash
# Check resource usage
kubectl top pods -n kafka-ops
kubectl top nodes

# Check HPA status
kubectl get hpa -n kafka-ops

# Check metrics
curl http://kafka-ops.example.com/metrics

# Check slow queries
kubectl exec -it <postgres-pod> -n kafka-ops -- \
  psql -c "SELECT query, mean_time, calls FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"
```

### Debugging Commands

```bash
# Port forward for local access
kubectl port-forward service/kafka-ops-agent 8000:8000 -n kafka-ops

# Execute shell in pod
kubectl exec -it <pod-name> -n kafka-ops -- /bin/bash

# Check configuration
kubectl get configmap kafka-ops-config -n kafka-ops -o yaml

# Check secrets
kubectl get secret kafka-ops-secrets -n kafka-ops -o yaml

# Check ingress
kubectl describe ingress kafka-ops-agent -n kafka-ops
```

### Log Analysis

```bash
# Follow logs
kubectl logs -f deployment/kafka-ops-agent -n kafka-ops

# Search logs
kubectl logs deployment/kafka-ops-agent -n kafka-ops | grep ERROR

# Export logs
kubectl logs deployment/kafka-ops-agent -n kafka-ops > kafka-ops.log
```

### Health Check Debugging

```bash
# Check all health endpoints
curl -v http://kafka-ops.example.com/health
curl -v http://kafka-ops.example.com/health/ready
curl -v http://kafka-ops.example.com/health/live
curl -v http://kafka-ops.example.com/health/checks

# Check specific health check
curl -v http://kafka-ops.example.com/health/checks/database
curl -v http://kafka-ops.example.com/health/checks/kafka
```

### Performance Monitoring

```bash
# Check metrics
curl http://kafka-ops.example.com/metrics | grep kafka_ops

# Check Prometheus format
curl http://kafka-ops.example.com/metrics/prometheus

# Check system status
curl http://kafka-ops.example.com/status
```

## Maintenance

### Regular Tasks

1. **Monitor Resource Usage**: Check CPU, memory, and disk usage
2. **Review Logs**: Look for errors and warnings
3. **Update Dependencies**: Keep base images and dependencies updated
4. **Backup Database**: Regular database backups
5. **Security Updates**: Apply security patches
6. **Performance Review**: Monitor and optimize performance
7. **Capacity Planning**: Plan for growth and scaling

### Upgrade Procedure

1. **Backup Database**: Create database backup
2. **Test in Staging**: Deploy to staging environment first
3. **Run Migrations**: Apply any database migrations
4. **Rolling Update**: Use rolling update strategy
5. **Verify Health**: Check health endpoints after deployment
6. **Monitor Metrics**: Watch metrics for any issues
7. **Rollback Plan**: Be ready to rollback if issues occur

This deployment guide provides comprehensive instructions for deploying the Kafka Operations Agent in various environments with proper security, monitoring, and high availability configurations.