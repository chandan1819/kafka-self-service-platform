# Kafka Ops Agent Monitoring System

A comprehensive monitoring, health checking, and alerting system for the Kafka Operations Agent.

## Overview

The monitoring system provides:

- **Health Checks**: Configurable health checks for system components
- **Metrics Collection**: Counter, gauge, histogram, and timer metrics
- **Alerting**: Rule-based alerting with multiple notification channels
- **HTTP Endpoints**: REST API for monitoring data access
- **Prometheus Integration**: Metrics in Prometheus exposition format

## Components

### Health Checks (`health_checks.py`)

The health check system monitors the status of various system components:

#### Built-in Health Checks

- **DatabaseHealthCheck**: Tests database connectivity
- **KafkaHealthCheck**: Verifies Kafka cluster accessibility
- **ServiceHealthCheck**: Monitors internal service health
- **DiskSpaceHealthCheck**: Monitors disk usage with thresholds
- **MemoryHealthCheck**: Monitors memory usage with thresholds

#### Health Status Types

- `HEALTHY`: Component is functioning normally
- `DEGRADED`: Component has issues but is still functional
- `UNHEALTHY`: Component is not functioning
- `UNKNOWN`: Component status cannot be determined

#### Usage Example

```python
from kafka_ops_agent.monitoring.health_checks import (
    get_health_check_manager, DatabaseHealthCheck
)

# Get the global health check manager
health_manager = get_health_check_manager()

# Register a database health check
async def get_db_connection():
    # Your database connection logic
    return db_connection

db_check = DatabaseHealthCheck(get_db_connection)
health_manager.register_health_check(db_check)

# Run all health checks
summary = await health_manager.run_all_health_checks()
print(f"Overall status: {summary.overall_status}")
```

### Metrics Collection (`metrics.py`)

The metrics system collects and stores application metrics:

#### Metric Types

- **Counter**: Monotonically increasing values (e.g., request count)
- **Gauge**: Point-in-time values (e.g., active connections)
- **Histogram**: Distribution of values (e.g., response times)
- **Timer**: Specialized histogram for timing operations

#### Built-in Metrics

- `kafka_ops_requests_total`: Total requests processed
- `kafka_ops_request_duration_seconds`: Request duration histogram
- `kafka_ops_active_connections`: Active connection count
- `kafka_ops_topics_total`: Total topics managed
- `kafka_ops_clusters_total`: Total clusters managed
- `kafka_ops_errors_total`: Total errors encountered

#### Usage Example

```python
from kafka_ops_agent.monitoring.metrics import get_metrics_collector

# Get the global metrics collector
metrics = get_metrics_collector()

# Increment a counter
metrics.increment_counter("api_requests_total", 1, {"endpoint": "/topics"})

# Set a gauge value
metrics.set_gauge("active_users", 42)

# Observe a histogram value
metrics.observe_histogram("response_time_seconds", 0.25)

# Time an operation
with metrics.time_operation("database_query"):
    # Your operation here
    pass
```

### Alerting System (`alerts.py`)

The alerting system provides rule-based alerting with configurable notifications:

#### Alert Rules

- **HealthStatusAlertRule**: Alerts based on health check status
- **MetricThresholdAlertRule**: Alerts when metrics exceed thresholds
- **ErrorRateAlertRule**: Alerts on high error rates

#### Alert Severities

- `CRITICAL`: Requires immediate attention
- `WARNING`: Requires attention but not urgent
- `INFO`: Informational alerts

#### Notifiers

- **LogAlertNotifier**: Sends alerts to application logs
- **WebhookAlertNotifier**: Sends alerts via HTTP webhooks

#### Usage Example

```python
from kafka_ops_agent.monitoring.alerts import (
    initialize_alert_manager, HealthStatusAlertRule, 
    AlertSeverity, LogAlertNotifier
)
from kafka_ops_agent.monitoring.health_checks import HealthStatus

# Initialize alert manager
alert_manager = initialize_alert_manager(health_manager, metrics_collector)

# Add alert rule
alert_manager.add_alert_rule(
    HealthStatusAlertRule(
        "system_unhealthy",
        AlertSeverity.CRITICAL,
        HealthStatus.UNHEALTHY
    )
)

# Add notifier
alert_manager.add_notifier(LogAlertNotifier())

# Start alert manager
await alert_manager.start()
```

### HTTP Endpoints (`endpoints.py`)

The monitoring system provides HTTP endpoints for accessing monitoring data:

#### Health Endpoints

- `GET /health` - Overall system health
- `GET /health/ready` - Kubernetes readiness probe
- `GET /health/live` - Kubernetes liveness probe
- `GET /health/checks` - List all health checks
- `GET /health/checks/{name}` - Run specific health check
- `GET /health/history` - Health check history

#### Metrics Endpoints

- `GET /metrics` - Current metrics (JSON format)
- `GET /metrics/prometheus` - Prometheus exposition format
- `GET /metrics/history` - Metrics history

#### Alert Endpoints

- `GET /alerts` - Current alerts and summary
- `GET /alerts/history` - Alert history

#### System Endpoints

- `GET /status` - Overall system status
- `GET /info` - System information

## Getting Started

### 1. Start the Monitoring Server

```bash
# Start the monitoring server
python scripts/start_monitoring_server.py

# Server will be available at http://localhost:8080
```

### 2. Test the Endpoints

```bash
# Test all monitoring endpoints
python scripts/test_monitoring_endpoints.py

# Test with custom URL
python scripts/test_monitoring_endpoints.py --url http://localhost:8080
```

### 3. Run the Demo

```bash
# Run automated demo
python scripts/demo_monitoring_system.py --duration 5

# Run interactive demo
python scripts/demo_monitoring_system.py --interactive
```

## Configuration

### Environment Variables

- `MONITORING_PORT`: Port for monitoring server (default: 8080)
- `MONITORING_HOST`: Host for monitoring server (default: 0.0.0.0)
- `HEALTH_CHECK_TIMEOUT`: Default health check timeout (default: 30s)
- `METRICS_RETENTION_HOURS`: Metrics retention period (default: 24h)
- `ALERT_CHECK_INTERVAL`: Alert evaluation interval (default: 60s)

### Custom Health Checks

Create custom health checks by extending the `HealthCheck` base class:

```python
from kafka_ops_agent.monitoring.health_checks import HealthCheck, HealthCheckResult, HealthStatus

class CustomHealthCheck(HealthCheck):
    def __init__(self):
        super().__init__("custom_service", timeout_seconds=10.0)
    
    async def check(self) -> HealthCheckResult:
        try:
            # Your health check logic here
            is_healthy = await check_service_health()
            
            if is_healthy:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.HEALTHY,
                    message="Service is healthy"
                )
            else:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message="Service is unhealthy"
                )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}"
            )
```

### Custom Metrics

Register custom metrics for your application:

```python
from kafka_ops_agent.monitoring.metrics import get_metrics_collector, MetricType

metrics = get_metrics_collector()

# Register custom metrics
metrics.register_metric(
    "custom_operations_total",
    MetricType.COUNTER,
    "Total custom operations performed"
)

metrics.register_metric(
    "custom_queue_size",
    MetricType.GAUGE,
    "Current queue size"
)
```

### Custom Alert Rules

Create custom alert rules by extending the `AlertRule` base class:

```python
from kafka_ops_agent.monitoring.alerts import AlertRule, Alert, AlertSeverity, AlertState

class CustomAlertRule(AlertRule):
    def __init__(self):
        super().__init__("custom_alert", AlertSeverity.WARNING)
    
    async def evaluate(self, health_summary, metrics):
        # Your alert logic here
        if should_trigger_alert():
            return Alert(
                name=self.name,
                severity=self.severity,
                state=AlertState.FIRING,
                message="Custom condition triggered"
            )
        return None
```

## Kubernetes Integration

The monitoring system is designed for Kubernetes deployments:

### Deployment Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ops-agent
spec:
  template:
    spec:
      containers:
      - name: kafka-ops-agent
        image: kafka-ops-agent:latest
        ports:
        - containerPort: 8080
          name: monitoring
        livenessProbe:
          httpGet:
            path: /health/live
            port: monitoring
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: monitoring
          initialDelaySeconds: 5
          periodSeconds: 5
```

### Service Monitor for Prometheus

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafka-ops-agent
spec:
  selector:
    matchLabels:
      app: kafka-ops-agent
  endpoints:
  - port: monitoring
    path: /metrics/prometheus
    interval: 30s
```

## Testing

Run the test suite:

```bash
# Run all monitoring tests
python -m pytest tests/test_monitoring.py -v

# Run specific test class
python -m pytest tests/test_monitoring.py::TestHealthChecks -v
```

## Troubleshooting

### Common Issues

1. **Health checks timing out**
   - Increase timeout values in health check configuration
   - Check network connectivity to dependent services

2. **Metrics not appearing**
   - Verify metrics are being registered before use
   - Check for metric name conflicts

3. **Alerts not firing**
   - Verify alert rules are properly configured
   - Check alert manager is started
   - Review alert evaluation logs

### Debug Mode

Enable debug logging for detailed monitoring information:

```python
import logging
logging.getLogger('kafka_ops_agent.monitoring').setLevel(logging.DEBUG)
```

## Performance Considerations

- Health checks run concurrently to minimize total check time
- Metrics are stored in memory with configurable retention
- Alert evaluation is throttled to prevent excessive resource usage
- HTTP endpoints use async handlers for better concurrency

## Security

- All endpoints support authentication middleware
- Sensitive information is excluded from health check details
- Alert notifications can be configured with secure channels
- Metrics can be filtered to exclude sensitive data

## Contributing

When adding new monitoring components:

1. Follow the existing patterns for health checks, metrics, and alerts
2. Add comprehensive tests for new functionality
3. Update documentation and examples
4. Consider performance and security implications
5. Test with the demo scripts to ensure integration works