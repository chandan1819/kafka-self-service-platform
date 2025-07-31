# Integration Tests

Comprehensive integration tests for the Kafka Operations Agent, covering end-to-end workflows, cross-provider deployments, performance testing, and data cleanup procedures.

## Overview

The integration test suite provides:

- **End-to-End Workflows**: Complete provisioning and topic management workflows
- **Cross-Provider Testing**: Validation across Docker, Kubernetes, and Terraform providers
- **Performance Testing**: Concurrent operations and load testing
- **Data Cleanup**: Automated cleanup and environment reset procedures
- **Monitoring Integration**: Verification of health checks, metrics, and alerts

## Test Structure

```
tests/integration/
├── conftest.py                      # Test configuration and fixtures
├── test_end_to_end_workflows.py     # Complete workflow tests
├── test_cross_provider_deployment.py # Provider-specific deployment tests
├── test_performance.py              # Performance and load tests
├── test_data_cleanup.py             # Cleanup and reset procedures
└── README.md                        # This file
```

## Prerequisites

### Required Software

- **Docker**: For containerized test environment
- **Docker Compose**: For service orchestration
- **Python 3.11+**: For running tests
- **Make**: For using the Makefile (optional)

### Optional Software

- **Kubernetes**: For Kubernetes provider tests (kubectl, kind/minikube)
- **Terraform**: For Terraform provider tests
- **AWS CLI**: For AWS Terraform provider tests

### Installation

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Install the package in development mode
pip install -e .

# Set up pre-commit hooks (optional)
pre-commit install
```

## Running Tests

### Quick Start

```bash
# Run all integration tests
make test-integration

# Or use the script directly
python scripts/run_integration_tests.py
```

### Test Categories

#### 1. End-to-End Workflow Tests

Tests complete workflows from start to finish:

```bash
# Run all workflow tests
python scripts/run_integration_tests.py --test-file test_end_to_end_workflows.py

# Run specific workflow test
python scripts/run_integration_tests.py --test-pattern "test_full_kafka_cluster_provisioning_workflow"
```

**Test Coverage:**
- Complete Kafka cluster provisioning workflow
- Topic lifecycle management (create, read, update, delete)
- Bulk topic operations
- Error handling workflows
- Concurrent operations
- Monitoring integration during operations

#### 2. Cross-Provider Deployment Tests

Tests deployment across different infrastructure providers:

```bash
# Run all provider tests
python scripts/run_integration_tests.py --test-file test_cross_provider_deployment.py

# Run Docker provider tests only
python scripts/run_integration_tests.py --test-pattern "Docker"

# Run Kubernetes provider tests only
python scripts/run_integration_tests.py --test-pattern "Kubernetes"

# Run Terraform provider tests only
python scripts/run_integration_tests.py --test-pattern "Terraform"
```

**Test Coverage:**
- Docker container deployment and management
- Kubernetes StatefulSet deployment with Helm
- Terraform infrastructure provisioning (AWS, GCP, Azure)
- Provider feature parity validation
- Performance comparison across providers

#### 3. Performance Tests

Tests system performance under various load conditions:

```bash
# Run all performance tests
python scripts/run_integration_tests.py --test-file test_performance.py

# Run concurrent operations test
python scripts/run_integration_tests.py --test-pattern "concurrent"

# Run sustained load test
python scripts/run_integration_tests.py --test-pattern "sustained_load"
```

**Test Coverage:**
- Concurrent topic creation performance
- Mixed operations performance (CRUD)
- Sustained load testing
- Response time analysis
- Throughput measurement
- Resource utilization monitoring

#### 4. Data Cleanup Tests

Tests cleanup procedures and environment reset:

```bash
# Run all cleanup tests
python scripts/run_integration_tests.py --test-file test_data_cleanup.py

# Run comprehensive cleanup test
python scripts/run_integration_tests.py --test-pattern "comprehensive_cleanup"

# Run environment reset test
python scripts/run_integration_tests.py --test-pattern "environment_reset"
```

**Test Coverage:**
- Comprehensive cleanup workflows
- Orphaned resource detection and cleanup
- Environment reset procedures
- Age-based cleanup utilities
- Cleanup verification and validation

### Advanced Test Execution

#### Parallel Execution

```bash
# Run tests in parallel (requires pytest-xdist)
python scripts/run_integration_tests.py --parallel 4
```

#### Selective Test Execution

```bash
# Run tests matching a pattern
python scripts/run_integration_tests.py --test-pattern "topic_lifecycle"

# Run specific test file
python scripts/run_integration_tests.py --test-file test_performance.py

# Run with verbose output
python scripts/run_integration_tests.py --verbose
```

#### Environment Management

```bash
# Skip environment setup (assume services running)
python scripts/run_integration_tests.py --skip-setup

# Skip environment cleanup (leave services running)
python scripts/run_integration_tests.py --skip-cleanup

# Clean Docker images after tests
python scripts/run_integration_tests.py --clean-images
```

## Test Environment

### Docker Compose Setup

The integration tests use Docker Compose to create a complete test environment:

```yaml
services:
  zookeeper:     # Apache Zookeeper for Kafka
  kafka:         # Apache Kafka broker
  postgres:      # PostgreSQL for metadata storage
  redis:         # Redis for caching (optional)
  kafka-ops-api: # Kafka Ops Agent API server
  test-runner:   # Test execution container
```

### Environment Variables

The test environment uses these configuration variables:

```bash
# API endpoints
TEST_API_URL=http://localhost:8000
TEST_MONITORING_URL=http://localhost:8080

# Kafka configuration
TEST_KAFKA_SERVERS=localhost:9092

# Database configuration
TEST_DATABASE_URL=postgresql://kafka_ops:test_password@localhost:5432/kafka_ops_test

# Test execution settings
TEST_TIMEOUT=300
TEST_RETRY_ATTEMPTS=5
TEST_RETRY_DELAY=2.0
```

### Service Health Checks

The test framework waits for all services to be healthy before running tests:

- **Kafka**: Broker API availability check
- **PostgreSQL**: Database connection check
- **API Server**: Health endpoint check (`/health`)
- **Monitoring**: Health endpoint check (`/health`)

## Test Fixtures and Utilities

### Core Fixtures

- **`api_client`**: HTTP client for API interactions
- **`monitoring_client`**: HTTP client for monitoring endpoints
- **`test_config`**: Test configuration dictionary
- **`clean_test_data`**: Automatic test data cleanup
- **`wait_for_services`**: Service readiness verification

### Utility Functions

- **`wait_for_condition()`**: Wait for async conditions with timeout
- **`retry_async()`**: Retry async operations with backoff
- **`cleanup_test_topics()`**: Clean up test topics
- **`cleanup_test_instances()`**: Clean up test service instances

### Sample Data Fixtures

- **`test_topic_name`**: Unique test topic name
- **`test_instance_id`**: Unique test instance ID
- **`sample_topic_config`**: Sample topic configuration
- **`sample_service_config`**: Sample service configuration

## Writing New Tests

### Test Class Structure

```python
class TestMyFeature:
    """Test my feature functionality."""
    
    @pytest.mark.asyncio
    async def test_my_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test my specific workflow."""
        print("\\n🚀 Starting my test workflow...")
        
        # Step 1: Setup
        print("📝 Step 1: Setting up test data...")
        # ... setup code ...
        
        # Step 2: Execute
        print("⚡ Step 2: Executing operations...")
        # ... test operations ...
        
        # Step 3: Verify
        print("✅ Step 3: Verifying results...")
        # ... assertions ...
        
        print("🎉 My test workflow completed successfully!")
```

### Best Practices

1. **Use descriptive test names** that explain what is being tested
2. **Include step-by-step logging** for easier debugging
3. **Use the `clean_test_data` fixture** for automatic cleanup
4. **Test both success and failure scenarios**
5. **Verify monitoring metrics** are updated correctly
6. **Use unique test data names** to avoid conflicts
7. **Include performance assertions** where appropriate
8. **Test cleanup procedures** as part of the workflow

### Error Handling

```python
try:
    response = await api_client.create_topic(topic_config)
    assert response['status'] == 'success'
except Exception as e:
    pytest.fail(f"Topic creation failed: {e}")
```

### Async Testing

```python
@pytest.mark.asyncio
async def test_async_operation(self, api_client):
    """Test async operation."""
    # Use async/await for all API calls
    result = await api_client.get_topic("test-topic")
    assert result['name'] == "test-topic"
```

## Debugging Tests

### Viewing Logs

```bash
# View service logs during test execution
docker-compose -f docker-compose.test.yml logs -f kafka-ops-api

# View all service logs
docker-compose -f docker-compose.test.yml logs
```

### Interactive Debugging

```bash
# Run tests with verbose output
python scripts/run_integration_tests.py --verbose

# Run specific failing test
python scripts/run_integration_tests.py --test-pattern "test_failing_test" --verbose

# Keep environment running for manual testing
python scripts/run_integration_tests.py --skip-cleanup
```

### Common Issues

#### Services Not Ready

```bash
# Check service health
curl http://localhost:8000/health
curl http://localhost:8080/health

# Check Docker containers
docker-compose -f docker-compose.test.yml ps

# View service logs
docker-compose -f docker-compose.test.yml logs kafka-ops-api
```

#### Test Data Conflicts

```bash
# Clean up test environment
python scripts/run_integration_tests.py --test-file test_data_cleanup.py

# Reset environment completely
make docker-clean
```

#### Performance Test Failures

- Adjust timeout values in test configuration
- Check system resources (CPU, memory, disk)
- Verify network connectivity between services
- Review performance thresholds in test assertions

## Continuous Integration

### GitHub Actions

```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
          pip install -e .
      - name: Run integration tests
        run: make test-integration
      - name: Upload test results
        uses: actions/upload-artifact@v3
        if: always()
        with:
          name: test-results
          path: test-results/
```

### Test Reports

Integration tests generate several types of reports:

- **JUnit XML**: `test-results/integration-tests.xml`
- **HTML Report**: `test-results/integration-tests.html`
- **Coverage Report**: `test-results/coverage-html/`

## Performance Benchmarks

### Expected Performance Metrics

| Operation | Concurrent Level | Expected Throughput | Max Latency (P95) |
|-----------|------------------|--------------------|--------------------|
| Topic Creation | 1 | 10 ops/sec | 2s |
| Topic Creation | 10 | 50 ops/sec | 5s |
| Topic Listing | 10 | 100 ops/sec | 1s |
| Mixed Operations | 10 | 30 ops/sec | 3s |

### Load Test Thresholds

- **Success Rate**: ≥ 90%
- **Average Response Time**: < 5s
- **P95 Response Time**: < 10s
- **P99 Response Time**: < 20s

## Contributing

### Adding New Tests

1. **Create test file** in `tests/integration/`
2. **Follow naming convention**: `test_<feature>.py`
3. **Use existing fixtures** and utilities
4. **Include comprehensive documentation**
5. **Add performance assertions** where appropriate
6. **Test both success and failure paths**
7. **Update this README** with new test information

### Test Review Checklist

- [ ] Tests are properly categorized and named
- [ ] All async operations use `await`
- [ ] Test data is properly cleaned up
- [ ] Error scenarios are tested
- [ ] Performance metrics are verified
- [ ] Monitoring integration is tested
- [ ] Documentation is updated

## Troubleshooting

### Common Solutions

1. **Port conflicts**: Change ports in `docker-compose.test.yml`
2. **Memory issues**: Increase Docker memory limits
3. **Timeout issues**: Increase timeout values in test config
4. **Network issues**: Check Docker network configuration
5. **Permission issues**: Ensure Docker daemon is accessible

### Getting Help

- Check the main project README for general setup
- Review Docker Compose logs for service issues
- Check GitHub Issues for known problems
- Contact the development team for support

## Maintenance

### Regular Tasks

- **Update dependencies** in `requirements-dev.txt`
- **Review performance thresholds** as system improves
- **Clean up obsolete test data** patterns
- **Update Docker images** to latest versions
- **Review and update documentation**

### Monitoring Test Health

- Track test execution times
- Monitor test failure rates
- Review performance benchmark trends
- Update test data cleanup procedures
- Validate cross-provider compatibility