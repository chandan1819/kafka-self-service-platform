"""Pytest configuration for integration tests."""

import pytest
import asyncio
import os
import time
import aiohttp
from typing import Dict, Any, AsyncGenerator
from pathlib import Path

# Test configuration
TEST_CONFIG = {
    'api_url': os.getenv('TEST_API_URL', 'http://localhost:8000'),
    'monitoring_url': os.getenv('TEST_MONITORING_URL', 'http://localhost:8080'),
    'kafka_servers': os.getenv('TEST_KAFKA_SERVERS', 'localhost:9092'),
    'database_url': os.getenv('TEST_DATABASE_URL', 'postgresql://kafka_ops:test_password@localhost:5432/kafka_ops_test'),
    'timeout': int(os.getenv('TEST_TIMEOUT', '300')),
    'retry_attempts': int(os.getenv('TEST_RETRY_ATTEMPTS', '5')),
    'retry_delay': float(os.getenv('TEST_RETRY_DELAY', '2.0'))
}


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_config() -> Dict[str, Any]:
    """Provide test configuration."""
    return TEST_CONFIG.copy()


@pytest.fixture(scope="session")
async def http_session() -> AsyncGenerator[aiohttp.ClientSession, None]:
    """Provide HTTP session for API calls."""
    timeout = aiohttp.ClientTimeout(total=TEST_CONFIG['timeout'])
    async with aiohttp.ClientSession(timeout=timeout) as session:
        yield session


@pytest.fixture(scope="session")
async def wait_for_services(test_config: Dict[str, Any]) -> None:
    """Wait for all services to be ready."""
    print("Waiting for services to be ready...")
    
    async def check_service(url: str, endpoint: str = "/health") -> bool:
        """Check if a service is ready."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{url}{endpoint}") as response:
                    return response.status < 400
        except Exception:
            return False
    
    services = [
        (test_config['api_url'], "/health"),
        (test_config['monitoring_url'], "/health")
    ]
    
    max_attempts = test_config['retry_attempts']
    retry_delay = test_config['retry_delay']
    
    for service_url, endpoint in services:
        for attempt in range(max_attempts):
            if await check_service(service_url, endpoint):
                print(f"✓ Service ready: {service_url}")
                break
            
            if attempt < max_attempts - 1:
                print(f"⏳ Waiting for service: {service_url} (attempt {attempt + 1}/{max_attempts})")
                await asyncio.sleep(retry_delay)
        else:
            pytest.fail(f"Service not ready after {max_attempts} attempts: {service_url}")
    
    print("✅ All services are ready")


@pytest.fixture(scope="session")
async def api_client(http_session: aiohttp.ClientSession, test_config: Dict[str, Any], wait_for_services) -> 'APIClient':
    """Provide API client for testing."""
    return APIClient(http_session, test_config['api_url'])


@pytest.fixture(scope="session")
async def monitoring_client(http_session: aiohttp.ClientSession, test_config: Dict[str, Any], wait_for_services) -> 'MonitoringClient':
    """Provide monitoring client for testing."""
    return MonitoringClient(http_session, test_config['monitoring_url'])


class APIClient:
    """Client for interacting with the Kafka Ops Agent API."""
    
    def __init__(self, session: aiohttp.ClientSession, base_url: str):
        """Initialize API client.
        
        Args:
            session: HTTP session
            base_url: Base URL of the API
        """
        self.session = session
        self.base_url = base_url.rstrip('/')
    
    async def get(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Make GET request."""
        url = f"{self.base_url}{endpoint}"
        return await self.session.get(url, **kwargs)
    
    async def post(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Make POST request."""
        url = f"{self.base_url}{endpoint}"
        return await self.session.post(url, **kwargs)
    
    async def put(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Make PUT request."""
        url = f"{self.base_url}{endpoint}"
        return await self.session.put(url, **kwargs)
    
    async def delete(self, endpoint: str, **kwargs) -> aiohttp.ClientResponse:
        """Make DELETE request."""
        url = f"{self.base_url}{endpoint}"
        return await self.session.delete(url, **kwargs)
    
    async def get_json(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make GET request and return JSON response."""
        async with await self.get(endpoint, **kwargs) as response:
            response.raise_for_status()
            return await response.json()
    
    async def post_json(self, endpoint: str, data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
        """Make POST request with JSON data and return JSON response."""
        if data is not None:
            kwargs['json'] = data
        async with await self.post(endpoint, **kwargs) as response:
            response.raise_for_status()
            return await response.json()
    
    async def put_json(self, endpoint: str, data: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
        """Make PUT request with JSON data and return JSON response."""
        if data is not None:
            kwargs['json'] = data
        async with await self.put(endpoint, **kwargs) as response:
            response.raise_for_status()
            return await response.json()
    
    # Service Broker API methods
    async def get_catalog(self) -> Dict[str, Any]:
        """Get service catalog."""
        return await self.get_json("/v2/catalog")
    
    async def provision_service(self, instance_id: str, service_data: Dict[str, Any]) -> Dict[str, Any]:
        """Provision a service instance."""
        return await self.put_json(f"/v2/service_instances/{instance_id}", service_data)
    
    async def deprovision_service(self, instance_id: str) -> Dict[str, Any]:
        """Deprovision a service instance."""
        async with await self.delete(f"/v2/service_instances/{instance_id}") as response:
            response.raise_for_status()
            return await response.json()
    
    async def get_last_operation(self, instance_id: str) -> Dict[str, Any]:
        """Get last operation status."""
        return await self.get_json(f"/v2/service_instances/{instance_id}/last_operation")
    
    # Topic Management API methods
    async def create_topic(self, topic_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a topic."""
        return await self.post_json("/api/v1/topics", topic_data)
    
    async def get_topic(self, topic_name: str) -> Dict[str, Any]:
        """Get topic information."""
        return await self.get_json(f"/api/v1/topics/{topic_name}")
    
    async def list_topics(self) -> Dict[str, Any]:
        """List all topics."""
        return await self.get_json("/api/v1/topics")
    
    async def update_topic(self, topic_name: str, topic_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update topic configuration."""
        return await self.put_json(f"/api/v1/topics/{topic_name}", topic_data)
    
    async def delete_topic(self, topic_name: str) -> Dict[str, Any]:
        """Delete a topic."""
        async with await self.delete(f"/api/v1/topics/{topic_name}") as response:
            response.raise_for_status()
            return await response.json()


class MonitoringClient:
    """Client for interacting with the monitoring endpoints."""
    
    def __init__(self, session: aiohttp.ClientSession, base_url: str):
        """Initialize monitoring client.
        
        Args:
            session: HTTP session
            base_url: Base URL of the monitoring API
        """
        self.session = session
        self.base_url = base_url.rstrip('/')
    
    async def get_json(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make GET request and return JSON response."""
        url = f"{self.base_url}{endpoint}"
        async with self.session.get(url, **kwargs) as response:
            response.raise_for_status()
            return await response.json()
    
    async def get_health(self) -> Dict[str, Any]:
        """Get health status."""
        return await self.get_json("/health")
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get metrics."""
        return await self.get_json("/metrics")
    
    async def get_alerts(self) -> Dict[str, Any]:
        """Get alerts."""
        return await self.get_json("/alerts")
    
    async def get_status(self) -> Dict[str, Any]:
        """Get overall status."""
        return await self.get_json("/status")


@pytest.fixture
async def clean_test_data(api_client: APIClient):
    """Clean up test data before and after tests."""
    # Cleanup before test
    await cleanup_test_topics(api_client)
    await cleanup_test_instances(api_client)
    
    yield
    
    # Cleanup after test
    await cleanup_test_topics(api_client)
    await cleanup_test_instances(api_client)


async def cleanup_test_topics(api_client: APIClient):
    """Clean up test topics."""
    try:
        topics_response = await api_client.list_topics()
        topics = topics_response.get('topics', [])
        
        for topic in topics:
            topic_name = topic.get('name', '')
            if topic_name.startswith('test-') or topic_name.startswith('integration-'):
                try:
                    await api_client.delete_topic(topic_name)
                    print(f"Cleaned up test topic: {topic_name}")
                except Exception as e:
                    print(f"Failed to clean up topic {topic_name}: {e}")
    except Exception as e:
        print(f"Failed to list topics for cleanup: {e}")


async def cleanup_test_instances(api_client: APIClient):
    """Clean up test service instances."""
    # Note: This would require implementing a list instances endpoint
    # For now, we'll rely on individual test cleanup
    pass


@pytest.fixture
def test_topic_name() -> str:
    """Generate unique test topic name."""
    import uuid
    return f"test-topic-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def test_instance_id() -> str:
    """Generate unique test instance ID."""
    import uuid
    return f"test-instance-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_topic_config() -> Dict[str, Any]:
    """Provide sample topic configuration."""
    return {
        "name": "test-topic",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": "604800000",  # 7 days
            "cleanup.policy": "delete"
        }
    }


@pytest.fixture
def sample_service_config() -> Dict[str, Any]:
    """Provide sample service configuration."""
    return {
        "service_id": "kafka-cluster",
        "plan_id": "small",
        "parameters": {
            "cluster_name": "test-cluster",
            "broker_count": 1,
            "storage_size": "10Gi"
        }
    }


# Utility functions for tests
async def wait_for_condition(condition_func, timeout: int = 60, interval: int = 2):
    """Wait for a condition to be true.
    
    Args:
        condition_func: Async function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        interval: Check interval in seconds
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if await condition_func():
            return True
        await asyncio.sleep(interval)
    return False


async def retry_async(func, max_attempts: int = 3, delay: float = 1.0):
    """Retry an async function with exponential backoff.
    
    Args:
        func: Async function to retry
        max_attempts: Maximum number of attempts
        delay: Initial delay between attempts
    """
    for attempt in range(max_attempts):
        try:
            return await func()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            await asyncio.sleep(delay * (2 ** attempt))