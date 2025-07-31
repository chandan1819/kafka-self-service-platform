"""Pytest configuration and fixtures."""

import pytest
import tempfile
import os
from unittest.mock import Mock
from kafka_ops_agent.config import Config, DatabaseConfig


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def test_config(temp_db):
    """Create test configuration."""
    config = Config()
    config.database = DatabaseConfig(
        type="sqlite",
        sqlite_path=temp_db
    )
    config.api.debug = True
    config.logging.level = "DEBUG"
    return config


@pytest.fixture
def mock_docker_client():
    """Mock Docker client for testing."""
    mock_client = Mock()
    mock_client.containers = Mock()
    mock_client.networks = Mock()
    mock_client.volumes = Mock()
    return mock_client


@pytest.fixture
def mock_k8s_client():
    """Mock Kubernetes client for testing."""
    mock_client = Mock()
    mock_client.create_namespaced_deployment = Mock()
    mock_client.delete_namespaced_deployment = Mock()
    mock_client.read_namespaced_deployment_status = Mock()
    return mock_client