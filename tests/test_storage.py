"""Tests for storage layer."""

import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import AsyncMock, patch

from kafka_ops_agent.storage.sqlite_store import SQLiteMetadataStore, SQLiteAuditStore
from kafka_ops_agent.storage.factory import StorageFactory
from kafka_ops_agent.models.cluster import ServiceInstance, ClusterStatus, RuntimeProvider, ConnectionInfo
from kafka_ops_agent.models.factory import ServiceInstanceFactory
from kafka_ops_agent.config import Config, DatabaseConfig


class TestSQLiteMetadataStore:
    """Test SQLite metadata store."""
    
    @pytest.fixture
    async def store(self):
        """Create test store."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        store = SQLiteMetadataStore(db_path)
        await store.initialize()
        
        yield store
        
        await store.close()
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    @pytest.mark.asyncio
    async def test_initialize(self, store):
        """Test store initialization."""
        assert store.connection is not None
        
        # Check tables exist
        cursor = store.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        assert 'service_instances' in tables
        assert 'audit_logs' in tables
    
    @pytest.mark.asyncio
    async def test_create_instance(self, store):
        """Test creating service instance."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        
        result = await store.create_instance(instance)
        assert result is True
        
        # Try to create duplicate
        result = await store.create_instance(instance)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_instance(self, store):
        """Test retrieving service instance."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        await store.create_instance(instance)
        
        retrieved = await store.get_instance("test-instance")
        assert retrieved is not None
        assert retrieved.instance_id == "test-instance"
        assert retrieved.status == ClusterStatus.PENDING
        
        # Test non-existent instance
        retrieved = await store.get_instance("non-existent")
        assert retrieved is None
    
    @pytest.mark.asyncio
    async def test_update_instance(self, store):
        """Test updating service instance."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        await store.create_instance(instance)
        
        # Update status
        instance.status = ClusterStatus.RUNNING
        instance.connection_info = ConnectionInfo(
            bootstrap_servers=['localhost:9092'],
            zookeeper_connect='localhost:2181'
        )
        
        result = await store.update_instance(instance)
        assert result is True
        
        # Verify update
        retrieved = await store.get_instance("test-instance")
        assert retrieved.status == ClusterStatus.RUNNING
        assert retrieved.connection_info is not None
        assert retrieved.connection_info.bootstrap_servers == ['localhost:9092']
        
        # Test updating non-existent instance
        instance.instance_id = "non-existent"
        result = await store.update_instance(instance)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_delete_instance(self, store):
        """Test deleting service instance."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        await store.create_instance(instance)
        
        result = await store.delete_instance("test-instance")
        assert result is True
        
        # Verify deletion
        retrieved = await store.get_instance("test-instance")
        assert retrieved is None
        
        # Test deleting non-existent instance
        result = await store.delete_instance("non-existent")
        assert result is False
    
    @pytest.mark.asyncio
    async def test_list_instances(self, store):
        """Test listing service instances."""
        # Create test instances
        instance1 = ServiceInstanceFactory.create_default("instance-1")
        instance1.status = ClusterStatus.RUNNING
        instance1.runtime_provider = RuntimeProvider.DOCKER
        
        instance2 = ServiceInstanceFactory.create_default("instance-2")
        instance2.status = ClusterStatus.PENDING
        instance2.runtime_provider = RuntimeProvider.KUBERNETES
        
        await store.create_instance(instance1)
        await store.create_instance(instance2)
        
        # List all instances
        instances = await store.list_instances()
        assert len(instances) == 2
        
        # List with status filter
        running_instances = await store.list_instances({'status': 'running'})
        assert len(running_instances) == 1
        assert running_instances[0].instance_id == "instance-1"
        
        # List with provider filter
        docker_instances = await store.list_instances({'runtime_provider': 'docker'})
        assert len(docker_instances) == 1
        assert docker_instances[0].instance_id == "instance-1"
    
    @pytest.mark.asyncio
    async def test_instance_exists(self, store):
        """Test checking instance existence."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        await store.create_instance(instance)
        
        assert await store.instance_exists("test-instance") is True
        assert await store.instance_exists("non-existent") is False
    
    @pytest.mark.asyncio
    async def test_get_instances_by_status(self, store):
        """Test getting instances by status."""
        instance1 = ServiceInstanceFactory.create_default("instance-1")
        instance1.status = ClusterStatus.RUNNING
        
        instance2 = ServiceInstanceFactory.create_default("instance-2")
        instance2.status = ClusterStatus.RUNNING
        
        instance3 = ServiceInstanceFactory.create_default("instance-3")
        instance3.status = ClusterStatus.PENDING
        
        await store.create_instance(instance1)
        await store.create_instance(instance2)
        await store.create_instance(instance3)
        
        running_instances = await store.get_instances_by_status('running')
        assert len(running_instances) == 2
        
        pending_instances = await store.get_instances_by_status('pending')
        assert len(pending_instances) == 1


class TestSQLiteAuditStore:
    """Test SQLite audit store."""
    
    @pytest.fixture
    async def stores(self):
        """Create test stores."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        metadata_store = SQLiteMetadataStore(db_path)
        await metadata_store.initialize()
        audit_store = SQLiteAuditStore(metadata_store)
        
        yield metadata_store, audit_store
        
        await metadata_store.close()
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    @pytest.mark.asyncio
    async def test_log_operation(self, stores):
        """Test logging operations."""
        metadata_store, audit_store = stores
        
        # Create instance first
        instance = ServiceInstanceFactory.create_default("test-instance")
        await metadata_store.create_instance(instance)
        
        # Log operation
        await audit_store.log_operation(
            "test-instance",
            "provision",
            "user-123",
            {"cluster_size": 3}
        )
        
        # Verify log was created
        logs = await audit_store.get_audit_logs("test-instance")
        assert len(logs) == 1
        assert logs[0]['operation'] == 'provision'
        assert logs[0]['user_id'] == 'user-123'
        assert logs[0]['details']['cluster_size'] == 3
    
    @pytest.mark.asyncio
    async def test_get_audit_logs(self, stores):
        """Test retrieving audit logs."""
        metadata_store, audit_store = stores
        
        # Create instance
        instance = ServiceInstanceFactory.create_default("test-instance")
        await metadata_store.create_instance(instance)
        
        # Log multiple operations
        await audit_store.log_operation("test-instance", "provision", "user-1")
        await audit_store.log_operation("test-instance", "update", "user-2")
        await audit_store.log_operation("test-instance", "deprovision", "user-1")
        
        # Get all logs for instance
        logs = await audit_store.get_audit_logs("test-instance")
        assert len(logs) == 3
        
        # Get logs by operation
        provision_logs = await audit_store.get_audit_logs(operation="provision")
        assert len(provision_logs) == 1
        assert provision_logs[0]['operation'] == 'provision'
        
        # Get logs with limit
        limited_logs = await audit_store.get_audit_logs(limit=2)
        assert len(limited_logs) == 2


class TestStorageFactory:
    """Test storage factory."""
    
    @pytest.mark.asyncio
    async def test_create_sqlite_stores(self):
        """Test creating SQLite stores."""
        with patch('kafka_ops_agent.storage.factory.config') as mock_config:
            mock_config.database.type = 'sqlite'
            mock_config.database.sqlite_path = ':memory:'
            
            metadata_store, audit_store = await StorageFactory.create_stores()
            
            assert isinstance(metadata_store, SQLiteMetadataStore)
            assert isinstance(audit_store, SQLiteAuditStore)
            
            await metadata_store.close()
    
    @pytest.mark.asyncio
    async def test_unsupported_database_type(self):
        """Test unsupported database type."""
        with patch('kafka_ops_agent.storage.factory.config') as mock_config:
            mock_config.database.type = 'unsupported'
            
            with pytest.raises(ValueError, match="Unsupported database type"):
                await StorageFactory.create_stores()
    
    @pytest.mark.asyncio
    async def test_create_metadata_store_only(self):
        """Test creating only metadata store."""
        with patch('kafka_ops_agent.storage.factory.config') as mock_config:
            mock_config.database.type = 'sqlite'
            mock_config.database.sqlite_path = ':memory:'
            
            metadata_store = await StorageFactory.create_metadata_store()
            assert isinstance(metadata_store, SQLiteMetadataStore)
            
            await metadata_store.close()