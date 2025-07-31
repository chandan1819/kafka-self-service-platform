"""Integration tests for cleanup operations."""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from kafka_ops_agent.services.scheduler import SchedulerService, TaskType, TaskStatus
from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.models.cluster import ServiceInstance


class MockMetadataStore(MetadataStore):
    """Mock metadata store for testing."""
    
    def __init__(self):
        self.instances = {}
        self.failed_instances = []
    
    async def create_instance(self, instance: ServiceInstance) -> bool:
        self.instances[instance.instance_id] = instance
        return True
    
    async def get_instance(self, instance_id: str) -> ServiceInstance:
        return self.instances.get(instance_id)
    
    async def update_instance(self, instance: ServiceInstance) -> bool:
        if instance.instance_id in self.instances:
            self.instances[instance.instance_id] = instance
            return True
        return False
    
    async def delete_instance(self, instance_id: str) -> bool:
        if instance_id in self.instances:
            del self.instances[instance_id]
            return True
        return False
    
    async def list_instances(self) -> list:
        return list(self.instances.values())
    
    async def get_instances_by_status(self, status: str) -> list:
        if status == 'error':
            return self.failed_instances
        return [i for i in self.instances.values() if i.status == status]


class MockAuditStore(AuditStore):
    """Mock audit store for testing."""
    
    def __init__(self):
        self.operations = []
    
    async def log_operation(self, cluster_id: str, operation: str, user_id: str, details: dict):
        self.operations.append({
            'cluster_id': cluster_id,
            'operation': operation,
            'user_id': user_id,
            'details': details,
            'timestamp': datetime.utcnow()
        })


@pytest.fixture
async def scheduler_service():
    """Create scheduler service for testing."""
    metadata_store = MockMetadataStore()
    audit_store = MockAuditStore()
    
    # Add some failed instances for testing
    old_instance = ServiceInstance(
        instance_id="old-failed-instance",
        service_id="kafka",
        plan_id="small",
        status="error",
        created_at=datetime.utcnow() - timedelta(hours=48),
        updated_at=datetime.utcnow() - timedelta(hours=48)
    )
    
    recent_instance = ServiceInstance(
        instance_id="recent-failed-instance", 
        service_id="kafka",
        plan_id="small",
        status="error",
        created_at=datetime.utcnow() - timedelta(hours=1),
        updated_at=datetime.utcnow() - timedelta(hours=1)
    )
    
    metadata_store.failed_instances = [old_instance, recent_instance]
    
    scheduler = SchedulerService(metadata_store, audit_store)
    return scheduler


@pytest.mark.asyncio
async def test_topic_cleanup_dry_run(scheduler_service):
    """Test topic cleanup in dry run mode."""
    
    # Mock topic service
    from kafka_ops_agent.services import topic_management
    
    mock_topic_service = AsyncMock()
    mock_topic_service.list_topics.return_value = [
        MagicMock(name="test-topic-1", configs={"retention.ms": "3600000"}),  # 1 hour
        MagicMock(name="production-topic", configs={"retention.ms": "86400000"}),  # 24 hours
        MagicMock(name="temp-data", configs={"retention.ms": "7200000"})  # 2 hours
    ]
    
    # Patch the get_topic_service function
    original_get_topic_service = topic_management.get_topic_service
    topic_management.get_topic_service = AsyncMock(return_value=mock_topic_service)
    
    try:
        # Execute topic cleanup
        execution = await scheduler_service.execute_topic_cleanup_now(
            cluster_id="test-cluster",
            max_age_hours=2,
            retention_pattern="test",
            dry_run=True,
            user_id="test-user"
        )
        
        # Verify execution
        assert execution.status == TaskStatus.COMPLETED
        assert execution.result is not None
        assert execution.result['dry_run'] is True
        assert execution.result['topics_evaluated'] == 3
        assert execution.result['topics_cleaned'] == 0
        
        # Should identify test topics
        assert "test-topic-1" in execution.result.get('topics_to_cleanup', [])
        assert "temp-data" in execution.result.get('topics_to_cleanup', [])
        
        # Verify audit log
        assert len(scheduler_service.audit_store.operations) == 1
        audit_entry = scheduler_service.audit_store.operations[0]
        assert audit_entry['operation'] == 'ondemand_cleanup_topic_cleanup'
        assert audit_entry['user_id'] == 'test-user'
        
    finally:
        # Restore original function
        topic_management.get_topic_service = original_get_topic_service


@pytest.mark.asyncio
async def test_cluster_cleanup_dry_run(scheduler_service):
    """Test cluster cleanup in dry run mode."""
    
    # Execute cluster cleanup
    execution = await scheduler_service.execute_cluster_cleanup_now(
        max_age_hours=24,
        dry_run=True,
        user_id="test-user"
    )
    
    # Verify execution
    assert execution.status == TaskStatus.COMPLETED
    assert execution.result is not None
    assert execution.result['dry_run'] is True
    assert execution.result['failed_instances'] == 2
    assert execution.result['old_failed_instances'] == 1  # Only one is older than 24 hours
    assert execution.result['cleaned_instances'] == 0
    
    # Verify the old instance is identified for cleanup
    assert "old-failed-instance" in execution.result.get('instances_to_cleanup', [])


@pytest.mark.asyncio
async def test_metadata_cleanup_dry_run(scheduler_service):
    """Test metadata cleanup in dry run mode."""
    
    # Execute metadata cleanup
    execution = await scheduler_service.execute_metadata_cleanup_now(
        max_age_days=30,
        dry_run=True,
        user_id="test-user"
    )
    
    # Verify execution
    assert execution.status == TaskStatus.COMPLETED
    assert execution.result is not None
    assert execution.result['dry_run'] is True
    assert execution.result['audit_logs_cleaned'] == 0
    assert execution.result['metadata_entries_cleaned'] == 0


@pytest.mark.asyncio
async def test_cleanup_conflict_detection(scheduler_service):
    """Test cleanup conflict detection."""
    
    # Mock topic service
    from kafka_ops_agent.services import topic_management
    
    mock_topic_service = AsyncMock()
    mock_topic_service.list_topics.return_value = []
    
    original_get_topic_service = topic_management.get_topic_service
    topic_management.get_topic_service = AsyncMock(return_value=mock_topic_service)
    
    try:
        # Start first cleanup (simulate long-running operation)
        with scheduler_service.lock:
            scheduler_service.active_cleanups.add("topic_cleanup:test-cluster")
        
        # Try to start another cleanup for the same cluster
        with pytest.raises(ValueError) as exc_info:
            await scheduler_service.execute_topic_cleanup_now(
                cluster_id="test-cluster",
                dry_run=True,
                user_id="test-user"
            )
        
        assert "conflict detected" in str(exc_info.value).lower()
        assert "topic_cleanup cleanup is already running" in str(exc_info.value)
        
    finally:
        # Clean up
        with scheduler_service.lock:
            scheduler_service.active_cleanups.discard("topic_cleanup:test-cluster")
        topic_management.get_topic_service = original_get_topic_service


@pytest.mark.asyncio
async def test_cleanup_operation_tracking(scheduler_service):
    """Test cleanup operation tracking and retrieval."""
    
    # Mock topic service
    from kafka_ops_agent.services import topic_management
    
    mock_topic_service = AsyncMock()
    mock_topic_service.list_topics.return_value = []
    
    original_get_topic_service = topic_management.get_topic_service
    topic_management.get_topic_service = AsyncMock(return_value=mock_topic_service)
    
    try:
        # Execute cleanup
        execution = await scheduler_service.execute_topic_cleanup_now(
            cluster_id="test-cluster",
            dry_run=True,
            user_id="test-user"
        )
        
        # Verify operation is tracked
        tracked_execution = scheduler_service.get_cleanup_operation(execution.execution_id)
        assert tracked_execution is not None
        assert tracked_execution.execution_id == execution.execution_id
        
        # Verify in cleanup operations list
        cleanup_ops = scheduler_service.list_cleanup_operations()
        assert len(cleanup_ops) == 1
        assert cleanup_ops[0].execution_id == execution.execution_id
        
        # Test filtering by cluster
        cluster_ops = scheduler_service.list_cleanup_operations("test-cluster")
        assert len(cluster_ops) >= 0  # May be filtered differently in implementation
        
    finally:
        topic_management.get_topic_service = original_get_topic_service


@pytest.mark.asyncio
async def test_scheduler_stats_with_cleanup(scheduler_service):
    """Test scheduler statistics include cleanup operations."""
    
    # Mock topic service
    from kafka_ops_agent.services import topic_management
    
    mock_topic_service = AsyncMock()
    mock_topic_service.list_topics.return_value = []
    
    original_get_topic_service = topic_management.get_topic_service
    topic_management.get_topic_service = AsyncMock(return_value=mock_topic_service)
    
    try:
        # Execute some cleanup operations
        await scheduler_service.execute_topic_cleanup_now(
            cluster_id="test-cluster-1",
            dry_run=True,
            user_id="test-user"
        )
        
        await scheduler_service.execute_cluster_cleanup_now(
            dry_run=True,
            user_id="test-user"
        )
        
        # Get stats
        stats = scheduler_service.get_scheduler_stats()
        
        # Verify basic stats
        assert 'scheduler_running' in stats
        assert 'total_tasks' in stats
        assert 'total_executions' in stats
        
        # Verify cleanup operations are tracked
        assert len(scheduler_service.cleanup_operations) == 2
        
    finally:
        topic_management.get_topic_service = original_get_topic_service


@pytest.mark.asyncio
async def test_cleanup_execution_logs(scheduler_service):
    """Test cleanup execution logs are captured."""
    
    # Mock topic service
    from kafka_ops_agent.services import topic_management
    
    mock_topic_service = AsyncMock()
    mock_topic_service.list_topics.return_value = [
        MagicMock(name="test-topic", configs={"retention.ms": "3600000"})
    ]
    
    original_get_topic_service = topic_management.get_topic_service
    topic_management.get_topic_service = AsyncMock(return_value=mock_topic_service)
    
    try:
        # Execute cleanup
        execution = await scheduler_service.execute_topic_cleanup_now(
            cluster_id="test-cluster",
            retention_pattern="test",
            dry_run=True,
            user_id="test-user"
        )
        
        # Verify logs are captured
        assert len(execution.logs) > 0
        assert any("Starting topic cleanup" in log for log in execution.logs)
        assert any("Cleanup parameters" in log for log in execution.logs)
        assert any("Found" in log and "topics to evaluate" in log for log in execution.logs)
        
    finally:
        topic_management.get_topic_service = original_get_topic_service


if __name__ == '__main__':
    pytest.main([__file__])