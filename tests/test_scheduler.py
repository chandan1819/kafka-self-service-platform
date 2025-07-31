"""Tests for scheduler service."""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

from kafka_ops_agent.services.scheduler import (
    SchedulerService, ScheduledTask, TaskType, TaskStatus, 
    CronParser, TaskExecutor, TaskExecution
)
from kafka_ops_agent.models.topic import TopicInfo, TopicOperationResult


class TestCronParser:
    """Test cron expression parsing."""
    
    def test_parse_cron_valid(self):
        """Test parsing valid cron expressions."""
        result = CronParser.parse_cron("0 * * * *")
        
        assert result['minute'] == '0'
        assert result['hour'] == '*'
        assert result['day'] == '*'
        assert result['month'] == '*'
        assert result['weekday'] == '*'
    
    def test_parse_cron_invalid(self):
        """Test parsing invalid cron expressions."""
        with pytest.raises(ValueError, match="must have 5 parts"):
            CronParser.parse_cron("0 * * *")  # Missing weekday
    
    def test_next_run_time_hourly(self):
        """Test next run time calculation for hourly tasks."""
        base_time = datetime(2023, 1, 1, 10, 30, 0)
        next_run = CronParser.next_run_time("0 * * * *", base_time)
        
        expected = datetime(2023, 1, 1, 11, 0, 0)
        assert next_run == expected
    
    def test_next_run_time_daily(self):
        """Test next run time calculation for daily tasks."""
        base_time = datetime(2023, 1, 1, 10, 30, 0)
        next_run = CronParser.next_run_time("0 0 * * *", base_time)
        
        expected = datetime(2023, 1, 2, 0, 0, 0)
        assert next_run == expected
    
    def test_next_run_time_every_15_minutes(self):
        """Test next run time calculation for 15-minute intervals."""
        base_time = datetime(2023, 1, 1, 10, 7, 0)
        next_run = CronParser.next_run_time("*/15 * * * *", base_time)
        
        expected = datetime(2023, 1, 1, 10, 15, 0)
        assert next_run == expected
    
    def test_next_run_time_every_5_minutes(self):
        """Test next run time calculation for 5-minute intervals."""
        base_time = datetime(2023, 1, 1, 10, 7, 0)
        next_run = CronParser.next_run_time("*/5 * * * *", base_time)
        
        expected = datetime(2023, 1, 1, 10, 10, 0)
        assert next_run == expected


class TestScheduledTask:
    """Test scheduled task model."""
    
    def test_task_creation(self):
        """Test creating a scheduled task."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.TOPIC_CLEANUP,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *",
            target_cluster="test-cluster"
        )
        
        assert task.task_id == "test-task"
        assert task.task_type == TaskType.TOPIC_CLEANUP
        assert task.enabled is True
        assert task.run_count == 0
        assert task.failure_count == 0


class TestTaskExecutor:
    """Test task executor."""
    
    @pytest.fixture
    def mock_stores(self):
        """Create mock stores."""
        metadata_store = AsyncMock()
        audit_store = AsyncMock()
        return metadata_store, audit_store
    
    @pytest.fixture
    def executor(self, mock_stores):
        """Create task executor."""
        metadata_store, audit_store = mock_stores
        return TaskExecutor(metadata_store, audit_store)
    
    @pytest.mark.asyncio
    async def test_execute_health_check_task(self, executor):
        """Test executing health check task."""
        task = ScheduledTask(
            task_id="health-check",
            task_type=TaskType.HEALTH_CHECK,
            name="Health Check",
            description="Check cluster health",
            cron_expression="*/5 * * * *",
            target_cluster="test-cluster"
        )
        
        with patch('kafka_ops_agent.services.scheduler.get_topic_service') as mock_service:
            mock_topic_service = AsyncMock()
            mock_topic_service.get_cluster_info.return_value = {
                'cluster_id': 'test-cluster',
                'broker_count': 3,
                'topic_count': 10
            }
            mock_service.return_value = mock_topic_service
            
            execution = await executor.execute_task(task)
            
            assert execution.status == TaskStatus.COMPLETED
            assert execution.result['cluster_accessible'] is True
            assert execution.result['broker_count'] == 3
            assert execution.result['topic_count'] == 10
    
    @pytest.mark.asyncio
    async def test_execute_health_check_task_failure(self, executor):
        """Test executing health check task when cluster is unavailable."""
        task = ScheduledTask(
            task_id="health-check",
            task_type=TaskType.HEALTH_CHECK,
            name="Health Check",
            description="Check cluster health",
            cron_expression="*/5 * * * *",
            target_cluster="test-cluster"
        )
        
        with patch('kafka_ops_agent.services.scheduler.get_topic_service') as mock_service:
            mock_topic_service = AsyncMock()
            mock_topic_service.get_cluster_info.return_value = None
            mock_service.return_value = mock_topic_service
            
            execution = await executor.execute_task(task)
            
            assert execution.status == TaskStatus.COMPLETED
            assert execution.result['cluster_accessible'] is False
            assert execution.result['broker_count'] == 0
    
    @pytest.mark.asyncio
    async def test_execute_topic_cleanup_task(self, executor):
        """Test executing topic cleanup task."""
        task = ScheduledTask(
            task_id="topic-cleanup",
            task_type=TaskType.TOPIC_CLEANUP,
            name="Topic Cleanup",
            description="Clean up old topics",
            cron_expression="0 2 * * *",
            target_cluster="test-cluster",
            parameters={
                'max_age_hours': 24,
                'retention_pattern': 'test',
                'dry_run': True
            }
        )
        
        with patch('kafka_ops_agent.services.scheduler.get_topic_service') as mock_service:
            mock_topic_service = AsyncMock()
            mock_topic_service.list_topics.return_value = [
                TopicInfo(name='test-topic-1', partitions=3, replication_factor=1),
                TopicInfo(name='prod-topic-1', partitions=6, replication_factor=2),
                TopicInfo(name='temp-topic-1', partitions=1, replication_factor=1)
            ]
            mock_service.return_value = mock_topic_service
            
            execution = await executor.execute_task(task)
            
            assert execution.status == TaskStatus.COMPLETED
            assert execution.result['topics_evaluated'] == 3
            assert execution.result['topics_identified'] == 2  # test- and temp- topics
            assert execution.result['dry_run'] is True
            assert 'test-topic-1' in execution.result['topics_to_cleanup']
            assert 'temp-topic-1' in execution.result['topics_to_cleanup']
    
    @pytest.mark.asyncio
    async def test_execute_cluster_cleanup_task(self, executor, mock_stores):
        """Test executing cluster cleanup task."""
        metadata_store, audit_store = mock_stores
        
        # Mock failed instances
        from kafka_ops_agent.models.factory import ServiceInstanceFactory
        old_instance = ServiceInstanceFactory.create_default("old-failed")
        old_instance.updated_at = datetime.utcnow() - timedelta(hours=48)
        
        recent_instance = ServiceInstanceFactory.create_default("recent-failed")
        recent_instance.updated_at = datetime.utcnow() - timedelta(hours=1)
        
        metadata_store.get_instances_by_status.return_value = [old_instance, recent_instance]
        metadata_store.delete_instance.return_value = True
        
        task = ScheduledTask(
            task_id="cluster-cleanup",
            task_type=TaskType.CLUSTER_CLEANUP,
            name="Cluster Cleanup",
            description="Clean up failed instances",
            cron_expression="0 0 * * *",
            parameters={
                'max_age_hours': 24,
                'dry_run': True
            }
        )
        
        execution = await executor.execute_task(task)
        
        assert execution.status == TaskStatus.COMPLETED
        assert execution.result['failed_instances'] == 2
        assert execution.result['old_failed_instances'] == 1
        assert execution.result['dry_run'] is True
    
    @pytest.mark.asyncio
    async def test_execute_task_exception(self, executor):
        """Test task execution with exception."""
        task = ScheduledTask(
            task_id="health-check",
            task_type=TaskType.HEALTH_CHECK,
            name="Health Check",
            description="Check cluster health",
            cron_expression="*/5 * * * *"
            # Missing target_cluster - should cause exception
        )
        
        execution = await executor.execute_task(task)
        
        assert execution.status == TaskStatus.FAILED
        assert "requires target_cluster" in execution.error_message


class TestSchedulerService:
    """Test scheduler service."""
    
    @pytest.fixture
    def mock_stores(self):
        """Create mock stores."""
        metadata_store = AsyncMock()
        audit_store = AsyncMock()
        return metadata_store, audit_store
    
    @pytest.fixture
    def scheduler(self, mock_stores):
        """Create scheduler service."""
        metadata_store, audit_store = mock_stores
        return SchedulerService(metadata_store, audit_store)
    
    def test_add_task(self, scheduler):
        """Test adding a scheduled task."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *",
            target_cluster="test-cluster"
        )
        
        result = scheduler.add_task(task)
        
        assert result is True
        assert "test-task" in scheduler.tasks
        assert scheduler.tasks["test-task"].next_run is not None
    
    def test_add_duplicate_task(self, scheduler):
        """Test adding duplicate task."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *"
        )
        
        # Add first time
        result1 = scheduler.add_task(task)
        assert result1 is True
        
        # Add second time (duplicate)
        result2 = scheduler.add_task(task)
        assert result2 is False
    
    def test_remove_task(self, scheduler):
        """Test removing a scheduled task."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *"
        )
        
        scheduler.add_task(task)
        
        result = scheduler.remove_task("test-task")
        assert result is True
        assert "test-task" not in scheduler.tasks
        
        # Try to remove non-existent task
        result2 = scheduler.remove_task("non-existent")
        assert result2 is False
    
    def test_get_task(self, scheduler):
        """Test getting a task by ID."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *"
        )
        
        scheduler.add_task(task)
        
        retrieved_task = scheduler.get_task("test-task")
        assert retrieved_task is not None
        assert retrieved_task.task_id == "test-task"
        
        # Test non-existent task
        non_existent = scheduler.get_task("non-existent")
        assert non_existent is None
    
    def test_list_tasks(self, scheduler):
        """Test listing all tasks."""
        task1 = ScheduledTask(
            task_id="task-1",
            task_type=TaskType.HEALTH_CHECK,
            name="Task 1",
            description="First task",
            cron_expression="0 * * * *"
        )
        
        task2 = ScheduledTask(
            task_id="task-2",
            task_type=TaskType.TOPIC_CLEANUP,
            name="Task 2",
            description="Second task",
            cron_expression="0 0 * * *"
        )
        
        scheduler.add_task(task1)
        scheduler.add_task(task2)
        
        tasks = scheduler.list_tasks()
        assert len(tasks) == 2
        task_ids = [task.task_id for task in tasks]
        assert "task-1" in task_ids
        assert "task-2" in task_ids
    
    def test_enable_disable_task(self, scheduler):
        """Test enabling and disabling tasks."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *"
        )
        
        scheduler.add_task(task)
        
        # Test disable
        result = scheduler.disable_task("test-task")
        assert result is True
        assert not scheduler.tasks["test-task"].enabled
        
        # Test enable
        result = scheduler.enable_task("test-task")
        assert result is True
        assert scheduler.tasks["test-task"].enabled
        
        # Test non-existent task
        result = scheduler.disable_task("non-existent")
        assert result is False
    
    @pytest.mark.asyncio
    async def test_execute_task_now(self, scheduler):
        """Test immediate task execution."""
        task = ScheduledTask(
            task_id="test-task",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Task",
            description="Test description",
            cron_expression="0 * * * *",
            target_cluster="test-cluster"
        )
        
        scheduler.add_task(task)
        
        with patch('kafka_ops_agent.services.scheduler.get_topic_service') as mock_service:
            mock_topic_service = AsyncMock()
            mock_topic_service.get_cluster_info.return_value = {
                'cluster_id': 'test-cluster',
                'broker_count': 1
            }
            mock_service.return_value = mock_topic_service
            
            execution = await scheduler.execute_task_now("test-task")
            
            assert execution is not None
            assert execution.task_id == "test-task"
            assert execution.status == TaskStatus.COMPLETED
            
            # Check task statistics were updated
            updated_task = scheduler.get_task("test-task")
            assert updated_task.run_count == 1
            assert updated_task.last_run is not None
    
    @pytest.mark.asyncio
    async def test_execute_nonexistent_task(self, scheduler):
        """Test executing non-existent task."""
        execution = await scheduler.execute_task_now("non-existent")
        assert execution is None
    
    def test_get_scheduler_stats(self, scheduler):
        """Test getting scheduler statistics."""
        # Add some tasks
        task1 = ScheduledTask(
            task_id="task-1",
            task_type=TaskType.HEALTH_CHECK,
            name="Task 1",
            description="First task",
            cron_expression="0 * * * *",
            enabled=True
        )
        
        task2 = ScheduledTask(
            task_id="task-2",
            task_type=TaskType.TOPIC_CLEANUP,
            name="Task 2",
            description="Second task",
            cron_expression="0 0 * * *",
            enabled=False
        )
        
        scheduler.add_task(task1)
        scheduler.add_task(task2)
        
        stats = scheduler.get_scheduler_stats()
        
        assert stats['total_tasks'] == 2
        assert stats['enabled_tasks'] == 1
        assert stats['running_tasks'] == 0
        assert stats['failed_tasks'] == 0
        assert stats['scheduler_running'] is False
    
    def test_scheduler_lifecycle(self, scheduler):
        """Test scheduler start/stop lifecycle."""
        # Initially not running
        assert not scheduler.running
        
        # Start scheduler
        scheduler.start_scheduler()
        assert scheduler.running
        assert scheduler.scheduler_thread is not None
        
        # Stop scheduler
        scheduler.stop_scheduler()
        assert not scheduler.running
    
    def test_list_executions(self, scheduler):
        """Test listing task executions."""
        # Create some mock executions
        execution1 = TaskExecution(
            execution_id="exec-1",
            task_id="task-1",
            status=TaskStatus.COMPLETED,
            started_at=datetime.utcnow() - timedelta(minutes=10)
        )
        
        execution2 = TaskExecution(
            execution_id="exec-2",
            task_id="task-2",
            status=TaskStatus.FAILED,
            started_at=datetime.utcnow() - timedelta(minutes=5)
        )
        
        scheduler.executions["exec-1"] = execution1
        scheduler.executions["exec-2"] = execution2
        
        # Test list all executions
        all_executions = scheduler.list_executions()
        assert len(all_executions) == 2
        # Should be sorted by start time, most recent first
        assert all_executions[0].execution_id == "exec-2"
        assert all_executions[1].execution_id == "exec-1"
        
        # Test list executions for specific task
        task1_executions = scheduler.list_executions("task-1")
        assert len(task1_executions) == 1
        assert task1_executions[0].execution_id == "exec-1"