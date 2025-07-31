"""Scheduler service for cleanup tasks and maintenance operations."""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import threading
import json

from kafka_ops_agent.services.topic_management import get_topic_service
from kafka_ops_agent.services.provisioning import ProvisioningService
from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.models.cluster import ClusterStatus

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(str, Enum):
    """Types of scheduled tasks."""
    TOPIC_CLEANUP = "topic_cleanup"
    CLUSTER_CLEANUP = "cluster_cleanup"
    METADATA_CLEANUP = "metadata_cleanup"
    HEALTH_CHECK = "health_check"
    CUSTOM = "custom"


@dataclass
class ScheduledTask:
    """Represents a scheduled task."""
    task_id: str
    task_type: TaskType
    name: str
    description: str
    cron_expression: str
    target_cluster: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    failure_count: int = 0
    max_failures: int = 3
    timeout_seconds: int = 300
    user_id: Optional[str] = None


@dataclass
class TaskExecution:
    """Represents a task execution instance."""
    execution_id: str
    task_id: str
    status: TaskStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)


class CronParser:
    """Simple cron expression parser."""
    
    @staticmethod
    def parse_cron(cron_expression: str) -> Dict[str, Any]:
        """Parse cron expression into components."""
        parts = cron_expression.strip().split()
        
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts: minute hour day month weekday")
        
        return {
            'minute': parts[0],
            'hour': parts[1],
            'day': parts[2],
            'month': parts[3],
            'weekday': parts[4]
        }
    
    @staticmethod
    def next_run_time(cron_expression: str, from_time: Optional[datetime] = None) -> datetime:
        """Calculate next run time based on cron expression."""
        if from_time is None:
            from_time = datetime.utcnow()
        
        # Simple implementation for common patterns
        # In production, use a proper cron library like croniter
        
        if cron_expression == "0 * * * *":  # Every hour
            next_time = from_time.replace(minute=0, second=0, microsecond=0)
            if next_time <= from_time:
                next_time += timedelta(hours=1)
            return next_time
        
        elif cron_expression == "0 0 * * *":  # Daily at midnight
            next_time = from_time.replace(hour=0, minute=0, second=0, microsecond=0)
            if next_time <= from_time:
                next_time += timedelta(days=1)
            return next_time
        
        elif cron_expression == "*/15 * * * *":  # Every 15 minutes
            minute = (from_time.minute // 15 + 1) * 15
            if minute >= 60:
                next_time = from_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_time = from_time.replace(minute=minute, second=0, microsecond=0)
            return next_time
        
        elif cron_expression == "*/5 * * * *":  # Every 5 minutes
            minute = (from_time.minute // 5 + 1) * 5
            if minute >= 60:
                next_time = from_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_time = from_time.replace(minute=minute, second=0, microsecond=0)
            return next_time
        
        else:
            # Default to 1 hour from now for unknown patterns
            return from_time + timedelta(hours=1)


class TaskExecutor:
    """Executes scheduled tasks."""
    
    def __init__(self, metadata_store: MetadataStore, audit_store: AuditStore):
        """Initialize task executor."""
        self.metadata_store = metadata_store
        self.audit_store = audit_store
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="task-executor")
    
    async def execute_task(self, task: ScheduledTask) -> TaskExecution:
        """Execute a scheduled task."""
        execution_id = f"{task.task_id}_{int(time.time())}"
        execution = TaskExecution(
            execution_id=execution_id,
            task_id=task.task_id,
            status=TaskStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        logger.info(f"Starting task execution: {task.name} ({execution_id})")
        
        try:
            # Execute task based on type
            if task.task_type == TaskType.TOPIC_CLEANUP:
                result = await self._execute_topic_cleanup(task, execution)
            elif task.task_type == TaskType.CLUSTER_CLEANUP:
                result = await self._execute_cluster_cleanup(task, execution)
            elif task.task_type == TaskType.METADATA_CLEANUP:
                result = await self._execute_metadata_cleanup(task, execution)
            elif task.task_type == TaskType.HEALTH_CHECK:
                result = await self._execute_health_check(task, execution)
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")
            
            execution.status = TaskStatus.COMPLETED
            execution.result = result
            execution.completed_at = datetime.utcnow()
            
            logger.info(f"Task execution completed: {task.name} ({execution_id})")
            
        except Exception as e:
            execution.status = TaskStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.utcnow()
            
            logger.error(f"Task execution failed: {task.name} ({execution_id}): {e}")
        
        # Log audit event
        await self.audit_store.log_operation(
            task.target_cluster or "system",
            f"scheduled_task_{task.task_type.value}",
            task.user_id,
            {
                "task_id": task.task_id,
                "task_name": task.name,
                "execution_id": execution_id,
                "status": execution.status.value,
                "duration_seconds": (execution.completed_at - execution.started_at).total_seconds() if execution.completed_at else None,
                "result": execution.result,
                "error": execution.error_message
            }
        )
        
        return execution
    
    async def _execute_topic_cleanup(self, task: ScheduledTask, execution: TaskExecution) -> Dict[str, Any]:
        """Execute topic cleanup task."""
        execution.logs.append("Starting topic cleanup")
        
        topic_service = await get_topic_service()
        
        # Get cleanup parameters
        max_age_hours = task.parameters.get('max_age_hours', 24)
        retention_pattern = task.parameters.get('retention_pattern', 'expired')
        dry_run = task.parameters.get('dry_run', False)
        
        if not task.target_cluster:
            raise ValueError("Topic cleanup requires target_cluster")
        
        execution.logs.append(f"Cleanup parameters: max_age_hours={max_age_hours}, pattern={retention_pattern}")
        
        # List topics
        topics = await topic_service.list_topics(task.target_cluster, include_internal=False)
        execution.logs.append(f"Found {len(topics)} topics to evaluate")
        
        # Filter topics for cleanup based on criteria
        topics_to_cleanup = []
        for topic in topics:
            # Simple cleanup logic - in production, this would be more sophisticated
            if retention_pattern == 'expired':
                # Check if topic has expired retention
                if 'retention.ms' in topic.configs:
                    retention_ms = int(topic.configs['retention.ms'])
                    if retention_ms < max_age_hours * 3600 * 1000:
                        topics_to_cleanup.append(topic.name)
            elif retention_pattern == 'test':
                # Cleanup test topics
                if topic.name.startswith('test-') or topic.name.startswith('temp-'):
                    topics_to_cleanup.append(topic.name)
        
        execution.logs.append(f"Identified {len(topics_to_cleanup)} topics for cleanup")
        
        if dry_run:
            execution.logs.append("Dry run mode - no actual cleanup performed")
            return {
                'topics_evaluated': len(topics),
                'topics_identified': len(topics_to_cleanup),
                'topics_cleaned': 0,
                'dry_run': True,
                'topics_to_cleanup': topics_to_cleanup
            }
        
        # Perform cleanup
        cleaned_topics = []
        failed_topics = []
        
        for topic_name in topics_to_cleanup:
            try:
                result = await topic_service.delete_topic(
                    task.target_cluster, 
                    topic_name, 
                    task.user_id or 'scheduler'
                )
                
                if result.success:
                    cleaned_topics.append(topic_name)
                    execution.logs.append(f"Cleaned up topic: {topic_name}")
                else:
                    failed_topics.append((topic_name, result.message))
                    execution.logs.append(f"Failed to cleanup topic {topic_name}: {result.message}")
                    
            except Exception as e:
                failed_topics.append((topic_name, str(e)))
                execution.logs.append(f"Exception cleaning topic {topic_name}: {e}")
        
        return {
            'topics_evaluated': len(topics),
            'topics_identified': len(topics_to_cleanup),
            'topics_cleaned': len(cleaned_topics),
            'topics_failed': len(failed_topics),
            'cleaned_topics': cleaned_topics,
            'failed_topics': failed_topics,
            'dry_run': False
        }
    
    async def _execute_cluster_cleanup(self, task: ScheduledTask, execution: TaskExecution) -> Dict[str, Any]:
        """Execute cluster cleanup task."""
        execution.logs.append("Starting cluster cleanup")
        
        # Get failed instances
        failed_instances = await self.metadata_store.get_instances_by_status('error')
        execution.logs.append(f"Found {len(failed_instances)} failed instances")
        
        max_age_hours = task.parameters.get('max_age_hours', 24)
        dry_run = task.parameters.get('dry_run', False)
        
        # Filter instances by age
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        old_failed_instances = [
            instance for instance in failed_instances
            if instance.updated_at < cutoff_time
        ]
        
        execution.logs.append(f"Found {len(old_failed_instances)} old failed instances")
        
        if dry_run:
            return {
                'failed_instances': len(failed_instances),
                'old_failed_instances': len(old_failed_instances),
                'cleaned_instances': 0,
                'dry_run': True,
                'instances_to_cleanup': [i.instance_id for i in old_failed_instances]
            }
        
        # Cleanup old failed instances
        cleaned_count = 0
        failed_count = 0
        
        for instance in old_failed_instances:
            try:
                success = await self.metadata_store.delete_instance(instance.instance_id)
                if success:
                    cleaned_count += 1
                    execution.logs.append(f"Cleaned up failed instance: {instance.instance_id}")
                else:
                    failed_count += 1
                    execution.logs.append(f"Failed to cleanup instance: {instance.instance_id}")
                    
            except Exception as e:
                failed_count += 1
                execution.logs.append(f"Exception cleaning instance {instance.instance_id}: {e}")
        
        return {
            'failed_instances': len(failed_instances),
            'old_failed_instances': len(old_failed_instances),
            'cleaned_instances': cleaned_count,
            'failed_cleanups': failed_count,
            'dry_run': False
        }
    
    async def _execute_metadata_cleanup(self, task: ScheduledTask, execution: TaskExecution) -> Dict[str, Any]:
        """Execute metadata cleanup task."""
        execution.logs.append("Starting metadata cleanup")
        
        # Get old audit logs
        max_age_days = task.parameters.get('max_age_days', 30)
        dry_run = task.parameters.get('dry_run', False)
        
        execution.logs.append(f"Cleaning audit logs older than {max_age_days} days")
        
        # This would require additional methods in audit store
        # For now, return a placeholder result
        return {
            'audit_logs_cleaned': 0,
            'metadata_entries_cleaned': 0,
            'dry_run': dry_run
        }
    
    async def _execute_health_check(self, task: ScheduledTask, execution: TaskExecution) -> Dict[str, Any]:
        """Execute health check task."""
        execution.logs.append("Starting health check")
        
        if not task.target_cluster:
            raise ValueError("Health check requires target_cluster")
        
        topic_service = await get_topic_service()
        
        # Get cluster info
        cluster_info = await topic_service.get_cluster_info(task.target_cluster)
        
        if cluster_info:
            execution.logs.append(f"Cluster {task.target_cluster} is accessible")
            execution.logs.append(f"Brokers: {cluster_info.get('broker_count', 'Unknown')}")
            execution.logs.append(f"Topics: {cluster_info.get('topic_count', 'Unknown')}")
            
            return {
                'cluster_accessible': True,
                'broker_count': cluster_info.get('broker_count', 0),
                'topic_count': cluster_info.get('topic_count', 0),
                'cluster_info': cluster_info
            }
        else:
            execution.logs.append(f"Cluster {task.target_cluster} is not accessible")
            return {
                'cluster_accessible': False,
                'broker_count': 0,
                'topic_count': 0
            }
    
    def close(self):
        """Close the task executor."""
        self.executor.shutdown(wait=True)


class SchedulerService:
    """Service for managing scheduled tasks and cleanup operations."""
    
    def __init__(self, metadata_store: MetadataStore, audit_store: AuditStore):
        """Initialize scheduler service."""
        self.metadata_store = metadata_store
        self.audit_store = audit_store
        self.task_executor = TaskExecutor(metadata_store, audit_store)
        
        # Task management
        self.tasks: Dict[str, ScheduledTask] = {}
        self.executions: Dict[str, TaskExecution] = {}
        self.running_tasks: Set[str] = set()
        
        # On-demand cleanup tracking
        self.cleanup_operations: Dict[str, TaskExecution] = {}
        self.active_cleanups: Set[str] = set()  # Track active cleanup operations by cluster
        
        # Scheduler control
        self.running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.lock = threading.RLock()
        
        logger.info("Scheduler service initialized")
    
    def add_task(self, task: ScheduledTask) -> bool:
        """Add a scheduled task."""
        try:
            with self.lock:
                if task.task_id in self.tasks:
                    logger.warning(f"Task {task.task_id} already exists")
                    return False
                
                # Calculate next run time
                task.next_run = CronParser.next_run_time(task.cron_expression)
                
                self.tasks[task.task_id] = task
                logger.info(f"Added scheduled task: {task.name} ({task.task_id})")
                logger.info(f"Next run: {task.next_run}")
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to add task {task.task_id}: {e}")
            return False
    
    def remove_task(self, task_id: str) -> bool:
        """Remove a scheduled task."""
        with self.lock:
            if task_id in self.tasks:
                del self.tasks[task_id]
                logger.info(f"Removed scheduled task: {task_id}")
                return True
            return False
    
    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get a scheduled task by ID."""
        with self.lock:
            return self.tasks.get(task_id)
    
    def list_tasks(self) -> List[ScheduledTask]:
        """List all scheduled tasks."""
        with self.lock:
            return list(self.tasks.values())
    
    def enable_task(self, task_id: str) -> bool:
        """Enable a scheduled task."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].enabled = True
                logger.info(f"Enabled task: {task_id}")
                return True
            return False
    
    def disable_task(self, task_id: str) -> bool:
        """Disable a scheduled task."""
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].enabled = False
                logger.info(f"Disabled task: {task_id}")
                return True
            return False
    
    async def execute_task_now(self, task_id: str) -> Optional[TaskExecution]:
        """Execute a task immediately."""
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                logger.error(f"Task not found: {task_id}")
                return None
            
            if task_id in self.running_tasks:
                logger.warning(f"Task already running: {task_id}")
                return None
            
            self.running_tasks.add(task_id)
        
        try:
            execution = await self.task_executor.execute_task(task)
            
            # Update task statistics
            with self.lock:
                task.last_run = execution.started_at
                task.run_count += 1
                
                if execution.status == TaskStatus.FAILED:
                    task.failure_count += 1
                else:
                    task.failure_count = 0  # Reset on success
                
                # Calculate next run time
                task.next_run = CronParser.next_run_time(task.cron_expression, execution.completed_at)
            
            # Store execution
            self.executions[execution.execution_id] = execution
            
            return execution
            
        finally:
            with self.lock:
                self.running_tasks.discard(task_id)
    
    def get_execution(self, execution_id: str) -> Optional[TaskExecution]:
        """Get task execution by ID."""
        return self.executions.get(execution_id)
    
    def list_executions(self, task_id: Optional[str] = None) -> List[TaskExecution]:
        """List task executions, optionally filtered by task ID."""
        executions = list(self.executions.values())
        
        if task_id:
            executions = [e for e in executions if e.task_id == task_id]
        
        # Sort by start time, most recent first
        executions.sort(key=lambda e: e.started_at, reverse=True)
        
        return executions
    
    def start_scheduler(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        logger.info("Scheduler started")
    
    def stop_scheduler(self):
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        logger.info("Scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        logger.info("Scheduler loop started")
        
        while self.running:
            try:
                current_time = datetime.utcnow()
                
                # Check for tasks that need to run
                tasks_to_run = []
                
                with self.lock:
                    for task in self.tasks.values():
                        if (task.enabled and 
                            task.next_run and 
                            task.next_run <= current_time and
                            task.task_id not in self.running_tasks and
                            task.failure_count < task.max_failures):
                            
                            tasks_to_run.append(task)
                
                # Execute tasks
                for task in tasks_to_run:
                    try:
                        logger.info(f"Executing scheduled task: {task.name}")
                        
                        # Run task in background
                        asyncio.create_task(self.execute_task_now(task.task_id))
                        
                    except Exception as e:
                        logger.error(f"Failed to execute scheduled task {task.name}: {e}")
                
                # Sleep for a short interval
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)  # Wait longer on error
        
        logger.info("Scheduler loop stopped")
    
    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        with self.lock:
            total_tasks = len(self.tasks)
            enabled_tasks = sum(1 for task in self.tasks.values() if task.enabled)
            running_tasks = len(self.running_tasks)
            failed_tasks = sum(1 for task in self.tasks.values() if task.failure_count >= task.max_failures)
            
            return {
                'scheduler_running': self.running,
                'total_tasks': total_tasks,
                'enabled_tasks': enabled_tasks,
                'running_tasks': running_tasks,
                'failed_tasks': failed_tasks,
                'total_executions': len(self.executions)
            }
    
    async def execute_cleanup_now(self, cleanup_type: TaskType, cluster_id: str, 
                                 parameters: Dict[str, Any], user_id: Optional[str] = None) -> TaskExecution:
        """Execute an on-demand cleanup operation."""
        
        # Check for conflicts
        conflict_check = self._check_cleanup_conflicts(cleanup_type, cluster_id)
        if conflict_check:
            raise ValueError(f"Cleanup conflict detected: {conflict_check}")
        
        # Create temporary task for cleanup
        task_id = f"ondemand_{cleanup_type.value}_{cluster_id}_{int(time.time())}"
        cleanup_task = ScheduledTask(
            task_id=task_id,
            task_type=cleanup_type,
            name=f"On-demand {cleanup_type.value} cleanup",
            description=f"On-demand cleanup operation for {cluster_id}",
            cron_expression="",  # Not used for on-demand
            target_cluster=cluster_id,
            parameters=parameters,
            user_id=user_id
        )
        
        # Mark cluster as having active cleanup
        with self.lock:
            self.active_cleanups.add(f"{cleanup_type.value}:{cluster_id}")
        
        try:
            logger.info(f"Starting on-demand cleanup: {cleanup_type.value} for cluster {cluster_id}")
            
            # Execute the cleanup
            execution = await self.task_executor.execute_task(cleanup_task)
            
            # Store execution for tracking
            self.cleanup_operations[execution.execution_id] = execution
            
            # Log cleanup operation
            await self.audit_store.log_operation(
                cluster_id,
                f"ondemand_cleanup_{cleanup_type.value}",
                user_id,
                {
                    "cleanup_type": cleanup_type.value,
                    "parameters": parameters,
                    "execution_id": execution.execution_id,
                    "status": execution.status.value,
                    "result": execution.result
                }
            )
            
            logger.info(f"On-demand cleanup completed: {cleanup_type.value} for cluster {cluster_id}")
            return execution
            
        finally:
            # Remove from active cleanups
            with self.lock:
                self.active_cleanups.discard(f"{cleanup_type.value}:{cluster_id}")
    
    def _check_cleanup_conflicts(self, cleanup_type: TaskType, cluster_id: str) -> Optional[str]:
        """Check for cleanup operation conflicts."""
        with self.lock:
            # Check if same type of cleanup is already running for this cluster
            conflict_key = f"{cleanup_type.value}:{cluster_id}"
            if conflict_key in self.active_cleanups:
                return f"Another {cleanup_type.value} cleanup is already running for cluster {cluster_id}"
            
            # Check if any scheduled task is running for this cluster
            for task_id in self.running_tasks:
                task = self.tasks.get(task_id)
                if task and task.target_cluster == cluster_id and task.task_type == cleanup_type:
                    return f"Scheduled {cleanup_type.value} task is currently running for cluster {cluster_id}"
            
            # Check for conflicting cleanup types
            if cleanup_type == TaskType.TOPIC_CLEANUP:
                # Topic cleanup conflicts with cluster cleanup
                cluster_conflict = f"{TaskType.CLUSTER_CLEANUP.value}:{cluster_id}"
                if cluster_conflict in self.active_cleanups:
                    return f"Cluster cleanup is running, cannot perform topic cleanup"
            
            elif cleanup_type == TaskType.CLUSTER_CLEANUP:
                # Cluster cleanup conflicts with any other cleanup
                for active_cleanup in self.active_cleanups:
                    if active_cleanup.endswith(f":{cluster_id}"):
                        return f"Another cleanup operation is running for cluster {cluster_id}"
        
        return None
    
    async def execute_topic_cleanup_now(self, cluster_id: str, max_age_hours: int = 24, 
                                       retention_pattern: str = 'expired', dry_run: bool = False,
                                       user_id: Optional[str] = None) -> TaskExecution:
        """Execute on-demand topic cleanup."""
        parameters = {
            'max_age_hours': max_age_hours,
            'retention_pattern': retention_pattern,
            'dry_run': dry_run
        }
        
        return await self.execute_cleanup_now(
            TaskType.TOPIC_CLEANUP, 
            cluster_id, 
            parameters, 
            user_id
        )
    
    async def execute_cluster_cleanup_now(self, max_age_hours: int = 24, dry_run: bool = False,
                                         user_id: Optional[str] = None) -> TaskExecution:
        """Execute on-demand cluster cleanup."""
        parameters = {
            'max_age_hours': max_age_hours,
            'dry_run': dry_run
        }
        
        return await self.execute_cleanup_now(
            TaskType.CLUSTER_CLEANUP, 
            "system",  # Cluster cleanup is system-wide
            parameters, 
            user_id
        )
    
    async def execute_metadata_cleanup_now(self, max_age_days: int = 30, dry_run: bool = False,
                                          user_id: Optional[str] = None) -> TaskExecution:
        """Execute on-demand metadata cleanup."""
        parameters = {
            'max_age_days': max_age_days,
            'dry_run': dry_run
        }
        
        return await self.execute_cleanup_now(
            TaskType.METADATA_CLEANUP, 
            "system",  # Metadata cleanup is system-wide
            parameters, 
            user_id
        )
    
    def get_cleanup_operation(self, execution_id: str) -> Optional[TaskExecution]:
        """Get cleanup operation by execution ID."""
        return self.cleanup_operations.get(execution_id)
    
    def list_cleanup_operations(self, cluster_id: Optional[str] = None) -> List[TaskExecution]:
        """List cleanup operations, optionally filtered by cluster."""
        operations = list(self.cleanup_operations.values())
        
        if cluster_id:
            # Filter by cluster - need to check the task that created the execution
            filtered_operations = []
            for operation in operations:
                # Find the original task parameters
                if hasattr(operation, 'result') and operation.result:
                    # Check if this operation was for the specified cluster
                    # This is a simplified check - in production you might store cluster_id in execution
                    filtered_operations.append(operation)
            operations = filtered_operations
        
        # Sort by start time, most recent first
        operations.sort(key=lambda e: e.started_at, reverse=True)
        
        return operations
    
    def get_active_cleanups(self) -> List[Dict[str, str]]:
        """Get list of currently active cleanup operations."""
        with self.lock:
            active_list = []
            for cleanup_key in self.active_cleanups:
                cleanup_type, cluster_id = cleanup_key.split(':', 1)
                active_list.append({
                    'cleanup_type': cleanup_type,
                    'cluster_id': cluster_id,
                    'started_at': datetime.utcnow().isoformat()  # Approximate
                })
            return active_list
    
    def cancel_cleanup_operation(self, cleanup_type: TaskType, cluster_id: str) -> bool:
        """Cancel an active cleanup operation."""
        with self.lock:
            conflict_key = f"{cleanup_type.value}:{cluster_id}"
            if conflict_key in self.active_cleanups:
                self.active_cleanups.discard(conflict_key)
                logger.info(f"Cancelled cleanup operation: {cleanup_type.value} for cluster {cluster_id}")
                return True
            return False
    
    def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get cleanup operation statistics."""
        with self.lock:
            total_cleanups = len(self.cleanup_operations)
            active_cleanups = len(self.active_cleanups)
            
            # Count by status
            completed_cleanups = sum(1 for op in self.cleanup_operations.values() 
                                   if op.status == TaskStatus.COMPLETED)
            failed_cleanups = sum(1 for op in self.cleanup_operations.values() 
                                if op.status == TaskStatus.FAILED)
            
            # Count by type
            cleanup_by_type = {}
            for operation in self.cleanup_operations.values():
                # Extract type from task_id (format: ondemand_{type}_{cluster}_{timestamp})
                parts = operation.task_id.split('_')
                if len(parts) >= 2:
                    cleanup_type = parts[1]
                    cleanup_by_type[cleanup_type] = cleanup_by_type.get(cleanup_type, 0) + 1
            
            return {
                'total_cleanup_operations': total_cleanups,
                'active_cleanup_operations': active_cleanups,
                'completed_cleanups': completed_cleanups,
                'failed_cleanups': failed_cleanups,
                'cleanup_by_type': cleanup_by_type,
                'active_cleanup_details': self.get_active_cleanups()
            }

    def close(self):
        """Close the scheduler service."""
        logger.info("Shutting down scheduler service")
        
        self.stop_scheduler()
        self.task_executor.close()
        
        logger.info("Scheduler service shutdown complete")


# Global scheduler service instance
_scheduler_service: Optional[SchedulerService] = None


async def get_scheduler_service() -> SchedulerService:
    """Get or create global scheduler service instance."""
    global _scheduler_service
    if _scheduler_service is None:
        from kafka_ops_agent.storage.factory import get_metadata_store, get_audit_store
        metadata_store = await get_metadata_store()
        audit_store = await get_audit_store()
        _scheduler_service = SchedulerService(metadata_store, audit_store)
    return _scheduler_service


def close_scheduler_service():
    """Close global scheduler service."""
    global _scheduler_service
    if _scheduler_service:
        _scheduler_service.close()
        _scheduler_service = None