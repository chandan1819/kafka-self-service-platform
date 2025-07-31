#!/usr/bin/env python3
"""Script to test the scheduler service."""

import sys
import asyncio
import time
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.services.scheduler import (
    get_scheduler_service, ScheduledTask, TaskType, CronParser
)
from kafka_ops_agent.logging_config import setup_logging

# Setup logging
setup_logging()


async def test_cron_parser():
    """Test cron expression parsing."""
    print("🕐 Testing Cron Parser")
    
    test_cases = [
        ("0 * * * *", "Every hour"),
        ("0 0 * * *", "Daily at midnight"),
        ("*/15 * * * *", "Every 15 minutes"),
        ("*/5 * * * *", "Every 5 minutes")
    ]
    
    for cron_expr, description in test_cases:
        try:
            next_run = CronParser.next_run_time(cron_expr)
            print(f"✅ {description}: {cron_expr} -> Next run: {next_run}")
        except Exception as e:
            print(f"❌ Failed to parse {cron_expr}: {e}")
            return False
    
    return True


async def test_task_management():
    """Test task management operations."""
    print("\n📋 Testing Task Management")
    
    try:
        scheduler = await get_scheduler_service()
        
        # Create test tasks
        task1 = ScheduledTask(
            task_id="test-topic-cleanup",
            task_type=TaskType.TOPIC_CLEANUP,
            name="Test Topic Cleanup",
            description="Clean up test topics",
            cron_expression="0 2 * * *",  # Daily at 2 AM
            target_cluster="test-cluster",
            parameters={
                'max_age_hours': 24,
                'retention_pattern': 'test',
                'dry_run': True
            },
            user_id="scheduler-test"
        )
        
        task2 = ScheduledTask(
            task_id="test-health-check",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Health Check",
            description="Check cluster health",
            cron_expression="*/5 * * * *",  # Every 5 minutes
            target_cluster="test-cluster",
            parameters={},
            user_id="scheduler-test"
        )
        
        # Add tasks
        success1 = scheduler.add_task(task1)
        success2 = scheduler.add_task(task2)
        
        if success1 and success2:
            print("✅ Tasks added successfully")
        else:
            print("❌ Failed to add tasks")
            return False
        
        # List tasks
        tasks = scheduler.list_tasks()
        print(f"✅ Found {len(tasks)} tasks:")
        for task in tasks:
            print(f"   - {task.name} ({task.task_id}): {task.cron_expression}")
            print(f"     Next run: {task.next_run}")
            print(f"     Enabled: {task.enabled}")
        
        # Test task retrieval
        retrieved_task = scheduler.get_task("test-topic-cleanup")
        if retrieved_task and retrieved_task.name == "Test Topic Cleanup":
            print("✅ Task retrieval works")
        else:
            print("❌ Task retrieval failed")
            return False
        
        # Test task disable/enable
        scheduler.disable_task("test-health-check")
        disabled_task = scheduler.get_task("test-health-check")
        if not disabled_task.enabled:
            print("✅ Task disable works")
        else:
            print("❌ Task disable failed")
            return False
        
        scheduler.enable_task("test-health-check")
        enabled_task = scheduler.get_task("test-health-check")
        if enabled_task.enabled:
            print("✅ Task enable works")
        else:
            print("❌ Task enable failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Task management test failed: {e}")
        return False


async def test_task_execution():
    """Test task execution."""
    print("\n⚙️  Testing Task Execution")
    
    try:
        scheduler = await get_scheduler_service()
        
        # Create a simple health check task
        task = ScheduledTask(
            task_id="test-execution",
            task_type=TaskType.HEALTH_CHECK,
            name="Test Execution",
            description="Test task execution",
            cron_expression="*/5 * * * *",
            target_cluster="test-cluster",
            parameters={},
            user_id="scheduler-test"
        )
        
        scheduler.add_task(task)
        
        # Execute task immediately
        print("   Executing task...")
        execution = await scheduler.execute_task_now("test-execution")
        
        if execution:
            print(f"✅ Task executed successfully")
            print(f"   Execution ID: {execution.execution_id}")
            print(f"   Status: {execution.status.value}")
            print(f"   Started: {execution.started_at}")
            print(f"   Completed: {execution.completed_at}")
            
            if execution.result:
                print(f"   Result: {execution.result}")
            
            if execution.error_message:
                print(f"   Error: {execution.error_message}")
            
            if execution.logs:
                print("   Logs:")
                for log in execution.logs:
                    print(f"     - {log}")
            
            return True
        else:
            print("❌ Task execution failed")
            return False
            
    except Exception as e:
        print(f"❌ Task execution test failed: {e}")
        return False


async def test_cleanup_tasks():
    """Test cleanup task types."""
    print("\n🧹 Testing Cleanup Tasks")
    
    try:
        scheduler = await get_scheduler_service()
        
        # Test topic cleanup task
        topic_cleanup_task = ScheduledTask(
            task_id="test-topic-cleanup-exec",
            task_type=TaskType.TOPIC_CLEANUP,
            name="Topic Cleanup Test",
            description="Test topic cleanup execution",
            cron_expression="0 * * * *",
            target_cluster="test-cluster",
            parameters={
                'max_age_hours': 1,
                'retention_pattern': 'test',
                'dry_run': True  # Safe dry run
            },
            user_id="scheduler-test"
        )
        
        scheduler.add_task(topic_cleanup_task)
        
        print("   Executing topic cleanup task...")
        execution = await scheduler.execute_task_now("test-topic-cleanup-exec")
        
        if execution:
            print(f"✅ Topic cleanup task executed: {execution.status.value}")
            if execution.result:
                result = execution.result
                print(f"   Topics evaluated: {result.get('topics_evaluated', 0)}")
                print(f"   Topics identified: {result.get('topics_identified', 0)}")
                print(f"   Dry run: {result.get('dry_run', False)}")
        else:
            print("❌ Topic cleanup task failed")
        
        # Test cluster cleanup task
        cluster_cleanup_task = ScheduledTask(
            task_id="test-cluster-cleanup-exec",
            task_type=TaskType.CLUSTER_CLEANUP,
            name="Cluster Cleanup Test",
            description="Test cluster cleanup execution",
            cron_expression="0 0 * * *",
            parameters={
                'max_age_hours': 24,
                'dry_run': True  # Safe dry run
            },
            user_id="scheduler-test"
        )
        
        scheduler.add_task(cluster_cleanup_task)
        
        print("   Executing cluster cleanup task...")
        execution = await scheduler.execute_task_now("test-cluster-cleanup-exec")
        
        if execution:
            print(f"✅ Cluster cleanup task executed: {execution.status.value}")
            if execution.result:
                result = execution.result
                print(f"   Failed instances: {result.get('failed_instances', 0)}")
                print(f"   Old failed instances: {result.get('old_failed_instances', 0)}")
                print(f"   Dry run: {result.get('dry_run', False)}")
        else:
            print("❌ Cluster cleanup task failed")
        
        return True
        
    except Exception as e:
        print(f"❌ Cleanup tasks test failed: {e}")
        return False


async def test_scheduler_stats():
    """Test scheduler statistics."""
    print("\n📊 Testing Scheduler Statistics")
    
    try:
        scheduler = await get_scheduler_service()
        
        stats = scheduler.get_scheduler_stats()
        
        print("✅ Scheduler statistics:")
        print(f"   Scheduler running: {stats['scheduler_running']}")
        print(f"   Total tasks: {stats['total_tasks']}")
        print(f"   Enabled tasks: {stats['enabled_tasks']}")
        print(f"   Running tasks: {stats['running_tasks']}")
        print(f"   Failed tasks: {stats['failed_tasks']}")
        print(f"   Total executions: {stats['total_executions']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Scheduler stats test failed: {e}")
        return False


async def test_execution_history():
    """Test execution history."""
    print("\n📜 Testing Execution History")
    
    try:
        scheduler = await get_scheduler_service()
        
        # List all executions
        executions = scheduler.list_executions()
        print(f"✅ Found {len(executions)} total executions")
        
        # Show recent executions
        for execution in executions[:5]:  # Show last 5
            print(f"   - {execution.execution_id}: {execution.status.value}")
            print(f"     Task: {execution.task_id}")
            print(f"     Started: {execution.started_at}")
            if execution.completed_at:
                duration = (execution.completed_at - execution.started_at).total_seconds()
                print(f"     Duration: {duration:.2f} seconds")
        
        # Test filtering by task ID
        if executions:
            task_id = executions[0].task_id
            task_executions = scheduler.list_executions(task_id)
            print(f"✅ Found {len(task_executions)} executions for task {task_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Execution history test failed: {e}")
        return False


async def test_scheduler_lifecycle():
    """Test scheduler start/stop lifecycle."""
    print("\n🔄 Testing Scheduler Lifecycle")
    
    try:
        scheduler = await get_scheduler_service()
        
        # Test start
        print("   Starting scheduler...")
        scheduler.start_scheduler()
        
        stats = scheduler.get_scheduler_stats()
        if stats['scheduler_running']:
            print("✅ Scheduler started successfully")
        else:
            print("❌ Scheduler failed to start")
            return False
        
        # Let it run briefly
        print("   Letting scheduler run for 5 seconds...")
        await asyncio.sleep(5)
        
        # Test stop
        print("   Stopping scheduler...")
        scheduler.stop_scheduler()
        
        stats = scheduler.get_scheduler_stats()
        if not stats['scheduler_running']:
            print("✅ Scheduler stopped successfully")
        else:
            print("❌ Scheduler failed to stop")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Scheduler lifecycle test failed: {e}")
        return False


async def main():
    """Run all scheduler tests."""
    print("🧪 Testing Scheduler Service")
    print("=" * 50)
    
    tests = [
        ("Cron Parser", test_cron_parser),
        ("Task Management", test_task_management),
        ("Task Execution", test_task_execution),
        ("Cleanup Tasks", test_cleanup_tasks),
        ("Scheduler Stats", test_scheduler_stats),
        ("Execution History", test_execution_history),
        ("Scheduler Lifecycle", test_scheduler_lifecycle)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n📊 Test Results Summary")
    print("-" * 30)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n🎉 All scheduler tests passed!")
        return True
    else:
        print("\n⚠️  Some scheduler tests failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        sys.exit(1)