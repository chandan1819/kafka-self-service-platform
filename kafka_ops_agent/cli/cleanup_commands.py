"""CLI commands for cleanup operations."""

import asyncio
import click
import json
from typing import Optional
from datetime import datetime

from kafka_ops_agent.services.scheduler import SchedulerService, TaskType
from kafka_ops_agent.storage.factory import get_metadata_store, get_audit_store
from kafka_ops_agent.cli.config import get_current_config


async def get_scheduler_service() -> SchedulerService:
    """Get scheduler service instance."""
    metadata_store = await get_metadata_store()
    audit_store = await get_audit_store()
    return SchedulerService(metadata_store, audit_store)


@click.group()
def cleanup():
    """Cleanup operations commands."""
    pass


@cleanup.command()
@click.option('--cluster-id', required=True, help='Target cluster ID')
@click.option('--max-age-hours', default=24, type=int, help='Maximum age in hours for cleanup (default: 24)')
@click.option('--retention-pattern', default='expired', 
              type=click.Choice(['expired', 'test', 'all']),
              help='Retention pattern for cleanup (default: expired)')
@click.option('--dry-run', is_flag=True, help='Perform dry run without actual cleanup')
@click.option('--user-id', help='User ID for audit trail')
@click.option('--wait', is_flag=True, help='Wait for cleanup to complete')
def topic(cluster_id: str, max_age_hours: int, retention_pattern: str, 
          dry_run: bool, user_id: Optional[str], wait: bool):
    """Execute on-demand topic cleanup."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            
            click.echo(f"Starting topic cleanup for cluster: {cluster_id}")
            click.echo(f"Parameters:")
            click.echo(f"  - Max age: {max_age_hours} hours")
            click.echo(f"  - Retention pattern: {retention_pattern}")
            click.echo(f"  - Dry run: {dry_run}")
            
            if dry_run:
                click.echo(f"  - Mode: SIMULATION (no actual cleanup)")
            else:
                click.echo(f"  - Mode: ACTUAL CLEANUP")
            
            # Execute cleanup
            execution = await scheduler.execute_topic_cleanup_now(
                cluster_id=cluster_id,
                max_age_hours=max_age_hours,
                retention_pattern=retention_pattern,
                dry_run=dry_run,
                user_id=user_id
            )
            
            click.echo(f"\nCleanup started:")
            click.echo(f"  - Execution ID: {execution.execution_id}")
            click.echo(f"  - Status: {execution.status.value}")
            click.echo(f"  - Started at: {execution.started_at}")
            
            if wait:
                click.echo("\nWaiting for cleanup to complete...")
                
                # Poll for completion
                while execution.status.value in ['pending', 'running']:
                    await asyncio.sleep(2)
                    execution = scheduler.get_cleanup_operation(execution.execution_id)
                    if not execution:
                        click.echo("Error: Could not track cleanup progress")
                        return
                
                click.echo(f"\nCleanup completed:")
                click.echo(f"  - Status: {execution.status.value}")
                click.echo(f"  - Duration: {(execution.completed_at - execution.started_at).total_seconds():.1f}s")
                
                if execution.error_message:
                    click.echo(f"  - Error: {execution.error_message}")
                
                if execution.result:
                    result = execution.result
                    click.echo(f"\nResults:")
                    click.echo(f"  - Topics evaluated: {result.get('topics_evaluated', 0)}")
                    click.echo(f"  - Topics identified for cleanup: {result.get('topics_identified', 0)}")
                    click.echo(f"  - Topics cleaned: {result.get('topics_cleaned', 0)}")
                    
                    if result.get('topics_failed'):
                        click.echo(f"  - Topics failed: {len(result['topics_failed'])}")
                    
                    if result.get('cleaned_topics'):
                        click.echo(f"  - Cleaned topics: {', '.join(result['cleaned_topics'])}")
                
                # Show logs
                if execution.logs:
                    click.echo(f"\nExecution logs:")
                    for log in execution.logs:
                        click.echo(f"  {log}")
            else:
                click.echo(f"\nUse 'kafka-ops-agent cleanup status {execution.execution_id}' to check progress")
                
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            exit(1)
        except Exception as e:
            click.echo(f"Unexpected error: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


@cleanup.command()
@click.option('--max-age-hours', default=24, type=int, help='Maximum age in hours for cleanup (default: 24)')
@click.option('--dry-run', is_flag=True, help='Perform dry run without actual cleanup')
@click.option('--user-id', help='User ID for audit trail')
@click.option('--wait', is_flag=True, help='Wait for cleanup to complete')
def cluster(max_age_hours: int, dry_run: bool, user_id: Optional[str], wait: bool):
    """Execute on-demand cluster cleanup."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            
            click.echo(f"Starting cluster cleanup")
            click.echo(f"Parameters:")
            click.echo(f"  - Max age: {max_age_hours} hours")
            
            if dry_run:
                click.echo(f"  - Mode: SIMULATION (no actual cleanup)")
            else:
                click.echo(f"  - Mode: ACTUAL CLEANUP")
            
            # Execute cleanup
            execution = await scheduler.execute_cluster_cleanup_now(
                max_age_hours=max_age_hours,
                dry_run=dry_run,
                user_id=user_id
            )
            
            click.echo(f"\nCleanup started:")
            click.echo(f"  - Execution ID: {execution.execution_id}")
            click.echo(f"  - Status: {execution.status.value}")
            click.echo(f"  - Started at: {execution.started_at}")
            
            if wait:
                click.echo("\nWaiting for cleanup to complete...")
                
                # Poll for completion
                while execution.status.value in ['pending', 'running']:
                    await asyncio.sleep(2)
                    execution = scheduler.get_cleanup_operation(execution.execution_id)
                    if not execution:
                        click.echo("Error: Could not track cleanup progress")
                        return
                
                click.echo(f"\nCleanup completed:")
                click.echo(f"  - Status: {execution.status.value}")
                click.echo(f"  - Duration: {(execution.completed_at - execution.started_at).total_seconds():.1f}s")
                
                if execution.error_message:
                    click.echo(f"  - Error: {execution.error_message}")
                
                if execution.result:
                    result = execution.result
                    click.echo(f"\nResults:")
                    click.echo(f"  - Failed instances found: {result.get('failed_instances', 0)}")
                    click.echo(f"  - Old failed instances: {result.get('old_failed_instances', 0)}")
                    click.echo(f"  - Instances cleaned: {result.get('cleaned_instances', 0)}")
                    
                    if result.get('failed_cleanups'):
                        click.echo(f"  - Failed cleanups: {result.get('failed_cleanups', 0)}")
                
                # Show logs
                if execution.logs:
                    click.echo(f"\nExecution logs:")
                    for log in execution.logs:
                        click.echo(f"  {log}")
            else:
                click.echo(f"\nUse 'kafka-ops-agent cleanup status {execution.execution_id}' to check progress")
                
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            exit(1)
        except Exception as e:
            click.echo(f"Unexpected error: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


@cleanup.command()
@click.option('--max-age-days', default=30, type=int, help='Maximum age in days for cleanup (default: 30)')
@click.option('--dry-run', is_flag=True, help='Perform dry run without actual cleanup')
@click.option('--user-id', help='User ID for audit trail')
@click.option('--wait', is_flag=True, help='Wait for cleanup to complete')
def metadata(max_age_days: int, dry_run: bool, user_id: Optional[str], wait: bool):
    """Execute on-demand metadata cleanup."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            
            click.echo(f"Starting metadata cleanup")
            click.echo(f"Parameters:")
            click.echo(f"  - Max age: {max_age_days} days")
            
            if dry_run:
                click.echo(f"  - Mode: SIMULATION (no actual cleanup)")
            else:
                click.echo(f"  - Mode: ACTUAL CLEANUP")
            
            # Execute cleanup
            execution = await scheduler.execute_metadata_cleanup_now(
                max_age_days=max_age_days,
                dry_run=dry_run,
                user_id=user_id
            )
            
            click.echo(f"\nCleanup started:")
            click.echo(f"  - Execution ID: {execution.execution_id}")
            click.echo(f"  - Status: {execution.status.value}")
            click.echo(f"  - Started at: {execution.started_at}")
            
            if wait:
                click.echo("\nWaiting for cleanup to complete...")
                
                # Poll for completion
                while execution.status.value in ['pending', 'running']:
                    await asyncio.sleep(2)
                    execution = scheduler.get_cleanup_operation(execution.execution_id)
                    if not execution:
                        click.echo("Error: Could not track cleanup progress")
                        return
                
                click.echo(f"\nCleanup completed:")
                click.echo(f"  - Status: {execution.status.value}")
                click.echo(f"  - Duration: {(execution.completed_at - execution.started_at).total_seconds():.1f}s")
                
                if execution.error_message:
                    click.echo(f"  - Error: {execution.error_message}")
                
                if execution.result:
                    result = execution.result
                    click.echo(f"\nResults:")
                    click.echo(f"  - Audit logs cleaned: {result.get('audit_logs_cleaned', 0)}")
                    click.echo(f"  - Metadata entries cleaned: {result.get('metadata_entries_cleaned', 0)}")
                
                # Show logs
                if execution.logs:
                    click.echo(f"\nExecution logs:")
                    for log in execution.logs:
                        click.echo(f"  {log}")
            else:
                click.echo(f"\nUse 'kafka-ops-agent cleanup status {execution.execution_id}' to check progress")
                
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            exit(1)
        except Exception as e:
            click.echo(f"Unexpected error: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


@cleanup.command()
@click.argument('execution_id')
@click.option('--show-logs', is_flag=True, help='Show execution logs')
def status(execution_id: str, show_logs: bool):
    """Get cleanup operation status."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            
            # Try to get from cleanup operations first
            execution = scheduler.get_cleanup_operation(execution_id)
            
            # If not found, try regular executions
            if not execution:
                execution = scheduler.get_execution(execution_id)
            
            if not execution:
                click.echo(f"Cleanup operation not found: {execution_id}", err=True)
                exit(1)
            
            click.echo(f"Cleanup Operation Status:")
            click.echo(f"  - Execution ID: {execution.execution_id}")
            click.echo(f"  - Task ID: {execution.task_id}")
            click.echo(f"  - Status: {execution.status.value}")
            click.echo(f"  - Started at: {execution.started_at}")
            
            if execution.completed_at:
                duration = (execution.completed_at - execution.started_at).total_seconds()
                click.echo(f"  - Completed at: {execution.completed_at}")
                click.echo(f"  - Duration: {duration:.1f}s")
            
            if execution.error_message:
                click.echo(f"  - Error: {execution.error_message}")
            
            if execution.result:
                click.echo(f"\nResults:")
                result_json = json.dumps(execution.result, indent=2, default=str)
                click.echo(result_json)
            
            if show_logs and execution.logs:
                click.echo(f"\nExecution Logs:")
                for i, log in enumerate(execution.logs, 1):
                    click.echo(f"  {i:2d}. {log}")
                    
        except Exception as e:
            click.echo(f"Error getting cleanup status: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


@cleanup.command()
@click.option('--cluster-id', help='Filter by cluster ID')
@click.option('--limit', default=10, type=int, help='Maximum number of operations to show (default: 10)')
def list(cluster_id: Optional[str], limit: int):
    """List cleanup operations."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            
            operations = scheduler.list_cleanup_operations(cluster_id)
            
            if limit > 0:
                operations = operations[:limit]
            
            if not operations:
                if cluster_id:
                    click.echo(f"No cleanup operations found for cluster: {cluster_id}")
                else:
                    click.echo("No cleanup operations found")
                return
            
            click.echo(f"Cleanup Operations:")
            if cluster_id:
                click.echo(f"  Filtered by cluster: {cluster_id}")
            click.echo(f"  Showing {len(operations)} operations\n")
            
            for operation in operations:
                click.echo(f"  {operation.execution_id}")
                click.echo(f"    Task: {operation.task_id}")
                click.echo(f"    Status: {operation.status.value}")
                click.echo(f"    Started: {operation.started_at}")
                
                if operation.completed_at:
                    duration = (operation.completed_at - operation.started_at).total_seconds()
                    click.echo(f"    Duration: {duration:.1f}s")
                
                if operation.error_message:
                    click.echo(f"    Error: {operation.error_message}")
                
                click.echo()
                    
        except Exception as e:
            click.echo(f"Error listing cleanup operations: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


@cleanup.command()
@click.argument('cluster_id')
@click.option('--type', 'cleanup_type', default='topic_cleanup',
              type=click.Choice(['topic_cleanup', 'cluster_cleanup', 'metadata_cleanup']),
              help='Cleanup type to check (default: topic_cleanup)')
def conflicts(cluster_id: str, cleanup_type: str):
    """Check for cleanup operation conflicts."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            
            # Map string to TaskType
            type_mapping = {
                'topic_cleanup': TaskType.TOPIC_CLEANUP,
                'cluster_cleanup': TaskType.CLUSTER_CLEANUP,
                'metadata_cleanup': TaskType.METADATA_CLEANUP
            }
            
            task_type = type_mapping[cleanup_type]
            
            # Check for conflicts
            conflict = scheduler._check_cleanup_conflicts(task_type, cluster_id)
            
            click.echo(f"Cleanup Conflict Check:")
            click.echo(f"  - Cluster ID: {cluster_id}")
            click.echo(f"  - Cleanup Type: {cleanup_type}")
            click.echo(f"  - Has Conflict: {'Yes' if conflict else 'No'}")
            
            if conflict:
                click.echo(f"  - Conflict Reason: {conflict}")
                click.echo(f"  - Can Proceed: No")
            else:
                click.echo(f"  - Can Proceed: Yes")
                    
        except Exception as e:
            click.echo(f"Error checking cleanup conflicts: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


@cleanup.command()
def stats():
    """Show scheduler and cleanup statistics."""
    
    async def _execute():
        try:
            scheduler = await get_scheduler_service()
            stats = scheduler.get_scheduler_stats()
            
            # Add cleanup-specific stats
            with scheduler.lock:
                active_cleanups = len(scheduler.active_cleanups)
                total_cleanup_operations = len(scheduler.cleanup_operations)
            
            click.echo(f"Scheduler Statistics:")
            click.echo(f"  - Scheduler Running: {'Yes' if stats['scheduler_running'] else 'No'}")
            click.echo(f"  - Total Tasks: {stats['total_tasks']}")
            click.echo(f"  - Enabled Tasks: {stats['enabled_tasks']}")
            click.echo(f"  - Running Tasks: {stats['running_tasks']}")
            click.echo(f"  - Failed Tasks: {stats['failed_tasks']}")
            click.echo(f"  - Total Executions: {stats['total_executions']}")
            
            click.echo(f"\nCleanup Statistics:")
            click.echo(f"  - Active Cleanups: {active_cleanups}")
            click.echo(f"  - Total Cleanup Operations: {total_cleanup_operations}")
                    
        except Exception as e:
            click.echo(f"Error getting scheduler stats: {e}", err=True)
            exit(1)
    
    asyncio.run(_execute())


if __name__ == '__main__':
    cleanup()