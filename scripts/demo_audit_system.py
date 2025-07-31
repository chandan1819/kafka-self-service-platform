#!/usr/bin/env python3
"""Demo script for the comprehensive audit and logging system."""

import asyncio
import sys
import os
import tempfile
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_ops_agent.audit.audit_logger import (
    AuditLogger, AuditEventType, AuditLevel, set_audit_context, get_audit_logger
)
from kafka_ops_agent.audit.log_analyzer import (
    LogAnalyzer, LogQuery, LogFilter, FilterOperator,
    create_time_range_filter, create_user_activity_query
)
from kafka_ops_agent.audit.log_management import (
    LogManager, LogRotationConfig, LogAggregationConfig
)
from kafka_ops_agent.audit.structured_logging import (
    setup_structured_logging, get_structured_logger, log_with_context
)
from kafka_ops_agent.storage.base import AuditStore


class MockAuditStore(AuditStore):
    """Mock audit store for demo purposes."""
    
    def __init__(self):
        self.operations = []
    
    async def log_operation(self, cluster_id: str, operation: str, user_id: str, details: dict):
        """Log operation to memory."""
        self.operations.append({
            'cluster_id': cluster_id,
            'operation': operation,
            'user_id': user_id,
            'details': details,
            'timestamp': datetime.utcnow().isoformat()
        })
        print(f"📝 Stored audit operation: {operation} by {user_id}")


async def demo_audit_logging():
    """Demonstrate audit logging capabilities."""
    print("🔍 Demo: Audit Logging System")
    print("=" * 50)
    
    # Create audit logger with mock store
    mock_store = MockAuditStore()
    audit_logger = AuditLogger(mock_store)
    
    # Set request context
    set_audit_context(
        request_id="demo-req-123",
        user_id="demo-user",
        session_id="demo-session-456",
        source_ip="192.168.1.100"
    )
    
    print("1. Logging various audit events...")
    
    # Log authentication
    await audit_logger.log_authentication(
        user_id="demo-user",
        success=True,
        method="api_key",
        source_ip="192.168.1.100"
    )
    
    # Log cluster operations
    await audit_logger.log_cluster_operation(
        operation="create",
        cluster_id="demo-cluster",
        user_id="demo-user",
        success=True,
        details={"provider": "docker", "size": "small"}
    )
    
    # Log topic operations
    await audit_logger.log_topic_operation(
        operation="create",
        topic_name="demo-topic",
        cluster_id="demo-cluster",
        user_id="demo-user",
        success=True,
        details={"partitions": 3, "replication_factor": 1}
    )
    
    # Log API requests
    await audit_logger.log_api_request(
        method="POST",
        path="/api/v1/topics",
        status_code=201,
        user_id="demo-user",
        duration_ms=125.5
    )
    
    # Log cleanup operation
    await audit_logger.log_cleanup_operation(
        cleanup_type="topic",
        target="demo-cluster",
        user_id="demo-user",
        success=True,
        items_cleaned=5,
        details={"pattern": "test-*", "dry_run": False}
    )
    
    # Log system event
    await audit_logger.log_system_event(
        event="start",
        message="Kafka Ops Agent started successfully",
        level=AuditLevel.INFO,
        details={"version": "1.0.0", "config": "production"}
    )
    
    print(f"✅ Logged {len(mock_store.operations)} audit events")
    
    # Show some logged events
    print("\n📋 Sample audit events:")
    for i, operation in enumerate(mock_store.operations[:3], 1):
        print(f"  {i}. {operation['operation']} by {operation['user_id']}")
        print(f"     Details: {operation['details'].get('message', 'N/A')}")
    
    print()
    return mock_store


def demo_structured_logging():
    """Demonstrate structured logging."""
    print("📊 Demo: Structured Logging")
    print("=" * 50)
    
    # Setup structured logging
    loggers = setup_structured_logging(
        log_level="INFO",
        log_file=None,  # Console only for demo
        enable_console=True
    )
    
    print("1. Logging with different loggers and contexts...")
    
    # Application logger
    app_logger = loggers['app']
    app_logger.info("Application started", 
                   component="main", 
                   version="1.0.0",
                   environment="demo")
    
    # API logger with context
    api_logger = loggers['api']
    log_with_context(
        api_logger, 
        "info", 
        "API request processed",
        endpoint="/api/v1/topics",
        method="GET",
        status_code=200,
        duration_ms=45.2,
        user_id="demo-user"
    )
    
    # Service logger
    service_logger = loggers['service']
    service_logger.warning("Service degradation detected",
                          service="topic-management",
                          metric="response_time",
                          value=1500,
                          threshold=1000)
    
    # Error logging
    try:
        raise ValueError("Demo error for logging")
    except Exception as e:
        service_logger.error("Operation failed",
                           operation="topic_create",
                           error_type=type(e).__name__,
                           error_message=str(e),
                           exc_info=True)
    
    print("✅ Structured logging demo completed")
    print()


async def demo_log_analysis():
    """Demonstrate log analysis capabilities."""
    print("🔎 Demo: Log Analysis")
    print("=" * 50)
    
    # Create a temporary log file with sample data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
        sample_logs = [
            {
                'timestamp': '2023-01-01T10:00:00Z',
                'level': 'INFO',
                'event_type': 'topic.create',
                'user_id': 'alice',
                'message': 'Topic created successfully',
                'resource_type': 'topic',
                'resource_id': 'orders-topic'
            },
            {
                'timestamp': '2023-01-01T10:15:00Z',
                'level': 'INFO',
                'event_type': 'topic.create',
                'user_id': 'bob',
                'message': 'Topic created successfully',
                'resource_type': 'topic',
                'resource_id': 'users-topic'
            },
            {
                'timestamp': '2023-01-01T10:30:00Z',
                'level': 'ERROR',
                'event_type': 'topic.delete',
                'user_id': 'alice',
                'message': 'Topic deletion failed',
                'resource_type': 'topic',
                'resource_id': 'temp-topic'
            },
            {
                'timestamp': '2023-01-01T11:00:00Z',
                'level': 'INFO',
                'event_type': 'cluster.create',
                'user_id': 'admin',
                'message': 'Cluster provisioned',
                'resource_type': 'cluster',
                'resource_id': 'prod-cluster'
            },
            {
                'timestamp': '2023-01-01T11:30:00Z',
                'level': 'WARNING',
                'event_type': 'auth.failure',
                'user_id': 'unknown',
                'message': 'Authentication failed',
                'details': {'source_ip': '192.168.1.200'}
            }
        ]
        
        for log_entry in sample_logs:
            f.write(json.dumps(log_entry) + '\n')
        
        log_file_path = f.name
    
    try:
        analyzer = LogAnalyzer(log_file_path)
        
        print("1. Basic log statistics...")
        query = LogQuery(filters=[])
        stats = analyzer.get_log_statistics(query)
        
        print(f"   Total entries: {stats['total_entries']}")
        print(f"   Log levels: {stats['levels']}")
        print(f"   Event types: {stats['event_types']}")
        print(f"   Users: {stats['users']}")
        
        print("\n2. Filtering logs by user...")
        user_query = create_user_activity_query("alice", 24)
        alice_logs = list(analyzer.read_logs(user_query))
        print(f"   Alice's activities: {len(alice_logs)} events")
        for log in alice_logs:
            print(f"     - {log['event_type']}: {log['message']}")
        
        print("\n3. Searching for errors...")
        error_query = LogQuery(filters=[
            LogFilter(
                field='level',
                operator=FilterOperator.EQUALS,
                value='ERROR'
            )
        ])
        error_logs = list(analyzer.read_logs(error_query))
        print(f"   Error events: {len(error_logs)}")
        for log in error_logs:
            print(f"     - {log['user_id']}: {log['message']}")
        
        print("\n4. Aggregating by event type...")
        aggregation = analyzer.aggregate_logs(query, 'event_type')
        print("   Event type distribution:")
        for event_type, count in aggregation.items():
            print(f"     - {event_type}: {count}")
        
        print("\n5. Text search...")
        search_results = analyzer.search_logs("created", limit=5)
        print(f"   Search results for 'created': {len(search_results)} matches")
        
        print("✅ Log analysis demo completed")
        
    finally:
        # Cleanup
        Path(log_file_path).unlink()
    
    print()


def demo_log_management():
    """Demonstrate log management capabilities."""
    print("🗂️  Demo: Log Management")
    print("=" * 50)
    
    # Create log manager with custom configuration
    rotation_config = LogRotationConfig(
        max_file_size=1024,  # Small size for demo
        max_files=3,
        compress_rotated=True
    )
    
    aggregation_config = LogAggregationConfig(
        enabled=True,
        interval_minutes=60,
        retention_days=30
    )
    
    log_manager = LogManager(rotation_config, aggregation_config)
    
    print("1. Log management configuration:")
    print(f"   Max file size: {rotation_config.max_file_size} bytes")
    print(f"   Max files: {rotation_config.max_files}")
    print(f"   Compression: {rotation_config.compress_rotated}")
    print(f"   Aggregation enabled: {aggregation_config.enabled}")
    
    # Create a temporary log file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
        # Write some content
        for i in range(100):
            f.write(f"Log entry {i}: This is a sample log message\n")
        log_file_path = f.name
    
    try:
        print(f"\n2. Checking log file health...")
        health_status = log_manager.get_log_health_status([log_file_path])
        
        print(f"   Overall status: {health_status['overall_status']}")
        file_status = health_status['files'][log_file_path]
        print(f"   File status: {file_status['status']}")
        print(f"   File size: {file_status['size_mb']} MB")
        print(f"   File age: {file_status['age_hours']:.1f} hours")
        
        print("\n3. Testing rotation logic...")
        should_rotate = log_manager.rotation_manager.should_rotate(log_file_path)
        print(f"   Should rotate: {should_rotate}")
        
        if should_rotate:
            print("   Performing rotation...")
            success = log_manager.rotation_manager.rotate_log(log_file_path)
            print(f"   Rotation success: {success}")
            
            # List rotated files
            rotated_files = log_manager.rotation_manager.get_rotated_files(log_file_path)
            print(f"   Rotated files: {len(rotated_files)}")
            for rotated_file in rotated_files:
                print(f"     - {rotated_file.name}")
        
        print("✅ Log management demo completed")
        
    finally:
        # Cleanup
        try:
            Path(log_file_path).unlink()
        except FileNotFoundError:
            pass
        
        # Cleanup rotated files
        log_path = Path(log_file_path)
        for rotated_file in log_path.parent.glob(f"{log_path.stem}_*"):
            rotated_file.unlink()
    
    print()


async def main():
    """Run all audit system demos."""
    print("🚀 Kafka Ops Agent - Audit & Logging System Demo")
    print("=" * 60)
    print()
    
    # Run all demos
    await demo_audit_logging()
    demo_structured_logging()
    await demo_log_analysis()
    demo_log_management()
    
    print("🎉 Audit and logging system demo completed!")
    print("=" * 60)
    
    print("\n📚 Key Features Demonstrated:")
    print("  ✅ Comprehensive audit event logging")
    print("  ✅ Structured JSON logging with context")
    print("  ✅ Advanced log filtering and analysis")
    print("  ✅ Log aggregation and statistics")
    print("  ✅ Automated log rotation and management")
    print("  ✅ Health monitoring and status checking")
    print("  ✅ Full-text search capabilities")
    print("  ✅ User activity tracking")
    print("  ✅ Error detection and reporting")


if __name__ == "__main__":
    asyncio.run(main())