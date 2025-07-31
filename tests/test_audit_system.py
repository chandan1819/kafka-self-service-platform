"""Tests for the audit and logging system."""

import pytest
import json
import tempfile
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch

from kafka_ops_agent.audit.audit_logger import (
    AuditLogger, AuditEvent, AuditEventType, AuditLevel,
    set_audit_context, clear_audit_context
)
from kafka_ops_agent.audit.log_analyzer import (
    LogAnalyzer, LogQuery, LogFilter, FilterOperator,
    create_time_range_filter, create_user_activity_query
)
from kafka_ops_agent.audit.log_management import (
    LogRotationManager, LogAggregator, LogManager,
    LogRotationConfig, LogAggregationConfig
)


class TestAuditLogger:
    """Test audit logging functionality."""
    
    @pytest.fixture
    def mock_audit_store(self):
        """Create mock audit store."""
        store = AsyncMock()
        store.log_operation = AsyncMock()
        return store
    
    @pytest.fixture
    def audit_logger(self, mock_audit_store):
        """Create audit logger with mock store."""
        return AuditLogger(mock_audit_store)
    
    @pytest.mark.asyncio
    async def test_log_basic_event(self, audit_logger):
        """Test logging a basic audit event."""
        event = await audit_logger.log_event(
            event_type=AuditEventType.TOPIC_CREATE,
            message="Topic created successfully",
            user_id="test-user",
            resource_type="topic",
            resource_id="test-topic"
        )
        
        assert event.event_type == AuditEventType.TOPIC_CREATE
        assert event.message == "Topic created successfully"
        assert event.user_id == "test-user"
        assert event.resource_type == "topic"
        assert event.resource_id == "test-topic"
        assert event.level == AuditLevel.INFO
        assert event.event_id is not None
    
    @pytest.mark.asyncio
    async def test_log_event_with_context(self, audit_logger):
        """Test logging event with request context."""
        # Set context
        set_audit_context(
            request_id="req-123",
            user_id="context-user",
            session_id="session-456"
        )
        
        try:
            event = await audit_logger.log_event(
                event_type=AuditEventType.API_REQUEST,
                message="API request processed"
            )
            
            assert event.request_id == "req-123"
            assert event.user_id == "context-user"
            assert event.session_id == "session-456"
            
        finally:
            clear_audit_context()
    
    @pytest.mark.asyncio
    async def test_log_authentication_success(self, audit_logger):
        """Test logging successful authentication."""
        await audit_logger.log_authentication(
            user_id="test-user",
            success=True,
            method="api_key",
            source_ip="192.168.1.1"
        )
        
        # Verify audit store was called
        audit_logger.audit_store.log_operation.assert_called_once()
        call_args = audit_logger.audit_store.log_operation.call_args
        
        assert call_args[1]['operation'] == AuditEventType.LOGIN.value
        assert call_args[1]['user_id'] == "test-user"
    
    @pytest.mark.asyncio
    async def test_log_authentication_failure(self, audit_logger):
        """Test logging failed authentication."""
        await audit_logger.log_authentication(
            user_id="test-user",
            success=False,
            method="api_key"
        )
        
        # Verify audit store was called
        call_args = audit_logger.audit_store.log_operation.call_args
        assert call_args[1]['operation'] == AuditEventType.AUTH_FAILURE.value
    
    @pytest.mark.asyncio
    async def test_log_cluster_operation(self, audit_logger):
        """Test logging cluster operations."""
        await audit_logger.log_cluster_operation(
            operation="create",
            cluster_id="test-cluster",
            user_id="test-user",
            success=True,
            details={"provider": "docker"}
        )
        
        call_args = audit_logger.audit_store.log_operation.call_args
        assert call_args[1]['operation'] == AuditEventType.CLUSTER_CREATE.value
        
        details = call_args[1]['details']
        assert details['resource_id'] == "test-cluster"
        assert details['operation_result'] == "success"
    
    @pytest.mark.asyncio
    async def test_log_topic_operation(self, audit_logger):
        """Test logging topic operations."""
        await audit_logger.log_topic_operation(
            operation="delete",
            topic_name="test-topic",
            cluster_id="test-cluster",
            user_id="test-user",
            success=False,
            details={"reason": "topic not found"}
        )
        
        call_args = audit_logger.audit_store.log_operation.call_args
        assert call_args[1]['operation'] == AuditEventType.TOPIC_DELETE.value
        
        details = call_args[1]['details']
        assert details['topic_name'] == "test-topic"
        assert details['cluster_id'] == "test-cluster"
        assert details['operation_result'] == "failure"
    
    @pytest.mark.asyncio
    async def test_log_api_request(self, audit_logger):
        """Test logging API requests."""
        await audit_logger.log_api_request(
            method="POST",
            path="/api/v1/topics",
            status_code=201,
            user_id="test-user",
            duration_ms=150.5
        )
        
        call_args = audit_logger.audit_store.log_operation.call_args
        details = call_args[1]['details']
        
        assert details['method'] == "POST"
        assert details['path'] == "/api/v1/topics"
        assert details['status_code'] == 201
        assert details['duration_ms'] == 150.5
    
    def test_audit_event_serialization(self):
        """Test audit event serialization."""
        event = AuditEvent(
            event_id="test-123",
            event_type=AuditEventType.TOPIC_CREATE,
            timestamp=datetime.utcnow(),
            level=AuditLevel.INFO,
            message="Test event",
            user_id="test-user",
            details={"key": "value"}
        )
        
        # Test to_dict
        event_dict = event.to_dict()
        assert event_dict['event_id'] == "test-123"
        assert event_dict['event_type'] == AuditEventType.TOPIC_CREATE.value
        assert event_dict['user_id'] == "test-user"
        assert event_dict['details'] == {"key": "value"}
        
        # Test to_json
        event_json = event.to_json()
        parsed = json.loads(event_json)
        assert parsed['event_id'] == "test-123"


class TestLogAnalyzer:
    """Test log analysis functionality."""
    
    @pytest.fixture
    def sample_log_file(self):
        """Create a sample log file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
            # Write sample log entries
            log_entries = [
                {
                    'timestamp': '2023-01-01T10:00:00Z',
                    'level': 'INFO',
                    'event_type': 'topic.create',
                    'user_id': 'user1',
                    'message': 'Topic created',
                    'resource_type': 'topic',
                    'resource_id': 'topic1'
                },
                {
                    'timestamp': '2023-01-01T11:00:00Z',
                    'level': 'ERROR',
                    'event_type': 'topic.delete',
                    'user_id': 'user2',
                    'message': 'Topic deletion failed',
                    'resource_type': 'topic',
                    'resource_id': 'topic2'
                },
                {
                    'timestamp': '2023-01-01T12:00:00Z',
                    'level': 'INFO',
                    'event_type': 'cluster.create',
                    'user_id': 'user1',
                    'message': 'Cluster created',
                    'resource_type': 'cluster',
                    'resource_id': 'cluster1'
                }
            ]
            
            for entry in log_entries:
                f.write(json.dumps(entry) + '\n')
            
            return f.name
    
    def test_parse_log_line(self):
        """Test parsing log lines."""
        analyzer = LogAnalyzer()
        
        # Test JSON log line
        json_line = '{"timestamp": "2023-01-01T10:00:00Z", "level": "INFO", "message": "test"}'
        parsed = analyzer.parse_log_line(json_line)
        
        assert parsed is not None
        assert parsed['timestamp'] == '2023-01-01T10:00:00Z'
        assert parsed['level'] == 'INFO'
        assert parsed['message'] == 'test'
    
    def test_read_logs_with_filters(self, sample_log_file):
        """Test reading logs with filters."""
        analyzer = LogAnalyzer(sample_log_file)
        
        # Filter by user
        query = LogQuery(filters=[
            LogFilter(
                field='user_id',
                operator=FilterOperator.EQUALS,
                value='user1'
            )
        ])
        
        entries = list(analyzer.read_logs(query))
        assert len(entries) == 2
        assert all(entry['user_id'] == 'user1' for entry in entries)
    
    def test_read_logs_with_time_range(self, sample_log_file):
        """Test reading logs with time range filter."""
        analyzer = LogAnalyzer(sample_log_file)
        
        query = LogQuery(
            filters=[],
            start_time=datetime(2023, 1, 1, 10, 30),
            end_time=datetime(2023, 1, 1, 12, 30)
        )
        
        entries = list(analyzer.read_logs(query))
        assert len(entries) == 2  # Should exclude the 10:00 entry
    
    def test_aggregate_logs(self, sample_log_file):
        """Test log aggregation."""
        analyzer = LogAnalyzer(sample_log_file)
        
        query = LogQuery(filters=[])
        aggregation = analyzer.aggregate_logs(query, 'level')
        
        assert aggregation['INFO'] == 2
        assert aggregation['ERROR'] == 1
    
    def test_get_log_statistics(self, sample_log_file):
        """Test getting log statistics."""
        analyzer = LogAnalyzer(sample_log_file)
        
        query = LogQuery(filters=[])
        stats = analyzer.get_log_statistics(query)
        
        assert stats['total_entries'] == 3
        assert stats['levels']['INFO'] == 2
        assert stats['levels']['ERROR'] == 1
        assert stats['users']['user1'] == 2
        assert stats['users']['user2'] == 1
    
    def test_search_logs(self, sample_log_file):
        """Test searching logs."""
        analyzer = LogAnalyzer(sample_log_file)
        
        results = analyzer.search_logs("created", limit=10)
        assert len(results) == 2  # Two entries contain "created"
        
        # Test case-insensitive search
        results = analyzer.search_logs("TOPIC", case_sensitive=False, limit=10)
        assert len(results) == 2  # Two entries contain "topic"
    
    def test_filter_operators(self, sample_log_file):
        """Test different filter operators."""
        analyzer = LogAnalyzer(sample_log_file)
        
        # Test CONTAINS
        query = LogQuery(filters=[
            LogFilter(
                field='message',
                operator=FilterOperator.CONTAINS,
                value='created'
            )
        ])
        entries = list(analyzer.read_logs(query))
        assert len(entries) == 2
        
        # Test IN operator
        query = LogQuery(filters=[
            LogFilter(
                field='level',
                operator=FilterOperator.IN,
                value=['INFO', 'DEBUG']
            )
        ])
        entries = list(analyzer.read_logs(query))
        assert len(entries) == 2  # Only INFO entries
        
        # Test NOT_EQUALS
        query = LogQuery(filters=[
            LogFilter(
                field='user_id',
                operator=FilterOperator.NOT_EQUALS,
                value='user1'
            )
        ])
        entries = list(analyzer.read_logs(query))
        assert len(entries) == 1
        assert entries[0]['user_id'] == 'user2'


class TestLogManagement:
    """Test log management functionality."""
    
    def test_log_rotation_config(self):
        """Test log rotation configuration."""
        config = LogRotationConfig(
            max_file_size=1024,
            max_files=5,
            compress_rotated=True
        )
        
        assert config.max_file_size == 1024
        assert config.max_files == 5
        assert config.compress_rotated is True
    
    def test_should_rotate_by_size(self):
        """Test rotation decision based on file size."""
        config = LogRotationConfig(max_file_size=100)
        manager = LogRotationManager(config)
        
        # Create a temporary file larger than threshold
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b'x' * 200)  # Write 200 bytes
            temp_path = f.name
        
        try:
            assert manager.should_rotate(temp_path) is True
        finally:
            Path(temp_path).unlink()
    
    def test_log_aggregation_config(self):
        """Test log aggregation configuration."""
        config = LogAggregationConfig(
            enabled=True,
            interval_minutes=30,
            retention_days=60
        )
        
        assert config.enabled is True
        assert config.interval_minutes == 30
        assert config.retention_days == 60
    
    @pytest.mark.asyncio
    async def test_log_aggregation(self, sample_log_file):
        """Test log aggregation functionality."""
        config = LogAggregationConfig()
        aggregator = LogAggregator(config)
        
        start_time = datetime(2023, 1, 1, 9, 0)
        end_time = datetime(2023, 1, 1, 13, 0)
        
        result = aggregator.aggregate_logs(
            sample_log_file,
            start_time,
            end_time
        )
        
        assert result['period']['start'] == start_time.isoformat()
        assert result['period']['end'] == end_time.isoformat()
        assert result['statistics']['total_entries'] == 3
        assert 'aggregations' in result
    
    def test_log_manager_initialization(self):
        """Test log manager initialization."""
        rotation_config = LogRotationConfig(max_file_size=1024)
        aggregation_config = LogAggregationConfig(enabled=False)
        
        manager = LogManager(rotation_config, aggregation_config)
        
        assert manager.rotation_config.max_file_size == 1024
        assert manager.aggregation_config.enabled is False
    
    def test_log_health_status(self):
        """Test log health status checking."""
        manager = LogManager()
        
        # Create a temporary log file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b'test log content')
            temp_path = f.name
        
        try:
            status = manager.get_log_health_status([temp_path])
            
            assert status['overall_status'] == 'healthy'
            assert temp_path in status['files']
            assert status['files'][temp_path]['status'] == 'healthy'
            assert status['files'][temp_path]['size_mb'] > 0
            
        finally:
            Path(temp_path).unlink()


class TestLogQueryHelpers:
    """Test log query helper functions."""
    
    def test_create_time_range_filter(self):
        """Test creating time range filters."""
        query = create_time_range_filter(24)
        
        assert query.start_time is not None
        assert query.end_time is not None
        assert query.end_time > query.start_time
        
        # Check that the range is approximately 24 hours
        duration = query.end_time - query.start_time
        assert abs(duration.total_seconds() - 24 * 3600) < 60  # Within 1 minute
    
    def test_create_user_activity_query(self):
        """Test creating user activity queries."""
        query = create_user_activity_query("test-user", 12)
        
        assert len(query.filters) == 1
        assert query.filters[0].field == 'user_id'
        assert query.filters[0].value == 'test-user'
        assert query.start_time is not None
        assert query.end_time is not None


if __name__ == '__main__':
    pytest.main([__file__])