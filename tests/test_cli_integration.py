"""Integration tests for CLI commands."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from click.testing import CliRunner

from kafka_ops_agent.cli.main import cli
from kafka_ops_agent.cli.topic_commands import topic_cli
from kafka_ops_agent.cli.cleanup_commands import cleanup
from kafka_ops_agent.models.topic import TopicInfo, TopicDetails
from kafka_ops_agent.models.cluster import ConnectionInfo


@pytest.fixture
def cli_runner():
    """Create CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def mock_config():
    """Mock CLI configuration."""
    return {
        'cluster_id': 'test-cluster',
        'bootstrap_servers': ['localhost:9092'],
        'zookeeper_connect': 'localhost:2181',
        'user_id': 'test-user'
    }


@pytest.fixture
def temp_config_file(mock_config):
    """Create temporary config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(mock_config, f)
        return Path(f.name)


@pytest.fixture
def mock_topic_service():
    """Mock topic service."""
    service = AsyncMock()
    
    # Mock topic list
    service.list_topics.return_value = [
        TopicInfo(
            name="test-topic-1",
            partitions=3,
            replication_factor=1,
            configs={"retention.ms": "604800000"}
        ),
        TopicInfo(
            name="production-topic",
            partitions=6,
            replication_factor=2,
            configs={"retention.ms": "2592000000"}
        )
    ]
    
    # Mock topic details
    service.describe_topic.return_value = TopicDetails(
        name="test-topic-1",
        partitions=3,
        replication_factor=1,
        configs={"retention.ms": "604800000", "cleanup.policy": "delete"},
        partition_details=[
            {"partition": 0, "leader": 0, "replicas": [0], "isr": [0]},
            {"partition": 1, "leader": 0, "replicas": [0], "isr": [0]},
            {"partition": 2, "leader": 0, "replicas": [0], "isr": [0]}
        ],
        total_messages=1000,
        total_size_bytes=1024000
    )
    
    # Mock operation results
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.message = "Operation successful"
    mock_result.details = {"partitions": 3, "replication_factor": 1}
    
    service.create_topic.return_value = mock_result
    service.update_topic_config.return_value = mock_result
    service.delete_topic.return_value = mock_result
    service.purge_topic.return_value = mock_result
    service.bulk_create_topics.return_value = {
        "topic1": mock_result,
        "topic2": mock_result
    }
    
    return service


@pytest.fixture
def mock_client_manager():
    """Mock client manager."""
    manager = MagicMock()
    manager.register_cluster = MagicMock()
    return manager


class TestTopicCLI:
    """Test topic CLI commands."""
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_list_topics_table_format(self, mock_get_client_manager, mock_get_topic_service, 
                                     cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test listing topics in table format."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'list'
        ])
        
        assert result.exit_code == 0
        assert "test-topic-1" in result.output
        assert "production-topic" in result.output
        assert "Total: 2 topics" in result.output
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_list_topics_json_format(self, mock_get_client_manager, mock_get_topic_service,
                                    cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test listing topics in JSON format."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'list', '--format', 'json'
        ])
        
        assert result.exit_code == 0
        
        # Parse JSON output
        output_data = json.loads(result.output)
        assert len(output_data) == 2
        assert output_data[0]['name'] == 'test-topic-1'
        assert output_data[1]['name'] == 'production-topic'
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_create_topic(self, mock_get_client_manager, mock_get_topic_service,
                         cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test creating a topic."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'create', 'new-topic',
            '--partitions', '6',
            '--replication-factor', '2',
            '--retention-hours', '72'
        ])
        
        assert result.exit_code == 0
        assert "Topic 'new-topic' created successfully" in result.output
        
        # Verify service was called with correct parameters
        mock_topic_service.create_topic.assert_called_once()
        call_args = mock_topic_service.create_topic.call_args
        topic_config = call_args[0][1]  # Second argument is the TopicConfig
        
        assert topic_config.name == 'new-topic'
        assert topic_config.partitions == 6
        assert topic_config.replication_factor == 2
        assert topic_config.retention_ms == 72 * 3600 * 1000  # 72 hours in ms
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_describe_topic(self, mock_get_client_manager, mock_get_topic_service,
                           cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test describing a topic."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'describe', 'test-topic-1'
        ])
        
        assert result.exit_code == 0
        assert "Topic: test-topic-1" in result.output
        assert "Partitions: 3" in result.output
        assert "Replication Factor: 1" in result.output
        assert "Total Messages: 1,000" in result.output
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_update_topic_config(self, mock_get_client_manager, mock_get_topic_service,
                                cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test updating topic configuration."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'update-config', 'test-topic-1',
            '--config', 'retention.ms=86400000',
            '--config', 'cleanup.policy=compact'
        ])
        
        assert result.exit_code == 0
        assert "Topic 'test-topic-1' configuration updated" in result.output
        
        # Verify service was called with correct configs
        mock_topic_service.update_topic_config.assert_called_once()
        call_args = mock_topic_service.update_topic_config.call_args
        configs = call_args[0][2]  # Third argument is the configs dict
        
        assert configs['retention.ms'] == '86400000'
        assert configs['cleanup.policy'] == 'compact'
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_delete_topic_with_force(self, mock_get_client_manager, mock_get_topic_service,
                                    cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test deleting a topic with force flag."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'delete', 'test-topic-1', '--force'
        ])
        
        assert result.exit_code == 0
        assert "Topic 'test-topic-1' deleted successfully" in result.output
        
        # Verify service was called
        mock_topic_service.delete_topic.assert_called_once()
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_search_topics(self, mock_get_client_manager, mock_get_topic_service,
                          cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test searching topics by pattern."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'search', 'test'
        ])
        
        assert result.exit_code == 0
        assert "test-topic-1" in result.output
        assert "production-topic" not in result.output
        assert "Found: 1 topics" in result.output
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_validate_topic(self, mock_get_client_manager, mock_get_topic_service,
                           cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test validating a topic."""
        
        mock_get_topic_service.return_value = mock_topic_service
        mock_get_client_manager.return_value = mock_client_manager
        
        result = cli_runner.invoke(cli, [
            '--config-file', str(temp_config_file),
            'topic', 'validate', 'test-topic-1'
        ])
        
        assert result.exit_code == 0
        assert "Validation results for topic 'test-topic-1'" in result.output
        assert "Partitions: 3" in result.output
        assert "Replication Factor: 1" in result.output
    
    def test_bulk_create_topics(self, cli_runner, temp_config_file, mock_topic_service, mock_client_manager):
        """Test bulk creating topics from JSON file."""
        
        # Create temporary JSON file with topic configurations
        topics_config = [
            {
                "name": "bulk-topic-1",
                "partitions": 3,
                "replication_factor": 1,
                "retention_ms": 604800000
            },
            {
                "name": "bulk-topic-2", 
                "partitions": 6,
                "replication_factor": 2,
                "retention_ms": 2592000000
            }
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(topics_config, f)
            topics_file = Path(f.name)
        
        try:
            with patch('kafka_ops_agent.cli.topic_commands.get_topic_service') as mock_get_topic_service, \
                 patch('kafka_ops_agent.cli.topic_commands.get_client_manager') as mock_get_client_manager:
                
                mock_get_topic_service.return_value = mock_topic_service
                mock_get_client_manager.return_value = mock_client_manager
                
                result = cli_runner.invoke(cli, [
                    '--config-file', str(temp_config_file),
                    'topic', 'bulk-create', '--file', str(topics_file)
                ])
                
                assert result.exit_code == 0
                assert "Bulk create completed" in result.output
                assert "Successful: 2" in result.output
                assert "Failed: 0" in result.output
        
        finally:
            topics_file.unlink()


class TestCleanupCLI:
    """Test cleanup CLI commands."""
    
    @patch('kafka_ops_agent.cli.cleanup_commands.get_scheduler_service')
    def test_topic_cleanup_dry_run(self, mock_get_scheduler_service, cli_runner):
        """Test topic cleanup in dry run mode."""
        
        # Mock scheduler service
        mock_scheduler = AsyncMock()
        mock_execution = MagicMock()
        mock_execution.execution_id = "test-execution-123"
        mock_execution.status.value = "completed"
        mock_execution.started_at = "2023-01-01T00:00:00"
        mock_execution.completed_at = "2023-01-01T00:01:00"
        mock_execution.result = {
            "topics_evaluated": 5,
            "topics_identified": 2,
            "topics_cleaned": 0,
            "dry_run": True
        }
        mock_execution.logs = ["Starting topic cleanup", "Dry run mode"]
        
        mock_scheduler.execute_topic_cleanup_now.return_value = mock_execution
        mock_scheduler.get_cleanup_operation.return_value = mock_execution
        mock_get_scheduler_service.return_value = mock_scheduler
        
        result = cli_runner.invoke(cleanup, [
            'topic',
            '--cluster-id', 'test-cluster',
            '--max-age-hours', '24',
            '--retention-pattern', 'test',
            '--dry-run',
            '--wait'
        ])
        
        assert result.exit_code == 0
        assert "Starting topic cleanup for cluster: test-cluster" in result.output
        assert "Mode: SIMULATION" in result.output
        assert "Cleanup completed" in result.output
    
    @patch('kafka_ops_agent.cli.cleanup_commands.get_scheduler_service')
    def test_cluster_cleanup(self, mock_get_scheduler_service, cli_runner):
        """Test cluster cleanup."""
        
        # Mock scheduler service
        mock_scheduler = AsyncMock()
        mock_execution = MagicMock()
        mock_execution.execution_id = "cluster-cleanup-123"
        mock_execution.status.value = "completed"
        mock_execution.started_at = "2023-01-01T00:00:00"
        mock_execution.completed_at = "2023-01-01T00:02:00"
        mock_execution.result = {
            "failed_instances": 3,
            "old_failed_instances": 1,
            "cleaned_instances": 1,
            "dry_run": False
        }
        mock_execution.logs = ["Starting cluster cleanup", "Cleaned 1 instance"]
        
        mock_scheduler.execute_cluster_cleanup_now.return_value = mock_execution
        mock_scheduler.get_cleanup_operation.return_value = mock_execution
        mock_get_scheduler_service.return_value = mock_scheduler
        
        result = cli_runner.invoke(cleanup, [
            'cluster',
            '--max-age-hours', '48',
            '--wait'
        ])
        
        assert result.exit_code == 0
        assert "Starting cluster cleanup" in result.output
        assert "Mode: ACTUAL CLEANUP" in result.output
        assert "Instances cleaned: 1" in result.output
    
    @patch('kafka_ops_agent.cli.cleanup_commands.get_scheduler_service')
    def test_cleanup_status(self, mock_get_scheduler_service, cli_runner):
        """Test getting cleanup status."""
        
        # Mock scheduler service
        mock_scheduler = AsyncMock()
        mock_execution = MagicMock()
        mock_execution.execution_id = "test-execution-123"
        mock_execution.task_id = "ondemand_topic_cleanup_test-cluster_123"
        mock_execution.status.value = "completed"
        mock_execution.started_at = "2023-01-01T00:00:00"
        mock_execution.completed_at = "2023-01-01T00:01:00"
        mock_execution.error_message = None
        mock_execution.result = {"topics_cleaned": 2}
        mock_execution.logs = ["Starting cleanup", "Completed cleanup"]
        
        mock_scheduler.get_cleanup_operation.return_value = mock_execution
        mock_get_scheduler_service.return_value = mock_scheduler
        
        result = cli_runner.invoke(cleanup, [
            'status', 'test-execution-123', '--show-logs'
        ])
        
        assert result.exit_code == 0
        assert "Cleanup Operation Status" in result.output
        assert "Execution ID: test-execution-123" in result.output
        assert "Status: completed" in result.output
        assert "Execution Logs:" in result.output
    
    @patch('kafka_ops_agent.cli.cleanup_commands.get_scheduler_service')
    def test_cleanup_list(self, mock_get_scheduler_service, cli_runner):
        """Test listing cleanup operations."""
        
        # Mock scheduler service
        mock_scheduler = AsyncMock()
        
        mock_execution1 = MagicMock()
        mock_execution1.execution_id = "cleanup-1"
        mock_execution1.task_id = "ondemand_topic_cleanup"
        mock_execution1.status.value = "completed"
        mock_execution1.started_at = "2023-01-01T00:00:00"
        mock_execution1.completed_at = "2023-01-01T00:01:00"
        mock_execution1.error_message = None
        
        mock_execution2 = MagicMock()
        mock_execution2.execution_id = "cleanup-2"
        mock_execution2.task_id = "ondemand_cluster_cleanup"
        mock_execution2.status.value = "failed"
        mock_execution2.started_at = "2023-01-01T01:00:00"
        mock_execution2.completed_at = "2023-01-01T01:01:00"
        mock_execution2.error_message = "Connection failed"
        
        mock_scheduler.list_cleanup_operations.return_value = [mock_execution1, mock_execution2]
        mock_get_scheduler_service.return_value = mock_scheduler
        
        result = cli_runner.invoke(cleanup, [
            'list', '--limit', '10'
        ])
        
        assert result.exit_code == 0
        assert "Cleanup Operations:" in result.output
        assert "cleanup-1" in result.output
        assert "cleanup-2" in result.output
        assert "completed" in result.output
        assert "failed" in result.output


class TestCLIErrorHandling:
    """Test CLI error handling."""
    
    def test_missing_cluster_config(self, cli_runner):
        """Test error when cluster is not configured."""
        
        # Create empty config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({}, f)
            empty_config_file = Path(f.name)
        
        try:
            result = cli_runner.invoke(cli, [
                '--config-file', str(empty_config_file),
                'topic', 'list'
            ])
            
            assert result.exit_code != 0
            assert "Cluster ID not configured" in result.output
        
        finally:
            empty_config_file.unlink()
    
    def test_invalid_config_format(self, cli_runner):
        """Test error with invalid config format."""
        
        result = cli_runner.invoke(cli, [
            'topic', 'create', 'test-topic',
            '--config', 'invalid-format'
        ])
        
        assert result.exit_code != 0
        assert "Invalid config format" in result.output
    
    def test_invalid_json_file(self, cli_runner, temp_config_file):
        """Test error with invalid JSON file for bulk create."""
        
        # Create invalid JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json content")
            invalid_json_file = Path(f.name)
        
        try:
            result = cli_runner.invoke(cli, [
                '--config-file', str(temp_config_file),
                'topic', 'bulk-create', '--file', str(invalid_json_file)
            ])
            
            assert result.exit_code != 0
            assert "Invalid JSON file" in result.output
        
        finally:
            invalid_json_file.unlink()


if __name__ == '__main__':
    pytest.main([__file__])