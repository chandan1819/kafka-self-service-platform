"""Tests for CLI interface."""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch, AsyncMock, Mock
from click.testing import CliRunner

from kafka_ops_agent.cli.main import cli
from kafka_ops_agent.cli.config import load_cli_config, save_cli_config
from kafka_ops_agent.models.topic import TopicInfo, TopicDetails, TopicOperationResult


class TestCLIConfig:
    """Test CLI configuration management."""
    
    def test_load_default_config(self):
        """Test loading default configuration."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=True) as f:
            config_path = Path(f.name)
        
        # File doesn't exist, should return defaults
        config = load_cli_config(config_path)
        
        assert config['cluster_id'] is None
        assert config['bootstrap_servers'] == ['localhost:9092']
        assert config['zookeeper_connect'] == 'localhost:2181'
        assert config['user_id'] == 'cli-user'
    
    def test_save_and_load_config(self):
        """Test saving and loading configuration."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            config_path = Path(f.name)
        
        try:
            # Save config
            test_config = {
                'cluster_id': 'test-cluster',
                'bootstrap_servers': ['localhost:9092', 'localhost:9093'],
                'user_id': 'test-user'
            }
            
            save_cli_config(config_path, test_config)
            
            # Load config
            loaded_config = load_cli_config(config_path)
            
            assert loaded_config['cluster_id'] == 'test-cluster'
            assert loaded_config['bootstrap_servers'] == ['localhost:9092', 'localhost:9093']
            assert loaded_config['user_id'] == 'test-user'
            
        finally:
            config_path.unlink(missing_ok=True)


class TestCLICommands:
    """Test CLI commands."""
    
    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_config(self):
        """Create temporary config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {
                'cluster_id': 'test-cluster',
                'bootstrap_servers': ['localhost:9092'],
                'zookeeper_connect': 'localhost:2181',
                'user_id': 'test-user'
            }
            json.dump(config, f)
            config_path = f.name
        
        yield config_path
        
        # Cleanup
        Path(config_path).unlink(missing_ok=True)
    
    def test_cli_help(self, runner):
        """Test main CLI help."""
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert 'Kafka Ops Agent CLI' in result.output
        assert 'topic' in result.output
        assert 'cluster' in result.output
    
    def test_version_command(self, runner):
        """Test version command."""
        result = runner.invoke(cli, ['version'])
        
        assert result.exit_code == 0
        assert 'Kafka Ops Agent CLI' in result.output
        assert 'Version: 0.1.0' in result.output
    
    def test_configure_command(self, runner):
        """Test configure command."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            config_file = f.name
        
        try:
            result = runner.invoke(cli, [
                '--config-file', config_file,
                'configure',
                '--cluster-id', 'test-cluster',
                '--bootstrap-servers', 'localhost:9092,localhost:9093',
                '--user-id', 'test-user'
            ])
            
            assert result.exit_code == 0
            assert 'Configuration saved' in result.output
            
            # Verify config was saved
            with open(config_file, 'r') as f:
                saved_config = json.load(f)
            
            assert saved_config['cluster_id'] == 'test-cluster'
            assert saved_config['bootstrap_servers'] == ['localhost:9092', 'localhost:9093']
            assert saved_config['user_id'] == 'test-user'
            
        finally:
            Path(config_file).unlink(missing_ok=True)
    
    def test_config_show_command(self, runner, temp_config):
        """Test config show command."""
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'config'
        ])
        
        assert result.exit_code == 0
        assert 'test-cluster' in result.output
        assert 'localhost:9092' in result.output
    
    def test_topic_help(self, runner):
        """Test topic command help."""
        result = runner.invoke(cli, ['topic', '--help'])
        
        assert result.exit_code == 0
        assert 'Topic management commands' in result.output
        assert 'list' in result.output
        assert 'create' in result.output
        assert 'describe' in result.output
    
    def test_cluster_help(self, runner):
        """Test cluster command help."""
        result = runner.invoke(cli, ['cluster', '--help'])
        
        assert result.exit_code == 0
        assert 'Cluster management commands' in result.output
        assert 'info' in result.output
        assert 'health' in result.output


class TestTopicCommands:
    """Test topic management commands."""
    
    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_config(self):
        """Create temporary config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {
                'cluster_id': 'test-cluster',
                'bootstrap_servers': ['localhost:9092'],
                'zookeeper_connect': 'localhost:2181',
                'user_id': 'test-user'
            }
            json.dump(config, f)
            config_path = f.name
        
        yield config_path
        
        # Cleanup
        Path(config_path).unlink(missing_ok=True)
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_topic_list_command(self, mock_client_manager, mock_topic_service, runner, temp_config):
        """Test topic list command."""
        # Mock topic service
        mock_service = AsyncMock()
        mock_service.list_topics.return_value = [
            TopicInfo(name='topic1', partitions=3, replication_factor=1),
            TopicInfo(name='topic2', partitions=6, replication_factor=2)
        ]
        mock_topic_service.return_value = mock_service
        
        # Mock client manager
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'topic', 'list'
        ])
        
        assert result.exit_code == 0
        assert 'topic1' in result.output
        assert 'topic2' in result.output
        assert 'Total: 2 topics' in result.output
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_topic_list_json_format(self, mock_client_manager, mock_topic_service, runner, temp_config):
        """Test topic list with JSON format."""
        # Mock topic service
        mock_service = AsyncMock()
        mock_service.list_topics.return_value = [
            TopicInfo(name='topic1', partitions=3, replication_factor=1, configs={})
        ]
        mock_topic_service.return_value = mock_service
        
        # Mock client manager
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'topic', 'list',
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        
        # Should be valid JSON
        output_data = json.loads(result.output)
        assert len(output_data) == 1
        assert output_data[0]['name'] == 'topic1'
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_topic_create_command(self, mock_client_manager, mock_topic_service, runner, temp_config):
        """Test topic create command."""
        # Mock topic service
        mock_service = AsyncMock()
        mock_service.create_topic.return_value = TopicOperationResult(
            success=True,
            message="Topic created successfully",
            topic_name="test-topic",
            details={"partitions": 3, "replication_factor": 1}
        )
        mock_topic_service.return_value = mock_service
        
        # Mock client manager
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'topic', 'create', 'test-topic',
            '--partitions', '3',
            '--replication-factor', '1',
            '--config', 'retention.ms=3600000'
        ])
        
        assert result.exit_code == 0
        assert 'created successfully' in result.output
        
        # Verify service was called with correct parameters
        mock_service.create_topic.assert_called_once()
        call_args = mock_service.create_topic.call_args
        topic_config = call_args[0][1]  # Second argument is topic_config
        assert topic_config.name == 'test-topic'
        assert topic_config.partitions == 3
        assert topic_config.replication_factor == 1
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_topic_describe_command(self, mock_client_manager, mock_topic_service, runner, temp_config):
        """Test topic describe command."""
        # Mock topic service
        mock_service = AsyncMock()
        mock_service.describe_topic.return_value = TopicDetails(
            name='test-topic',
            partitions=3,
            replication_factor=1,
            configs={'retention.ms': '604800000'},
            partition_details=[
                {'partition': 0, 'leader': 0, 'replicas': [0], 'isr': [0]}
            ]
        )
        mock_topic_service.return_value = mock_service
        
        # Mock client manager
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'topic', 'describe', 'test-topic'
        ])
        
        assert result.exit_code == 0
        assert 'Topic: test-topic' in result.output
        assert 'Partitions: 3' in result.output
        assert 'Configurations:' in result.output
    
    @patch('kafka_ops_agent.cli.topic_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.topic_commands.get_client_manager')
    def test_topic_delete_command(self, mock_client_manager, mock_topic_service, runner, temp_config):
        """Test topic delete command."""
        # Mock topic service
        mock_service = AsyncMock()
        mock_service.delete_topic.return_value = TopicOperationResult(
            success=True,
            message="Topic deleted successfully",
            topic_name="test-topic"
        )
        mock_topic_service.return_value = mock_service
        
        # Mock client manager
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'topic', 'delete', 'test-topic',
            '--force'  # Skip confirmation
        ])
        
        assert result.exit_code == 0
        assert 'deleted successfully' in result.output
    
    def test_topic_bulk_create_help(self, runner):
        """Test bulk create help."""
        result = runner.invoke(cli, ['topic', 'bulk-create', '--help'])
        
        assert result.exit_code == 0
        assert 'JSON file with topic configurations' in result.output
    
    def test_invalid_config_format(self, runner, temp_config):
        """Test invalid config format handling."""
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'topic', 'create', 'test-topic',
            '--config', 'invalid-format'  # Missing = sign
        ])
        
        assert result.exit_code == 1
        assert 'Invalid config format' in result.output


class TestClusterCommands:
    """Test cluster management commands."""
    
    @pytest.fixture
    def runner(self):
        """Create CLI test runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_config(self):
        """Create temporary config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            config = {
                'cluster_id': 'test-cluster',
                'bootstrap_servers': ['localhost:9092'],
                'zookeeper_connect': 'localhost:2181',
                'user_id': 'test-user'
            }
            json.dump(config, f)
            config_path = f.name
        
        yield config_path
        
        # Cleanup
        Path(config_path).unlink(missing_ok=True)
    
    @patch('kafka_ops_agent.cli.cluster_commands.get_topic_service')
    @patch('kafka_ops_agent.cli.cluster_commands.get_client_manager')
    def test_cluster_info_command(self, mock_client_manager, mock_topic_service, runner, temp_config):
        """Test cluster info command."""
        # Mock topic service
        mock_service = AsyncMock()
        mock_service.get_cluster_info.return_value = {
            'cluster_id': 'test-cluster-id',
            'broker_count': 3,
            'topic_count': 10,
            'brokers': [
                {'id': 0, 'host': 'localhost', 'port': 9092}
            ]
        }
        mock_topic_service.return_value = mock_service
        
        # Mock client manager
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'cluster', 'info'
        ])
        
        assert result.exit_code == 0
        assert 'Broker Count: 3' in result.output
        assert 'Topic Count: 10' in result.output
    
    @patch('kafka_ops_agent.cli.cluster_commands.get_client_manager')
    def test_cluster_health_command(self, mock_client_manager, runner, temp_config):
        """Test cluster health command."""
        # Mock client manager and connection
        mock_connection = Mock()
        mock_connection.health_check.return_value = True
        mock_connection.get_stats.return_value = {
            'age_seconds': 120.5,
            'use_count': 5,
            'idle_seconds': 10.2
        }
        
        mock_manager = Mock()
        mock_manager.register_cluster.return_value = True
        mock_manager.get_connection.return_value = mock_connection
        mock_client_manager.return_value = mock_manager
        
        result = runner.invoke(cli, [
            '--config-file', temp_config,
            'cluster', 'health'
        ])
        
        assert result.exit_code == 0
        assert 'is healthy' in result.output
        assert 'Connection age: 120.5 seconds' in result.output