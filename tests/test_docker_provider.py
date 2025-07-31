"""Tests for Docker provider."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from kafka_ops_agent.providers.docker_provider import DockerProvider
from kafka_ops_agent.providers.base import ProvisioningStatus


class TestDockerProvider:
    """Test cases for DockerProvider."""
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_init_success(self, mock_docker):
        """Test successful initialization."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        provider = DockerProvider()
        
        assert provider.client == mock_client
        mock_client.ping.assert_called_once()
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_init_failure(self, mock_docker):
        """Test initialization failure."""
        mock_docker.from_env.side_effect = Exception("Docker not available")
        
        with pytest.raises(Exception, match="Docker not available"):
            DockerProvider()
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_parse_config(self, mock_docker):
        """Test configuration parsing."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        provider = DockerProvider()
        
        config = {
            'cluster_size': 3,
            'replication_factor': 2,
            'retention_hours': 48
        }
        
        cluster_config = provider._parse_config(config)
        
        assert cluster_config.cluster_size == 3
        assert cluster_config.replication_factor == 2
        assert cluster_config.retention_hours == 48
        assert cluster_config.partition_count == 3  # default
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_generate_compose_config_single_node(self, mock_docker):
        """Test Docker Compose config generation for single node."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        provider = DockerProvider()
        
        from kafka_ops_agent.models.cluster import ClusterConfig
        config = ClusterConfig(cluster_size=1)
        
        compose_config = provider._generate_compose_config("test-cluster", config)
        
        assert 'services' in compose_config
        assert 'zookeeper' in compose_config['services']
        assert 'kafka' in compose_config['services']
        assert 'volumes' in compose_config
        assert 'networks' in compose_config
        
        # Check Kafka environment
        kafka_env = compose_config['services']['kafka']['environment']
        assert kafka_env['KAFKA_BROKER_ID'] == 1
        assert 'test-cluster-zookeeper:2181' in kafka_env['KAFKA_ZOOKEEPER_CONNECT']
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_generate_compose_config_multi_node(self, mock_docker):
        """Test Docker Compose config generation for multi-node."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        provider = DockerProvider()
        
        from kafka_ops_agent.models.cluster import ClusterConfig
        config = ClusterConfig(cluster_size=3, replication_factor=2)
        
        compose_config = provider._generate_compose_config("test-cluster", config)
        
        # Should have 3 Kafka brokers
        kafka_services = [k for k in compose_config['services'].keys() if k.startswith('kafka')]
        assert len(kafka_services) == 3
        
        # Check broker IDs
        for i, service_name in enumerate(kafka_services, 1):
            kafka_env = compose_config['services'][service_name]['environment']
            assert kafka_env['KAFKA_BROKER_ID'] == i
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_get_cluster_status_running(self, mock_docker):
        """Test cluster status when all containers are running."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        # Mock running containers
        mock_container1 = Mock()
        mock_container1.status = 'running'
        mock_container2 = Mock()
        mock_container2.status = 'running'
        
        mock_client.containers.list.return_value = [mock_container1, mock_container2]
        
        provider = DockerProvider()
        status = provider.get_cluster_status("test-cluster")
        
        assert status == ProvisioningStatus.SUCCEEDED
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_get_cluster_status_partial(self, mock_docker):
        """Test cluster status when some containers are not running."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        # Mock mixed status containers
        mock_container1 = Mock()
        mock_container1.status = 'running'
        mock_container2 = Mock()
        mock_container2.status = 'starting'
        
        mock_client.containers.list.return_value = [mock_container1, mock_container2]
        
        provider = DockerProvider()
        status = provider.get_cluster_status("test-cluster")
        
        assert status == ProvisioningStatus.IN_PROGRESS
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_get_connection_info(self, mock_docker):
        """Test getting connection information."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        # Mock Kafka container
        mock_kafka = Mock()
        mock_kafka.name = 'test-cluster-kafka'
        mock_kafka.ports = {'9092/tcp': [{'HostPort': '9092'}]}
        
        # Mock Zookeeper container
        mock_zk = Mock()
        mock_zk.name = 'test-cluster-zookeeper'
        mock_zk.ports = {'2181/tcp': [{'HostPort': '2181'}]}
        
        mock_client.containers.list.return_value = [mock_kafka, mock_zk]
        
        provider = DockerProvider()
        connection_info = provider.get_connection_info("test-cluster")
        
        assert connection_info is not None
        assert connection_info['bootstrap_servers'] == ['localhost:9092']
        assert connection_info['zookeeper_connect'] == 'localhost:2181'
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_health_check_healthy(self, mock_docker):
        """Test health check for healthy cluster."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        # Mock healthy containers
        mock_container1 = Mock()
        mock_container1.status = 'running'
        mock_container2 = Mock()
        mock_container2.status = 'running'
        
        mock_client.containers.list.return_value = [mock_container1, mock_container2]
        
        provider = DockerProvider()
        health = provider.health_check("test-cluster")
        
        assert health is True
    
    @patch('kafka_ops_agent.providers.docker_provider.docker')
    def test_health_check_unhealthy(self, mock_docker):
        """Test health check for unhealthy cluster."""
        mock_client = Mock()
        mock_docker.from_env.return_value = mock_client
        
        # Mock unhealthy containers
        mock_container1 = Mock()
        mock_container1.status = 'running'
        mock_container2 = Mock()
        mock_container2.status = 'exited'
        
        mock_client.containers.list.return_value = [mock_container1, mock_container2]
        
        provider = DockerProvider()
        health = provider.health_check("test-cluster")
        
        assert health is False