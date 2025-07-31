"""Tests for Kubernetes runtime provider."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from kubernetes.client.rest import ApiException

from kafka_ops_agent.providers.kubernetes_provider import KubernetesProvider
from kafka_ops_agent.providers.base import ProvisioningStatus
from kafka_ops_agent.models.cluster import ClusterConfig


class TestKubernetesProvider:
    """Test cases for KubernetesProvider."""
    
    @pytest.fixture
    def mock_k8s_clients(self):
        """Mock Kubernetes clients."""
        with patch('kafka_ops_agent.providers.kubernetes_provider.client') as mock_client, \
             patch('kafka_ops_agent.providers.kubernetes_provider.config') as mock_config:
            
            # Mock client instances
            mock_apps_v1 = Mock()
            mock_core_v1 = Mock()
            mock_storage_v1 = Mock()
            
            mock_client.AppsV1Api.return_value = mock_apps_v1
            mock_client.CoreV1Api.return_value = mock_core_v1
            mock_client.StorageV1Api.return_value = mock_storage_v1
            
            # Mock namespace check
            mock_core_v1.read_namespace.return_value = Mock()
            
            yield {
                'apps_v1': mock_apps_v1,
                'core_v1': mock_core_v1,
                'storage_v1': mock_storage_v1,
                'config': mock_config
            }
    
    @pytest.fixture
    def provider(self, mock_k8s_clients):
        """Create KubernetesProvider instance with mocked clients."""
        return KubernetesProvider(namespace="test-namespace")
    
    @pytest.fixture
    def sample_config(self):
        """Sample cluster configuration."""
        return {
            'cluster_size': 3,
            'replication_factor': 2,
            'partition_count': 6,
            'retention_hours': 168,
            'storage_size_gb': 20,
            'enable_ssl': False,
            'enable_sasl': False,
            'custom_properties': {
                'log.segment.bytes': '1073741824'
            }
        }
    
    def test_init_with_kubeconfig(self, mock_k8s_clients):
        """Test initialization with kubeconfig path."""
        provider = KubernetesProvider(
            namespace="custom-namespace",
            kubeconfig_path="/path/to/kubeconfig"
        )
        
        mock_k8s_clients['config'].load_kube_config.assert_called_with(
            config_file="/path/to/kubeconfig"
        )
        assert provider.namespace == "custom-namespace"
    
    def test_init_in_cluster_config(self, mock_k8s_clients):
        """Test initialization with in-cluster config."""
        mock_k8s_clients['config'].load_incluster_config.side_effect = None
        
        provider = KubernetesProvider()
        
        mock_k8s_clients['config'].load_incluster_config.assert_called_once()
        assert provider.namespace == "kafka-clusters"
    
    def test_init_fallback_to_local_config(self, mock_k8s_clients):
        """Test fallback to local kubeconfig when in-cluster fails."""
        from kubernetes.config import ConfigException
        mock_k8s_clients['config'].load_incluster_config.side_effect = ConfigException("Not in cluster")
        
        provider = KubernetesProvider()
        
        mock_k8s_clients['config'].load_incluster_config.assert_called_once()
        mock_k8s_clients['config'].load_kube_config.assert_called_once()
    
    def test_ensure_namespace_exists(self, provider, mock_k8s_clients):
        """Test namespace creation when it doesn't exist."""
        mock_k8s_clients['core_v1'].read_namespace.side_effect = ApiException(status=404)
        
        provider._ensure_namespace()
        
        mock_k8s_clients['core_v1'].create_namespace.assert_called_once()
    
    def test_parse_config(self, provider, sample_config):
        """Test configuration parsing."""
        cluster_config = provider._parse_config(sample_config)
        
        assert isinstance(cluster_config, ClusterConfig)
        assert cluster_config.cluster_size == 3
        assert cluster_config.replication_factor == 2
        assert cluster_config.partition_count == 6
        assert cluster_config.retention_hours == 168
        assert cluster_config.storage_size_gb == 20
        assert cluster_config.custom_properties['log.segment.bytes'] == '1073741824'
    
    def test_generate_zookeeper_manifests(self, provider, sample_config):
        """Test Zookeeper manifest generation."""
        cluster_config = provider._parse_config(sample_config)
        manifests = provider._generate_zookeeper_manifests("test-cluster", cluster_config)
        
        assert "test-cluster-zookeeper-service" in manifests
        assert "test-cluster-zookeeper-statefulset" in manifests
        
        # Check service manifest
        service = manifests["test-cluster-zookeeper-service"]
        assert service["kind"] == "Service"
        assert service["metadata"]["name"] == "test-cluster-zookeeper"
        assert service["spec"]["ports"][0]["port"] == 2181
        
        # Check StatefulSet manifest
        statefulset = manifests["test-cluster-zookeeper-statefulset"]
        assert statefulset["kind"] == "StatefulSet"
        assert statefulset["spec"]["replicas"] == 1
        assert statefulset["spec"]["template"]["spec"]["containers"][0]["image"] == "confluentinc/cp-zookeeper:7.4.0"
    
    def test_generate_kafka_manifests(self, provider, sample_config):
        """Test Kafka manifest generation."""
        cluster_config = provider._parse_config(sample_config)
        manifests = provider._generate_kafka_manifests("test-cluster", cluster_config)
        
        assert "test-cluster-kafka-service" in manifests
        assert "test-cluster-kafka-statefulset" in manifests
        
        # Check service manifest
        service = manifests["test-cluster-kafka-service"]
        assert service["kind"] == "Service"
        assert service["metadata"]["name"] == "test-cluster-kafka"
        assert service["spec"]["ports"][0]["port"] == 9092
        
        # Check StatefulSet manifest
        statefulset = manifests["test-cluster-kafka-statefulset"]
        assert statefulset["kind"] == "StatefulSet"
        assert statefulset["spec"]["replicas"] == 3
        
        # Check environment variables
        container = statefulset["spec"]["template"]["spec"]["containers"][0]
        env_vars = {env["name"]: env["value"] for env in container["env"] if "value" in env}
        assert env_vars["KAFKA_LOG_RETENTION_HOURS"] == "168"
        assert env_vars["KAFKA_NUM_PARTITIONS"] == "6"
        assert env_vars["KAFKA_LOG_SEGMENT_BYTES"] == "1073741824"
    
    def test_apply_manifests_success(self, provider, mock_k8s_clients):
        """Test successful manifest application."""
        manifests = {
            "test-service": {
                "kind": "Service",
                "metadata": {"name": "test-service"}
            },
            "test-statefulset": {
                "kind": "StatefulSet",
                "metadata": {"name": "test-statefulset"}
            }
        }
        
        provider._apply_manifests("test-cluster", manifests)
        
        mock_k8s_clients['core_v1'].create_namespaced_service.assert_called_once()
        mock_k8s_clients['apps_v1'].create_namespaced_stateful_set.assert_called_once()
    
    def test_apply_manifests_already_exists(self, provider, mock_k8s_clients):
        """Test manifest application when resources already exist."""
        manifests = {
            "test-service": {
                "kind": "Service",
                "metadata": {"name": "test-service"}
            }
        }
        
        mock_k8s_clients['core_v1'].create_namespaced_service.side_effect = ApiException(status=409)
        
        # Should not raise exception
        provider._apply_manifests("test-cluster", manifests)
    
    def test_provision_cluster_success(self, provider, mock_k8s_clients, sample_config):
        """Test successful cluster provisioning."""
        # Mock successful StatefulSet status
        mock_sts = Mock()
        mock_sts.status.ready_replicas = 3
        mock_sts.spec.replicas = 3
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        # Mock service for connection info
        mock_service = Mock()
        mock_service.spec.type = "ClusterIP"
        mock_service.spec.cluster_ip = "10.0.0.1"
        mock_k8s_clients['core_v1'].read_namespaced_service.return_value = mock_service
        
        with patch.object(provider, '_wait_for_cluster_ready') as mock_wait:
            from kafka_ops_agent.models.cluster import ConnectionInfo
            mock_wait.return_value = ConnectionInfo(
                bootstrap_servers=["10.0.0.1:9092"],
                zookeeper_connect="test-cluster-zookeeper.test-namespace.svc.cluster.local:2181"
            )
            
            result = provider.provision_cluster("test-cluster", sample_config)
        
        assert result.status == ProvisioningStatus.SUCCEEDED
        assert result.instance_id == "test-cluster"
        assert result.connection_info is not None
    
    def test_provision_cluster_failure(self, provider, mock_k8s_clients, sample_config):
        """Test cluster provisioning failure."""
        mock_k8s_clients['core_v1'].create_namespaced_service.side_effect = Exception("Network error")
        
        with patch.object(provider, '_cleanup_cluster') as mock_cleanup:
            result = provider.provision_cluster("test-cluster", sample_config)
        
        assert result.status == ProvisioningStatus.FAILED
        assert result.instance_id == "test-cluster"
        assert "Network error" in result.error_message
        mock_cleanup.assert_called_once_with("test-cluster")
    
    def test_deprovision_cluster_success(self, provider, mock_k8s_clients):
        """Test successful cluster deprovisioning."""
        with patch.object(provider, '_cleanup_cluster') as mock_cleanup:
            result = provider.deprovision_cluster("test-cluster")
        
        assert result.status == ProvisioningStatus.SUCCEEDED
        assert result.instance_id == "test-cluster"
        mock_cleanup.assert_called_once_with("test-cluster")
    
    def test_deprovision_cluster_failure(self, provider, mock_k8s_clients):
        """Test cluster deprovisioning failure."""
        with patch.object(provider, '_cleanup_cluster') as mock_cleanup:
            mock_cleanup.side_effect = Exception("Cleanup failed")
            result = provider.deprovision_cluster("test-cluster")
        
        assert result.status == ProvisioningStatus.FAILED
        assert result.instance_id == "test-cluster"
        assert "Cleanup failed" in result.error_message
    
    def test_get_cluster_status_succeeded(self, provider, mock_k8s_clients):
        """Test cluster status when all replicas are ready."""
        mock_sts = Mock()
        mock_sts.status.ready_replicas = 3
        mock_sts.spec.replicas = 3
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        status = provider.get_cluster_status("test-cluster")
        
        assert status == ProvisioningStatus.SUCCEEDED
    
    def test_get_cluster_status_in_progress(self, provider, mock_k8s_clients):
        """Test cluster status when replicas are not ready."""
        mock_sts = Mock()
        mock_sts.status.ready_replicas = 1
        mock_sts.spec.replicas = 3
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        status = provider.get_cluster_status("test-cluster")
        
        assert status == ProvisioningStatus.IN_PROGRESS
    
    def test_get_cluster_status_failed(self, provider, mock_k8s_clients):
        """Test cluster status when no StatefulSets found."""
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = []
        
        status = provider.get_cluster_status("test-cluster")
        
        assert status == ProvisioningStatus.FAILED
    
    def test_get_connection_info_cluster_ip(self, provider, mock_k8s_clients):
        """Test connection info for ClusterIP service."""
        mock_service = Mock()
        mock_service.spec.type = "ClusterIP"
        mock_service.spec.cluster_ip = "10.0.0.1"
        mock_k8s_clients['core_v1'].read_namespaced_service.return_value = mock_service
        
        connection_info = provider.get_connection_info("test-cluster")
        
        assert connection_info is not None
        assert connection_info["bootstrap_servers"] == ["10.0.0.1:9092"]
        assert "test-cluster-zookeeper.test-namespace.svc.cluster.local:2181" in connection_info["zookeeper_connect"]
    
    def test_get_connection_info_node_port(self, provider, mock_k8s_clients):
        """Test connection info for NodePort service."""
        mock_service = Mock()
        mock_service.spec.type = "NodePort"
        mock_port = Mock()
        mock_port.name = "kafka"
        mock_port.node_port = 30092
        mock_service.spec.ports = [mock_port]
        mock_k8s_clients['core_v1'].read_namespaced_service.return_value = mock_service
        
        # Mock node
        mock_node = Mock()
        mock_address = Mock()
        mock_address.address = "192.168.1.100"
        mock_node.status.addresses = [mock_address]
        mock_k8s_clients['core_v1'].list_node.return_value.items = [mock_node]
        
        connection_info = provider.get_connection_info("test-cluster")
        
        assert connection_info is not None
        assert connection_info["bootstrap_servers"] == ["192.168.1.100:30092"]
    
    def test_get_connection_info_service_not_found(self, provider, mock_k8s_clients):
        """Test connection info when service is not found."""
        mock_k8s_clients['core_v1'].read_namespaced_service.side_effect = ApiException(status=404)
        
        connection_info = provider.get_connection_info("test-cluster")
        
        assert connection_info is None
    
    def test_health_check_healthy(self, provider, mock_k8s_clients):
        """Test health check for healthy cluster."""
        # Mock healthy StatefulSet
        mock_sts = Mock()
        mock_sts.status.ready_replicas = 3
        mock_sts.spec.replicas = 3
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        # Mock services
        mock_service = Mock()
        mock_k8s_clients['core_v1'].list_namespaced_service.return_value.items = [mock_service]
        
        is_healthy = provider.health_check("test-cluster")
        
        assert is_healthy is True
    
    def test_health_check_unhealthy(self, provider, mock_k8s_clients):
        """Test health check for unhealthy cluster."""
        # Mock unhealthy StatefulSet
        mock_sts = Mock()
        mock_sts.status.ready_replicas = 1
        mock_sts.spec.replicas = 3
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        is_healthy = provider.health_check("test-cluster")
        
        assert is_healthy is False
    
    def test_cleanup_cluster(self, provider, mock_k8s_clients):
        """Test cluster cleanup."""
        # Mock StatefulSets
        mock_sts = Mock()
        mock_sts.metadata.name = "test-cluster-kafka"
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        # Mock Services
        mock_service = Mock()
        mock_service.metadata.name = "test-cluster-kafka"
        mock_k8s_clients['core_v1'].list_namespaced_service.return_value.items = [mock_service]
        
        # Mock PVCs
        mock_pvc = Mock()
        mock_pvc.metadata.name = "kafka-data-test-cluster-kafka-0"
        mock_k8s_clients['core_v1'].list_namespaced_persistent_volume_claim.return_value.items = [mock_pvc]
        
        provider._cleanup_cluster("test-cluster")
        
        mock_k8s_clients['apps_v1'].delete_namespaced_stateful_set.assert_called_once()
        mock_k8s_clients['core_v1'].delete_namespaced_service.assert_called_once()
        mock_k8s_clients['core_v1'].delete_namespaced_persistent_volume_claim.assert_called_once()
    
    def test_cleanup_cluster_resource_not_found(self, provider, mock_k8s_clients):
        """Test cluster cleanup when resources don't exist."""
        # Mock StatefulSets
        mock_sts = Mock()
        mock_sts.metadata.name = "test-cluster-kafka"
        mock_k8s_clients['apps_v1'].list_namespaced_stateful_set.return_value.items = [mock_sts]
        
        # Mock 404 error for deletion
        mock_k8s_clients['apps_v1'].delete_namespaced_stateful_set.side_effect = ApiException(status=404)
        
        # Should not raise exception
        provider._cleanup_cluster("test-cluster")
    
    def test_wait_for_cluster_ready_success(self, provider, mock_k8s_clients):
        """Test waiting for cluster to be ready - success case."""
        cluster_config = ClusterConfig(cluster_size=1, replication_factor=1, partition_count=3, 
                                     retention_hours=168, storage_size_gb=10, enable_ssl=False, 
                                     enable_sasl=False, custom_properties={})
        
        with patch.object(provider, 'get_cluster_status') as mock_status, \
             patch.object(provider, 'get_connection_info') as mock_conn_info:
            
            mock_status.return_value = ProvisioningStatus.SUCCEEDED
            mock_conn_info.return_value = {
                "bootstrap_servers": ["10.0.0.1:9092"],
                "zookeeper_connect": "test-zk:2181"
            }
            
            connection_info = provider._wait_for_cluster_ready("test-cluster", cluster_config, timeout=30)
        
        assert connection_info is not None
        assert connection_info.bootstrap_servers == ["10.0.0.1:9092"]
    
    def test_wait_for_cluster_ready_timeout(self, provider, mock_k8s_clients):
        """Test waiting for cluster to be ready - timeout case."""
        cluster_config = ClusterConfig(cluster_size=1, replication_factor=1, partition_count=3, 
                                     retention_hours=168, storage_size_gb=10, enable_ssl=False, 
                                     enable_sasl=False, custom_properties={})
        
        with patch.object(provider, 'get_cluster_status') as mock_status:
            mock_status.return_value = ProvisioningStatus.IN_PROGRESS
            
            with pytest.raises(Exception, match="did not become ready within"):
                provider._wait_for_cluster_ready("test-cluster", cluster_config, timeout=1)


if __name__ == "__main__":
    pytest.main([__file__])