"""Integration tests for Kubernetes provider with local cluster."""

import pytest
import time
import os
from pathlib import Path

from kafka_ops_agent.providers.kubernetes_provider import KubernetesProvider
from kafka_ops_agent.providers.base import ProvisioningStatus


@pytest.mark.integration
@pytest.mark.kubernetes
class TestKubernetesIntegration:
    """Integration tests for KubernetesProvider with local cluster."""
    
    @pytest.fixture(scope="class")
    def provider(self):
        """Create KubernetesProvider for integration tests."""
        # Skip if no Kubernetes cluster available
        try:
            provider = KubernetesProvider(namespace="kafka-integration-test")
            # Test basic connectivity
            provider.core_v1.list_namespace()
            return provider
        except Exception as e:
            pytest.skip(f"Kubernetes cluster not available: {e}")
    
    @pytest.fixture
    def cluster_config(self):
        """Sample cluster configuration for integration tests."""
        return {
            'cluster_size': 1,  # Single node for faster testing
            'replication_factor': 1,
            'partition_count': 3,
            'retention_hours': 1,  # Short retention for testing
            'storage_size_gb': 1,  # Minimal storage
            'enable_ssl': False,
            'enable_sasl': False,
            'custom_properties': {
                'log.segment.bytes': '104857600'  # 100MB segments
            }
        }
    
    def test_provision_and_deprovision_cluster(self, provider, cluster_config):
        """Test full cluster lifecycle - provision and deprovision."""
        instance_id = "test-integration-cluster"
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, cluster_config)
            
            assert result.status == ProvisioningStatus.SUCCEEDED
            assert result.instance_id == instance_id
            assert result.connection_info is not None
            
            # Verify cluster is running
            status = provider.get_cluster_status(instance_id)
            assert status == ProvisioningStatus.SUCCEEDED
            
            # Verify health check
            is_healthy = provider.health_check(instance_id)
            assert is_healthy is True
            
            # Get connection info
            connection_info = provider.get_connection_info(instance_id)
            assert connection_info is not None
            assert "bootstrap_servers" in connection_info
            assert "zookeeper_connect" in connection_info
            
        finally:
            # Always cleanup
            deprovision_result = provider.deprovision_cluster(instance_id)
            assert deprovision_result.status == ProvisioningStatus.SUCCEEDED
            
            # Verify cleanup
            time.sleep(10)  # Wait for cleanup to complete
            status = provider.get_cluster_status(instance_id)
            assert status == ProvisioningStatus.FAILED  # No resources found
    
    def test_multi_broker_cluster(self, provider, cluster_config):
        """Test provisioning a multi-broker cluster."""
        instance_id = "test-multi-broker-cluster"
        
        # Configure for 3 brokers
        multi_broker_config = cluster_config.copy()
        multi_broker_config.update({
            'cluster_size': 3,
            'replication_factor': 2
        })
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, multi_broker_config)
            
            assert result.status == ProvisioningStatus.SUCCEEDED
            assert result.instance_id == instance_id
            
            # Wait a bit longer for multi-broker cluster
            time.sleep(30)
            
            # Verify all brokers are ready
            status = provider.get_cluster_status(instance_id)
            assert status == ProvisioningStatus.SUCCEEDED
            
            # Verify StatefulSet has correct replica count
            statefulsets = provider._get_cluster_statefulsets(instance_id)
            kafka_sts = next((sts for sts in statefulsets if "kafka" in sts.metadata.name), None)
            assert kafka_sts is not None
            assert kafka_sts.spec.replicas == 3
            assert kafka_sts.status.ready_replicas == 3
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)
    
    def test_cluster_with_custom_properties(self, provider, cluster_config):
        """Test cluster with custom Kafka properties."""
        instance_id = "test-custom-props-cluster"
        
        # Add custom properties
        custom_config = cluster_config.copy()
        custom_config['custom_properties'] = {
            'log.retention.bytes': '1073741824',  # 1GB
            'log.segment.bytes': '268435456',     # 256MB
            'num.network.threads': '8',
            'num.io.threads': '8'
        }
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, custom_config)
            
            assert result.status == ProvisioningStatus.SUCCEEDED
            
            # Verify custom properties are applied
            statefulsets = provider._get_cluster_statefulsets(instance_id)
            kafka_sts = next((sts for sts in statefulsets if "kafka" in sts.metadata.name), None)
            assert kafka_sts is not None
            
            # Check environment variables in container spec
            container = kafka_sts.spec.template.spec.containers[0]
            env_vars = {env.name: env.value for env in container.env if hasattr(env, 'value')}
            
            assert env_vars.get('KAFKA_LOG_RETENTION_BYTES') == '1073741824'
            assert env_vars.get('KAFKA_LOG_SEGMENT_BYTES') == '268435456'
            assert env_vars.get('KAFKA_NUM_NETWORK_THREADS') == '8'
            assert env_vars.get('KAFKA_NUM_IO_THREADS') == '8'
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)
    
    def test_cluster_persistence(self, provider, cluster_config):
        """Test that cluster data persists with PVCs."""
        instance_id = "test-persistence-cluster"
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, cluster_config)
            assert result.status == ProvisioningStatus.SUCCEEDED
            
            # Verify PVCs are created
            pvcs = provider.core_v1.list_namespaced_persistent_volume_claim(
                namespace=provider.namespace,
                label_selector=f"cluster={instance_id}"
            )
            
            # Should have PVCs for Kafka and Zookeeper
            pvc_names = [pvc.metadata.name for pvc in pvcs.items]
            kafka_pvcs = [name for name in pvc_names if "kafka-data" in name]
            zk_pvcs = [name for name in pvc_names if "zk-data" in name]
            
            assert len(kafka_pvcs) >= 1  # At least one Kafka PVC
            assert len(zk_pvcs) >= 1     # At least one ZK PVC
            
            # Verify PVC sizes
            for pvc in pvcs.items:
                storage_request = pvc.spec.resources.requests.get('storage')
                assert storage_request == f"{cluster_config['storage_size_gb']}Gi"
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)
    
    def test_cluster_networking(self, provider, cluster_config):
        """Test cluster networking and service discovery."""
        instance_id = "test-networking-cluster"
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, cluster_config)
            assert result.status == ProvisioningStatus.SUCCEEDED
            
            # Verify services are created
            services = provider._get_cluster_services(instance_id)
            service_names = [svc.metadata.name for svc in services]
            
            assert f"{instance_id}-kafka" in service_names
            assert f"{instance_id}-zookeeper" in service_names
            
            # Verify Kafka service configuration
            kafka_service = provider._get_kafka_service(instance_id)
            assert kafka_service is not None
            assert kafka_service.spec.type == "ClusterIP"  # Default type
            
            # Verify port configuration
            kafka_port = next((port for port in kafka_service.spec.ports if port.name == "kafka"), None)
            assert kafka_port is not None
            assert kafka_port.port == 9092
            assert kafka_port.target_port == 9092
            
            # Verify Zookeeper service
            zk_service = provider._get_zookeeper_service(instance_id)
            assert zk_service is not None
            
            zk_client_port = next((port for port in zk_service.spec.ports if port.name == "client"), None)
            assert zk_client_port is not None
            assert zk_client_port.port == 2181
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)
    
    def test_cluster_resource_limits(self, provider, cluster_config):
        """Test that resource limits are properly set."""
        instance_id = "test-resources-cluster"
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, cluster_config)
            assert result.status == ProvisioningStatus.SUCCEEDED
            
            # Check Kafka StatefulSet resources
            statefulsets = provider._get_cluster_statefulsets(instance_id)
            kafka_sts = next((sts for sts in statefulsets if "kafka" in sts.metadata.name), None)
            assert kafka_sts is not None
            
            kafka_container = kafka_sts.spec.template.spec.containers[0]
            resources = kafka_container.resources
            
            # Verify resource requests and limits are set
            assert resources.requests is not None
            assert resources.limits is not None
            
            assert resources.requests['memory'] == '1Gi'
            assert resources.requests['cpu'] == '500m'
            assert resources.limits['memory'] == '2Gi'
            assert resources.limits['cpu'] == '1000m'
            
            # Check Zookeeper StatefulSet resources
            zk_sts = next((sts for sts in statefulsets if "zookeeper" in sts.metadata.name), None)
            assert zk_sts is not None
            
            zk_container = zk_sts.spec.template.spec.containers[0]
            zk_resources = zk_container.resources
            
            assert zk_resources.requests['memory'] == '512Mi'
            assert zk_resources.requests['cpu'] == '250m'
            assert zk_resources.limits['memory'] == '1Gi'
            assert zk_resources.limits['cpu'] == '500m'
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)
    
    def test_cluster_health_probes(self, provider, cluster_config):
        """Test that health probes are configured correctly."""
        instance_id = "test-probes-cluster"
        
        try:
            # Provision cluster
            result = provider.provision_cluster(instance_id, cluster_config)
            assert result.status == ProvisioningStatus.SUCCEEDED
            
            # Check Kafka container probes
            statefulsets = provider._get_cluster_statefulsets(instance_id)
            kafka_sts = next((sts for sts in statefulsets if "kafka" in sts.metadata.name), None)
            kafka_container = kafka_sts.spec.template.spec.containers[0]
            
            # Verify readiness probe
            assert kafka_container.readiness_probe is not None
            assert kafka_container.readiness_probe.tcp_socket.port == 9092
            assert kafka_container.readiness_probe.initial_delay_seconds == 30
            
            # Verify liveness probe
            assert kafka_container.liveness_probe is not None
            assert kafka_container.liveness_probe.tcp_socket.port == 9092
            assert kafka_container.liveness_probe.initial_delay_seconds == 60
            
            # Check Zookeeper container probes
            zk_sts = next((sts for sts in statefulsets if "zookeeper" in sts.metadata.name), None)
            zk_container = zk_sts.spec.template.spec.containers[0]
            
            assert zk_container.readiness_probe is not None
            assert zk_container.readiness_probe.tcp_socket.port == 2181
            assert zk_container.liveness_probe is not None
            assert zk_container.liveness_probe.tcp_socket.port == 2181
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)
    
    @pytest.mark.slow
    def test_cluster_startup_time(self, provider, cluster_config):
        """Test cluster startup time and readiness."""
        instance_id = "test-startup-time-cluster"
        
        try:
            start_time = time.time()
            
            # Provision cluster
            result = provider.provision_cluster(instance_id, cluster_config)
            
            provision_time = time.time() - start_time
            
            assert result.status == ProvisioningStatus.SUCCEEDED
            
            # Log startup time for monitoring
            print(f"Cluster startup time: {provision_time:.2f} seconds")
            
            # Verify cluster is actually ready for connections
            connection_info = provider.get_connection_info(instance_id)
            assert connection_info is not None
            
            # Basic connectivity test could be added here
            # (would require Kafka client libraries)
            
        finally:
            # Cleanup
            provider.deprovision_cluster(instance_id)


@pytest.mark.integration
@pytest.mark.kubernetes
class TestKubernetesProviderErrorHandling:
    """Test error handling scenarios for Kubernetes provider."""
    
    @pytest.fixture
    def provider(self):
        """Create KubernetesProvider for error testing."""
        try:
            return KubernetesProvider(namespace="kafka-error-test")
        except Exception as e:
            pytest.skip(f"Kubernetes cluster not available: {e}")
    
    def test_provision_invalid_config(self, provider):
        """Test provisioning with invalid configuration."""
        invalid_config = {
            'cluster_size': -1,  # Invalid
            'replication_factor': 0,  # Invalid
            'storage_size_gb': 0  # Invalid
        }
        
        # Should handle gracefully and return failure
        result = provider.provision_cluster("invalid-cluster", invalid_config)
        
        # The provider should handle this gracefully
        # (actual behavior depends on Kubernetes validation)
        assert result.instance_id == "invalid-cluster"
    
    def test_deprovision_nonexistent_cluster(self, provider):
        """Test deprovisioning a cluster that doesn't exist."""
        result = provider.deprovision_cluster("nonexistent-cluster")
        
        # Should succeed (idempotent operation)
        assert result.status == ProvisioningStatus.SUCCEEDED
        assert result.instance_id == "nonexistent-cluster"
    
    def test_get_status_nonexistent_cluster(self, provider):
        """Test getting status of nonexistent cluster."""
        status = provider.get_cluster_status("nonexistent-cluster")
        
        assert status == ProvisioningStatus.FAILED
    
    def test_health_check_nonexistent_cluster(self, provider):
        """Test health check of nonexistent cluster."""
        is_healthy = provider.health_check("nonexistent-cluster")
        
        assert is_healthy is False
    
    def test_connection_info_nonexistent_cluster(self, provider):
        """Test getting connection info for nonexistent cluster."""
        connection_info = provider.get_connection_info("nonexistent-cluster")
        
        assert connection_info is None


if __name__ == "__main__":
    # Run integration tests
    pytest.main([
        __file__,
        "-v",
        "-m", "integration and kubernetes",
        "--tb=short"
    ])