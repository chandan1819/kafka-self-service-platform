"""Tests for provisioning service."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime

from kafka_ops_agent.services.provisioning import ProvisioningService
from kafka_ops_agent.providers.base import ProvisioningResult, DeprovisioningResult, ProvisioningStatus
from kafka_ops_agent.models.cluster import ServiceInstance, ClusterStatus, RuntimeProvider, ConnectionInfo
from kafka_ops_agent.models.factory import ServiceInstanceFactory


class TestProvisioningService:
    """Test provisioning service."""
    
    @pytest.fixture
    def mock_metadata_store(self):
        """Mock metadata store."""
        store = AsyncMock()
        store.instance_exists.return_value = False
        store.create_instance.return_value = True
        store.update_instance.return_value = True
        store.delete_instance.return_value = True
        return store
    
    @pytest.fixture
    def mock_audit_store(self):
        """Mock audit store."""
        store = AsyncMock()
        return store
    
    @pytest.fixture
    def mock_docker_provider(self):
        """Mock Docker provider."""
        provider = Mock()
        provider.provision_cluster.return_value = ProvisioningResult(
            status=ProvisioningStatus.SUCCEEDED,
            instance_id="test-instance",
            connection_info={
                'bootstrap_servers': ['localhost:9092'],
                'zookeeper_connect': 'localhost:2181'
            }
        )
        provider.deprovision_cluster.return_value = DeprovisioningResult(
            status=ProvisioningStatus.SUCCEEDED,
            instance_id="test-instance"
        )
        provider.get_cluster_status.return_value = ProvisioningStatus.SUCCEEDED
        provider.get_connection_info.return_value = {
            'bootstrap_servers': ['localhost:9092'],
            'zookeeper_connect': 'localhost:2181'
        }
        provider.health_check.return_value = True
        return provider
    
    @pytest.fixture
    def provisioning_service(self, mock_metadata_store, mock_audit_store, mock_docker_provider):
        """Create provisioning service with mocked dependencies."""
        with patch('kafka_ops_agent.services.provisioning.DockerProvider', return_value=mock_docker_provider):
            service = ProvisioningService(mock_metadata_store, mock_audit_store)
            return service
    
    @pytest.mark.asyncio
    async def test_provision_cluster_success(self, provisioning_service, mock_metadata_store, mock_audit_store):
        """Test successful cluster provisioning."""
        result = await provisioning_service.provision_cluster(
            instance_id="test-instance",
            service_id="kafka-service",
            plan_id="standard",
            organization_guid="org-123",
            space_guid="space-456",
            parameters={'cluster_size': 3},
            user_id="user-123"
        )
        
        assert result.status == ProvisioningStatus.SUCCEEDED
        assert result.instance_id == "test-instance"
        assert result.connection_info is not None
        
        # Verify metadata store calls
        mock_metadata_store.instance_exists.assert_called_once_with("test-instance")
        mock_metadata_store.create_instance.assert_called_once()
        assert mock_metadata_store.update_instance.call_count >= 2  # Status updates
        
        # Verify audit logging
        assert mock_audit_store.log_operation.call_count >= 2  # Start and success
    
    @pytest.mark.asyncio
    async def test_provision_cluster_already_exists(self, provisioning_service, mock_metadata_store):
        """Test provisioning when instance already exists."""
        mock_metadata_store.instance_exists.return_value = True
        
        result = await provisioning_service.provision_cluster(
            instance_id="existing-instance",
            service_id="kafka-service",
            plan_id="standard",
            organization_guid="org-123",
            space_guid="space-456",
            parameters={}
        )
        
        assert result.status == ProvisioningStatus.FAILED
        assert "already exists" in result.error_message
        
        # Should not create instance
        mock_metadata_store.create_instance.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_provision_cluster_unsupported_provider(self, provisioning_service, mock_metadata_store):
        """Test provisioning with unsupported provider."""
        result = await provisioning_service.provision_cluster(
            instance_id="test-instance",
            service_id="kafka-service",
            plan_id="standard",
            organization_guid="org-123",
            space_guid="space-456",
            parameters={'runtime_provider': 'unsupported'}
        )
        
        assert result.status == ProvisioningStatus.FAILED
        assert "Unsupported runtime provider" in result.error_message
    
    @pytest.mark.asyncio
    async def test_provision_cluster_provider_failure(self, provisioning_service, mock_metadata_store, mock_audit_store):
        """Test provisioning when provider fails."""
        # Mock provider to return failure
        provisioning_service.providers['docker'].provision_cluster.return_value = ProvisioningResult(
            status=ProvisioningStatus.FAILED,
            instance_id="test-instance",
            error_message="Provider error"
        )
        
        result = await provisioning_service.provision_cluster(
            instance_id="test-instance",
            service_id="kafka-service",
            plan_id="standard",
            organization_guid="org-123",
            space_guid="space-456",
            parameters={}
        )
        
        assert result.status == ProvisioningStatus.FAILED
        assert result.error_message == "Provider error"
        
        # Should update instance with error status
        update_calls = mock_metadata_store.update_instance.call_args_list
        final_update = update_calls[-1][0][0]  # Last update call, first argument
        assert final_update.status == ClusterStatus.ERROR
        assert final_update.error_message == "Provider error"
    
    @pytest.mark.asyncio
    async def test_deprovision_cluster_success(self, provisioning_service, mock_metadata_store, mock_audit_store):
        """Test successful cluster deprovisioning."""
        # Mock existing instance
        instance = ServiceInstanceFactory.create_running("test-instance")
        mock_metadata_store.get_instance.return_value = instance
        
        result = await provisioning_service.deprovision_cluster("test-instance", "user-123")
        
        assert result.status == ProvisioningStatus.SUCCEEDED
        assert result.instance_id == "test-instance"
        
        # Verify metadata store calls
        mock_metadata_store.get_instance.assert_called_once_with("test-instance")
        mock_metadata_store.delete_instance.assert_called_once_with("test-instance")
        
        # Verify audit logging
        assert mock_audit_store.log_operation.call_count >= 2  # Start and success
    
    @pytest.mark.asyncio
    async def test_deprovision_cluster_not_found(self, provisioning_service, mock_metadata_store):
        """Test deprovisioning non-existent instance."""
        mock_metadata_store.get_instance.return_value = None
        
        result = await provisioning_service.deprovision_cluster("non-existent")
        
        assert result.status == ProvisioningStatus.FAILED
        assert "not found" in result.error_message
        
        # Should not attempt deletion
        mock_metadata_store.delete_instance.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_cluster_status(self, provisioning_service, mock_metadata_store):
        """Test getting cluster status."""
        instance = ServiceInstanceFactory.create_running("test-instance")
        mock_metadata_store.get_instance.return_value = instance
        
        status = await provisioning_service.get_cluster_status("test-instance")
        
        assert status == ClusterStatus.RUNNING
        mock_metadata_store.get_instance.assert_called_once_with("test-instance")
    
    @pytest.mark.asyncio
    async def test_get_cluster_status_not_found(self, provisioning_service, mock_metadata_store):
        """Test getting status for non-existent cluster."""
        mock_metadata_store.get_instance.return_value = None
        
        status = await provisioning_service.get_cluster_status("non-existent")
        
        assert status is None
    
    @pytest.mark.asyncio
    async def test_get_connection_info(self, provisioning_service, mock_metadata_store):
        """Test getting connection information."""
        instance = ServiceInstanceFactory.create_running("test-instance")
        mock_metadata_store.get_instance.return_value = instance
        
        connection_info = await provisioning_service.get_connection_info("test-instance")
        
        assert connection_info is not None
        assert 'bootstrap_servers' in connection_info
        assert connection_info['bootstrap_servers'] == ['localhost:9092']
    
    @pytest.mark.asyncio
    async def test_get_connection_info_not_running(self, provisioning_service, mock_metadata_store):
        """Test getting connection info for non-running cluster."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        instance.status = ClusterStatus.PENDING
        mock_metadata_store.get_instance.return_value = instance
        
        connection_info = await provisioning_service.get_connection_info("test-instance")
        
        assert connection_info is None
    
    @pytest.mark.asyncio
    async def test_health_check(self, provisioning_service, mock_metadata_store):
        """Test cluster health check."""
        instance = ServiceInstanceFactory.create_running("test-instance")
        mock_metadata_store.get_instance.return_value = instance
        
        health = await provisioning_service.health_check("test-instance")
        
        assert health is True
        provisioning_service.providers['docker'].health_check.assert_called_once_with("test-instance")
    
    @pytest.mark.asyncio
    async def test_health_check_not_running(self, provisioning_service, mock_metadata_store):
        """Test health check for non-running cluster."""
        instance = ServiceInstanceFactory.create_default("test-instance")
        instance.status = ClusterStatus.PENDING
        mock_metadata_store.get_instance.return_value = instance
        
        health = await provisioning_service.health_check("test-instance")
        
        assert health is False
        # Should not call provider health check
        provisioning_service.providers['docker'].health_check.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_list_instances(self, provisioning_service, mock_metadata_store):
        """Test listing instances."""
        instances = [
            ServiceInstanceFactory.create_running("instance-1"),
            ServiceInstanceFactory.create_default("instance-2")
        ]
        mock_metadata_store.list_instances.return_value = instances
        
        result = await provisioning_service.list_instances()
        
        assert len(result) == 2
        assert result[0].instance_id == "instance-1"
        assert result[1].instance_id == "instance-2"
        
        mock_metadata_store.list_instances.assert_called_once_with(None)
    
    @pytest.mark.asyncio
    async def test_list_instances_with_filters(self, provisioning_service, mock_metadata_store):
        """Test listing instances with filters."""
        filters = {'status': 'running'}
        mock_metadata_store.list_instances.return_value = []
        
        await provisioning_service.list_instances(filters)
        
        mock_metadata_store.list_instances.assert_called_once_with(filters)
    
    @pytest.mark.asyncio
    async def test_cleanup_failed_instances(self, provisioning_service, mock_metadata_store):
        """Test cleaning up failed instances."""
        failed_instances = [
            ServiceInstanceFactory.create_default("failed-1"),
            ServiceInstanceFactory.create_default("failed-2")
        ]
        for instance in failed_instances:
            instance.status = ClusterStatus.ERROR
        
        mock_metadata_store.get_instances_by_status.return_value = failed_instances
        
        cleanup_count = await provisioning_service.cleanup_failed_instances()
        
        assert cleanup_count == 2
        
        # Verify provider cleanup calls
        docker_provider = provisioning_service.providers['docker']
        assert docker_provider.deprovision_cluster.call_count == 2
        
        # Verify metadata deletion
        assert mock_metadata_store.delete_instance.call_count == 2
    
    def test_parameters_to_config(self, provisioning_service):
        """Test parameter to configuration conversion."""
        parameters = {
            'plan_id': 'basic',
            'cluster_size': 1,
            'replication_factor': 1,
            'retention_hours': 24,
            'custom_properties': {'auto.create.topics.enable': 'false'}
        }
        
        config = provisioning_service._parameters_to_config(parameters)
        
        assert config['cluster_size'] == 1
        assert config['replication_factor'] == 1
        assert config['retention_hours'] == 24
        assert config['custom_properties']['auto.create.topics.enable'] == 'false'
    
    def test_parameters_to_config_premium_plan(self, provisioning_service):
        """Test parameter conversion for premium plan."""
        parameters = {'plan_id': 'premium'}
        
        config = provisioning_service._parameters_to_config(parameters)
        
        # Should use production config as base
        assert config['cluster_size'] == 5
        assert config['enable_ssl'] is True
        assert config['enable_sasl'] is True