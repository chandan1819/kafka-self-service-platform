"""Tests for Open Service Broker API."""

import pytest
import json
from unittest.mock import AsyncMock, Mock, patch

from kafka_ops_agent.api.service_broker import create_app
from kafka_ops_agent.providers.base import ProvisioningResult, DeprovisioningResult, ProvisioningStatus
from kafka_ops_agent.models.cluster import ClusterStatus


class TestServiceBrokerAPI:
    """Test Open Service Broker API endpoints."""
    
    @pytest.fixture
    def mock_provisioning_service(self):
        """Mock provisioning service."""
        service = AsyncMock()
        
        # Mock successful provisioning
        service.provision_cluster.return_value = ProvisioningResult(
            status=ProvisioningStatus.SUCCEEDED,
            instance_id="test-instance",
            connection_info={
                'bootstrap_servers': ['localhost:9092'],
                'zookeeper_connect': 'localhost:2181'
            }
        )
        
        # Mock successful deprovisioning
        service.deprovision_cluster.return_value = DeprovisioningResult(
            status=ProvisioningStatus.SUCCEEDED,
            instance_id="test-instance"
        )
        
        # Mock status check
        service.get_cluster_status.return_value = ClusterStatus.RUNNING
        
        return service
    
    @pytest.fixture
    def app(self, mock_provisioning_service):
        """Create test Flask app."""
        with patch('kafka_ops_agent.api.service_broker.get_provisioning_service', return_value=mock_provisioning_service):
            with patch('kafka_ops_agent.api.service_broker.initialize_provisioning_service'):
                app = create_app()
                app.config['TESTING'] = True
                return app
    
    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return app.test_client()
    
    def test_get_catalog(self, client):
        """Test catalog endpoint."""
        response = client.get('/v2/catalog')
        
        assert response.status_code == 200
        
        data = json.loads(response.data)
        assert 'services' in data
        assert len(data['services']) == 1
        
        service = data['services'][0]
        assert service['name'] == 'Apache Kafka'
        assert service['id'] == 'kafka-service'
        assert len(service['plans']) == 3
    
    def test_provision_service_instance(self, client, mock_provisioning_service):
        """Test service instance provisioning."""
        provision_data = {
            "service_id": "kafka-service",
            "plan_id": "basic",
            "organization_guid": "test-org",
            "space_guid": "test-space",
            "parameters": {
                "cluster_size": 1
            }
        }
        
        response = client.put(
            '/v2/service_instances/test-instance',
            data=json.dumps(provision_data),
            content_type='application/json'
        )
        
        assert response.status_code == 201
        
        # Verify provisioning service was called
        mock_provisioning_service.provision_cluster.assert_called_once()
        call_args = mock_provisioning_service.provision_cluster.call_args
        assert call_args[1]['instance_id'] == 'test-instance'
        assert call_args[1]['service_id'] == 'kafka-service'
        assert call_args[1]['plan_id'] == 'basic'
    
    def test_provision_service_instance_invalid_service(self, client):
        """Test provisioning with invalid service ID."""
        provision_data = {
            "service_id": "invalid-service",
            "plan_id": "basic",
            "organization_guid": "test-org",
            "space_guid": "test-space"
        }
        
        response = client.put(
            '/v2/service_instances/test-instance',
            data=json.dumps(provision_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'BadRequest'
        assert 'Invalid service ID' in data['description']
    
    def test_provision_service_instance_invalid_plan(self, client):
        """Test provisioning with invalid plan ID."""
        provision_data = {
            "service_id": "kafka-service",
            "plan_id": "invalid-plan",
            "organization_guid": "test-org",
            "space_guid": "test-space"
        }
        
        response = client.put(
            '/v2/service_instances/test-instance',
            data=json.dumps(provision_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'BadRequest'
        assert 'Invalid plan ID' in data['description']
    
    def test_provision_service_instance_already_exists(self, client, mock_provisioning_service):
        """Test provisioning when instance already exists."""
        mock_provisioning_service.provision_cluster.return_value = ProvisioningResult(
            status=ProvisioningStatus.FAILED,
            instance_id="test-instance",
            error_message="Instance already exists"
        )
        
        provision_data = {
            "service_id": "kafka-service",
            "plan_id": "basic",
            "organization_guid": "test-org",
            "space_guid": "test-space"
        }
        
        response = client.put(
            '/v2/service_instances/test-instance',
            data=json.dumps(provision_data),
            content_type='application/json'
        )
        
        assert response.status_code == 409
        data = json.loads(response.data)
        assert data['error'] == 'Conflict'
    
    def test_provision_service_instance_async(self, client, mock_provisioning_service):
        """Test asynchronous provisioning."""
        mock_provisioning_service.provision_cluster.return_value = ProvisioningResult(
            status=ProvisioningStatus.IN_PROGRESS,
            instance_id="test-instance",
            operation_id="operation-123"
        )
        
        provision_data = {
            "service_id": "kafka-service",
            "plan_id": "standard",
            "organization_guid": "test-org",
            "space_guid": "test-space"
        }
        
        response = client.put(
            '/v2/service_instances/test-instance',
            data=json.dumps(provision_data),
            content_type='application/json'
        )
        
        assert response.status_code == 202
        data = json.loads(response.data)
        assert data['operation'] == 'operation-123'
    
    def test_deprovision_service_instance(self, client, mock_provisioning_service):
        """Test service instance deprovisioning."""
        response = client.delete(
            '/v2/service_instances/test-instance?service_id=kafka-service&plan_id=basic'
        )
        
        assert response.status_code == 200
        
        # Verify deprovisioning service was called
        mock_provisioning_service.deprovision_cluster.assert_called_once_with('test-instance', None)
    
    def test_deprovision_service_instance_not_found(self, client, mock_provisioning_service):
        """Test deprovisioning non-existent instance."""
        mock_provisioning_service.deprovision_cluster.return_value = DeprovisioningResult(
            status=ProvisioningStatus.FAILED,
            instance_id="test-instance",
            error_message="Instance not found"
        )
        
        response = client.delete(
            '/v2/service_instances/test-instance?service_id=kafka-service&plan_id=basic'
        )
        
        assert response.status_code == 410
        data = json.loads(response.data)
        assert data['error'] == 'Gone'
    
    def test_deprovision_service_instance_missing_params(self, client):
        """Test deprovisioning without required parameters."""
        response = client.delete('/v2/service_instances/test-instance')
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'BadRequest'
        assert 'service_id' in data['description']
    
    def test_get_last_operation(self, client, mock_provisioning_service):
        """Test last operation endpoint."""
        response = client.get(
            '/v2/service_instances/test-instance/last_operation?service_id=kafka-service&plan_id=basic'
        )
        
        assert response.status_code == 200
        
        data = json.loads(response.data)
        assert data['state'] == 'succeeded'
        assert 'description' in data
        
        # Verify status check was called
        mock_provisioning_service.get_cluster_status.assert_called_once_with('test-instance')
    
    def test_get_last_operation_in_progress(self, client, mock_provisioning_service):
        """Test last operation for in-progress operation."""
        mock_provisioning_service.get_cluster_status.return_value = ClusterStatus.CREATING
        
        response = client.get(
            '/v2/service_instances/test-instance/last_operation?service_id=kafka-service&plan_id=basic'
        )
        
        assert response.status_code == 200
        
        data = json.loads(response.data)
        assert data['state'] == 'in progress'
    
    def test_get_last_operation_failed(self, client, mock_provisioning_service):
        """Test last operation for failed operation."""
        mock_provisioning_service.get_cluster_status.return_value = ClusterStatus.ERROR
        
        response = client.get(
            '/v2/service_instances/test-instance/last_operation?service_id=kafka-service&plan_id=basic'
        )
        
        assert response.status_code == 200
        
        data = json.loads(response.data)
        assert data['state'] == 'failed'
    
    def test_get_last_operation_not_found(self, client, mock_provisioning_service):
        """Test last operation for non-existent instance."""
        mock_provisioning_service.get_cluster_status.return_value = None
        
        response = client.get(
            '/v2/service_instances/test-instance/last_operation?service_id=kafka-service&plan_id=basic'
        )
        
        assert response.status_code == 410
        data = json.loads(response.data)
        assert data['error'] == 'Gone'
    
    def test_update_service_instance_not_supported(self, client):
        """Test service instance update (not supported)."""
        update_data = {
            "service_id": "kafka-service",
            "plan_id": "standard"
        }
        
        response = client.patch(
            '/v2/service_instances/test-instance',
            data=json.dumps(update_data),
            content_type='application/json'
        )
        
        assert response.status_code == 422
        data = json.loads(response.data)
        assert data['error'] == 'NotSupported'
    
    def test_create_service_binding_not_supported(self, client):
        """Test service binding creation (not supported)."""
        response = client.put('/v2/service_instances/test-instance/service_bindings/test-binding')
        
        assert response.status_code == 422
        data = json.loads(response.data)
        assert data['error'] == 'NotSupported'
    
    def test_delete_service_binding_not_supported(self, client):
        """Test service binding deletion (not supported)."""
        response = client.delete('/v2/service_instances/test-instance/service_bindings/test-binding')
        
        assert response.status_code == 422
        data = json.loads(response.data)
        assert data['error'] == 'NotSupported'
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get('/health')
        
        assert response.status_code == 200
        
        data = json.loads(response.data)
        assert data['status'] == 'healthy'
        assert data['service'] == 'kafka-ops-agent'
        assert 'version' in data
    
    def test_not_found_endpoint(self, client):
        """Test 404 error handling."""
        response = client.get('/v2/nonexistent')
        
        assert response.status_code == 404
        data = json.loads(response.data)
        assert data['error'] == 'NotFound'
    
    def test_method_not_allowed(self, client):
        """Test 405 error handling."""
        response = client.post('/v2/catalog')
        
        assert response.status_code == 405
        data = json.loads(response.data)
        assert data['error'] == 'MethodNotAllowed'
    
    def test_invalid_json(self, client):
        """Test invalid JSON handling."""
        response = client.put(
            '/v2/service_instances/test-instance',
            data='invalid json',
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'BadRequest'
    
    def test_missing_request_body(self, client):
        """Test missing request body."""
        response = client.put(
            '/v2/service_instances/test-instance',
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'BadRequest'
        assert 'Request body is required' in data['description']