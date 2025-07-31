"""Tests for topic management REST API."""

import pytest
import json
from unittest.mock import AsyncMock, Mock, patch

from kafka_ops_agent.api.topic_management import create_topic_management_app
from kafka_ops_agent.models.topic import TopicInfo, TopicDetails, TopicOperationResult
from kafka_ops_agent.models.cluster import ClusterStatus


class TestTopicManagementAPI:
    """Test topic management REST API endpoints."""
    
    @pytest.fixture
    def mock_topic_service(self):
        """Mock topic management service."""
        service = AsyncMock()
        
        # Mock successful topic creation
        service.create_topic.return_value = TopicOperationResult(
            success=True,
            message="Topic created successfully",
            topic_name="test-topic",
            details={"partitions": 3, "replication_factor": 1}
        )
        
        # Mock topic listing
        service.list_topics.return_value = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1),
            TopicInfo(name="topic2", partitions=6, replication_factor=2)
        ]
        
        # Mock topic description
        service.describe_topic.return_value = TopicDetails(
            name="test-topic",
            partitions=3,
            replication_factor=1,
            configs={'retention.ms': '604800000'},
            partition_details=[]
        )
        
        # Mock config update
        service.update_topic_config.return_value = TopicOperationResult(
            success=True,
            message="Configuration updated",
            topic_name="test-topic",
            details={"updated_configs": {"retention.ms": "86400000"}}
        )
        
        # Mock topic deletion
        service.delete_topic.return_value = TopicOperationResult(
            success=True,
            message="Topic deleted successfully",
            topic_name="test-topic"
        )
        
        # Mock topic purging
        service.purge_topic.return_value = TopicOperationResult(
            success=True,
            message="Topic purged successfully",
            topic_name="test-topic",
            details={"retention_ms": 1000}
        )
        
        # Mock bulk operations
        service.bulk_create_topics.return_value = {
            "topic1": TopicOperationResult(success=True, message="Created", topic_name="topic1"),
            "topic2": TopicOperationResult(success=True, message="Created", topic_name="topic2")
        }
        
        service.bulk_delete_topics.return_value = {
            "topic1": TopicOperationResult(success=True, message="Deleted", topic_name="topic1"),
            "topic2": TopicOperationResult(success=True, message="Deleted", topic_name="topic2")
        }
        
        # Mock cluster info
        service.get_cluster_info.return_value = {
            'cluster_id': 'test-cluster',
            'broker_count': 3,
            'topic_count': 10
        }
        
        return service
    
    @pytest.fixture
    def app(self, mock_topic_service):
        """Create test Flask app."""
        with patch('kafka_ops_agent.api.topic_management.get_topic_service', return_value=mock_topic_service):
            app = create_topic_management_app()
            app.config['TESTING'] = True
            return app
    
    @pytest.fixture
    def client(self, app):
        """Create test client."""
        return app.test_client()
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get('/api/v1/health')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'healthy'
        assert data['service'] == 'topic-management-api'
    
    def test_create_topic_success(self, client, mock_topic_service):
        """Test successful topic creation."""
        topic_data = {
            'name': 'test-topic',
            'partitions': 3,
            'replication_factor': 1,
            'retention_ms': 3600000
        }
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics',
            data=json.dumps(topic_data),
            content_type='application/json',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 201
        data = json.loads(response.data)
        assert data['success'] is True
        assert data['topic']['name'] == 'test-topic'
        
        # Verify service was called
        mock_topic_service.create_topic.assert_called_once()
    
    def test_create_topic_invalid_cluster(self, client):
        """Test topic creation with invalid cluster ID."""
        topic_data = {'name': 'test-topic'}
        
        response = client.post(
            '/api/v1/clusters/x/topics',  # Invalid cluster ID
            data=json.dumps(topic_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'INVALID_CLUSTER_ID'
    
    def test_create_topic_missing_body(self, client):
        """Test topic creation without request body."""
        response = client.post('/api/v1/clusters/test-cluster/topics')
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'MISSING_REQUEST_BODY'
    
    def test_create_topic_invalid_config(self, client):
        """Test topic creation with invalid configuration."""
        topic_data = {
            'name': '',  # Invalid empty name
            'partitions': -1  # Invalid negative partitions
        }
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics',
            data=json.dumps(topic_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'INVALID_TOPIC_CONFIG'
    
    def test_create_topic_failure(self, client, mock_topic_service):
        """Test topic creation failure."""
        mock_topic_service.create_topic.return_value = TopicOperationResult(
            success=False,
            message="Creation failed",
            topic_name="test-topic",
            error_code="CREATION_FAILED"
        )
        
        topic_data = {'name': 'test-topic'}
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics',
            data=json.dumps(topic_data),
            content_type='application/json'
        )
        
        assert response.status_code == 500
        data = json.loads(response.data)
        assert data['error'] == 'CREATION_FAILED'
    
    def test_list_topics_success(self, client, mock_topic_service):
        """Test successful topic listing."""
        response = client.get(
            '/api/v1/clusters/test-cluster/topics',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['cluster_id'] == 'test-cluster'
        assert len(data['topics']) == 2
        assert data['topics'][0]['name'] == 'topic1'
        assert data['total_count'] == 2
        
        mock_topic_service.list_topics.assert_called_once()
    
    def test_list_topics_with_internal(self, client, mock_topic_service):
        """Test topic listing with internal topics."""
        response = client.get(
            '/api/v1/clusters/test-cluster/topics?include_internal=true',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        
        # Verify service called with include_internal=True
        call_args = mock_topic_service.list_topics.call_args
        assert call_args[0][1] is True  # include_internal parameter
    
    def test_describe_topic_success(self, client, mock_topic_service):
        """Test successful topic description."""
        response = client.get(
            '/api/v1/clusters/test-cluster/topics/test-topic',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['cluster_id'] == 'test-cluster'
        assert data['topic']['name'] == 'test-topic'
        assert data['topic']['partitions'] == 3
        
        mock_topic_service.describe_topic.assert_called_once()
    
    def test_describe_topic_not_found(self, client, mock_topic_service):
        """Test topic description when topic not found."""
        mock_topic_service.describe_topic.return_value = None
        
        response = client.get('/api/v1/clusters/test-cluster/topics/nonexistent')
        
        assert response.status_code == 404
        data = json.loads(response.data)
        assert data['error'] == 'TOPIC_NOT_FOUND'
    
    def test_update_topic_config_success(self, client, mock_topic_service):
        """Test successful topic configuration update."""
        config_data = {
            'configs': {
                'retention.ms': '86400000'
            }
        }
        
        response = client.put(
            '/api/v1/clusters/test-cluster/topics/test-topic/config',
            data=json.dumps(config_data),
            content_type='application/json',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert data['topic_name'] == 'test-topic'
        
        mock_topic_service.update_topic_config.assert_called_once()
    
    def test_update_topic_config_missing_configs(self, client):
        """Test config update without configs."""
        response = client.put(
            '/api/v1/clusters/test-cluster/topics/test-topic/config',
            data=json.dumps({}),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'MISSING_CONFIGS'
    
    def test_delete_topic_success(self, client, mock_topic_service):
        """Test successful topic deletion."""
        response = client.delete(
            '/api/v1/clusters/test-cluster/topics/test-topic',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert data['topic_name'] == 'test-topic'
        
        mock_topic_service.delete_topic.assert_called_once()
    
    def test_purge_topic_success(self, client, mock_topic_service):
        """Test successful topic purging."""
        purge_data = {'retention_ms': 2000}
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics/test-topic/purge',
            data=json.dumps(purge_data),
            content_type='application/json',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert data['topic_name'] == 'test-topic'
        
        mock_topic_service.purge_topic.assert_called_once()
    
    def test_purge_topic_invalid_retention(self, client):
        """Test topic purging with invalid retention."""
        purge_data = {'retention_ms': -1}  # Invalid negative value
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics/test-topic/purge',
            data=json.dumps(purge_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'INVALID_RETENTION'
    
    def test_bulk_create_topics_success(self, client, mock_topic_service):
        """Test successful bulk topic creation."""
        bulk_data = {
            'operation': 'create',
            'topics': [
                {'name': 'topic1', 'partitions': 3},
                {'name': 'topic2', 'partitions': 6}
            ]
        }
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics/bulk',
            data=json.dumps(bulk_data),
            content_type='application/json',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['operation'] == 'create'
        assert data['total'] == 2
        assert data['successful'] == 2
        assert data['failed'] == 0
        
        mock_topic_service.bulk_create_topics.assert_called_once()
    
    def test_bulk_delete_topics_success(self, client, mock_topic_service):
        """Test successful bulk topic deletion."""
        bulk_data = {
            'operation': 'delete',
            'topic_names': ['topic1', 'topic2']
        }
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics/bulk',
            data=json.dumps(bulk_data),
            content_type='application/json',
            headers={'X-User-ID': 'test-user'}
        )
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['operation'] == 'delete'
        assert data['total'] == 2
        assert data['successful'] == 2
        
        mock_topic_service.bulk_delete_topics.assert_called_once()
    
    def test_bulk_operations_invalid_operation(self, client):
        """Test bulk operations with invalid operation."""
        bulk_data = {
            'operation': 'invalid',
            'topics': []
        }
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics/bulk',
            data=json.dumps(bulk_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'INVALID_OPERATION'
    
    def test_bulk_create_missing_topics(self, client):
        """Test bulk create without topics list."""
        bulk_data = {
            'operation': 'create',
            'topics': []
        }
        
        response = client.post(
            '/api/v1/clusters/test-cluster/topics/bulk',
            data=json.dumps(bulk_data),
            content_type='application/json'
        )
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'MISSING_TOPICS'
    
    def test_get_cluster_info_success(self, client, mock_topic_service):
        """Test successful cluster info retrieval."""
        response = client.get('/api/v1/clusters/test-cluster/info')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['cluster_id'] == 'test-cluster'
        assert data['info']['broker_count'] == 3
        
        mock_topic_service.get_cluster_info.assert_called_once()
    
    def test_get_cluster_info_not_available(self, client, mock_topic_service):
        """Test cluster info when cluster not available."""
        mock_topic_service.get_cluster_info.return_value = {}
        
        response = client.get('/api/v1/clusters/test-cluster/info')
        
        assert response.status_code == 404
        data = json.loads(response.data)
        assert data['error'] == 'CLUSTER_NOT_AVAILABLE'
    
    def test_invalid_cluster_id_validation(self, client):
        """Test cluster ID validation."""
        # Test with very short cluster ID
        response = client.get('/api/v1/clusters/x/topics')
        
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'INVALID_CLUSTER_ID'
    
    def test_user_id_extraction(self, client, mock_topic_service):
        """Test user ID extraction from headers."""
        # Test with X-User-ID header
        response = client.get(
            '/api/v1/clusters/test-cluster/topics',
            headers={'X-User-ID': 'user123'}
        )
        
        assert response.status_code == 200
        
        # Verify user_id was passed to service
        call_args = mock_topic_service.list_topics.call_args
        assert call_args[0][2] == 'user123'  # user_id parameter
    
    def test_error_handlers(self, client):
        """Test error handlers."""
        # Test 404
        response = client.get('/api/v1/nonexistent')
        assert response.status_code == 404
        data = json.loads(response.data)
        assert data['error'] == 'ENDPOINT_NOT_FOUND'
        
        # Test 405
        response = client.post('/api/v1/health')
        assert response.status_code == 405
        data = json.loads(response.data)
        assert data['error'] == 'METHOD_NOT_ALLOWED'