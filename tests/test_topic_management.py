"""Tests for topic management service."""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from kafka_ops_agent.services.topic_management import TopicManagementService
from kafka_ops_agent.models.topic import TopicConfig, TopicInfo, TopicDetails
from kafka_ops_agent.models.cluster import ClusterStatus, ServiceInstance
from kafka_ops_agent.models.factory import ServiceInstanceFactory


class TestTopicManagementService:
    """Test topic management service."""
    
    @pytest.fixture
    def mock_metadata_store(self):
        """Mock metadata store."""
        store = AsyncMock()
        # Mock running cluster
        instance = ServiceInstanceFactory.create_running("test-cluster")
        store.get_instance.return_value = instance
        return store
    
    @pytest.fixture
    def mock_audit_store(self):
        """Mock audit store."""
        return AsyncMock()
    
    @pytest.fixture
    def mock_client_manager(self):
        """Mock client manager."""
        manager = Mock()
        connection = Mock()
        manager.get_connection.return_value = connection
        return manager, connection
    
    @pytest.fixture
    def topic_service(self, mock_metadata_store, mock_audit_store, mock_client_manager):
        """Create topic management service with mocked dependencies."""
        manager, connection = mock_client_manager
        
        with patch('kafka_ops_agent.services.topic_management.get_client_manager', return_value=manager):
            service = TopicManagementService(mock_metadata_store, mock_audit_store)
            return service, connection
    
    @pytest.mark.asyncio
    async def test_create_topic_success(self, topic_service, mock_audit_store):
        """Test successful topic creation."""
        service, connection = topic_service
        
        # Mock admin operations
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.create_topic.return_value = True
            mock_admin_class.return_value = mock_admin
            
            topic_config = TopicConfig(name="test-topic", partitions=3)
            result = await service.create_topic("test-cluster", topic_config, "user-123")
            
            assert result.success is True
            assert result.topic_name == "test-topic"
            assert "created successfully" in result.message
            
            # Verify admin operations called
            mock_admin.create_topic.assert_called_once_with(topic_config)
            
            # Verify audit logging
            mock_audit_store.log_operation.assert_called_once()
            audit_call = mock_audit_store.log_operation.call_args
            assert audit_call[0][1] == "topic_create"  # operation
            assert audit_call[0][2] == "user-123"  # user_id
    
    @pytest.mark.asyncio
    async def test_create_topic_cluster_not_available(self, topic_service, mock_metadata_store):
        """Test topic creation when cluster is not available."""
        service, connection = topic_service
        
        # Mock cluster not running
        instance = ServiceInstanceFactory.create_default("test-cluster")
        instance.status = ClusterStatus.PENDING
        mock_metadata_store.get_instance.return_value = instance
        
        topic_config = TopicConfig(name="test-topic")
        result = await service.create_topic("test-cluster", topic_config)
        
        assert result.success is False
        assert result.error_code == "CLUSTER_NOT_AVAILABLE"
        assert "not available" in result.message
    
    @pytest.mark.asyncio
    async def test_create_topic_connection_failed(self, topic_service, mock_client_manager):
        """Test topic creation when connection fails."""
        service, connection = topic_service
        manager, _ = mock_client_manager
        
        # Mock connection failure
        manager.get_connection.return_value = None
        
        topic_config = TopicConfig(name="test-topic")
        result = await service.create_topic("test-cluster", topic_config)
        
        assert result.success is False
        assert result.error_code == "CONNECTION_FAILED"
    
    @pytest.mark.asyncio
    async def test_create_topic_creation_failed(self, topic_service, mock_audit_store):
        """Test topic creation when Kafka operation fails."""
        service, connection = topic_service
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.create_topic.return_value = False
            mock_admin_class.return_value = mock_admin
            
            topic_config = TopicConfig(name="test-topic")
            result = await service.create_topic("test-cluster", topic_config, "user-123")
            
            assert result.success is False
            assert result.error_code == "CREATION_FAILED"
            
            # Should still log audit event for failure
            mock_audit_store.log_operation.assert_called_once()
            audit_call = mock_audit_store.log_operation.call_args
            assert audit_call[0][1] == "topic_create_failed"
    
    @pytest.mark.asyncio
    async def test_update_topic_config_success(self, topic_service, mock_audit_store):
        """Test successful topic configuration update."""
        service, connection = topic_service
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.update_topic_config.return_value = True
            mock_admin_class.return_value = mock_admin
            
            configs = {'retention.ms': '86400000'}
            result = await service.update_topic_config("test-cluster", "test-topic", configs, "user-123")
            
            assert result.success is True
            assert result.topic_name == "test-topic"
            assert "configuration updated" in result.message
            
            mock_admin.update_topic_config.assert_called_once_with("test-topic", configs)
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_topic_success(self, topic_service, mock_audit_store):
        """Test successful topic deletion."""
        service, connection = topic_service
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.delete_topic.return_value = True
            mock_admin_class.return_value = mock_admin
            
            result = await service.delete_topic("test-cluster", "test-topic", "user-123")
            
            assert result.success is True
            assert result.topic_name == "test-topic"
            assert "deleted successfully" in result.message
            
            mock_admin.delete_topic.assert_called_once_with("test-topic")
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_purge_topic_success(self, topic_service, mock_audit_store):
        """Test successful topic purging."""
        service, connection = topic_service
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.purge_topic.return_value = True
            mock_admin_class.return_value = mock_admin
            
            result = await service.purge_topic("test-cluster", "test-topic", 2000, "user-123")
            
            assert result.success is True
            assert result.topic_name == "test-topic"
            assert "purged successfully" in result.message
            
            mock_admin.purge_topic.assert_called_once_with("test-topic", 2000)
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_topics_success(self, topic_service, mock_audit_store):
        """Test successful topic listing."""
        service, connection = topic_service
        
        mock_topics = [
            TopicInfo(name="topic1", partitions=3, replication_factor=1),
            TopicInfo(name="topic2", partitions=6, replication_factor=2)
        ]
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.list_topics.return_value = mock_topics
            mock_admin_class.return_value = mock_admin
            
            topics = await service.list_topics("test-cluster", include_internal=True, user_id="user-123")
            
            assert len(topics) == 2
            assert topics[0].name == "topic1"
            assert topics[1].name == "topic2"
            
            mock_admin.list_topics.assert_called_once_with(True)
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_list_topics_cluster_not_available(self, topic_service, mock_metadata_store):
        """Test topic listing when cluster is not available."""
        service, connection = topic_service
        
        # Mock cluster not running
        instance = ServiceInstanceFactory.create_default("test-cluster")
        instance.status = ClusterStatus.ERROR
        mock_metadata_store.get_instance.return_value = instance
        
        topics = await service.list_topics("test-cluster")
        
        assert topics == []
    
    @pytest.mark.asyncio
    async def test_describe_topic_success(self, topic_service, mock_audit_store):
        """Test successful topic description."""
        service, connection = topic_service
        
        mock_details = TopicDetails(
            name="test-topic",
            partitions=3,
            replication_factor=1,
            configs={'retention.ms': '604800000'},
            partition_details=[]
        )
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.describe_topic.return_value = mock_details
            mock_admin_class.return_value = mock_admin
            
            details = await service.describe_topic("test-cluster", "test-topic", "user-123")
            
            assert details is not None
            assert details.name == "test-topic"
            assert details.partitions == 3
            
            mock_admin.describe_topic.assert_called_once_with("test-topic")
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_create_topics_success(self, topic_service, mock_audit_store):
        """Test successful bulk topic creation."""
        service, connection = topic_service
        
        topic_configs = [
            TopicConfig(name="topic1", partitions=3),
            TopicConfig(name="topic2", partitions=6)
        ]
        
        with patch('kafka_ops_agent.services.topic_management.ConfluentKafkaAdminOperations') as mock_confluent_class:
            mock_confluent = Mock()
            mock_confluent.create_topics_batch.return_value = {
                "topic1": True,
                "topic2": True
            }
            mock_confluent_class.return_value = mock_confluent
            
            results = await service.bulk_create_topics("test-cluster", topic_configs, "user-123")
            
            assert len(results) == 2
            assert results["topic1"].success is True
            assert results["topic2"].success is True
            
            mock_confluent.create_topics_batch.assert_called_once_with(topic_configs)
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_bulk_create_topics_partial_failure(self, topic_service, mock_audit_store):
        """Test bulk topic creation with partial failures."""
        service, connection = topic_service
        
        topic_configs = [
            TopicConfig(name="topic1", partitions=3),
            TopicConfig(name="topic2", partitions=6)
        ]
        
        with patch('kafka_ops_agent.services.topic_management.ConfluentKafkaAdminOperations') as mock_confluent_class:
            mock_confluent = Mock()
            mock_confluent.create_topics_batch.return_value = {
                "topic1": True,
                "topic2": False
            }
            mock_confluent_class.return_value = mock_confluent
            
            results = await service.bulk_create_topics("test-cluster", topic_configs, "user-123")
            
            assert len(results) == 2
            assert results["topic1"].success is True
            assert results["topic2"].success is False
            assert results["topic2"].error_code == "CREATION_FAILED"
            
            # Verify audit log includes success/failure counts
            audit_call = mock_audit_store.log_operation.call_args
            audit_details = audit_call[0][3]  # details parameter
            assert audit_details["successful"] == 1
            assert audit_details["failed"] == 1
    
    @pytest.mark.asyncio
    async def test_bulk_delete_topics_success(self, topic_service, mock_audit_store):
        """Test successful bulk topic deletion."""
        service, connection = topic_service
        
        topic_names = ["topic1", "topic2"]
        
        with patch('kafka_ops_agent.services.topic_management.ConfluentKafkaAdminOperations') as mock_confluent_class:
            mock_confluent = Mock()
            mock_confluent.delete_topics_batch.return_value = {
                "topic1": True,
                "topic2": True
            }
            mock_confluent_class.return_value = mock_confluent
            
            results = await service.bulk_delete_topics("test-cluster", topic_names, "user-123")
            
            assert len(results) == 2
            assert results["topic1"].success is True
            assert results["topic2"].success is True
            
            mock_confluent.delete_topics_batch.assert_called_once_with(topic_names)
            mock_audit_store.log_operation.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cluster_info_success(self, topic_service):
        """Test getting cluster information."""
        service, connection = topic_service
        
        mock_info = {
            'cluster_id': 'test-cluster-id',
            'broker_count': 3,
            'topic_count': 10
        }
        
        with patch('kafka_ops_agent.services.topic_management.KafkaAdminOperations') as mock_admin_class:
            mock_admin = Mock()
            mock_admin.get_cluster_info.return_value = mock_info
            mock_admin_class.return_value = mock_admin
            
            info = await service.get_cluster_info("test-cluster")
            
            assert info == mock_info
            assert info['broker_count'] == 3
            mock_admin.get_cluster_info.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cluster_info_no_connection(self, topic_service, mock_client_manager):
        """Test getting cluster info when connection fails."""
        service, connection = topic_service
        manager, _ = mock_client_manager
        
        # Mock connection failure
        manager.get_connection.return_value = None
        
        info = await service.get_cluster_info("test-cluster")
        
        assert info == {}
    
    def test_close(self, topic_service):
        """Test closing the service."""
        service, connection = topic_service
        
        # Mock executor
        service.executor = Mock()
        
        service.close()
        
        service.executor.shutdown.assert_called_once_with(wait=True)