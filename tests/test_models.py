"""Tests for data models and validation."""

import pytest
from datetime import datetime
from pydantic import ValidationError

from kafka_ops_agent.models.cluster import (
    ClusterConfig, ConnectionInfo, ServiceInstance, ClusterStatus, RuntimeProvider
)
from kafka_ops_agent.models.topic import (
    TopicConfig, TopicCreateRequest, TopicUpdateRequest, CleanupPolicy, CompressionType
)
from kafka_ops_agent.models.service_broker import (
    ProvisionRequest, Service, ServicePlan, Catalog
)
from kafka_ops_agent.models.factory import (
    ClusterConfigFactory, ServiceInstanceFactory, TopicConfigFactory, ServiceBrokerFactory
)


class TestClusterConfig:
    """Test ClusterConfig model."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = ClusterConfig()
        
        assert config.cluster_size == 1
        assert config.replication_factor == 1
        assert config.partition_count == 3
        assert config.retention_hours == 168
        assert config.storage_size_gb == 10
        assert config.enable_ssl is False
        assert config.enable_sasl is False
        assert config.custom_properties == {}
    
    def test_valid_configuration(self):
        """Test valid configuration creation."""
        config = ClusterConfig(
            cluster_size=3,
            replication_factor=2,
            partition_count=6,
            retention_hours=72,
            storage_size_gb=50,
            enable_ssl=True,
            custom_properties={'auto.create.topics.enable': 'false'}
        )
        
        assert config.cluster_size == 3
        assert config.replication_factor == 2
        assert config.custom_properties['auto.create.topics.enable'] == 'false'
    
    def test_replication_factor_validation(self):
        """Test replication factor validation."""
        with pytest.raises(ValidationError) as exc_info:
            ClusterConfig(cluster_size=2, replication_factor=3)
        
        assert "replication_factor cannot exceed cluster_size" in str(exc_info.value)
    
    def test_cluster_size_bounds(self):
        """Test cluster size bounds validation."""
        with pytest.raises(ValidationError):
            ClusterConfig(cluster_size=0)
        
        with pytest.raises(ValidationError):
            ClusterConfig(cluster_size=11)
    
    def test_retention_hours_validation(self):
        """Test retention hours validation."""
        with pytest.raises(ValidationError):
            ClusterConfig(retention_hours=0)
        
        with pytest.raises(ValidationError):
            ClusterConfig(retention_hours=8761)  # More than 1 year


class TestTopicConfig:
    """Test TopicConfig model."""
    
    def test_default_values(self):
        """Test default topic configuration."""
        config = TopicConfig(name="test-topic")
        
        assert config.name == "test-topic"
        assert config.partitions == 3
        assert config.replication_factor == 1
        assert config.retention_ms == 604800000  # 7 days
        assert config.cleanup_policy == CleanupPolicy.DELETE
        assert config.compression_type == CompressionType.NONE
    
    def test_topic_name_validation(self):
        """Test topic name validation."""
        # Valid names
        TopicConfig(name="valid-topic")
        TopicConfig(name="valid_topic")
        TopicConfig(name="valid.topic")
        TopicConfig(name="ValidTopic123")
        
        # Invalid names
        with pytest.raises(ValidationError, match="Topic name cannot be empty"):
            TopicConfig(name="")
        
        with pytest.raises(ValidationError, match="Topic name cannot contain"):
            TopicConfig(name="invalid/topic")
        
        with pytest.raises(ValidationError, match="Topic name cannot contain"):
            TopicConfig(name="invalid topic")  # space
        
        with pytest.raises(ValidationError, match="Topic name cannot be"):
            TopicConfig(name=".")
        
        with pytest.raises(ValidationError, match="Topic name cannot be"):
            TopicConfig(name="..")
        
        with pytest.raises(ValidationError, match="Topic name cannot start with"):
            TopicConfig(name="__internal")
    
    def test_partition_bounds(self):
        """Test partition count bounds."""
        with pytest.raises(ValidationError):
            TopicConfig(name="test", partitions=0)
        
        with pytest.raises(ValidationError):
            TopicConfig(name="test", partitions=1001)
    
    def test_min_insync_replicas_validation(self):
        """Test min in-sync replicas validation."""
        with pytest.raises(ValidationError, match="min_insync_replicas cannot exceed replication_factor"):
            TopicConfig(name="test", replication_factor=2, min_insync_replicas=3)


class TestServiceBrokerModels:
    """Test Service Broker API models."""
    
    def test_provision_request_validation(self):
        """Test provision request validation."""
        # Valid request
        request = ProvisionRequest(
            service_id="kafka-service",
            plan_id="standard",
            organization_guid="org-123",
            space_guid="space-456",
            parameters={
                'cluster_size': 3,
                'replication_factor': 2,
                'retention_hours': 48
            }
        )
        
        assert request.parameters['cluster_size'] == 3
    
    def test_provision_request_parameter_validation(self):
        """Test provision request parameter validation."""
        # Invalid cluster size
        with pytest.raises(ValidationError, match="cluster_size must be an integer between 1 and 10"):
            ProvisionRequest(
                service_id="kafka-service",
                plan_id="standard",
                organization_guid="org-123",
                space_guid="space-456",
                parameters={'cluster_size': 0}
            )
        
        # Invalid replication factor
        with pytest.raises(ValidationError, match="replication_factor must be a positive integer"):
            ProvisionRequest(
                service_id="kafka-service",
                plan_id="standard",
                organization_guid="org-123",
                space_guid="space-456",
                parameters={'replication_factor': -1}
            )
    
    def test_service_plan_creation(self):
        """Test service plan creation."""
        plan = ServicePlan(
            id="basic",
            name="Basic Plan",
            description="Basic Kafka cluster",
            free=True,
            bindable=True
        )
        
        assert plan.id == "basic"
        assert plan.free is True
        assert plan.bindable is True


class TestTopicOperationModels:
    """Test topic operation models."""
    
    def test_topic_create_request(self):
        """Test topic creation request."""
        topic_config = TopicConfig(name="test-topic", partitions=6)
        request = TopicCreateRequest(
            cluster_id="cluster-123",
            topic_config=topic_config
        )
        
        assert request.cluster_id == "cluster-123"
        assert request.topic_config.name == "test-topic"
        assert request.topic_config.partitions == 6
    
    def test_topic_update_request_validation(self):
        """Test topic update request validation."""
        # Valid update
        request = TopicUpdateRequest(
            cluster_id="cluster-123",
            topic_name="test-topic",
            configs={'retention.ms': '86400000'}
        )
        
        assert request.configs['retention.ms'] == '86400000'
        
        # Invalid config
        with pytest.raises(ValidationError, match="Configuration .* is not updatable"):
            TopicUpdateRequest(
                cluster_id="cluster-123",
                topic_name="test-topic",
                configs={'invalid.config': 'value'}
            )
        
        # Empty configs
        with pytest.raises(ValidationError, match="At least one configuration must be provided"):
            TopicUpdateRequest(
                cluster_id="cluster-123",
                topic_name="test-topic",
                configs={}
            )


class TestFactories:
    """Test factory classes."""
    
    def test_cluster_config_factory(self):
        """Test ClusterConfigFactory."""
        # Default config
        config = ClusterConfigFactory.create_default()
        assert config.cluster_size == 1
        
        # Single node config
        config = ClusterConfigFactory.create_single_node()
        assert config.cluster_size == 1
        assert config.retention_hours == 24
        
        # Multi-node config
        config = ClusterConfigFactory.create_multi_node(5)
        assert config.cluster_size == 5
        assert config.replication_factor == 3
        
        # Production config
        config = ClusterConfigFactory.create_production()
        assert config.cluster_size == 5
        assert config.enable_ssl is True
        assert config.enable_sasl is True
    
    def test_service_instance_factory(self):
        """Test ServiceInstanceFactory."""
        # Default instance
        instance = ServiceInstanceFactory.create_default()
        assert instance.status == ClusterStatus.PENDING
        assert instance.runtime_provider == RuntimeProvider.DOCKER
        
        # Running instance
        instance = ServiceInstanceFactory.create_running()
        assert instance.status == ClusterStatus.RUNNING
        assert instance.connection_info is not None
        assert len(instance.connection_info.bootstrap_servers) > 0
    
    def test_topic_config_factory(self):
        """Test TopicConfigFactory."""
        # Default topic
        config = TopicConfigFactory.create_default("my-topic")
        assert config.name == "my-topic"
        assert config.partitions == 3
        
        # High throughput topic
        config = TopicConfigFactory.create_high_throughput("high-throughput")
        assert config.partitions == 12
        assert config.compression_type == CompressionType.LZ4
        
        # Compacted topic
        config = TopicConfigFactory.create_compacted("compacted")
        assert config.cleanup_policy == CleanupPolicy.COMPACT
        assert config.retention_ms == -1
        
        # Random topic
        config = TopicConfigFactory.create_random()
        assert config.name.startswith("topic-")
        assert 1 <= config.partitions <= 12
    
    def test_service_broker_factory(self):
        """Test ServiceBrokerFactory."""
        # Kafka service
        service = ServiceBrokerFactory.create_kafka_service()
        assert service.name == "Apache Kafka"
        assert len(service.plans) == 3
        assert "kafka" in service.tags
        
        # Catalog
        catalog = ServiceBrokerFactory.create_catalog()
        assert len(catalog.services) == 1
        assert catalog.services[0].name == "Apache Kafka"
        
        # Provision request
        request = ServiceBrokerFactory.create_provision_request("instance-123")
        assert request.service_id == "kafka-service"
        assert request.plan_id == "standard"