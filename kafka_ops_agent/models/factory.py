"""Factory classes for creating test data and model instances."""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import uuid
import random

from kafka_ops_agent.models.cluster import (
    ClusterConfig, ConnectionInfo, ServiceInstance, ClusterStatus, 
    RuntimeProvider, ClusterHealth, ClusterMetrics, SSLConfig, SASLConfig
)
from kafka_ops_agent.models.topic import (
    TopicConfig, TopicInfo, TopicDetails, CleanupPolicy, CompressionType
)
from kafka_ops_agent.models.service_broker import (
    Service, ServicePlan, Catalog, ProvisionRequest, ProvisionResponse
)


class ClusterConfigFactory:
    """Factory for creating ClusterConfig instances."""
    
    @staticmethod
    def create_default() -> ClusterConfig:
        """Create a default cluster configuration."""
        return ClusterConfig()
    
    @staticmethod
    def create_single_node() -> ClusterConfig:
        """Create a single-node cluster configuration."""
        return ClusterConfig(
            cluster_size=1,
            replication_factor=1,
            partition_count=3,
            retention_hours=24,
            storage_size_gb=5
        )
    
    @staticmethod
    def create_multi_node(cluster_size: int = 3) -> ClusterConfig:
        """Create a multi-node cluster configuration."""
        return ClusterConfig(
            cluster_size=cluster_size,
            replication_factor=min(3, cluster_size),
            partition_count=6,
            retention_hours=168,
            storage_size_gb=20
        )
    
    @staticmethod
    def create_production() -> ClusterConfig:
        """Create a production-ready cluster configuration."""
        return ClusterConfig(
            cluster_size=5,
            replication_factor=3,
            partition_count=12,
            retention_hours=720,  # 30 days
            storage_size_gb=100,
            enable_ssl=True,
            enable_sasl=True,
            custom_properties={
                'auto.create.topics.enable': 'false',
                'delete.topic.enable': 'true',
                'log.retention.check.interval.ms': '300000'
            }
        )
    
    @staticmethod
    def create_from_dict(data: Dict[str, Any]) -> ClusterConfig:
        """Create cluster config from dictionary."""
        return ClusterConfig(**data)


class ServiceInstanceFactory:
    """Factory for creating ServiceInstance instances."""
    
    @staticmethod
    def create_default(instance_id: Optional[str] = None) -> ServiceInstance:
        """Create a default service instance."""
        return ServiceInstance(
            instance_id=instance_id or str(uuid.uuid4()),
            service_id="kafka-service",
            plan_id="standard",
            organization_guid=str(uuid.uuid4()),
            space_guid=str(uuid.uuid4()),
            status=ClusterStatus.PENDING
        )
    
    @staticmethod
    def create_running(instance_id: Optional[str] = None) -> ServiceInstance:
        """Create a running service instance with connection info."""
        instance = ServiceInstanceFactory.create_default(instance_id)
        instance.status = ClusterStatus.RUNNING
        instance.connection_info = ConnectionInfo(
            bootstrap_servers=['localhost:9092'],
            zookeeper_connect='localhost:2181'
        )
        return instance
    
    @staticmethod
    def create_with_config(config: ClusterConfig, instance_id: Optional[str] = None) -> ServiceInstance:
        """Create service instance with specific cluster config."""
        instance = ServiceInstanceFactory.create_default(instance_id)
        instance.parameters = config.dict()
        return instance


class TopicConfigFactory:
    """Factory for creating TopicConfig instances."""
    
    @staticmethod
    def create_default(name: str = "test-topic") -> TopicConfig:
        """Create a default topic configuration."""
        return TopicConfig(name=name)
    
    @staticmethod
    def create_high_throughput(name: str) -> TopicConfig:
        """Create a high-throughput topic configuration."""
        return TopicConfig(
            name=name,
            partitions=12,
            replication_factor=3,
            retention_ms=86400000,  # 1 day
            cleanup_policy=CleanupPolicy.DELETE,
            compression_type=CompressionType.LZ4,
            max_message_bytes=10485760  # 10MB
        )
    
    @staticmethod
    def create_compacted(name: str) -> TopicConfig:
        """Create a compacted topic configuration."""
        return TopicConfig(
            name=name,
            partitions=6,
            replication_factor=3,
            retention_ms=-1,  # Infinite retention
            cleanup_policy=CleanupPolicy.COMPACT,
            compression_type=CompressionType.SNAPPY,
            custom_configs={
                'segment.ms': '604800000',  # 7 days
                'min.cleanable.dirty.ratio': '0.1'
            }
        )
    
    @staticmethod
    def create_random(name: Optional[str] = None) -> TopicConfig:
        """Create a topic with random configuration."""
        return TopicConfig(
            name=name or f"topic-{random.randint(1000, 9999)}",
            partitions=random.randint(1, 12),
            replication_factor=random.randint(1, 3),
            retention_ms=random.randint(3600000, 604800000),  # 1 hour to 7 days
            cleanup_policy=random.choice(list(CleanupPolicy)),
            compression_type=random.choice(list(CompressionType))
        )


class ServiceBrokerFactory:
    """Factory for creating Service Broker API models."""
    
    @staticmethod
    def create_kafka_service() -> Service:
        """Create Kafka service definition."""
        return Service(
            id="kafka-service",
            name="Apache Kafka",
            description="Distributed streaming platform",
            bindable=True,
            plan_updateable=False,
            plans=[
                ServicePlan(
                    id="basic",
                    name="Basic",
                    description="Single-node Kafka cluster for development",
                    free=True
                ),
                ServicePlan(
                    id="standard",
                    name="Standard",
                    description="Multi-node Kafka cluster for production",
                    free=False
                ),
                ServicePlan(
                    id="premium",
                    name="Premium",
                    description="High-availability Kafka cluster with SSL/SASL",
                    free=False
                )
            ],
            tags=["kafka", "streaming", "messaging"]
        )
    
    @staticmethod
    def create_catalog() -> Catalog:
        """Create service catalog."""
        return Catalog(
            services=[ServiceBrokerFactory.create_kafka_service()]
        )
    
    @staticmethod
    def create_provision_request(
        instance_id: str,
        plan_id: str = "standard",
        parameters: Optional[Dict[str, Any]] = None
    ) -> ProvisionRequest:
        """Create provision request."""
        return ProvisionRequest(
            service_id="kafka-service",
            plan_id=plan_id,
            organization_guid=str(uuid.uuid4()),
            space_guid=str(uuid.uuid4()),
            parameters=parameters or {}
        )


class TestDataFactory:
    """Factory for creating test data."""
    
    @staticmethod
    def create_cluster_health(instance_id: str, healthy: bool = True) -> ClusterHealth:
        """Create cluster health data."""
        return ClusterHealth(
            instance_id=instance_id,
            status=ClusterStatus.RUNNING if healthy else ClusterStatus.ERROR,
            healthy=healthy,
            broker_count=3 if healthy else 1,
            zookeeper_healthy=healthy,
            details={
                'uptime_seconds': random.randint(3600, 86400) if healthy else 0,
                'last_error': None if healthy else 'Connection timeout'
            }
        )
    
    @staticmethod
    def create_cluster_metrics(instance_id: str) -> ClusterMetrics:
        """Create cluster metrics data."""
        return ClusterMetrics(
            instance_id=instance_id,
            broker_count=3,
            topic_count=random.randint(5, 50),
            partition_count=random.randint(15, 150),
            total_messages=random.randint(10000, 1000000),
            total_size_bytes=random.randint(1024*1024, 1024*1024*1024),  # 1MB to 1GB
            cpu_usage_percent=random.uniform(10.0, 80.0),
            memory_usage_percent=random.uniform(20.0, 90.0),
            disk_usage_percent=random.uniform(5.0, 70.0)
        )
    
    @staticmethod
    def create_topic_details(name: str) -> TopicDetails:
        """Create detailed topic information."""
        partitions = random.randint(1, 12)
        return TopicDetails(
            name=name,
            partitions=partitions,
            replication_factor=random.randint(1, 3),
            configs={
                'retention.ms': str(random.randint(3600000, 604800000)),
                'cleanup.policy': random.choice(['delete', 'compact']),
                'compression.type': random.choice(['none', 'gzip', 'snappy', 'lz4'])
            },
            partition_details=[
                {
                    'partition': i,
                    'leader': random.randint(0, 2),
                    'replicas': [random.randint(0, 2) for _ in range(3)],
                    'isr': [random.randint(0, 2) for _ in range(2)]
                }
                for i in range(partitions)
            ],
            total_messages=random.randint(1000, 100000),
            total_size_bytes=random.randint(1024*100, 1024*1024*10)  # 100KB to 10MB
        )