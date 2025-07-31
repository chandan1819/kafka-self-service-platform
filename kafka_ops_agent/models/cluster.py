"""Cluster configuration models."""

from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional, List
from enum import Enum
from datetime import datetime


class ClusterStatus(str, Enum):
    """Cluster status enumeration."""
    PENDING = "pending"
    CREATING = "creating"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


class RuntimeProvider(str, Enum):
    """Supported runtime providers."""
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    TERRAFORM = "terraform"


class ClusterConfig(BaseModel):
    """Kafka cluster configuration."""
    cluster_size: int = Field(default=1, description="Number of Kafka brokers", ge=1, le=10)
    replication_factor: int = Field(default=1, description="Default replication factor", ge=1, le=10)
    partition_count: int = Field(default=3, description="Default partition count", ge=1, le=1000)
    retention_hours: int = Field(default=168, description="Default retention in hours", ge=1, le=8760)  # Max 1 year
    storage_size_gb: int = Field(default=10, description="Storage size per broker in GB", ge=1, le=1000)
    enable_ssl: bool = Field(default=False, description="Enable SSL encryption")
    enable_sasl: bool = Field(default=False, description="Enable SASL authentication")
    custom_properties: Dict[str, str] = Field(default_factory=dict, description="Custom Kafka properties")
    
    @validator('replication_factor')
    def validate_replication_factor(cls, v, values):
        """Validate replication factor doesn't exceed cluster size."""
        if 'cluster_size' in values and v > values['cluster_size']:
            raise ValueError("replication_factor cannot exceed cluster_size")
        return v


class SSLConfig(BaseModel):
    """SSL configuration for Kafka."""
    keystore_location: Optional[str] = None
    keystore_password: Optional[str] = None
    truststore_location: Optional[str] = None
    truststore_password: Optional[str] = None
    key_password: Optional[str] = None


class SASLConfig(BaseModel):
    """SASL configuration for Kafka."""
    mechanism: str = Field(default="PLAIN", description="SASL mechanism")
    username: Optional[str] = None
    password: Optional[str] = None
    
    @validator('mechanism')
    def validate_mechanism(cls, v):
        """Validate SASL mechanism."""
        valid_mechanisms = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI']
        if v not in valid_mechanisms:
            raise ValueError(f"mechanism must be one of {valid_mechanisms}")
        return v


class ConnectionInfo(BaseModel):
    """Kafka cluster connection information."""
    bootstrap_servers: List[str] = Field(..., description="List of bootstrap servers")
    zookeeper_connect: str = Field(..., description="Zookeeper connection string")
    admin_username: Optional[str] = Field(None, description="Admin username")
    admin_password: Optional[str] = Field(None, description="Admin password")
    ssl_config: Optional[SSLConfig] = Field(None, description="SSL configuration")
    sasl_config: Optional[SASLConfig] = Field(None, description="SASL configuration")


class ServiceInstance(BaseModel):
    """Service instance metadata."""
    instance_id: str = Field(..., description="Unique instance identifier")
    service_id: str = Field(..., description="Service identifier")
    plan_id: str = Field(..., description="Service plan identifier")
    organization_guid: str = Field(..., description="Organization GUID")
    space_guid: str = Field(..., description="Space GUID")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Provisioning parameters")
    status: ClusterStatus = Field(..., description="Current cluster status")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")
    connection_info: Optional[ConnectionInfo] = Field(None, description="Connection information")
    runtime_provider: RuntimeProvider = Field(default=RuntimeProvider.DOCKER, description="Runtime provider")
    runtime_config: Dict[str, Any] = Field(default_factory=dict, description="Provider-specific configuration")
    error_message: Optional[str] = Field(None, description="Error message if status is ERROR")


class ClusterHealth(BaseModel):
    """Cluster health information."""
    instance_id: str = Field(..., description="Instance identifier")
    status: ClusterStatus = Field(..., description="Overall cluster status")
    healthy: bool = Field(..., description="Whether cluster is healthy")
    broker_count: int = Field(..., description="Number of running brokers")
    zookeeper_healthy: bool = Field(..., description="Zookeeper health status")
    last_check: datetime = Field(default_factory=datetime.utcnow, description="Last health check timestamp")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional health details")


class ClusterMetrics(BaseModel):
    """Cluster metrics information."""
    instance_id: str = Field(..., description="Instance identifier")
    broker_count: int = Field(..., description="Number of brokers")
    topic_count: int = Field(..., description="Number of topics")
    partition_count: int = Field(..., description="Total number of partitions")
    total_messages: Optional[int] = Field(None, description="Total message count")
    total_size_bytes: Optional[int] = Field(None, description="Total data size in bytes")
    cpu_usage_percent: Optional[float] = Field(None, description="CPU usage percentage")
    memory_usage_percent: Optional[float] = Field(None, description="Memory usage percentage")
    disk_usage_percent: Optional[float] = Field(None, description="Disk usage percentage")
    collected_at: datetime = Field(default_factory=datetime.utcnow, description="Metrics collection timestamp")