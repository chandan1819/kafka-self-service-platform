"""Topic management data models."""

from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional, List
from enum import Enum


class CleanupPolicy(str, Enum):
    """Topic cleanup policy options."""
    DELETE = "delete"
    COMPACT = "compact"
    COMPACT_DELETE = "compact,delete"


class CompressionType(str, Enum):
    """Message compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class TopicConfig(BaseModel):
    """Kafka topic configuration."""
    name: str = Field(..., description="Topic name", min_length=1, max_length=249)
    partitions: int = Field(default=3, description="Number of partitions", ge=1, le=1000)
    replication_factor: int = Field(default=1, description="Replication factor", ge=1, le=10)
    retention_ms: int = Field(default=604800000, description="Message retention in milliseconds", ge=1)  # 7 days default
    cleanup_policy: CleanupPolicy = Field(default=CleanupPolicy.DELETE, description="Cleanup policy")
    compression_type: CompressionType = Field(default=CompressionType.NONE, description="Compression type")
    max_message_bytes: int = Field(default=1048576, description="Maximum message size in bytes", ge=1, le=104857600)  # 1MB default, 100MB max
    min_insync_replicas: int = Field(default=1, description="Minimum in-sync replicas", ge=1)
    custom_configs: Dict[str, str] = Field(default_factory=dict, description="Custom topic configurations")
    
    @validator('name')
    def validate_topic_name(cls, v):
        """Validate topic name according to Kafka rules."""
        if not v:
            raise ValueError("Topic name cannot be empty")
        
        # Kafka topic name restrictions
        invalid_chars = ['/', '\\', ',', '\u0000', ':', '"', "'", ';', '*', '?', ' ', '\t', '\r', '\n', '=']
        for char in invalid_chars:
            if char in v:
                raise ValueError(f"Topic name cannot contain '{char}'")
        
        if v in ['.', '..']:
            raise ValueError("Topic name cannot be '.' or '..'")
        
        if v.startswith('__'):
            raise ValueError("Topic name cannot start with '__' (reserved for internal topics)")
        
        return v
    
    @validator('replication_factor')
    def validate_replication_factor(cls, v, values):
        """Validate replication factor doesn't exceed cluster size."""
        # Note: This validation would need cluster size context in real implementation
        return v
    
    @validator('min_insync_replicas')
    def validate_min_insync_replicas(cls, v, values):
        """Validate min in-sync replicas doesn't exceed replication factor."""
        if 'replication_factor' in values and v > values['replication_factor']:
            raise ValueError("min_insync_replicas cannot exceed replication_factor")
        return v


class TopicInfo(BaseModel):
    """Topic information response."""
    name: str = Field(..., description="Topic name")
    partitions: int = Field(..., description="Number of partitions")
    replication_factor: int = Field(..., description="Replication factor")
    configs: Dict[str, str] = Field(default_factory=dict, description="Topic configurations")


class TopicDetails(BaseModel):
    """Detailed topic information."""
    name: str = Field(..., description="Topic name")
    partitions: int = Field(..., description="Number of partitions")
    replication_factor: int = Field(..., description="Replication factor")
    configs: Dict[str, str] = Field(default_factory=dict, description="Topic configurations")
    partition_details: List[Dict[str, Any]] = Field(default_factory=list, description="Partition details")
    total_messages: Optional[int] = Field(None, description="Total message count")
    total_size_bytes: Optional[int] = Field(None, description="Total size in bytes")


class TopicCreateRequest(BaseModel):
    """Topic creation request."""
    cluster_id: str = Field(..., description="Kafka cluster ID")
    topic_config: TopicConfig = Field(..., description="Topic configuration")


class TopicUpdateRequest(BaseModel):
    """Topic configuration update request."""
    cluster_id: str = Field(..., description="Kafka cluster ID")
    topic_name: str = Field(..., description="Topic name")
    configs: Dict[str, str] = Field(..., description="Configuration updates")
    
    @validator('configs')
    def validate_configs(cls, v):
        """Validate configuration updates."""
        if not v:
            raise ValueError("At least one configuration must be provided")
        
        # List of updatable configurations
        updatable_configs = {
            'retention.ms', 'retention.bytes', 'cleanup.policy',
            'compression.type', 'max.message.bytes', 'min.insync.replicas',
            'segment.ms', 'segment.bytes', 'delete.retention.ms'
        }
        
        for key in v.keys():
            if key not in updatable_configs:
                raise ValueError(f"Configuration '{key}' is not updatable")
        
        return v


class TopicDeleteRequest(BaseModel):
    """Topic deletion request."""
    cluster_id: str = Field(..., description="Kafka cluster ID")
    topic_name: str = Field(..., description="Topic name")


class TopicPurgeRequest(BaseModel):
    """Topic purge request."""
    cluster_id: str = Field(..., description="Kafka cluster ID")
    topic_name: str = Field(..., description="Topic name")
    retention_ms: int = Field(default=1000, description="Temporary retention for purge", ge=1, le=60000)  # 1 second to 1 minute


class TopicListRequest(BaseModel):
    """Topic list request."""
    cluster_id: str = Field(..., description="Kafka cluster ID")
    include_internal: bool = Field(default=False, description="Include internal topics")


class TopicOperationResult(BaseModel):
    """Result of topic operation."""
    success: bool = Field(..., description="Whether operation succeeded")
    message: str = Field(..., description="Result message")
    topic_name: Optional[str] = Field(None, description="Topic name")
    error_code: Optional[str] = Field(None, description="Error code if failed")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")


class BulkTopicOperation(BaseModel):
    """Bulk topic operation request."""
    cluster_id: str = Field(..., description="Kafka cluster ID")
    operation: str = Field(..., description="Operation type")
    topics: List[str] = Field(..., description="List of topic names", min_items=1)
    parameters: Optional[Dict[str, Any]] = Field(None, description="Operation parameters")
    
    @validator('operation')
    def validate_operation(cls, v):
        """Validate operation type."""
        valid_operations = ['delete', 'purge', 'update_config']
        if v not in valid_operations:
            raise ValueError(f"operation must be one of {valid_operations}")
        return v