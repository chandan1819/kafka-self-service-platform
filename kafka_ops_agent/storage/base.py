"""Abstract base classes for metadata storage."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from kafka_ops_agent.models.cluster import ServiceInstance


class MetadataStore(ABC):
    """Abstract interface for metadata storage."""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the storage backend."""
        pass
    
    @abstractmethod
    async def create_instance(self, instance: ServiceInstance) -> bool:
        """Create a new service instance record."""
        pass
    
    @abstractmethod
    async def get_instance(self, instance_id: str) -> Optional[ServiceInstance]:
        """Retrieve service instance by ID."""
        pass
    
    @abstractmethod
    async def update_instance(self, instance: ServiceInstance) -> bool:
        """Update service instance data."""
        pass
    
    @abstractmethod
    async def delete_instance(self, instance_id: str) -> bool:
        """Delete service instance record."""
        pass
    
    @abstractmethod
    async def list_instances(self, filters: Optional[Dict[str, Any]] = None) -> List[ServiceInstance]:
        """List all service instances with optional filters."""
        pass
    
    @abstractmethod
    async def instance_exists(self, instance_id: str) -> bool:
        """Check if instance exists."""
        pass
    
    @abstractmethod
    async def get_instances_by_status(self, status: str) -> List[ServiceInstance]:
        """Get instances by status."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close storage connections."""
        pass


class AuditStore(ABC):
    """Abstract interface for audit logging storage."""
    
    @abstractmethod
    async def log_operation(
        self, 
        instance_id: str, 
        operation: str, 
        user_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an operation for audit purposes."""
        pass
    
    @abstractmethod
    async def get_audit_logs(
        self, 
        instance_id: Optional[str] = None,
        operation: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Retrieve audit logs with optional filters."""
        pass