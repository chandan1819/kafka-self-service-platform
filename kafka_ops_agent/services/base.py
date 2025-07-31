"""Abstract base classes for services."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum


class OperationResult(Enum):
    """Result of service operations."""
    SUCCESS = "success"
    FAILURE = "failure"
    PENDING = "pending"


@dataclass
class ServiceResult:
    """Generic service operation result."""
    result: OperationResult
    message: str
    data: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None


class MetadataStore(ABC):
    """Abstract interface for metadata storage."""
    
    @abstractmethod
    def create_instance(self, instance_id: str, data: Dict[str, Any]) -> bool:
        """Create a new service instance record."""
        pass
    
    @abstractmethod
    def get_instance(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve service instance data."""
        pass
    
    @abstractmethod
    def update_instance(self, instance_id: str, data: Dict[str, Any]) -> bool:
        """Update service instance data."""
        pass
    
    @abstractmethod
    def delete_instance(self, instance_id: str) -> bool:
        """Delete service instance record."""
        pass
    
    @abstractmethod
    def list_instances(self) -> List[Dict[str, Any]]:
        """List all service instances."""
        pass