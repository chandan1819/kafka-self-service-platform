"""Abstract base classes for runtime providers."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class ProvisioningStatus(Enum):
    """Status of provisioning operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class ProvisioningResult:
    """Result of a provisioning operation."""
    status: ProvisioningStatus
    instance_id: str
    connection_info: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    operation_id: Optional[str] = None


@dataclass
class DeprovisioningResult:
    """Result of a deprovisioning operation."""
    status: ProvisioningStatus
    instance_id: str
    error_message: Optional[str] = None
    operation_id: Optional[str] = None


class RuntimeProvider(ABC):
    """Abstract base class for runtime providers."""
    
    @abstractmethod
    def provision_cluster(
        self, 
        instance_id: str, 
        config: Dict[str, Any]
    ) -> ProvisioningResult:
        """Provision a new Kafka cluster."""
        pass
    
    @abstractmethod
    def deprovision_cluster(self, instance_id: str) -> DeprovisioningResult:
        """Deprovision an existing Kafka cluster."""
        pass
    
    @abstractmethod
    def get_cluster_status(self, instance_id: str) -> ProvisioningStatus:
        """Get the current status of a cluster."""
        pass
    
    @abstractmethod
    def get_connection_info(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get connection information for a cluster."""
        pass
    
    @abstractmethod
    def health_check(self, instance_id: str) -> bool:
        """Check if a cluster is healthy and accessible."""
        pass