"""Open Service Broker API data models."""

from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional, List
from enum import Enum


class ServicePlanMetadata(BaseModel):
    """Service plan metadata."""
    displayName: Optional[str] = None
    bullets: Optional[List[str]] = None
    costs: Optional[List[Dict[str, Any]]] = None


class ServicePlan(BaseModel):
    """Service plan definition."""
    id: str = Field(..., description="Unique identifier for the service plan")
    name: str = Field(..., description="Human-readable name for the service plan")
    description: str = Field(..., description="Description of the service plan")
    free: bool = Field(default=True, description="Whether the plan is free")
    bindable: bool = Field(default=True, description="Whether the plan supports binding")
    metadata: Optional[ServicePlanMetadata] = None
    schemas: Optional[Dict[str, Any]] = None


class ServiceMetadata(BaseModel):
    """Service metadata."""
    displayName: Optional[str] = None
    imageUrl: Optional[str] = None
    longDescription: Optional[str] = None
    providerDisplayName: Optional[str] = None
    documentationUrl: Optional[str] = None
    supportUrl: Optional[str] = None


class Service(BaseModel):
    """Service definition for catalog."""
    id: str = Field(..., description="Unique identifier for the service")
    name: str = Field(..., description="Human-readable name for the service")
    description: str = Field(..., description="Description of the service")
    bindable: bool = Field(default=True, description="Whether the service supports binding")
    plan_updateable: bool = Field(default=False, description="Whether the service supports plan updates")
    plans: List[ServicePlan] = Field(..., description="List of service plans")
    tags: Optional[List[str]] = None
    metadata: Optional[ServiceMetadata] = None
    requires: Optional[List[str]] = None


class Catalog(BaseModel):
    """Service catalog response."""
    services: List[Service] = Field(..., description="List of available services")


class ProvisionRequest(BaseModel):
    """Service instance provisioning request."""
    service_id: str = Field(..., description="ID of the service being provisioned")
    plan_id: str = Field(..., description="ID of the plan being provisioned")
    context: Optional[Dict[str, Any]] = None
    organization_guid: str = Field(..., description="Organization GUID")
    space_guid: str = Field(..., description="Space GUID")
    parameters: Optional[Dict[str, Any]] = None
    
    @validator('parameters')
    def validate_parameters(cls, v):
        """Validate provisioning parameters."""
        if v is None:
            return {}
        
        # Validate cluster_size
        if 'cluster_size' in v:
            cluster_size = v['cluster_size']
            if not isinstance(cluster_size, int) or cluster_size < 1 or cluster_size > 10:
                raise ValueError("cluster_size must be an integer between 1 and 10")
        
        # Validate replication_factor
        if 'replication_factor' in v:
            replication_factor = v['replication_factor']
            if not isinstance(replication_factor, int) or replication_factor < 1:
                raise ValueError("replication_factor must be a positive integer")
        
        # Validate retention_hours
        if 'retention_hours' in v:
            retention_hours = v['retention_hours']
            if not isinstance(retention_hours, int) or retention_hours < 1:
                raise ValueError("retention_hours must be a positive integer")
        
        return v


class ProvisionResponse(BaseModel):
    """Service instance provisioning response."""
    dashboard_url: Optional[str] = None
    operation: Optional[str] = None


class UpdateRequest(BaseModel):
    """Service instance update request."""
    context: Optional[Dict[str, Any]] = None
    service_id: str = Field(..., description="ID of the service")
    plan_id: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    previous_values: Optional[Dict[str, Any]] = None


class UpdateResponse(BaseModel):
    """Service instance update response."""
    operation: Optional[str] = None


class DeprovisionResponse(BaseModel):
    """Service instance deprovisioning response."""
    operation: Optional[str] = None


class LastOperationResponse(BaseModel):
    """Last operation status response."""
    state: str = Field(..., description="State of the operation")
    description: Optional[str] = None
    
    @validator('state')
    def validate_state(cls, v):
        """Validate operation state."""
        valid_states = ['in progress', 'succeeded', 'failed']
        if v not in valid_states:
            raise ValueError(f"state must be one of {valid_states}")
        return v


class ErrorResponse(BaseModel):
    """Error response."""
    error: str = Field(..., description="Error code")
    description: str = Field(..., description="Error description")
    instance_usable: Optional[bool] = None
    update_repeatable: Optional[bool] = None