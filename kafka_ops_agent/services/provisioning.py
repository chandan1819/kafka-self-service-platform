"""Provisioning service for Kafka clusters."""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

from kafka_ops_agent.providers.base import RuntimeProvider, ProvisioningStatus, ProvisioningResult, DeprovisioningResult
from kafka_ops_agent.providers.docker_provider import DockerProvider
from kafka_ops_agent.providers.kubernetes_provider import KubernetesProvider
from kafka_ops_agent.providers.terraform_provider import TerraformProvider
from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.models.cluster import ServiceInstance, ClusterStatus, RuntimeProvider as RuntimeProviderEnum, ConnectionInfo
from kafka_ops_agent.models.factory import ClusterConfigFactory
from kafka_ops_agent.config import config

logger = logging.getLogger(__name__)


class ProvisioningService:
    """Service for provisioning and managing Kafka clusters."""
    
    def __init__(self, metadata_store: MetadataStore, audit_store: AuditStore):
        """Initialize provisioning service."""
        self.metadata_store = metadata_store
        self.audit_store = audit_store
        self.providers: Dict[str, RuntimeProvider] = {}
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialize runtime providers."""
        try:
            # Docker provider
            self.providers['docker'] = DockerProvider()
            logger.info("Docker provider initialized")
            
            # Kubernetes provider
            try:
                self.providers['kubernetes'] = KubernetesProvider()
                logger.info("Kubernetes provider initialized")
            except Exception as e:
                logger.warning(f"Kubernetes provider not available: {e}")
            
            # Terraform provider
            try:
                self.providers['terraform'] = TerraformProvider()
                logger.info("Terraform provider initialized")
            except Exception as e:
                logger.warning(f"Terraform provider not available: {e}")
            
        except Exception as e:
            logger.error(f"Failed to initialize providers: {e}")
            raise
    
    async def provision_cluster(
        self, 
        instance_id: str, 
        service_id: str,
        plan_id: str,
        organization_guid: str,
        space_guid: str,
        parameters: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> ProvisioningResult:
        """Provision a new Kafka cluster."""
        
        logger.info(f"Starting provisioning for instance {instance_id}")
        
        try:
            # Check if instance already exists
            if await self.metadata_store.instance_exists(instance_id):
                logger.warning(f"Instance {instance_id} already exists")
                return ProvisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message="Instance already exists"
                )
            
            # Determine runtime provider
            provider_name = parameters.get('runtime_provider', config.providers.default_provider)
            if provider_name not in self.providers:
                error_msg = f"Unsupported runtime provider: {provider_name}"
                logger.error(error_msg)
                return ProvisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message=error_msg
                )
            
            provider = self.providers[provider_name]
            
            # Create service instance record
            instance = ServiceInstance(
                instance_id=instance_id,
                service_id=service_id,
                plan_id=plan_id,
                organization_guid=organization_guid,
                space_guid=space_guid,
                parameters=parameters,
                status=ClusterStatus.PENDING,
                runtime_provider=RuntimeProviderEnum(provider_name),
                runtime_config={}
            )
            
            # Save initial instance
            if not await self.metadata_store.create_instance(instance):
                error_msg = "Failed to create instance record"
                logger.error(error_msg)
                return ProvisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message=error_msg
                )
            
            # Log audit event
            await self.audit_store.log_operation(
                instance_id, 
                "provision_start", 
                user_id,
                {"plan_id": plan_id, "provider": provider_name, "parameters": parameters}
            )
            
            # Update status to creating
            instance.status = ClusterStatus.CREATING
            await self.metadata_store.update_instance(instance)
            
            # Start provisioning (this could be async for long-running operations)
            try:
                result = await self._provision_with_provider(provider, instance_id, parameters)
                
                if result.status == ProvisioningStatus.SUCCEEDED:
                    # Update instance with connection info
                    instance.status = ClusterStatus.RUNNING
                    if result.connection_info:
                        instance.connection_info = ConnectionInfo(**result.connection_info)
                    
                    await self.metadata_store.update_instance(instance)
                    
                    # Log success
                    await self.audit_store.log_operation(
                        instance_id, 
                        "provision_success", 
                        user_id,
                        {"connection_info": result.connection_info}
                    )
                    
                    logger.info(f"Successfully provisioned cluster {instance_id}")
                    
                else:
                    # Update instance with error
                    instance.status = ClusterStatus.ERROR
                    instance.error_message = result.error_message
                    await self.metadata_store.update_instance(instance)
                    
                    # Log failure
                    await self.audit_store.log_operation(
                        instance_id, 
                        "provision_failed", 
                        user_id,
                        {"error": result.error_message}
                    )
                    
                    logger.error(f"Failed to provision cluster {instance_id}: {result.error_message}")
                
                return result
                
            except Exception as e:
                # Handle provisioning exception
                error_msg = f"Provisioning exception: {str(e)}"
                logger.error(error_msg)
                
                instance.status = ClusterStatus.ERROR
                instance.error_message = error_msg
                await self.metadata_store.update_instance(instance)
                
                await self.audit_store.log_operation(
                    instance_id, 
                    "provision_exception", 
                    user_id,
                    {"error": error_msg}
                )
                
                return ProvisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Service error during provisioning: {str(e)}"
            logger.error(error_msg)
            
            return ProvisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=error_msg
            )
    
    async def _provision_with_provider(
        self, 
        provider: RuntimeProvider, 
        instance_id: str, 
        parameters: Dict[str, Any]
    ) -> ProvisioningResult:
        """Provision cluster with specific provider."""
        
        # Convert parameters to cluster config
        cluster_config = self._parameters_to_config(parameters)
        
        # Call provider's provision method
        # Note: We need to handle the sync/async nature of providers
        if asyncio.iscoroutinefunction(provider.provision_cluster):
            return await provider.provision_cluster(instance_id, cluster_config)
        else:
            # Run sync provider in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, 
                provider.provision_cluster, 
                instance_id, 
                cluster_config
            )
    
    def _parameters_to_config(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Convert provision parameters to cluster configuration."""
        # Use factory to create base config, then override with parameters
        if parameters.get('plan_id') == 'basic':
            base_config = ClusterConfigFactory.create_single_node()
        elif parameters.get('plan_id') == 'premium':
            base_config = ClusterConfigFactory.create_production()
        else:
            base_config = ClusterConfigFactory.create_multi_node()
        
        # Override with provided parameters
        config_dict = base_config.dict()
        
        # Map common parameters
        param_mapping = {
            'cluster_size': 'cluster_size',
            'replication_factor': 'replication_factor',
            'partition_count': 'partition_count',
            'retention_hours': 'retention_hours',
            'storage_size_gb': 'storage_size_gb',
            'enable_ssl': 'enable_ssl',
            'enable_sasl': 'enable_sasl'
        }
        
        for param_key, config_key in param_mapping.items():
            if param_key in parameters:
                config_dict[config_key] = parameters[param_key]
        
        # Handle custom properties
        if 'custom_properties' in parameters:
            config_dict['custom_properties'].update(parameters['custom_properties'])
        
        return config_dict
    
    async def deprovision_cluster(
        self, 
        instance_id: str, 
        user_id: Optional[str] = None
    ) -> DeprovisioningResult:
        """Deprovision an existing Kafka cluster."""
        
        logger.info(f"Starting deprovisioning for instance {instance_id}")
        
        try:
            # Get instance
            instance = await self.metadata_store.get_instance(instance_id)
            if not instance:
                error_msg = f"Instance {instance_id} not found"
                logger.warning(error_msg)
                return DeprovisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message=error_msg
                )
            
            # Get provider
            provider_name = instance.runtime_provider.value
            if provider_name not in self.providers:
                error_msg = f"Provider {provider_name} not available"
                logger.error(error_msg)
                return DeprovisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message=error_msg
                )
            
            provider = self.providers[provider_name]
            
            # Log audit event
            await self.audit_store.log_operation(
                instance_id, 
                "deprovision_start", 
                user_id,
                {"provider": provider_name}
            )
            
            # Update status
            instance.status = ClusterStatus.STOPPING
            await self.metadata_store.update_instance(instance)
            
            # Deprovision with provider
            try:
                if asyncio.iscoroutinefunction(provider.deprovision_cluster):
                    result = await provider.deprovision_cluster(instance_id)
                else:
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(
                        None, 
                        provider.deprovision_cluster, 
                        instance_id
                    )
                
                if result.status == ProvisioningStatus.SUCCEEDED:
                    # Delete instance record
                    await self.metadata_store.delete_instance(instance_id)
                    
                    # Log success
                    await self.audit_store.log_operation(
                        instance_id, 
                        "deprovision_success", 
                        user_id
                    )
                    
                    logger.info(f"Successfully deprovisioned cluster {instance_id}")
                    
                else:
                    # Update with error but keep record
                    instance.status = ClusterStatus.ERROR
                    instance.error_message = result.error_message
                    await self.metadata_store.update_instance(instance)
                    
                    # Log failure
                    await self.audit_store.log_operation(
                        instance_id, 
                        "deprovision_failed", 
                        user_id,
                        {"error": result.error_message}
                    )
                    
                    logger.error(f"Failed to deprovision cluster {instance_id}: {result.error_message}")
                
                return result
                
            except Exception as e:
                error_msg = f"Deprovisioning exception: {str(e)}"
                logger.error(error_msg)
                
                instance.status = ClusterStatus.ERROR
                instance.error_message = error_msg
                await self.metadata_store.update_instance(instance)
                
                await self.audit_store.log_operation(
                    instance_id, 
                    "deprovision_exception", 
                    user_id,
                    {"error": error_msg}
                )
                
                return DeprovisioningResult(
                    status=ProvisioningStatus.FAILED,
                    instance_id=instance_id,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Service error during deprovisioning: {str(e)}"
            logger.error(error_msg)
            
            return DeprovisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=error_msg
            )
    
    async def get_cluster_status(self, instance_id: str) -> Optional[ClusterStatus]:
        """Get the current status of a cluster."""
        try:
            instance = await self.metadata_store.get_instance(instance_id)
            if not instance:
                return None
            
            # For running clusters, check with provider
            if instance.status == ClusterStatus.RUNNING:
                provider_name = instance.runtime_provider.value
                if provider_name in self.providers:
                    provider = self.providers[provider_name]
                    
                    try:
                        if asyncio.iscoroutinefunction(provider.get_cluster_status):
                            provider_status = await provider.get_cluster_status(instance_id)
                        else:
                            loop = asyncio.get_event_loop()
                            provider_status = await loop.run_in_executor(
                                None, 
                                provider.get_cluster_status, 
                                instance_id
                            )
                        
                        # Update instance status if different
                        if provider_status != ProvisioningStatus.SUCCEEDED:
                            if provider_status == ProvisioningStatus.FAILED:
                                instance.status = ClusterStatus.ERROR
                            else:
                                instance.status = ClusterStatus.CREATING
                            
                            await self.metadata_store.update_instance(instance)
                        
                    except Exception as e:
                        logger.warning(f"Failed to check provider status for {instance_id}: {e}")
            
            return instance.status
            
        except Exception as e:
            logger.error(f"Failed to get cluster status for {instance_id}: {e}")
            return None
    
    async def get_connection_info(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get connection information for a cluster."""
        try:
            instance = await self.metadata_store.get_instance(instance_id)
            if not instance or instance.status != ClusterStatus.RUNNING:
                return None
            
            # Try to get fresh connection info from provider
            provider_name = instance.runtime_provider.value
            if provider_name in self.providers:
                provider = self.providers[provider_name]
                
                try:
                    if asyncio.iscoroutinefunction(provider.get_connection_info):
                        connection_info = await provider.get_connection_info(instance_id)
                    else:
                        loop = asyncio.get_event_loop()
                        connection_info = await loop.run_in_executor(
                            None, 
                            provider.get_connection_info, 
                            instance_id
                        )
                    
                    if connection_info:
                        return connection_info
                        
                except Exception as e:
                    logger.warning(f"Failed to get provider connection info for {instance_id}: {e}")
            
            # Fall back to stored connection info
            if instance.connection_info:
                return instance.connection_info.dict()
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get connection info for {instance_id}: {e}")
            return None
    
    async def health_check(self, instance_id: str) -> bool:
        """Check if a cluster is healthy."""
        try:
            instance = await self.metadata_store.get_instance(instance_id)
            if not instance or instance.status != ClusterStatus.RUNNING:
                return False
            
            provider_name = instance.runtime_provider.value
            if provider_name not in self.providers:
                return False
            
            provider = self.providers[provider_name]
            
            if asyncio.iscoroutinefunction(provider.health_check):
                return await provider.health_check(instance_id)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None, 
                    provider.health_check, 
                    instance_id
                )
                
        except Exception as e:
            logger.error(f"Health check failed for {instance_id}: {e}")
            return False
    
    async def list_instances(
        self, 
        filters: Optional[Dict[str, Any]] = None
    ) -> List[ServiceInstance]:
        """List service instances with optional filters."""
        try:
            return await self.metadata_store.list_instances(filters)
        except Exception as e:
            logger.error(f"Failed to list instances: {e}")
            return []
    
    async def cleanup_failed_instances(self) -> int:
        """Clean up instances that failed during provisioning."""
        try:
            failed_instances = await self.metadata_store.get_instances_by_status('error')
            cleanup_count = 0
            
            for instance in failed_instances:
                try:
                    # Try to clean up resources with provider
                    provider_name = instance.runtime_provider.value
                    if provider_name in self.providers:
                        provider = self.providers[provider_name]
                        
                        if asyncio.iscoroutinefunction(provider.deprovision_cluster):
                            await provider.deprovision_cluster(instance.instance_id)
                        else:
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None, 
                                provider.deprovision_cluster, 
                                instance.instance_id
                            )
                    
                    # Delete instance record
                    await self.metadata_store.delete_instance(instance.instance_id)
                    cleanup_count += 1
                    
                    logger.info(f"Cleaned up failed instance {instance.instance_id}")
                    
                except Exception as e:
                    logger.warning(f"Failed to cleanup instance {instance.instance_id}: {e}")
            
            return cleanup_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup failed instances: {e}")
            return 0