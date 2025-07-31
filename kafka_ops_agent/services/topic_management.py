"""Topic management service for Kafka clusters."""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor

from kafka_ops_agent.clients.kafka_client import get_client_manager
from kafka_ops_agent.clients.admin_operations import KafkaAdminOperations, ConfluentKafkaAdminOperations
from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.models.topic import (
    TopicConfig, TopicInfo, TopicDetails, TopicOperationResult,
    TopicCreateRequest, TopicUpdateRequest, TopicDeleteRequest,
    TopicPurgeRequest, TopicListRequest, BulkTopicOperation
)
from kafka_ops_agent.models.cluster import ClusterStatus
from kafka_ops_agent.config import config
from kafka_ops_agent.exceptions import (
    TopicNotFoundError, TopicAlreadyExistsError, KafkaConnectionError,
    ValidationError, KafkaOpsError, ErrorCode
)
from kafka_ops_agent.utils.retry import retry_kafka_operation, kafka_circuit_breaker
from kafka_ops_agent.utils.error_handlers import log_error_context

logger = logging.getLogger(__name__)


class TopicManagementService:
    """Service for managing Kafka topics across clusters."""
    
    def __init__(self, metadata_store: MetadataStore, audit_store: AuditStore):
        """Initialize topic management service."""
        self.metadata_store = metadata_store
        self.audit_store = audit_store
        self.client_manager = get_client_manager()
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="topic-mgmt")
        
        logger.info("Topic management service initialized")
    
    async def create_topic(
        self, 
        cluster_id: str, 
        topic_config: TopicConfig, 
        user_id: Optional[str] = None
    ) -> TopicOperationResult:
        """Create a new Kafka topic."""
        
        logger.info(f"Creating topic {topic_config.name} in cluster {cluster_id}")
        
        try:
            # Validate cluster exists and is running
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                return TopicOperationResult(
                    success=False,
                    message=f"Cluster {cluster_id} is not available or not running",
                    topic_name=topic_config.name,
                    error_code="CLUSTER_NOT_AVAILABLE"
                )
            
            # Get client connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to get connection to cluster {cluster_id}",
                    topic_name=topic_config.name,
                    error_code="CONNECTION_FAILED"
                )
            
            # Perform topic creation
            admin_ops = KafkaAdminOperations(connection)
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                self.executor,
                admin_ops.create_topic,
                topic_config
            )
            
            if success:
                # Log audit event
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_create",
                    user_id,
                    {
                        "topic_name": topic_config.name,
                        "partitions": topic_config.partitions,
                        "replication_factor": topic_config.replication_factor,
                        "retention_ms": topic_config.retention_ms
                    }
                )
                
                logger.info(f"Successfully created topic {topic_config.name} in cluster {cluster_id}")
                
                return TopicOperationResult(
                    success=True,
                    message=f"Topic {topic_config.name} created successfully",
                    topic_name=topic_config.name,
                    details={
                        "partitions": topic_config.partitions,
                        "replication_factor": topic_config.replication_factor
                    }
                )
            else:
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_create_failed",
                    user_id,
                    {"topic_name": topic_config.name, "error": "Creation failed"}
                )
                
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to create topic {topic_config.name}",
                    topic_name=topic_config.name,
                    error_code="CREATION_FAILED"
                )
                
        except Exception as e:
            logger.error(f"Exception creating topic {topic_config.name}: {e}")
            
            await self.audit_store.log_operation(
                cluster_id,
                "topic_create_exception",
                user_id,
                {"topic_name": topic_config.name, "error": str(e)}
            )
            
            return TopicOperationResult(
                success=False,
                message=f"Exception creating topic: {str(e)}",
                topic_name=topic_config.name,
                error_code="EXCEPTION"
            )
    
    async def update_topic_config(
        self,
        cluster_id: str,
        topic_name: str,
        configs: Dict[str, str],
        user_id: Optional[str] = None
    ) -> TopicOperationResult:
        """Update topic configuration."""
        
        logger.info(f"Updating config for topic {topic_name} in cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                return TopicOperationResult(
                    success=False,
                    message=f"Cluster {cluster_id} is not available",
                    topic_name=topic_name,
                    error_code="CLUSTER_NOT_AVAILABLE"
                )
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to get connection to cluster {cluster_id}",
                    topic_name=topic_name,
                    error_code="CONNECTION_FAILED"
                )
            
            # Update configuration
            admin_ops = KafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                self.executor,
                admin_ops.update_topic_config,
                topic_name,
                configs
            )
            
            if success:
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_config_update",
                    user_id,
                    {"topic_name": topic_name, "configs": configs}
                )
                
                logger.info(f"Successfully updated config for topic {topic_name}")
                
                return TopicOperationResult(
                    success=True,
                    message=f"Topic {topic_name} configuration updated",
                    topic_name=topic_name,
                    details={"updated_configs": configs}
                )
            else:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to update configuration for topic {topic_name}",
                    topic_name=topic_name,
                    error_code="UPDATE_FAILED"
                )
                
        except Exception as e:
            logger.error(f"Exception updating topic config {topic_name}: {e}")
            
            return TopicOperationResult(
                success=False,
                message=f"Exception updating topic config: {str(e)}",
                topic_name=topic_name,
                error_code="EXCEPTION"
            )
    
    async def delete_topic(
        self,
        cluster_id: str,
        topic_name: str,
        user_id: Optional[str] = None
    ) -> TopicOperationResult:
        """Delete a Kafka topic."""
        
        logger.info(f"Deleting topic {topic_name} from cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                return TopicOperationResult(
                    success=False,
                    message=f"Cluster {cluster_id} is not available",
                    topic_name=topic_name,
                    error_code="CLUSTER_NOT_AVAILABLE"
                )
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to get connection to cluster {cluster_id}",
                    topic_name=topic_name,
                    error_code="CONNECTION_FAILED"
                )
            
            # Delete topic
            admin_ops = KafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                self.executor,
                admin_ops.delete_topic,
                topic_name
            )
            
            if success:
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_delete",
                    user_id,
                    {"topic_name": topic_name}
                )
                
                logger.info(f"Successfully deleted topic {topic_name}")
                
                return TopicOperationResult(
                    success=True,
                    message=f"Topic {topic_name} deleted successfully",
                    topic_name=topic_name
                )
            else:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to delete topic {topic_name}",
                    topic_name=topic_name,
                    error_code="DELETION_FAILED"
                )
                
        except Exception as e:
            logger.error(f"Exception deleting topic {topic_name}: {e}")
            
            return TopicOperationResult(
                success=False,
                message=f"Exception deleting topic: {str(e)}",
                topic_name=topic_name,
                error_code="EXCEPTION"
            )
    
    async def purge_topic(
        self,
        cluster_id: str,
        topic_name: str,
        retention_ms: int = 1000,
        user_id: Optional[str] = None
    ) -> TopicOperationResult:
        """Purge messages from a topic."""
        
        logger.info(f"Purging topic {topic_name} in cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                return TopicOperationResult(
                    success=False,
                    message=f"Cluster {cluster_id} is not available",
                    topic_name=topic_name,
                    error_code="CLUSTER_NOT_AVAILABLE"
                )
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to get connection to cluster {cluster_id}",
                    topic_name=topic_name,
                    error_code="CONNECTION_FAILED"
                )
            
            # Purge topic
            admin_ops = KafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                self.executor,
                admin_ops.purge_topic,
                topic_name,
                retention_ms
            )
            
            if success:
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_purge",
                    user_id,
                    {"topic_name": topic_name, "retention_ms": retention_ms}
                )
                
                logger.info(f"Successfully purged topic {topic_name}")
                
                return TopicOperationResult(
                    success=True,
                    message=f"Topic {topic_name} purged successfully",
                    topic_name=topic_name,
                    details={"retention_ms": retention_ms}
                )
            else:
                return TopicOperationResult(
                    success=False,
                    message=f"Failed to purge topic {topic_name}",
                    topic_name=topic_name,
                    error_code="PURGE_FAILED"
                )
                
        except Exception as e:
            logger.error(f"Exception purging topic {topic_name}: {e}")
            
            return TopicOperationResult(
                success=False,
                message=f"Exception purging topic: {str(e)}",
                topic_name=topic_name,
                error_code="EXCEPTION"
            )
    
    async def list_topics(
        self,
        cluster_id: str,
        include_internal: bool = False,
        user_id: Optional[str] = None
    ) -> List[TopicInfo]:
        """List topics in a cluster."""
        
        logger.debug(f"Listing topics in cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                logger.warning(f"Cluster {cluster_id} is not available for topic listing")
                return []
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                logger.error(f"Failed to get connection to cluster {cluster_id}")
                return []
            
            # List topics
            admin_ops = KafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            topics = await loop.run_in_executor(
                self.executor,
                admin_ops.list_topics,
                include_internal
            )
            
            # Log audit event (optional for read operations)
            if user_id:
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_list",
                    user_id,
                    {"include_internal": include_internal, "topic_count": len(topics)}
                )
            
            logger.debug(f"Listed {len(topics)} topics in cluster {cluster_id}")
            return topics
            
        except Exception as e:
            logger.error(f"Exception listing topics in cluster {cluster_id}: {e}")
            return []
    
    async def describe_topic(
        self,
        cluster_id: str,
        topic_name: str,
        user_id: Optional[str] = None
    ) -> Optional[TopicDetails]:
        """Get detailed information about a topic."""
        
        logger.debug(f"Describing topic {topic_name} in cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                logger.warning(f"Cluster {cluster_id} is not available")
                return None
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                logger.error(f"Failed to get connection to cluster {cluster_id}")
                return None
            
            # Describe topic
            admin_ops = KafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            topic_details = await loop.run_in_executor(
                self.executor,
                admin_ops.describe_topic,
                topic_name
            )
            
            # Log audit event (optional for read operations)
            if user_id and topic_details:
                await self.audit_store.log_operation(
                    cluster_id,
                    "topic_describe",
                    user_id,
                    {"topic_name": topic_name}
                )
            
            return topic_details
            
        except Exception as e:
            logger.error(f"Exception describing topic {topic_name}: {e}")
            return None
    
    async def bulk_create_topics(
        self,
        cluster_id: str,
        topic_configs: List[TopicConfig],
        user_id: Optional[str] = None
    ) -> Dict[str, TopicOperationResult]:
        """Create multiple topics in batch."""
        
        logger.info(f"Bulk creating {len(topic_configs)} topics in cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                error_result = TopicOperationResult(
                    success=False,
                    message=f"Cluster {cluster_id} is not available",
                    error_code="CLUSTER_NOT_AVAILABLE"
                )
                return {config.name: error_result for config in topic_configs}
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                error_result = TopicOperationResult(
                    success=False,
                    message=f"Failed to get connection to cluster {cluster_id}",
                    error_code="CONNECTION_FAILED"
                )
                return {config.name: error_result for config in topic_configs}
            
            # Use Confluent client for batch operations
            confluent_ops = ConfluentKafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                self.executor,
                confluent_ops.create_topics_batch,
                topic_configs
            )
            
            # Convert to TopicOperationResult format
            operation_results = {}
            for topic_name, success in results.items():
                if success:
                    operation_results[topic_name] = TopicOperationResult(
                        success=True,
                        message=f"Topic {topic_name} created successfully",
                        topic_name=topic_name
                    )
                else:
                    operation_results[topic_name] = TopicOperationResult(
                        success=False,
                        message=f"Failed to create topic {topic_name}",
                        topic_name=topic_name,
                        error_code="CREATION_FAILED"
                    )
            
            # Log audit events
            successful_topics = [name for name, result in operation_results.items() if result.success]
            failed_topics = [name for name, result in operation_results.items() if not result.success]
            
            await self.audit_store.log_operation(
                cluster_id,
                "topic_bulk_create",
                user_id,
                {
                    "total_topics": len(topic_configs),
                    "successful": len(successful_topics),
                    "failed": len(failed_topics),
                    "successful_topics": successful_topics,
                    "failed_topics": failed_topics
                }
            )
            
            logger.info(f"Bulk create completed: {len(successful_topics)} successful, {len(failed_topics)} failed")
            return operation_results
            
        except Exception as e:
            logger.error(f"Exception in bulk topic creation: {e}")
            
            error_result = TopicOperationResult(
                success=False,
                message=f"Exception in bulk creation: {str(e)}",
                error_code="EXCEPTION"
            )
            return {config.name: error_result for config in topic_configs}
    
    async def bulk_delete_topics(
        self,
        cluster_id: str,
        topic_names: List[str],
        user_id: Optional[str] = None
    ) -> Dict[str, TopicOperationResult]:
        """Delete multiple topics in batch."""
        
        logger.info(f"Bulk deleting {len(topic_names)} topics in cluster {cluster_id}")
        
        try:
            # Validate cluster
            cluster_status = await self._get_cluster_status(cluster_id)
            if not cluster_status or cluster_status != ClusterStatus.RUNNING:
                error_result = TopicOperationResult(
                    success=False,
                    message=f"Cluster {cluster_id} is not available",
                    error_code="CLUSTER_NOT_AVAILABLE"
                )
                return {name: error_result for name in topic_names}
            
            # Get connection
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                error_result = TopicOperationResult(
                    success=False,
                    message=f"Failed to get connection to cluster {cluster_id}",
                    error_code="CONNECTION_FAILED"
                )
                return {name: error_result for name in topic_names}
            
            # Use Confluent client for batch operations
            confluent_ops = ConfluentKafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                self.executor,
                confluent_ops.delete_topics_batch,
                topic_names
            )
            
            # Convert to TopicOperationResult format
            operation_results = {}
            for topic_name, success in results.items():
                if success:
                    operation_results[topic_name] = TopicOperationResult(
                        success=True,
                        message=f"Topic {topic_name} deleted successfully",
                        topic_name=topic_name
                    )
                else:
                    operation_results[topic_name] = TopicOperationResult(
                        success=False,
                        message=f"Failed to delete topic {topic_name}",
                        topic_name=topic_name,
                        error_code="DELETION_FAILED"
                    )
            
            # Log audit events
            successful_topics = [name for name, result in operation_results.items() if result.success]
            failed_topics = [name for name, result in operation_results.items() if not result.success]
            
            await self.audit_store.log_operation(
                cluster_id,
                "topic_bulk_delete",
                user_id,
                {
                    "total_topics": len(topic_names),
                    "successful": len(successful_topics),
                    "failed": len(failed_topics),
                    "successful_topics": successful_topics,
                    "failed_topics": failed_topics
                }
            )
            
            logger.info(f"Bulk delete completed: {len(successful_topics)} successful, {len(failed_topics)} failed")
            return operation_results
            
        except Exception as e:
            logger.error(f"Exception in bulk topic deletion: {e}")
            
            error_result = TopicOperationResult(
                success=False,
                message=f"Exception in bulk deletion: {str(e)}",
                error_code="EXCEPTION"
            )
            return {name: error_result for name in topic_names}
    
    async def get_cluster_info(self, cluster_id: str) -> Dict[str, Any]:
        """Get cluster information."""
        
        try:
            connection = self.client_manager.get_connection(cluster_id)
            if not connection:
                return {}
            
            admin_ops = KafkaAdminOperations(connection)
            
            loop = asyncio.get_event_loop()
            cluster_info = await loop.run_in_executor(
                self.executor,
                admin_ops.get_cluster_info
            )
            
            return cluster_info
            
        except Exception as e:
            logger.error(f"Exception getting cluster info for {cluster_id}: {e}")
            return {}
    
    async def _get_cluster_status(self, cluster_id: str) -> Optional[ClusterStatus]:
        """Get cluster status from metadata store."""
        try:
            instance = await self.metadata_store.get_instance(cluster_id)
            return instance.status if instance else None
        except Exception as e:
            logger.error(f"Failed to get cluster status for {cluster_id}: {e}")
            return None
    
    def close(self):
        """Close the topic management service."""
        logger.info("Shutting down topic management service")
        self.executor.shutdown(wait=True)
        logger.info("Topic management service shutdown complete")


# Global service instance
_topic_service: Optional[TopicManagementService] = None


async def get_topic_service() -> TopicManagementService:
    """Get or create global topic management service instance."""
    global _topic_service
    if _topic_service is None:
        from kafka_ops_agent.storage.factory import get_metadata_store, get_audit_store
        metadata_store = await get_metadata_store()
        audit_store = await get_audit_store()
        _topic_service = TopicManagementService(metadata_store, audit_store)
    return _topic_service


def close_topic_service():
    """Close global topic management service."""
    global _topic_service
    if _topic_service:
        _topic_service.close()
        _topic_service = None