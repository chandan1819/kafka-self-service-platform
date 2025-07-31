"""Kafka admin operations using client connections."""

import logging
from typing import Dict, Any, List, Optional, Set
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from confluent_kafka.admin import NewTopic as ConfluentNewTopic, ConfigResource as ConfluentConfigResource

from kafka_ops_agent.clients.kafka_client import KafkaClientConnection
from kafka_ops_agent.models.topic import TopicConfig, TopicInfo, TopicDetails

logger = logging.getLogger(__name__)


class KafkaAdminOperations:
    """High-level Kafka admin operations."""
    
    def __init__(self, connection: KafkaClientConnection):
        """Initialize admin operations with client connection."""
        self.connection = connection
    
    def create_topic(self, topic_config: TopicConfig) -> bool:
        """Create a new Kafka topic."""
        try:
            admin_client = self.connection.get_admin_client()
            
            # Create topic specification
            topic_spec = NewTopic(
                name=topic_config.name,
                num_partitions=topic_config.partitions,
                replication_factor=topic_config.replication_factor
            )
            
            # Create topic
            future_map = admin_client.create_topics([topic_spec])
            
            # Wait for creation to complete
            for topic_name, future in future_map.items():
                try:
                    future.result(timeout=30)  # 30 second timeout
                    logger.info(f"Successfully created topic {topic_name}")
                except TopicAlreadyExistsError:
                    logger.warning(f"Topic {topic_name} already exists")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    return False
            
            # Set topic configurations if provided
            if topic_config.custom_configs or self._has_non_default_configs(topic_config):
                return self._update_topic_configs(topic_config)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create topic {topic_config.name}: {e}")
            return False
    
    def delete_topic(self, topic_name: str) -> bool:
        """Delete a Kafka topic."""
        try:
            admin_client = self.connection.get_admin_client()
            
            # Delete topic
            future_map = admin_client.delete_topics([topic_name])
            
            # Wait for deletion to complete
            for name, future in future_map.items():
                try:
                    future.result(timeout=30)
                    logger.info(f"Successfully deleted topic {name}")
                    return True
                except UnknownTopicOrPartitionError:
                    logger.warning(f"Topic {name} does not exist")
                    return True
                except Exception as e:
                    logger.error(f"Failed to delete topic {name}: {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete topic {topic_name}: {e}")
            return False
    
    def list_topics(self, include_internal: bool = False) -> List[TopicInfo]:
        """List all topics in the cluster."""
        try:
            admin_client = self.connection.get_admin_client()
            
            # Get cluster metadata
            metadata = admin_client.describe_cluster()
            if not metadata:
                logger.error("Failed to get cluster metadata")
                return []
            
            topics = []
            for topic_name, topic_metadata in metadata.topics.items():
                # Skip internal topics if not requested
                if not include_internal and topic_name.startswith('__'):
                    continue
                
                topic_info = TopicInfo(
                    name=topic_name,
                    partitions=len(topic_metadata.partitions),
                    replication_factor=len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                    configs={}
                )
                topics.append(topic_info)
            
            logger.debug(f"Listed {len(topics)} topics")
            return topics
            
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    def describe_topic(self, topic_name: str) -> Optional[TopicDetails]:
        """Get detailed information about a topic."""
        try:
            admin_client = self.connection.get_admin_client()
            
            # Get topic metadata
            metadata = admin_client.describe_cluster()
            if not metadata or topic_name not in metadata.topics:
                logger.warning(f"Topic {topic_name} not found")
                return None
            
            topic_metadata = metadata.topics[topic_name]
            
            # Get topic configurations
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            config_future = admin_client.describe_configs([config_resource])
            
            configs = {}
            try:
                config_result = config_future[config_resource].result(timeout=10)
                configs = {entry.name: entry.value for entry in config_result.values()}
            except Exception as e:
                logger.warning(f"Failed to get topic configs for {topic_name}: {e}")
            
            # Build partition details
            partition_details = []
            for partition_id, partition_metadata in topic_metadata.partitions.items():
                partition_details.append({
                    'partition': partition_id,
                    'leader': partition_metadata.leader,
                    'replicas': list(partition_metadata.replicas),
                    'isr': list(partition_metadata.isr)
                })
            
            topic_details = TopicDetails(
                name=topic_name,
                partitions=len(topic_metadata.partitions),
                replication_factor=len(topic_metadata.partitions[0].replicas) if topic_metadata.partitions else 0,
                configs=configs,
                partition_details=partition_details
            )
            
            logger.debug(f"Described topic {topic_name}")
            return topic_details
            
        except Exception as e:
            logger.error(f"Failed to describe topic {topic_name}: {e}")
            return None
    
    def update_topic_config(self, topic_name: str, configs: Dict[str, str]) -> bool:
        """Update topic configuration."""
        try:
            admin_client = self.connection.get_admin_client()
            
            # Build config resource
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            
            # Update configurations
            future_map = admin_client.alter_configs({config_resource: configs})
            
            # Wait for update to complete
            for resource, future in future_map.items():
                try:
                    future.result(timeout=30)
                    logger.info(f"Successfully updated config for topic {topic_name}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to update config for topic {topic_name}: {e}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update topic config for {topic_name}: {e}")
            return False
    
    def purge_topic(self, topic_name: str, retention_ms: int = 1000) -> bool:
        """Purge topic by temporarily setting low retention."""
        try:
            logger.info(f"Starting purge for topic {topic_name}")
            
            # Get current retention setting
            current_configs = self._get_topic_configs(topic_name)
            if not current_configs:
                logger.error(f"Failed to get current configs for topic {topic_name}")
                return False
            
            original_retention = current_configs.get('retention.ms', '604800000')  # 7 days default
            
            # Set low retention
            if not self.update_topic_config(topic_name, {'retention.ms': str(retention_ms)}):
                return False
            
            # Wait for purge to take effect (retention_ms + some buffer)
            import time
            wait_time = max(retention_ms / 1000, 5)  # At least 5 seconds
            logger.info(f"Waiting {wait_time} seconds for purge to take effect")
            time.sleep(wait_time)
            
            # Restore original retention
            if not self.update_topic_config(topic_name, {'retention.ms': original_retention}):
                logger.warning(f"Failed to restore original retention for topic {topic_name}")
            
            logger.info(f"Successfully purged topic {topic_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to purge topic {topic_name}: {e}")
            return False
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information."""
        try:
            admin_client = self.connection.get_admin_client()
            
            # Get cluster metadata
            metadata = admin_client.describe_cluster()
            if not metadata:
                return {}
            
            broker_info = []
            for broker_id, broker_metadata in metadata.brokers.items():
                broker_info.append({
                    'id': broker_id,
                    'host': broker_metadata.host,
                    'port': broker_metadata.port
                })
            
            cluster_info = {
                'cluster_id': metadata.cluster_id,
                'controller_id': metadata.controller.id if metadata.controller else None,
                'brokers': broker_info,
                'broker_count': len(broker_info),
                'topic_count': len([t for t in metadata.topics.keys() if not t.startswith('__')])
            }
            
            logger.debug("Retrieved cluster information")
            return cluster_info
            
        except Exception as e:
            logger.error(f"Failed to get cluster info: {e}")
            return {}
    
    def _has_non_default_configs(self, topic_config: TopicConfig) -> bool:
        """Check if topic config has non-default values."""
        return (
            topic_config.retention_ms != 604800000 or  # 7 days
            topic_config.cleanup_policy.value != 'delete' or
            topic_config.compression_type.value != 'none' or
            topic_config.max_message_bytes != 1048576 or  # 1MB
            topic_config.min_insync_replicas != 1
        )
    
    def _update_topic_configs(self, topic_config: TopicConfig) -> bool:
        """Update topic configurations after creation."""
        configs = {}
        
        # Add standard configurations
        if topic_config.retention_ms != 604800000:
            configs['retention.ms'] = str(topic_config.retention_ms)
        
        if topic_config.cleanup_policy.value != 'delete':
            configs['cleanup.policy'] = topic_config.cleanup_policy.value
        
        if topic_config.compression_type.value != 'none':
            configs['compression.type'] = topic_config.compression_type.value
        
        if topic_config.max_message_bytes != 1048576:
            configs['max.message.bytes'] = str(topic_config.max_message_bytes)
        
        if topic_config.min_insync_replicas != 1:
            configs['min.insync.replicas'] = str(topic_config.min_insync_replicas)
        
        # Add custom configurations
        configs.update(topic_config.custom_configs)
        
        if configs:
            return self.update_topic_config(topic_config.name, configs)
        
        return True
    
    def _get_topic_configs(self, topic_name: str) -> Dict[str, str]:
        """Get current topic configurations."""
        try:
            admin_client = self.connection.get_admin_client()
            
            config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            config_future = admin_client.describe_configs([config_resource])
            
            config_result = config_future[config_resource].result(timeout=10)
            return {entry.name: entry.value for entry in config_result.values()}
            
        except Exception as e:
            logger.error(f"Failed to get topic configs for {topic_name}: {e}")
            return {}


class ConfluentKafkaAdminOperations:
    """Kafka admin operations using Confluent Kafka client."""
    
    def __init__(self, connection: KafkaClientConnection):
        """Initialize admin operations with client connection."""
        self.connection = connection
    
    def create_topics_batch(self, topic_configs: List[TopicConfig]) -> Dict[str, bool]:
        """Create multiple topics in batch using Confluent client."""
        try:
            admin_client = self.connection.get_confluent_admin()
            
            # Build topic specifications
            new_topics = []
            for topic_config in topic_configs:
                topic_spec = ConfluentNewTopic(
                    topic=topic_config.name,
                    num_partitions=topic_config.partitions,
                    replication_factor=topic_config.replication_factor
                )
                new_topics.append(topic_spec)
            
            # Create topics
            future_map = admin_client.create_topics(new_topics)
            
            # Wait for results
            results = {}
            for topic_name, future in future_map.items():
                try:
                    future.result(timeout=30)
                    results[topic_name] = True
                    logger.info(f"Successfully created topic {topic_name}")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    results[topic_name] = False
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to create topics batch: {e}")
            return {config.name: False for config in topic_configs}
    
    def delete_topics_batch(self, topic_names: List[str]) -> Dict[str, bool]:
        """Delete multiple topics in batch."""
        try:
            admin_client = self.connection.get_confluent_admin()
            
            # Delete topics
            future_map = admin_client.delete_topics(topic_names)
            
            # Wait for results
            results = {}
            for topic_name, future in future_map.items():
                try:
                    future.result(timeout=30)
                    results[topic_name] = True
                    logger.info(f"Successfully deleted topic {topic_name}")
                except Exception as e:
                    logger.error(f"Failed to delete topic {topic_name}: {e}")
                    results[topic_name] = False
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to delete topics batch: {e}")
            return {name: False for name in topic_names}