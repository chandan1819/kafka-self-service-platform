"""Kafka client connection management with pooling and health monitoring."""

import logging
import threading
import time
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import ssl

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource as ConfluentConfigResource

from kafka_ops_agent.models.cluster import ConnectionInfo, SSLConfig, SASLConfig
from kafka_ops_agent.config import config

logger = logging.getLogger(__name__)


@dataclass
class ClientConfig:
    """Kafka client configuration."""
    bootstrap_servers: List[str]
    security_protocol: str = 'PLAINTEXT'
    ssl_config: Optional[SSLConfig] = None
    sasl_config: Optional[SASLConfig] = None
    client_timeout_ms: int = 30000
    request_timeout_ms: int = 30000
    retry_backoff_ms: int = 100
    max_retries: int = 3


class KafkaClientConnection:
    """Individual Kafka client connection wrapper."""
    
    def __init__(self, client_config: ClientConfig, connection_id: str):
        """Initialize Kafka client connection."""
        self.client_config = client_config
        self.connection_id = connection_id
        self.created_at = time.time()
        self.last_used = time.time()
        self.use_count = 0
        self.is_healthy = True
        self.lock = threading.Lock()
        
        # Client instances
        self._admin_client: Optional[KafkaAdminClient] = None
        self._confluent_admin: Optional[AdminClient] = None
        self._producer: Optional[KafkaProducer] = None
        
        logger.debug(f"Created Kafka client connection {connection_id}")
    
    def get_admin_client(self) -> KafkaAdminClient:
        """Get or create Kafka admin client."""
        with self.lock:
            if self._admin_client is None:
                try:
                    client_config = self._build_kafka_python_config()
                    self._admin_client = KafkaAdminClient(**client_config)
                    logger.debug(f"Created admin client for connection {self.connection_id}")
                except Exception as e:
                    logger.error(f"Failed to create admin client: {e}")
                    self.is_healthy = False
                    raise
            
            self.last_used = time.time()
            self.use_count += 1
            return self._admin_client
    
    def get_confluent_admin(self) -> AdminClient:
        """Get or create Confluent Kafka admin client."""
        with self.lock:
            if self._confluent_admin is None:
                try:
                    client_config = self._build_confluent_config()
                    self._confluent_admin = AdminClient(client_config)
                    logger.debug(f"Created Confluent admin client for connection {self.connection_id}")
                except Exception as e:
                    logger.error(f"Failed to create Confluent admin client: {e}")
                    self.is_healthy = False
                    raise
            
            self.last_used = time.time()
            self.use_count += 1
            return self._confluent_admin
    
    def get_producer(self) -> KafkaProducer:
        """Get or create Kafka producer."""
        with self.lock:
            if self._producer is None:
                try:
                    client_config = self._build_kafka_python_config()
                    # Add producer-specific configs
                    client_config.update({
                        'value_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
                        'key_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
                        'acks': 'all',
                        'retries': self.client_config.max_retries,
                        'retry_backoff_ms': self.client_config.retry_backoff_ms
                    })
                    self._producer = KafkaProducer(**client_config)
                    logger.debug(f"Created producer for connection {self.connection_id}")
                except Exception as e:
                    logger.error(f"Failed to create producer: {e}")
                    self.is_healthy = False
                    raise
            
            self.last_used = time.time()
            self.use_count += 1
            return self._producer
    
    def create_consumer(self, topics: List[str], group_id: str, **kwargs) -> KafkaConsumer:
        """Create a new Kafka consumer (not pooled)."""
        try:
            client_config = self._build_kafka_python_config()
            # Add consumer-specific configs
            client_config.update({
                'group_id': group_id,
                'value_deserializer': lambda x: x.decode('utf-8') if x else None,
                'key_deserializer': lambda x: x.decode('utf-8') if x else None,
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': True,
                **kwargs
            })
            
            consumer = KafkaConsumer(*topics, **client_config)
            logger.debug(f"Created consumer for topics {topics} with group {group_id}")
            
            self.last_used = time.time()
            self.use_count += 1
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            self.is_healthy = False
            raise
    
    def _build_kafka_python_config(self) -> Dict[str, Any]:
        """Build configuration for kafka-python clients."""
        config_dict = {
            'bootstrap_servers': self.client_config.bootstrap_servers,
            'request_timeout_ms': self.client_config.request_timeout_ms,
            'retry_backoff_ms': self.client_config.retry_backoff_ms,
            'security_protocol': self.client_config.security_protocol
        }
        
        # Add SSL configuration
        if self.client_config.ssl_config:
            ssl_config = self.client_config.ssl_config
            if ssl_config.keystore_location:
                config_dict['ssl_keyfile'] = ssl_config.keystore_location
            if ssl_config.truststore_location:
                config_dict['ssl_cafile'] = ssl_config.truststore_location
            if ssl_config.key_password:
                config_dict['ssl_password'] = ssl_config.key_password
            
            config_dict['ssl_check_hostname'] = False
            config_dict['ssl_context'] = ssl.create_default_context()
        
        # Add SASL configuration
        if self.client_config.sasl_config:
            sasl_config = self.client_config.sasl_config
            config_dict['sasl_mechanism'] = sasl_config.mechanism
            if sasl_config.username:
                config_dict['sasl_plain_username'] = sasl_config.username
            if sasl_config.password:
                config_dict['sasl_plain_password'] = sasl_config.password
        
        return config_dict
    
    def _build_confluent_config(self) -> Dict[str, Any]:
        """Build configuration for Confluent Kafka clients."""
        config_dict = {
            'bootstrap.servers': ','.join(self.client_config.bootstrap_servers),
            'request.timeout.ms': self.client_config.request_timeout_ms,
            'retry.backoff.ms': self.client_config.retry_backoff_ms,
            'security.protocol': self.client_config.security_protocol
        }
        
        # Add SSL configuration
        if self.client_config.ssl_config:
            ssl_config = self.client_config.ssl_config
            if ssl_config.keystore_location:
                config_dict['ssl.key.location'] = ssl_config.keystore_location
            if ssl_config.truststore_location:
                config_dict['ssl.ca.location'] = ssl_config.truststore_location
            if ssl_config.key_password:
                config_dict['ssl.key.password'] = ssl_config.key_password
        
        # Add SASL configuration
        if self.client_config.sasl_config:
            sasl_config = self.client_config.sasl_config
            config_dict['sasl.mechanism'] = sasl_config.mechanism
            if sasl_config.username:
                config_dict['sasl.username'] = sasl_config.username
            if sasl_config.password:
                config_dict['sasl.password'] = sasl_config.password
        
        return config_dict
    
    def health_check(self) -> bool:
        """Perform health check on the connection."""
        try:
            admin_client = self.get_admin_client()
            # Try to get cluster metadata
            metadata = admin_client.describe_cluster()
            if metadata:
                self.is_healthy = True
                logger.debug(f"Health check passed for connection {self.connection_id}")
                return True
        except Exception as e:
            logger.warning(f"Health check failed for connection {self.connection_id}: {e}")
            self.is_healthy = False
        
        return False
    
    def close(self):
        """Close all client connections."""
        with self.lock:
            try:
                if self._admin_client:
                    self._admin_client.close()
                    self._admin_client = None
                
                if self._confluent_admin:
                    # Confluent admin client doesn't have explicit close
                    self._confluent_admin = None
                
                if self._producer:
                    self._producer.close()
                    self._producer = None
                
                logger.debug(f"Closed all clients for connection {self.connection_id}")
                
            except Exception as e:
                logger.warning(f"Error closing clients for connection {self.connection_id}: {e}")
    
    def is_expired(self, max_idle_time: int = 300) -> bool:
        """Check if connection has been idle too long."""
        return (time.time() - self.last_used) > max_idle_time
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            'connection_id': self.connection_id,
            'created_at': self.created_at,
            'last_used': self.last_used,
            'use_count': self.use_count,
            'is_healthy': self.is_healthy,
            'age_seconds': time.time() - self.created_at,
            'idle_seconds': time.time() - self.last_used
        }


class KafkaClientManager:
    """Manages Kafka client connections with pooling and health monitoring."""
    
    def __init__(self, max_connections: int = None):
        """Initialize client manager."""
        self.max_connections = max_connections or config.kafka.connection_pool_size
        self.connections: Dict[str, KafkaClientConnection] = {}
        self.connection_configs: Dict[str, ClientConfig] = {}
        self.lock = threading.RLock()
        self.health_check_interval = 60  # seconds
        self.max_idle_time = 300  # seconds
        self.cleanup_interval = 120  # seconds
        
        # Background thread for health checks and cleanup
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-client-manager")
        self.running = True
        self._start_background_tasks()
        
        logger.info(f"Kafka client manager initialized with max {self.max_connections} connections")
    
    def register_cluster(self, cluster_id: str, connection_info: ConnectionInfo) -> bool:
        """Register a Kafka cluster for client connections."""
        try:
            client_config = ClientConfig(
                bootstrap_servers=connection_info.bootstrap_servers,
                security_protocol='SSL' if connection_info.ssl_config else 'PLAINTEXT',
                ssl_config=connection_info.ssl_config,
                sasl_config=connection_info.sasl_config,
                client_timeout_ms=config.kafka.client_timeout_ms
            )
            
            with self.lock:
                self.connection_configs[cluster_id] = client_config
            
            logger.info(f"Registered cluster {cluster_id} with {len(connection_info.bootstrap_servers)} brokers")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register cluster {cluster_id}: {e}")
            return False
    
    def get_connection(self, cluster_id: str) -> Optional[KafkaClientConnection]:
        """Get or create a connection for the specified cluster."""
        with self.lock:
            # Check if cluster is registered
            if cluster_id not in self.connection_configs:
                logger.error(f"Cluster {cluster_id} not registered")
                return None
            
            # Check if connection already exists and is healthy
            if cluster_id in self.connections:
                connection = self.connections[cluster_id]
                if connection.is_healthy:
                    return connection
                else:
                    # Remove unhealthy connection
                    logger.warning(f"Removing unhealthy connection for cluster {cluster_id}")
                    connection.close()
                    del self.connections[cluster_id]
            
            # Check connection limit
            if len(self.connections) >= self.max_connections:
                # Try to clean up expired connections
                self._cleanup_expired_connections()
                
                if len(self.connections) >= self.max_connections:
                    logger.warning(f"Connection pool full ({self.max_connections}), cannot create new connection")
                    return None
            
            # Create new connection
            try:
                client_config = self.connection_configs[cluster_id]
                connection = KafkaClientConnection(client_config, cluster_id)
                self.connections[cluster_id] = connection
                
                logger.info(f"Created new connection for cluster {cluster_id}")
                return connection
                
            except Exception as e:
                logger.error(f"Failed to create connection for cluster {cluster_id}: {e}")
                return None
    
    def remove_cluster(self, cluster_id: str) -> bool:
        """Remove a cluster and close its connections."""
        with self.lock:
            try:
                # Close connection if exists
                if cluster_id in self.connections:
                    self.connections[cluster_id].close()
                    del self.connections[cluster_id]
                
                # Remove configuration
                if cluster_id in self.connection_configs:
                    del self.connection_configs[cluster_id]
                
                logger.info(f"Removed cluster {cluster_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to remove cluster {cluster_id}: {e}")
                return False
    
    def health_check_all(self) -> Dict[str, bool]:
        """Perform health check on all connections."""
        results = {}
        
        with self.lock:
            connections_to_check = list(self.connections.items())
        
        for cluster_id, connection in connections_to_check:
            try:
                is_healthy = connection.health_check()
                results[cluster_id] = is_healthy
                
                if not is_healthy:
                    with self.lock:
                        if cluster_id in self.connections:
                            logger.warning(f"Removing failed connection for cluster {cluster_id}")
                            connection.close()
                            del self.connections[cluster_id]
                            
            except Exception as e:
                logger.error(f"Health check error for cluster {cluster_id}: {e}")
                results[cluster_id] = False
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self.lock:
            connection_stats = [conn.get_stats() for conn in self.connections.values()]
            
            return {
                'total_connections': len(self.connections),
                'max_connections': self.max_connections,
                'registered_clusters': len(self.connection_configs),
                'connections': connection_stats,
                'pool_utilization': len(self.connections) / self.max_connections
            }
    
    def _cleanup_expired_connections(self):
        """Clean up expired connections (called with lock held)."""
        expired_clusters = []
        
        for cluster_id, connection in self.connections.items():
            if connection.is_expired(self.max_idle_time):
                expired_clusters.append(cluster_id)
        
        for cluster_id in expired_clusters:
            logger.info(f"Cleaning up expired connection for cluster {cluster_id}")
            self.connections[cluster_id].close()
            del self.connections[cluster_id]
    
    def _start_background_tasks(self):
        """Start background tasks for health checks and cleanup."""
        def health_check_task():
            while self.running:
                try:
                    time.sleep(self.health_check_interval)
                    if self.running:
                        logger.debug("Running background health checks")
                        self.health_check_all()
                except Exception as e:
                    logger.error(f"Background health check error: {e}")
        
        def cleanup_task():
            while self.running:
                try:
                    time.sleep(self.cleanup_interval)
                    if self.running:
                        with self.lock:
                            logger.debug("Running background cleanup")
                            self._cleanup_expired_connections()
                except Exception as e:
                    logger.error(f"Background cleanup error: {e}")
        
        self.executor.submit(health_check_task)
        self.executor.submit(cleanup_task)
    
    def close(self):
        """Close all connections and shutdown manager."""
        logger.info("Shutting down Kafka client manager")
        self.running = False
        
        with self.lock:
            for connection in self.connections.values():
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
            
            self.connections.clear()
            self.connection_configs.clear()
        
        self.executor.shutdown(wait=True)
        logger.info("Kafka client manager shutdown complete")


# Global client manager instance
_client_manager: Optional[KafkaClientManager] = None


def get_client_manager() -> KafkaClientManager:
    """Get or create global client manager instance."""
    global _client_manager
    if _client_manager is None:
        _client_manager = KafkaClientManager()
    return _client_manager


def close_client_manager():
    """Close global client manager."""
    global _client_manager
    if _client_manager:
        _client_manager.close()
        _client_manager = None