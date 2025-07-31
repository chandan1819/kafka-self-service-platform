"""Docker runtime provider for Kafka clusters."""

import docker
import time
import logging
import yaml
from typing import Dict, Any, Optional, List
from pathlib import Path
from kafka_ops_agent.providers.base import (
    RuntimeProvider, 
    ProvisioningResult, 
    DeprovisioningResult, 
    ProvisioningStatus
)
from kafka_ops_agent.models.cluster import ClusterConfig, ConnectionInfo

logger = logging.getLogger(__name__)


class DockerProvider(RuntimeProvider):
    """Docker-based Kafka cluster provider."""
    
    def __init__(self):
        """Initialize Docker provider."""
        try:
            self.client = docker.from_env()
            self.client.ping()
            logger.info("Docker client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Docker client: {e}")
            raise
    
    def provision_cluster(self, instance_id: str, config: Dict[str, Any]) -> ProvisioningResult:
        """Provision a new Kafka cluster using Docker Compose."""
        try:
            logger.info(f"Starting provisioning for cluster {instance_id}")
            
            # Parse configuration
            cluster_config = self._parse_config(config)
            
            # Create Docker Compose configuration
            compose_config = self._generate_compose_config(instance_id, cluster_config)
            
            # Write compose file
            compose_path = self._write_compose_file(instance_id, compose_config)
            
            # Start containers
            self._start_containers(instance_id, compose_path)
            
            # Wait for cluster to be ready
            connection_info = self._wait_for_cluster_ready(instance_id, cluster_config)
            
            logger.info(f"Successfully provisioned cluster {instance_id}")
            
            return ProvisioningResult(
                status=ProvisioningStatus.SUCCEEDED,
                instance_id=instance_id,
                connection_info=connection_info.__dict__ if connection_info else None
            )
            
        except Exception as e:
            logger.error(f"Failed to provision cluster {instance_id}: {e}")
            # Cleanup on failure
            self._cleanup_cluster(instance_id)
            
            return ProvisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=str(e)
            )
    
    def deprovision_cluster(self, instance_id: str) -> DeprovisioningResult:
        """Deprovision an existing Kafka cluster."""
        try:
            logger.info(f"Starting deprovisioning for cluster {instance_id}")
            
            self._cleanup_cluster(instance_id)
            
            logger.info(f"Successfully deprovisioned cluster {instance_id}")
            
            return DeprovisioningResult(
                status=ProvisioningStatus.SUCCEEDED,
                instance_id=instance_id
            )
            
        except Exception as e:
            logger.error(f"Failed to deprovision cluster {instance_id}: {e}")
            
            return DeprovisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=str(e)
            )
    
    def get_cluster_status(self, instance_id: str) -> ProvisioningStatus:
        """Get the current status of a cluster."""
        try:
            containers = self._get_cluster_containers(instance_id)
            
            if not containers:
                return ProvisioningStatus.FAILED
            
            # Check if all containers are running
            all_running = all(
                container.status == 'running' 
                for container in containers
            )
            
            if all_running:
                return ProvisioningStatus.SUCCEEDED
            else:
                return ProvisioningStatus.IN_PROGRESS
                
        except Exception as e:
            logger.error(f"Failed to get status for cluster {instance_id}: {e}")
            return ProvisioningStatus.FAILED
    
    def get_connection_info(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get connection information for a cluster."""
        try:
            containers = self._get_cluster_containers(instance_id)
            kafka_containers = [c for c in containers if 'kafka' in c.name]
            
            if not kafka_containers:
                return None
            
            # Get Kafka broker ports
            bootstrap_servers = []
            for container in kafka_containers:
                ports = container.ports.get('9092/tcp', [])
                if ports:
                    host_port = ports[0]['HostPort']
                    bootstrap_servers.append(f"localhost:{host_port}")
            
            # Get Zookeeper port
            zk_containers = [c for c in containers if 'zookeeper' in c.name]
            zk_port = "2181"  # Default
            if zk_containers:
                zk_ports = zk_containers[0].ports.get('2181/tcp', [])
                if zk_ports:
                    zk_port = zk_ports[0]['HostPort']
            
            connection_info = ConnectionInfo(
                bootstrap_servers=bootstrap_servers,
                zookeeper_connect=f"localhost:{zk_port}"
            )
            
            return connection_info.__dict__
            
        except Exception as e:
            logger.error(f"Failed to get connection info for cluster {instance_id}: {e}")
            return None
    
    def health_check(self, instance_id: str) -> bool:
        """Check if a cluster is healthy and accessible."""
        try:
            containers = self._get_cluster_containers(instance_id)
            
            if not containers:
                return False
            
            # Check container health
            for container in containers:
                if container.status != 'running':
                    return False
                
                # Check container health if health check is configured
                if hasattr(container, 'health') and container.health:
                    if container.health != 'healthy':
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed for cluster {instance_id}: {e}")
            return False
    
    def _parse_config(self, config: Dict[str, Any]) -> ClusterConfig:
        """Parse configuration into ClusterConfig object."""
        return ClusterConfig(
            cluster_size=config.get('cluster_size', 1),
            replication_factor=config.get('replication_factor', 1),
            partition_count=config.get('partition_count', 3),
            retention_hours=config.get('retention_hours', 168),
            storage_size_gb=config.get('storage_size_gb', 10),
            enable_ssl=config.get('enable_ssl', False),
            enable_sasl=config.get('enable_sasl', False),
            custom_properties=config.get('custom_properties', {})
        )
    
    def _generate_compose_config(self, instance_id: str, config: ClusterConfig) -> Dict[str, Any]:
        """Generate Docker Compose configuration."""
        services = {}
        
        # Zookeeper service
        services['zookeeper'] = {
            'image': 'confluentinc/cp-zookeeper:7.4.0',
            'container_name': f'{instance_id}-zookeeper',
            'environment': {
                'ZOOKEEPER_CLIENT_PORT': 2181,
                'ZOOKEEPER_TICK_TIME': 2000
            },
            'ports': ['2181:2181'],
            'volumes': [f'{instance_id}-zk-data:/var/lib/zookeeper/data'],
            'networks': [f'{instance_id}-network']
        }
        
        # Kafka brokers
        for i in range(config.cluster_size):
            broker_id = i + 1
            service_name = f'kafka-{broker_id}' if config.cluster_size > 1 else 'kafka'
            
            kafka_env = {
                'KAFKA_BROKER_ID': broker_id,
                'KAFKA_ZOOKEEPER_CONNECT': f'{instance_id}-zookeeper:2181',
                'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://localhost:{9092 + i}',
                'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT',
                'KAFKA_INTER_BROKER_LISTENER_NAME': 'PLAINTEXT',
                'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': min(config.replication_factor, config.cluster_size),
                'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': min(2, config.cluster_size),
                'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': min(config.replication_factor, config.cluster_size),
                'KAFKA_LOG_RETENTION_HOURS': config.retention_hours,
                'KAFKA_NUM_PARTITIONS': config.partition_count
            }
            
            # Add custom properties
            for key, value in config.custom_properties.items():
                kafka_env[f'KAFKA_{key.upper().replace(".", "_")}'] = value
            
            services[service_name] = {
                'image': 'confluentinc/cp-kafka:7.4.0',
                'container_name': f'{instance_id}-{service_name}',
                'depends_on': ['zookeeper'],
                'ports': [f'{9092 + i}:9092'],
                'environment': kafka_env,
                'volumes': [f'{instance_id}-kafka-{broker_id}-data:/var/lib/kafka/data'],
                'networks': [f'{instance_id}-network']
            }
        
        # Volumes
        volumes = {
            f'{instance_id}-zk-data': {},
        }
        for i in range(config.cluster_size):
            volumes[f'{instance_id}-kafka-{i+1}-data'] = {}
        
        # Networks
        networks = {
            f'{instance_id}-network': {
                'driver': 'bridge'
            }
        }
        
        return {
            'version': '3.8',
            'services': services,
            'volumes': volumes,
            'networks': networks
        }
    
    def _write_compose_file(self, instance_id: str, compose_config: Dict[str, Any]) -> Path:
        """Write Docker Compose file to disk."""
        compose_dir = Path(f'/tmp/kafka-clusters/{instance_id}')
        compose_dir.mkdir(parents=True, exist_ok=True)
        
        compose_path = compose_dir / 'docker-compose.yml'
        
        with open(compose_path, 'w') as f:
            yaml.dump(compose_config, f, default_flow_style=False)
        
        logger.info(f"Written compose file to {compose_path}")
        return compose_path
    
    def _start_containers(self, instance_id: str, compose_path: Path):
        """Start containers using docker-compose."""
        import subprocess
        
        cmd = ['docker-compose', '-f', str(compose_path), 'up', '-d']
        
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=compose_path.parent)
        
        if result.returncode != 0:
            raise Exception(f"Failed to start containers: {result.stderr}")
        
        logger.info(f"Started containers for cluster {instance_id}")
    
    def _wait_for_cluster_ready(self, instance_id: str, config: ClusterConfig, timeout: int = 300) -> Optional[ConnectionInfo]:
        """Wait for cluster to be ready and return connection info."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                if self.get_cluster_status(instance_id) == ProvisioningStatus.SUCCEEDED:
                    connection_info_dict = self.get_connection_info(instance_id)
                    if connection_info_dict:
                        return ConnectionInfo(**connection_info_dict)
                
                time.sleep(10)  # Wait 10 seconds before checking again
                
            except Exception as e:
                logger.warning(f"Error while waiting for cluster {instance_id}: {e}")
                time.sleep(10)
        
        raise Exception(f"Cluster {instance_id} did not become ready within {timeout} seconds")
    
    def _get_cluster_containers(self, instance_id: str) -> List:
        """Get all containers for a cluster."""
        try:
            containers = self.client.containers.list(
                filters={'name': instance_id}
            )
            return containers
        except Exception as e:
            logger.error(f"Failed to get containers for cluster {instance_id}: {e}")
            return []
    
    def _cleanup_cluster(self, instance_id: str):
        """Clean up all resources for a cluster."""
        try:
            # Stop and remove containers
            containers = self._get_cluster_containers(instance_id)
            for container in containers:
                try:
                    container.stop(timeout=30)
                    container.remove()
                    logger.info(f"Removed container {container.name}")
                except Exception as e:
                    logger.warning(f"Failed to remove container {container.name}: {e}")
            
            # Remove volumes
            try:
                volumes = self.client.volumes.list(filters={'name': instance_id})
                for volume in volumes:
                    volume.remove()
                    logger.info(f"Removed volume {volume.name}")
            except Exception as e:
                logger.warning(f"Failed to remove volumes for {instance_id}: {e}")
            
            # Remove network
            try:
                networks = self.client.networks.list(names=[f'{instance_id}-network'])
                for network in networks:
                    network.remove()
                    logger.info(f"Removed network {network.name}")
            except Exception as e:
                logger.warning(f"Failed to remove network for {instance_id}: {e}")
            
            # Remove compose file
            try:
                compose_dir = Path(f'/tmp/kafka-clusters/{instance_id}')
                if compose_dir.exists():
                    import shutil
                    shutil.rmtree(compose_dir)
                    logger.info(f"Removed compose directory {compose_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove compose directory: {e}")
                
        except Exception as e:
            logger.error(f"Error during cleanup of cluster {instance_id}: {e}")
            raise