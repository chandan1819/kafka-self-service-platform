"""Kubernetes runtime provider for Kafka clusters."""

import logging
import time
import yaml
import tempfile
from typing import Dict, Any, Optional, List
from pathlib import Path
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from kafka_ops_agent.providers.base import (
    RuntimeProvider, 
    ProvisioningResult, 
    DeprovisioningResult, 
    ProvisioningStatus
)
from kafka_ops_agent.models.cluster import ClusterConfig, ConnectionInfo

logger = logging.getLogger(__name__)


class KubernetesProvider(RuntimeProvider):
    """Kubernetes-based Kafka cluster provider."""
    
    def __init__(self, namespace: str = "kafka-clusters", kubeconfig_path: Optional[str] = None):
        """Initialize Kubernetes provider.
        
        Args:
            namespace: Kubernetes namespace for Kafka clusters
            kubeconfig_path: Path to kubeconfig file (None for in-cluster config)
        """
        self.namespace = namespace
        self.kubeconfig_path = kubeconfig_path
        
        try:
            # Load Kubernetes configuration
            if kubeconfig_path:
                config.load_kube_config(config_file=kubeconfig_path)
            else:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
            
            # Initialize Kubernetes clients
            self.apps_v1 = client.AppsV1Api()
            self.core_v1 = client.CoreV1Api()
            self.storage_v1 = client.StorageV1Api()
            
            # Ensure namespace exists
            self._ensure_namespace()
            
            logger.info(f"Kubernetes provider initialized for namespace: {self.namespace}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kubernetes provider: {e}")
            raise
    
    def provision_cluster(self, instance_id: str, config: Dict[str, Any]) -> ProvisioningResult:
        """Provision a new Kafka cluster using Kubernetes manifests."""
        try:
            logger.info(f"Starting Kubernetes provisioning for cluster {instance_id}")
            
            # Parse configuration
            cluster_config = self._parse_config(config)
            
            # Generate Kubernetes manifests
            manifests = self._generate_manifests(instance_id, cluster_config)
            
            # Apply manifests to cluster
            self._apply_manifests(instance_id, manifests)
            
            # Wait for cluster to be ready
            connection_info = self._wait_for_cluster_ready(instance_id, cluster_config)
            
            logger.info(f"Successfully provisioned Kubernetes cluster {instance_id}")
            
            return ProvisioningResult(
                status=ProvisioningStatus.SUCCEEDED,
                instance_id=instance_id,
                connection_info=connection_info.__dict__ if connection_info else None
            )
            
        except Exception as e:
            logger.error(f"Failed to provision Kubernetes cluster {instance_id}: {e}")
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
            logger.info(f"Starting Kubernetes deprovisioning for cluster {instance_id}")
            
            self._cleanup_cluster(instance_id)
            
            logger.info(f"Successfully deprovisioned Kubernetes cluster {instance_id}")
            
            return DeprovisioningResult(
                status=ProvisioningStatus.SUCCEEDED,
                instance_id=instance_id
            )
            
        except Exception as e:
            logger.error(f"Failed to deprovision Kubernetes cluster {instance_id}: {e}")
            
            return DeprovisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=str(e)
            )
    
    def get_cluster_status(self, instance_id: str) -> ProvisioningStatus:
        """Get the current status of a Kubernetes cluster."""
        try:
            # Check StatefulSets status
            statefulsets = self._get_cluster_statefulsets(instance_id)
            
            if not statefulsets:
                return ProvisioningStatus.FAILED
            
            all_ready = True
            for sts in statefulsets:
                if not sts.status.ready_replicas or sts.status.ready_replicas < sts.spec.replicas:
                    all_ready = False
                    break
            
            if all_ready:
                return ProvisioningStatus.SUCCEEDED
            else:
                return ProvisioningStatus.IN_PROGRESS
                
        except Exception as e:
            logger.error(f"Failed to get status for Kubernetes cluster {instance_id}: {e}")
            return ProvisioningStatus.FAILED
    
    def get_connection_info(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get connection information for a Kubernetes cluster."""
        try:
            # Get Kafka service
            kafka_service = self._get_kafka_service(instance_id)
            if not kafka_service:
                return None
            
            # Get Zookeeper service
            zk_service = self._get_zookeeper_service(instance_id)
            
            # Build bootstrap servers list
            bootstrap_servers = []
            
            # Check if service has external access (NodePort or LoadBalancer)
            if kafka_service.spec.type == "NodePort":
                # Get node IPs and ports
                nodes = self.core_v1.list_node()
                node_ip = nodes.items[0].status.addresses[0].address if nodes.items else "localhost"
                
                for port in kafka_service.spec.ports:
                    if port.name == "kafka":
                        bootstrap_servers.append(f"{node_ip}:{port.node_port}")
            
            elif kafka_service.spec.type == "LoadBalancer":
                # Get load balancer IP
                if kafka_service.status.load_balancer.ingress:
                    lb_ip = kafka_service.status.load_balancer.ingress[0].ip
                    bootstrap_servers.append(f"{lb_ip}:9092")
            
            else:
                # ClusterIP - internal access only
                cluster_ip = kafka_service.spec.cluster_ip
                bootstrap_servers.append(f"{cluster_ip}:9092")
            
            # Zookeeper connection
            zk_connect = f"{instance_id}-zookeeper.{self.namespace}.svc.cluster.local:2181"
            if zk_service and zk_service.spec.type != "ClusterIP":
                if zk_service.spec.type == "NodePort":
                    nodes = self.core_v1.list_node()
                    node_ip = nodes.items[0].status.addresses[0].address if nodes.items else "localhost"
                    zk_port = next((p.node_port for p in zk_service.spec.ports if p.name == "client"), 2181)
                    zk_connect = f"{node_ip}:{zk_port}"
            
            connection_info = ConnectionInfo(
                bootstrap_servers=bootstrap_servers,
                zookeeper_connect=zk_connect
            )
            
            return connection_info.__dict__
            
        except Exception as e:
            logger.error(f"Failed to get connection info for Kubernetes cluster {instance_id}: {e}")
            return None
    
    def health_check(self, instance_id: str) -> bool:
        """Check if a Kubernetes cluster is healthy and accessible."""
        try:
            # Check StatefulSets
            statefulsets = self._get_cluster_statefulsets(instance_id)
            
            if not statefulsets:
                return False
            
            for sts in statefulsets:
                if not sts.status.ready_replicas or sts.status.ready_replicas < sts.spec.replicas:
                    return False
            
            # Check Services
            services = self._get_cluster_services(instance_id)
            if not services:
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Health check failed for Kubernetes cluster {instance_id}: {e}")
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
    
    def _ensure_namespace(self):
        """Ensure the namespace exists."""
        try:
            self.core_v1.read_namespace(name=self.namespace)
            logger.info(f"Namespace {self.namespace} already exists")
        except ApiException as e:
            if e.status == 404:
                # Create namespace
                namespace = client.V1Namespace(
                    metadata=client.V1ObjectMeta(name=self.namespace)
                )
                self.core_v1.create_namespace(body=namespace)
                logger.info(f"Created namespace {self.namespace}")
            else:
                raise
    
    def _generate_manifests(self, instance_id: str, config: ClusterConfig) -> Dict[str, Any]:
        """Generate Kubernetes manifests for Kafka cluster."""
        manifests = {}
        
        # Generate Zookeeper manifests
        manifests.update(self._generate_zookeeper_manifests(instance_id, config))
        
        # Generate Kafka manifests
        manifests.update(self._generate_kafka_manifests(instance_id, config))
        
        return manifests
    
    def _generate_zookeeper_manifests(self, instance_id: str, config: ClusterConfig) -> Dict[str, Any]:
        """Generate Zookeeper StatefulSet and Service manifests."""
        zk_name = f"{instance_id}-zookeeper"
        
        # Zookeeper Service
        zk_service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": zk_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "zookeeper",
                    "cluster": instance_id
                }
            },
            "spec": {
                "ports": [
                    {"name": "client", "port": 2181, "targetPort": 2181},
                    {"name": "follower", "port": 2888, "targetPort": 2888},
                    {"name": "election", "port": 3888, "targetPort": 3888}
                ],
                "selector": {
                    "app": "zookeeper",
                    "cluster": instance_id
                },
                "clusterIP": "None"
            }
        }
        
        # Zookeeper StatefulSet
        zk_statefulset = {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": zk_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "zookeeper",
                    "cluster": instance_id
                }
            },
            "spec": {
                "serviceName": zk_name,
                "replicas": 1,  # Single ZK for simplicity
                "selector": {
                    "matchLabels": {
                        "app": "zookeeper",
                        "cluster": instance_id
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "zookeeper",
                            "cluster": instance_id
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": "zookeeper",
                                "image": "confluentinc/cp-zookeeper:7.4.0",
                                "ports": [
                                    {"containerPort": 2181, "name": "client"},
                                    {"containerPort": 2888, "name": "follower"},
                                    {"containerPort": 3888, "name": "election"}
                                ],
                                "env": [
                                    {"name": "ZOOKEEPER_CLIENT_PORT", "value": "2181"},
                                    {"name": "ZOOKEEPER_TICK_TIME", "value": "2000"},
                                    {"name": "ZOOKEEPER_SERVER_ID", "value": "1"}
                                ],
                                "volumeMounts": [
                                    {
                                        "name": "zk-data",
                                        "mountPath": "/var/lib/zookeeper/data"
                                    }
                                ],
                                "resources": {
                                    "requests": {
                                        "memory": "512Mi",
                                        "cpu": "250m"
                                    },
                                    "limits": {
                                        "memory": "1Gi",
                                        "cpu": "500m"
                                    }
                                },
                                "readinessProbe": {
                                    "tcpSocket": {"port": 2181},
                                    "initialDelaySeconds": 10,
                                    "periodSeconds": 5
                                },
                                "livenessProbe": {
                                    "tcpSocket": {"port": 2181},
                                    "initialDelaySeconds": 30,
                                    "periodSeconds": 10
                                }
                            }
                        ]
                    }
                },
                "volumeClaimTemplates": [
                    {
                        "metadata": {
                            "name": "zk-data"
                        },
                        "spec": {
                            "accessModes": ["ReadWriteOnce"],
                            "resources": {
                                "requests": {
                                    "storage": f"{config.storage_size_gb}Gi"
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        return {
            f"{zk_name}-service": zk_service,
            f"{zk_name}-statefulset": zk_statefulset
        }
    
    def _generate_kafka_manifests(self, instance_id: str, config: ClusterConfig) -> Dict[str, Any]:
        """Generate Kafka StatefulSet and Service manifests."""
        kafka_name = f"{instance_id}-kafka"
        
        # Kafka Service
        kafka_service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": kafka_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "kafka",
                    "cluster": instance_id
                }
            },
            "spec": {
                "ports": [
                    {"name": "kafka", "port": 9092, "targetPort": 9092}
                ],
                "selector": {
                    "app": "kafka",
                    "cluster": instance_id
                },
                "type": "ClusterIP"  # Can be changed to NodePort or LoadBalancer
            }
        }
        
        # Build Kafka environment variables
        kafka_env = [
            {"name": "KAFKA_ZOOKEEPER_CONNECT", "value": f"{instance_id}-zookeeper.{self.namespace}.svc.cluster.local:2181"},
            {"name": "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "value": "PLAINTEXT:PLAINTEXT"},
            {"name": "KAFKA_INTER_BROKER_LISTENER_NAME", "value": "PLAINTEXT"},
            {"name": "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "value": str(min(config.replication_factor, config.cluster_size))},
            {"name": "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "value": str(min(2, config.cluster_size))},
            {"name": "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "value": str(min(config.replication_factor, config.cluster_size))},
            {"name": "KAFKA_LOG_RETENTION_HOURS", "value": str(config.retention_hours)},
            {"name": "KAFKA_NUM_PARTITIONS", "value": str(config.partition_count)},
            {"name": "KAFKA_AUTO_CREATE_TOPICS_ENABLE", "value": "true"}
        ]
        
        # Add custom properties
        for key, value in config.custom_properties.items():
            env_key = f"KAFKA_{key.upper().replace('.', '_')}"
            kafka_env.append({"name": env_key, "value": str(value)})
        
        # Kafka StatefulSet
        kafka_statefulset = {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": kafka_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "kafka",
                    "cluster": instance_id
                }
            },
            "spec": {
                "serviceName": kafka_name,
                "replicas": config.cluster_size,
                "selector": {
                    "matchLabels": {
                        "app": "kafka",
                        "cluster": instance_id
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "kafka",
                            "cluster": instance_id
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": "kafka",
                                "image": "confluentinc/cp-kafka:7.4.0",
                                "ports": [
                                    {"containerPort": 9092, "name": "kafka"}
                                ],
                                "env": kafka_env + [
                                    {
                                        "name": "KAFKA_BROKER_ID",
                                        "valueFrom": {
                                            "fieldRef": {
                                                "fieldPath": "metadata.name"
                                            }
                                        }
                                    },
                                    {
                                        "name": "KAFKA_ADVERTISED_LISTENERS",
                                        "value": f"PLAINTEXT://$(hostname -f).{kafka_name}.{self.namespace}.svc.cluster.local:9092"
                                    }
                                ],
                                "volumeMounts": [
                                    {
                                        "name": "kafka-data",
                                        "mountPath": "/var/lib/kafka/data"
                                    }
                                ],
                                "resources": {
                                    "requests": {
                                        "memory": "1Gi",
                                        "cpu": "500m"
                                    },
                                    "limits": {
                                        "memory": "2Gi",
                                        "cpu": "1000m"
                                    }
                                },
                                "readinessProbe": {
                                    "tcpSocket": {"port": 9092},
                                    "initialDelaySeconds": 30,
                                    "periodSeconds": 10
                                },
                                "livenessProbe": {
                                    "tcpSocket": {"port": 9092},
                                    "initialDelaySeconds": 60,
                                    "periodSeconds": 15
                                }
                            }
                        ]
                    }
                },
                "volumeClaimTemplates": [
                    {
                        "metadata": {
                            "name": "kafka-data"
                        },
                        "spec": {
                            "accessModes": ["ReadWriteOnce"],
                            "resources": {
                                "requests": {
                                    "storage": f"{config.storage_size_gb}Gi"
                                }
                            }
                        }
                    }
                ]
            }
        }
        
        return {
            f"{kafka_name}-service": kafka_service,
            f"{kafka_name}-statefulset": kafka_statefulset
        }
    
    def _apply_manifests(self, instance_id: str, manifests: Dict[str, Any]):
        """Apply Kubernetes manifests to the cluster."""
        for name, manifest in manifests.items():
            try:
                kind = manifest["kind"]
                
                if kind == "Service":
                    self.core_v1.create_namespaced_service(
                        namespace=self.namespace,
                        body=manifest
                    )
                    logger.info(f"Created Service: {name}")
                
                elif kind == "StatefulSet":
                    self.apps_v1.create_namespaced_stateful_set(
                        namespace=self.namespace,
                        body=manifest
                    )
                    logger.info(f"Created StatefulSet: {name}")
                
                else:
                    logger.warning(f"Unknown manifest kind: {kind}")
                    
            except ApiException as e:
                if e.status == 409:  # Already exists
                    logger.info(f"Resource {name} already exists, skipping")
                else:
                    logger.error(f"Failed to create {name}: {e}")
                    raise
            except Exception as e:
                logger.error(f"Failed to create {name}: {e}")
                raise
    
    def _wait_for_cluster_ready(self, instance_id: str, config: ClusterConfig, timeout: int = 600) -> Optional[ConnectionInfo]:
        """Wait for cluster to be ready and return connection info."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                if self.get_cluster_status(instance_id) == ProvisioningStatus.SUCCEEDED:
                    connection_info_dict = self.get_connection_info(instance_id)
                    if connection_info_dict:
                        return ConnectionInfo(**connection_info_dict)
                
                time.sleep(15)  # Wait 15 seconds before checking again
                
            except Exception as e:
                logger.warning(f"Error while waiting for Kubernetes cluster {instance_id}: {e}")
                time.sleep(15)
        
        raise Exception(f"Kubernetes cluster {instance_id} did not become ready within {timeout} seconds")
    
    def _get_cluster_statefulsets(self, instance_id: str) -> List:
        """Get all StatefulSets for a cluster."""
        try:
            response = self.apps_v1.list_namespaced_stateful_set(
                namespace=self.namespace,
                label_selector=f"cluster={instance_id}"
            )
            return response.items
        except Exception as e:
            logger.error(f"Failed to get StatefulSets for cluster {instance_id}: {e}")
            return []
    
    def _get_cluster_services(self, instance_id: str) -> List:
        """Get all Services for a cluster."""
        try:
            response = self.core_v1.list_namespaced_service(
                namespace=self.namespace,
                label_selector=f"cluster={instance_id}"
            )
            return response.items
        except Exception as e:
            logger.error(f"Failed to get Services for cluster {instance_id}: {e}")
            return []
    
    def _get_kafka_service(self, instance_id: str):
        """Get the Kafka service for a cluster."""
        try:
            return self.core_v1.read_namespaced_service(
                name=f"{instance_id}-kafka",
                namespace=self.namespace
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def _get_zookeeper_service(self, instance_id: str):
        """Get the Zookeeper service for a cluster."""
        try:
            return self.core_v1.read_namespaced_service(
                name=f"{instance_id}-zookeeper",
                namespace=self.namespace
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def _cleanup_cluster(self, instance_id: str):
        """Clean up all Kubernetes resources for a cluster."""
        try:
            # Delete StatefulSets
            statefulsets = self._get_cluster_statefulsets(instance_id)
            for sts in statefulsets:
                try:
                    self.apps_v1.delete_namespaced_stateful_set(
                        name=sts.metadata.name,
                        namespace=self.namespace
                    )
                    logger.info(f"Deleted StatefulSet: {sts.metadata.name}")
                except ApiException as e:
                    if e.status != 404:
                        logger.warning(f"Failed to delete StatefulSet {sts.metadata.name}: {e}")
            
            # Delete Services
            services = self._get_cluster_services(instance_id)
            for svc in services:
                try:
                    self.core_v1.delete_namespaced_service(
                        name=svc.metadata.name,
                        namespace=self.namespace
                    )
                    logger.info(f"Deleted Service: {svc.metadata.name}")
                except ApiException as e:
                    if e.status != 404:
                        logger.warning(f"Failed to delete Service {svc.metadata.name}: {e}")
            
            # Delete PVCs (they don't get deleted automatically with StatefulSets)
            try:
                pvcs = self.core_v1.list_namespaced_persistent_volume_claim(
                    namespace=self.namespace,
                    label_selector=f"cluster={instance_id}"
                )
                for pvc in pvcs.items:
                    try:
                        self.core_v1.delete_namespaced_persistent_volume_claim(
                            name=pvc.metadata.name,
                            namespace=self.namespace
                        )
                        logger.info(f"Deleted PVC: {pvc.metadata.name}")
                    except ApiException as e:
                        if e.status != 404:
                            logger.warning(f"Failed to delete PVC {pvc.metadata.name}: {e}")
            except Exception as e:
                logger.warning(f"Failed to cleanup PVCs for cluster {instance_id}: {e}")
                
        except Exception as e:
            logger.error(f"Error during cleanup of Kubernetes cluster {instance_id}: {e}")
            raise