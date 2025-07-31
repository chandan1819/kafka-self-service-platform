#!/usr/bin/env python3
"""
Day 1 Kafka Cluster Provisioning Quick Start Script

This script demonstrates how to provision your first Kafka cluster
using the Kafka Self-Service Platform.
"""

import sys
import os
import time
import json
import requests
import argparse
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class KafkaOpsClient:
    """Simple client for Kafka Ops Agent API."""
    
    def __init__(self, base_url: str = "http://localhost:8000", api_key: str = "admin-secret-key"):
        """Initialize client.
        
        Args:
            base_url: Base URL of the API
            api_key: API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-Key': api_key,
            'Content-Type': 'application/json'
        })
    
    def get_catalog(self):
        """Get service catalog."""
        response = self.session.get(f"{self.base_url}/v2/catalog")
        response.raise_for_status()
        return response.json()
    
    def provision_service(self, instance_id: str, service_config: dict):
        """Provision a service instance."""
        response = self.session.put(
            f"{self.base_url}/v2/service_instances/{instance_id}",
            json=service_config
        )
        response.raise_for_status()
        return response.json()
    
    def get_last_operation(self, instance_id: str):
        """Get last operation status."""
        response = self.session.get(
            f"{self.base_url}/v2/service_instances/{instance_id}/last_operation"
        )
        response.raise_for_status()
        return response.json()
    
    def deprovision_service(self, instance_id: str):
        """Deprovision a service instance."""
        response = self.session.delete(
            f"{self.base_url}/v2/service_instances/{instance_id}"
        )
        response.raise_for_status()
        return response.json()
    
    def create_topic(self, topic_config: dict):
        """Create a topic."""
        response = self.session.post(
            f"{self.base_url}/api/v1/topics",
            json=topic_config
        )
        response.raise_for_status()
        return response.json()
    
    def list_topics(self):
        """List all topics."""
        response = self.session.get(f"{self.base_url}/api/v1/topics")
        response.raise_for_status()
        return response.json()
    
    def delete_topic(self, topic_name: str):
        """Delete a topic."""
        response = self.session.delete(f"{self.base_url}/api/v1/topics/{topic_name}")
        response.raise_for_status()
        return response.json()
    
    def get_health(self):
        """Get system health."""
        monitoring_url = self.base_url.replace(':8000', ':8080')
        response = requests.get(f"{monitoring_url}/health")
        response.raise_for_status()
        return response.json()


def wait_for_operation(client: KafkaOpsClient, instance_id: str, timeout: int = 300):
    """Wait for operation to complete.
    
    Args:
        client: Kafka Ops client
        instance_id: Service instance ID
        timeout: Timeout in seconds
        
    Returns:
        Final operation status
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            status = client.get_last_operation(instance_id)
            state = status.get('state', 'in progress')
            
            print(f"  Status: {state}")
            if status.get('description'):
                print(f"  Description: {status['description']}")
            
            if state == 'succeeded':
                return status
            elif state == 'failed':
                error_msg = status.get('description', 'Unknown error')
                raise Exception(f"Operation failed: {error_msg}")
            
            time.sleep(10)  # Wait 10 seconds before checking again
            
        except requests.exceptions.RequestException as e:
            print(f"  Error checking status: {e}")
            time.sleep(5)
    
    raise Exception(f"Operation timed out after {timeout} seconds")


def provision_docker_cluster(client: KafkaOpsClient, cluster_name: str):
    """Provision a Docker-based Kafka cluster.
    
    Args:
        client: Kafka Ops client
        cluster_name: Name for the cluster
        
    Returns:
        Instance ID and connection info
    """
    print(f"🐳 Provisioning Docker Kafka cluster: {cluster_name}")
    
    instance_id = f"docker-{cluster_name}-{int(time.time())}"
    
    service_config = {
        "service_id": "kafka-cluster",
        "plan_id": "docker-small",
        "parameters": {
            "cluster_name": cluster_name,
            "broker_count": 1,
            "zookeeper_count": 1,
            "storage_size": "1Gi",
            "network_name": "kafka-network"
        }
    }
    
    # Start provisioning
    print(f"  Instance ID: {instance_id}")
    provision_response = client.provision_service(instance_id, service_config)
    print(f"  Provisioning started: {provision_response.get('operation', 'N/A')}")
    
    # Wait for completion
    print("  Waiting for provisioning to complete...")
    final_status = wait_for_operation(client, instance_id)
    
    print("  ✅ Docker cluster provisioned successfully!")
    
    # Get connection info
    connection_info = final_status.get('connection_info', {})
    if connection_info:
        print(f"  Bootstrap servers: {connection_info.get('bootstrap_servers', 'N/A')}")
    
    return instance_id, connection_info


def provision_kubernetes_cluster(client: KafkaOpsClient, cluster_name: str):
    """Provision a Kubernetes-based Kafka cluster.
    
    Args:
        client: Kafka Ops client
        cluster_name: Name for the cluster
        
    Returns:
        Instance ID and connection info
    """
    print(f"☸️  Provisioning Kubernetes Kafka cluster: {cluster_name}")
    
    instance_id = f"k8s-{cluster_name}-{int(time.time())}"
    
    service_config = {
        "service_id": "kafka-cluster",
        "plan_id": "kubernetes-medium",
        "parameters": {
            "cluster_name": cluster_name,
            "namespace": "kafka-clusters",
            "broker_count": 3,
            "zookeeper_count": 3,
            "storage_class": "standard",
            "storage_size": "5Gi",
            "resource_limits": {
                "memory": "1Gi",
                "cpu": "500m"
            },
            "resource_requests": {
                "memory": "512Mi",
                "cpu": "250m"
            }
        }
    }
    
    # Start provisioning
    print(f"  Instance ID: {instance_id}")
    provision_response = client.provision_service(instance_id, service_config)
    print(f"  Provisioning started: {provision_response.get('operation', 'N/A')}")
    
    # Wait for completion (K8s takes longer)
    print("  Waiting for provisioning to complete (this may take several minutes)...")
    final_status = wait_for_operation(client, instance_id, timeout=600)  # 10 minutes
    
    print("  ✅ Kubernetes cluster provisioned successfully!")
    
    # Get connection info
    connection_info = final_status.get('connection_info', {})
    if connection_info:
        print(f"  Bootstrap servers: {connection_info.get('bootstrap_servers', 'N/A')}")
        print(f"  Namespace: {connection_info.get('namespace', 'N/A')}")
    
    return instance_id, connection_info


def provision_terraform_cluster(client: KafkaOpsClient, cluster_name: str, cloud_provider: str = "aws"):
    """Provision a Terraform-based Kafka cluster.
    
    Args:
        client: Kafka Ops client
        cluster_name: Name for the cluster
        cloud_provider: Cloud provider (aws, gcp, azure)
        
    Returns:
        Instance ID and connection info
    """
    print(f"🏗️  Provisioning Terraform Kafka cluster: {cluster_name} on {cloud_provider.upper()}")
    
    instance_id = f"terraform-{cluster_name}-{int(time.time())}"
    
    service_config = {
        "service_id": "kafka-cluster",
        "plan_id": f"terraform-{cloud_provider}-medium",
        "parameters": {
            "cluster_name": cluster_name,
            "cloud_provider": cloud_provider,
            "region": "us-west-2" if cloud_provider == "aws" else "us-central1",
            "instance_type": "t3.medium" if cloud_provider == "aws" else "n1-standard-2",
            "broker_count": 3,
            "zookeeper_count": 3,
            "storage_size": "50",
            "storage_type": "gp3" if cloud_provider == "aws" else "pd-ssd",
            "vpc_cidr": "10.0.0.0/16",
            "enable_monitoring": True,
            "enable_encryption": True
        }
    }
    
    # Start provisioning
    print(f"  Instance ID: {instance_id}")
    provision_response = client.provision_service(instance_id, service_config)
    print(f"  Provisioning started: {provision_response.get('operation', 'N/A')}")
    
    # Wait for completion (Terraform takes the longest)
    print("  Waiting for provisioning to complete (this may take 10-15 minutes)...")
    final_status = wait_for_operation(client, instance_id, timeout=1200)  # 20 minutes
    
    print("  ✅ Terraform cluster provisioned successfully!")
    
    # Get connection info
    connection_info = final_status.get('connection_info', {})
    terraform_outputs = final_status.get('terraform_outputs', {})
    
    if connection_info:
        print(f"  Bootstrap servers: {connection_info.get('bootstrap_servers', 'N/A')}")
    
    if terraform_outputs:
        print(f"  VPC ID: {terraform_outputs.get('vpc_id', 'N/A')}")
        print(f"  Security Group: {terraform_outputs.get('security_group_id', 'N/A')}")
    
    return instance_id, connection_info


def create_sample_topics(client: KafkaOpsClient):
    """Create sample topics for testing.
    
    Args:
        client: Kafka Ops client
        
    Returns:
        List of created topic names
    """
    print("📝 Creating sample topics...")
    
    topics = [
        {
            "name": "user-events",
            "partitions": 3,
            "replication_factor": 1,
            "config": {
                "retention.ms": "604800000",  # 7 days
                "cleanup.policy": "delete"
            }
        },
        {
            "name": "order-events",
            "partitions": 6,
            "replication_factor": 1,
            "config": {
                "retention.ms": "2592000000",  # 30 days
                "cleanup.policy": "delete",
                "compression.type": "snappy"
            }
        },
        {
            "name": "audit-logs",
            "partitions": 1,
            "replication_factor": 1,
            "config": {
                "retention.ms": "31536000000",  # 1 year
                "cleanup.policy": "delete",
                "compression.type": "gzip"
            }
        }
    ]
    
    created_topics = []
    
    for topic_config in topics:
        try:
            response = client.create_topic(topic_config)
            if response.get('status') == 'success':
                topic_name = topic_config['name']
                created_topics.append(topic_name)
                print(f"  ✅ Created topic: {topic_name}")
            else:
                print(f"  ❌ Failed to create topic: {topic_config['name']}")
        except Exception as e:
            print(f"  ❌ Error creating topic {topic_config['name']}: {e}")
    
    return created_topics


def cleanup_resources(client: KafkaOpsClient, instance_id: str, topic_names: list):
    """Clean up created resources.
    
    Args:
        client: Kafka Ops client
        instance_id: Service instance ID to deprovision
        topic_names: List of topic names to delete
    """
    print("🧹 Cleaning up resources...")
    
    # Delete topics
    for topic_name in topic_names:
        try:
            response = client.delete_topic(topic_name)
            if response.get('status') == 'success':
                print(f"  ✅ Deleted topic: {topic_name}")
            else:
                print(f"  ❌ Failed to delete topic: {topic_name}")
        except Exception as e:
            print(f"  ❌ Error deleting topic {topic_name}: {e}")
    
    # Deprovision cluster
    try:
        print(f"  Deprovisioning cluster: {instance_id}")
        client.deprovision_service(instance_id)
        
        # Wait for deprovisioning to complete
        print("  Waiting for deprovisioning to complete...")
        wait_for_operation(client, instance_id, timeout=300)
        print("  ✅ Cluster deprovisioned successfully!")
        
    except Exception as e:
        print(f"  ❌ Error deprovisioning cluster: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Day 1 Kafka Cluster Provisioning")
    parser.add_argument("--provider", choices=["docker", "kubernetes", "terraform"], 
                       default="docker", help="Provisioning provider")
    parser.add_argument("--cluster-name", default="my-first-kafka", 
                       help="Cluster name")
    parser.add_argument("--cloud-provider", choices=["aws", "gcp", "azure"], 
                       default="aws", help="Cloud provider for Terraform")
    parser.add_argument("--api-url", default="http://localhost:8000", 
                       help="API base URL")
    parser.add_argument("--api-key", default="admin-secret-key", 
                       help="API key")
    parser.add_argument("--skip-topics", action="store_true", 
                       help="Skip creating sample topics")
    parser.add_argument("--skip-cleanup", action="store_true", 
                       help="Skip cleanup (leave resources running)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be done without executing")
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No actual resources will be created")
        print(f"Would provision {args.provider} cluster named '{args.cluster_name}'")
        if args.provider == "terraform":
            print(f"Would use cloud provider: {args.cloud_provider}")
        if not args.skip_topics:
            print("Would create sample topics: user-events, order-events, audit-logs")
        if not args.skip_cleanup:
            print("Would clean up all resources after demonstration")
        return
    
    print("🚀 Kafka Self-Service Platform - Day 1 Provisioning")
    print("=" * 60)
    
    # Initialize client
    client = KafkaOpsClient(args.api_url, args.api_key)
    
    try:
        # Check system health
        print("🏥 Checking system health...")
        health = client.get_health()
        print(f"  System status: {health.get('overall_status', 'unknown')}")
        
        # Get service catalog
        print("📋 Getting service catalog...")
        catalog = client.get_catalog()
        services = catalog.get('services', [])
        print(f"  Available services: {len(services)}")
        
        # Provision cluster based on provider
        instance_id = None
        connection_info = {}
        
        if args.provider == "docker":
            instance_id, connection_info = provision_docker_cluster(client, args.cluster_name)
        elif args.provider == "kubernetes":
            instance_id, connection_info = provision_kubernetes_cluster(client, args.cluster_name)
        elif args.provider == "terraform":
            instance_id, connection_info = provision_terraform_cluster(
                client, args.cluster_name, args.cloud_provider
            )
        
        # Create sample topics
        created_topics = []
        if not args.skip_topics:
            created_topics = create_sample_topics(client)
            
            # List all topics
            print("📋 Listing all topics...")
            topics_response = client.list_topics()
            topics = topics_response.get('topics', [])
            print(f"  Total topics: {len(topics)}")
            for topic in topics:
                print(f"    - {topic.get('name', 'N/A')} ({topic.get('partitions', 0)} partitions)")
        
        # Show final status
        print("\\n🎉 Day 1 provisioning completed successfully!")
        print("=" * 60)
        print(f"Cluster ID: {instance_id}")
        if connection_info.get('bootstrap_servers'):
            print(f"Bootstrap Servers: {connection_info['bootstrap_servers']}")
        print(f"Provider: {args.provider}")
        print(f"Topics Created: {len(created_topics)}")
        
        # Cleanup if requested
        if not args.skip_cleanup:
            print("\\n⏳ Waiting 30 seconds before cleanup (Ctrl+C to skip)...")
            try:
                time.sleep(30)
                cleanup_resources(client, instance_id, created_topics)
            except KeyboardInterrupt:
                print("\\n⏭️  Cleanup skipped by user")
                print(f"To clean up later, run:")
                print(f"  curl -X DELETE -H 'X-API-Key: {args.api_key}' {args.api_url}/v2/service_instances/{instance_id}")
        else:
            print("\\n💡 Resources left running. To clean up:")
            print(f"  curl -X DELETE -H 'X-API-Key: {args.api_key}' {args.api_url}/v2/service_instances/{instance_id}")
        
    except KeyboardInterrupt:
        print("\\n⏹️  Operation interrupted by user")
    except Exception as e:
        print(f"\\n❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())