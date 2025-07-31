#!/usr/bin/env python3
"""Script to create a local Kafka cluster using Docker provider."""

import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.providers.docker_provider import DockerProvider
from kafka_ops_agent.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def create_local_cluster():
    """Create a local Kafka cluster for development."""
    
    print("🚀 Creating local Kafka cluster...")
    
    # Initialize Docker provider
    try:
        provider = DockerProvider()
        print("✅ Docker provider initialized")
    except Exception as e:
        print(f"❌ Failed to initialize Docker provider: {e}")
        return False
    
    # Cluster configuration
    instance_id = "local-kafka-dev"
    config = {
        'cluster_size': 1,
        'replication_factor': 1,
        'partition_count': 3,
        'retention_hours': 24,  # 1 day for local dev
        'storage_size_gb': 5,
        'enable_ssl': False,
        'enable_sasl': False,
        'custom_properties': {
            'auto.create.topics.enable': 'true',
            'delete.topic.enable': 'true'
        }
    }
    
    print(f"📋 Configuration: {config}")
    
    # Provision cluster
    print("⏳ Provisioning cluster (this may take a few minutes)...")
    result = provider.provision_cluster(instance_id, config)
    
    if result.status.value == "succeeded":
        print("✅ Cluster provisioned successfully!")
        print(f"📊 Instance ID: {result.instance_id}")
        
        if result.connection_info:
            print("🔗 Connection Information:")
            print(f"   Bootstrap Servers: {result.connection_info['bootstrap_servers']}")
            print(f"   Zookeeper: {result.connection_info['zookeeper_connect']}")
            
            print("\n📝 Usage Examples:")
            print("   # List topics")
            print(f"   kafka-topics --bootstrap-server {result.connection_info['bootstrap_servers'][0]} --list")
            print("\n   # Create a topic")
            print(f"   kafka-topics --bootstrap-server {result.connection_info['bootstrap_servers'][0]} --create --topic test-topic --partitions 3 --replication-factor 1")
            print("\n   # Produce messages")
            print(f"   kafka-console-producer --bootstrap-server {result.connection_info['bootstrap_servers'][0]} --topic test-topic")
            print("\n   # Consume messages")
            print(f"   kafka-console-consumer --bootstrap-server {result.connection_info['bootstrap_servers'][0]} --topic test-topic --from-beginning")
        
        print(f"\n🛑 To stop the cluster, run: python scripts/stop_local_cluster.py")
        return True
        
    else:
        print(f"❌ Failed to provision cluster: {result.error_message}")
        return False


def check_cluster_status():
    """Check the status of the local cluster."""
    try:
        provider = DockerProvider()
        instance_id = "local-kafka-dev"
        
        status = provider.get_cluster_status(instance_id)
        print(f"📊 Cluster Status: {status.value}")
        
        if status.value == "succeeded":
            connection_info = provider.get_connection_info(instance_id)
            if connection_info:
                print("🔗 Connection Information:")
                print(f"   Bootstrap Servers: {connection_info['bootstrap_servers']}")
                print(f"   Zookeeper: {connection_info['zookeeper_connect']}")
            
            health = provider.health_check(instance_id)
            print(f"💚 Health Check: {'Healthy' if health else 'Unhealthy'}")
        
        return status.value == "succeeded"
        
    except Exception as e:
        print(f"❌ Error checking cluster status: {e}")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage local Kafka cluster")
    parser.add_argument("--status", action="store_true", help="Check cluster status")
    
    args = parser.parse_args()
    
    if args.status:
        check_cluster_status()
    else:
        create_local_cluster()