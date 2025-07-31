#!/usr/bin/env python3
"""Script to test Kafka client connection management."""

import sys
import asyncio
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.clients.kafka_client import get_client_manager
from kafka_ops_agent.clients.admin_operations import KafkaAdminOperations
from kafka_ops_agent.models.cluster import ConnectionInfo
from kafka_ops_agent.models.topic import TopicConfig
from kafka_ops_agent.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def test_client_manager():
    """Test the Kafka client manager."""
    
    print("🧪 Testing Kafka Client Manager")
    
    # Get client manager
    print("📦 Initializing client manager...")
    client_manager = get_client_manager()
    print("✅ Client manager initialized")
    
    # Test connection info
    connection_info = ConnectionInfo(
        bootstrap_servers=['localhost:9092'],
        zookeeper_connect='localhost:2181'
    )
    
    # Register cluster
    print("🔗 Registering test cluster...")
    success = client_manager.register_cluster('test-cluster', connection_info)
    if success:
        print("✅ Cluster registered successfully")
    else:
        print("❌ Failed to register cluster")
        return False
    
    # Get connection
    print("🔌 Getting client connection...")
    connection = client_manager.get_connection('test-cluster')
    if connection:
        print("✅ Connection obtained successfully")
        print(f"   Connection ID: {connection.connection_id}")
    else:
        print("❌ Failed to get connection")
        return False
    
    # Test admin operations
    print("⚙️  Testing admin operations...")
    try:
        admin_ops = KafkaAdminOperations(connection)
        
        # Get cluster info
        print("📊 Getting cluster information...")
        cluster_info = admin_ops.get_cluster_info()
        if cluster_info:
            print("✅ Cluster info retrieved:")
            print(f"   Broker count: {cluster_info.get('broker_count', 'Unknown')}")
            print(f"   Topic count: {cluster_info.get('topic_count', 'Unknown')}")
        else:
            print("⚠️  Could not retrieve cluster info (cluster may not be running)")
        
        # List topics
        print("📋 Listing topics...")
        topics = admin_ops.list_topics()
        print(f"✅ Found {len(topics)} topics:")
        for topic in topics[:5]:  # Show first 5 topics
            print(f"   - {topic.name}: {topic.partitions} partitions, RF={topic.replication_factor}")
        
        if len(topics) > 5:
            print(f"   ... and {len(topics) - 5} more topics")
        
    except Exception as e:
        print(f"⚠️  Admin operations failed (expected if no cluster running): {e}")
    
    # Test health check
    print("💚 Testing health check...")
    try:
        health = connection.health_check()
        print(f"   Health status: {'Healthy' if health else 'Unhealthy'}")
    except Exception as e:
        print(f"   Health check failed: {e}")
    
    # Get connection stats
    print("📈 Getting connection statistics...")
    stats = connection.get_stats()
    print(f"   Use count: {stats['use_count']}")
    print(f"   Age: {stats['age_seconds']:.1f} seconds")
    print(f"   Idle: {stats['idle_seconds']:.1f} seconds")
    
    # Get manager stats
    print("📊 Getting manager statistics...")
    manager_stats = client_manager.get_stats()
    print(f"   Total connections: {manager_stats['total_connections']}")
    print(f"   Pool utilization: {manager_stats['pool_utilization']:.1%}")
    print(f"   Registered clusters: {manager_stats['registered_clusters']}")
    
    # Test multiple connections
    print("🔗 Testing connection reuse...")
    connection2 = client_manager.get_connection('test-cluster')
    if connection2 == connection:
        print("✅ Connection reused successfully")
    else:
        print("❌ Connection not reused")
    
    # Test health check all
    print("💚 Testing health check all...")
    health_results = client_manager.health_check_all()
    for cluster_id, is_healthy in health_results.items():
        print(f"   {cluster_id}: {'Healthy' if is_healthy else 'Unhealthy'}")
    
    print("\n🎉 Client manager tests completed!")
    return True


def test_topic_operations():
    """Test topic operations if cluster is available."""
    
    print("\n🧪 Testing Topic Operations")
    
    try:
        # Get client manager and connection
        client_manager = get_client_manager()
        connection = client_manager.get_connection('test-cluster')
        
        if not connection:
            print("❌ No connection available for topic operations")
            return False
        
        admin_ops = KafkaAdminOperations(connection)
        
        # Test topic creation
        test_topic_name = "kafka-ops-test-topic"
        print(f"📝 Creating test topic: {test_topic_name}")
        
        topic_config = TopicConfig(
            name=test_topic_name,
            partitions=3,
            replication_factor=1,
            retention_ms=3600000  # 1 hour
        )
        
        success = admin_ops.create_topic(topic_config)
        if success:
            print("✅ Topic created successfully")
        else:
            print("⚠️  Topic creation failed (may already exist)")
        
        # Test topic description
        print(f"📖 Describing topic: {test_topic_name}")
        topic_details = admin_ops.describe_topic(test_topic_name)
        if topic_details:
            print("✅ Topic described successfully:")
            print(f"   Partitions: {topic_details.partitions}")
            print(f"   Replication factor: {topic_details.replication_factor}")
            print(f"   Configs: {len(topic_details.configs)} settings")
        else:
            print("❌ Failed to describe topic")
        
        # Test topic config update
        print(f"⚙️  Updating topic configuration...")
        config_success = admin_ops.update_topic_config(
            test_topic_name, 
            {'retention.ms': '7200000'}  # 2 hours
        )
        if config_success:
            print("✅ Topic configuration updated")
        else:
            print("❌ Failed to update topic configuration")
        
        # Wait for user input before cleanup
        input("\n⏸️  Press Enter to delete the test topic...")
        
        # Test topic deletion
        print(f"🗑️  Deleting test topic: {test_topic_name}")
        delete_success = admin_ops.delete_topic(test_topic_name)
        if delete_success:
            print("✅ Topic deleted successfully")
        else:
            print("❌ Failed to delete topic")
        
        return True
        
    except Exception as e:
        print(f"❌ Topic operations failed: {e}")
        return False


def main():
    """Run all client tests."""
    
    print("🚀 Kafka Client Connection Management Tests")
    print("=" * 50)
    
    try:
        # Test client manager
        if not test_client_manager():
            return False
        
        # Ask user if they want to test topic operations
        response = input("\n❓ Do you want to test topic operations? (requires running Kafka cluster) [y/N]: ")
        if response.lower() in ['y', 'yes']:
            if not test_topic_operations():
                return False
        
        print("\n🎉 All tests completed successfully!")
        return True
        
    except KeyboardInterrupt:
        print("\n👋 Tests interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Tests failed with exception: {e}")
        logger.exception("Test failed")
        return False
    
    finally:
        # Cleanup
        try:
            from kafka_ops_agent.clients.kafka_client import close_client_manager
            close_client_manager()
            print("📦 Client manager closed")
        except Exception as e:
            print(f"⚠️  Warning: Failed to close client manager: {e}")


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)