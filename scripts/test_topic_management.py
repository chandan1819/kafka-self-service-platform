#!/usr/bin/env python3
"""Script to test topic management service."""

import sys
import asyncio
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.services.topic_management import get_topic_service
from kafka_ops_agent.clients.kafka_client import get_client_manager
from kafka_ops_agent.models.cluster import ConnectionInfo
from kafka_ops_agent.models.topic import TopicConfig
from kafka_ops_agent.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


async def setup_test_cluster():
    """Set up test cluster for topic operations."""
    
    print("🔗 Setting up test cluster connection...")
    
    # Get client manager and register cluster
    client_manager = get_client_manager()
    
    connection_info = ConnectionInfo(
        bootstrap_servers=['localhost:9092'],
        zookeeper_connect='localhost:2181'
    )
    
    success = client_manager.register_cluster('test-cluster', connection_info)
    if success:
        print("✅ Test cluster registered successfully")
        return True
    else:
        print("❌ Failed to register test cluster")
        return False


async def test_topic_creation():
    """Test topic creation functionality."""
    
    print("\n📝 Testing Topic Creation")
    
    try:
        topic_service = await get_topic_service()
        
        # Create test topic
        topic_config = TopicConfig(
            name="topic-mgmt-test-1",
            partitions=3,
            replication_factor=1,
            retention_ms=3600000,  # 1 hour
            custom_configs={
                'cleanup.policy': 'delete',
                'compression.type': 'snappy'
            }
        )
        
        print(f"   Creating topic: {topic_config.name}")
        result = await topic_service.create_topic("test-cluster", topic_config, "test-user")
        
        if result.success:
            print("✅ Topic created successfully")
            print(f"   Message: {result.message}")
            if result.details:
                print(f"   Details: {result.details}")
            return True
        else:
            print(f"❌ Topic creation failed: {result.message}")
            print(f"   Error code: {result.error_code}")
            return False
            
    except Exception as e:
        print(f"❌ Exception during topic creation: {e}")
        return False


async def test_topic_listing():
    """Test topic listing functionality."""
    
    print("\n📋 Testing Topic Listing")
    
    try:
        topic_service = await get_topic_service()
        
        # List topics
        print("   Listing all topics...")
        topics = await topic_service.list_topics("test-cluster", include_internal=False, user_id="test-user")
        
        print(f"✅ Found {len(topics)} topics:")
        for topic in topics:
            print(f"   - {topic.name}: {topic.partitions} partitions, RF={topic.replication_factor}")
        
        return True
        
    except Exception as e:
        print(f"❌ Exception during topic listing: {e}")
        return False


async def test_topic_description():
    """Test topic description functionality."""
    
    print("\n📖 Testing Topic Description")
    
    try:
        topic_service = await get_topic_service()
        
        # Describe test topic
        topic_name = "topic-mgmt-test-1"
        print(f"   Describing topic: {topic_name}")
        
        details = await topic_service.describe_topic("test-cluster", topic_name, "test-user")
        
        if details:
            print("✅ Topic described successfully:")
            print(f"   Name: {details.name}")
            print(f"   Partitions: {details.partitions}")
            print(f"   Replication Factor: {details.replication_factor}")
            print(f"   Configurations: {len(details.configs)} settings")
            
            # Show some key configurations
            key_configs = ['retention.ms', 'cleanup.policy', 'compression.type']
            for config_key in key_configs:
                if config_key in details.configs:
                    print(f"     {config_key}: {details.configs[config_key]}")
            
            return True
        else:
            print(f"❌ Failed to describe topic {topic_name}")
            return False
            
    except Exception as e:
        print(f"❌ Exception during topic description: {e}")
        return False


async def test_topic_config_update():
    """Test topic configuration update."""
    
    print("\n⚙️  Testing Topic Configuration Update")
    
    try:
        topic_service = await get_topic_service()
        
        # Update topic configuration
        topic_name = "topic-mgmt-test-1"
        new_configs = {
            'retention.ms': '7200000',  # 2 hours
            'max.message.bytes': '2097152'  # 2MB
        }
        
        print(f"   Updating config for topic: {topic_name}")
        print(f"   New configs: {new_configs}")
        
        result = await topic_service.update_topic_config(
            "test-cluster", 
            topic_name, 
            new_configs, 
            "test-user"
        )
        
        if result.success:
            print("✅ Topic configuration updated successfully")
            print(f"   Message: {result.message}")
            return True
        else:
            print(f"❌ Configuration update failed: {result.message}")
            print(f"   Error code: {result.error_code}")
            return False
            
    except Exception as e:
        print(f"❌ Exception during config update: {e}")
        return False


async def test_bulk_operations():
    """Test bulk topic operations."""
    
    print("\n🔄 Testing Bulk Operations")
    
    try:
        topic_service = await get_topic_service()
        
        # Create multiple topics
        topic_configs = [
            TopicConfig(name="bulk-test-1", partitions=2, replication_factor=1),
            TopicConfig(name="bulk-test-2", partitions=4, replication_factor=1),
            TopicConfig(name="bulk-test-3", partitions=1, replication_factor=1)
        ]
        
        print(f"   Creating {len(topic_configs)} topics in bulk...")
        results = await topic_service.bulk_create_topics("test-cluster", topic_configs, "test-user")
        
        successful = sum(1 for result in results.values() if result.success)
        failed = len(results) - successful
        
        print(f"✅ Bulk creation completed: {successful} successful, {failed} failed")
        
        for topic_name, result in results.items():
            status = "✅" if result.success else "❌"
            print(f"   {status} {topic_name}: {result.message}")
        
        return successful > 0
        
    except Exception as e:
        print(f"❌ Exception during bulk operations: {e}")
        return False


async def test_cluster_info():
    """Test cluster information retrieval."""
    
    print("\n📊 Testing Cluster Information")
    
    try:
        topic_service = await get_topic_service()
        
        print("   Getting cluster information...")
        cluster_info = await topic_service.get_cluster_info("test-cluster")
        
        if cluster_info:
            print("✅ Cluster information retrieved:")
            print(f"   Cluster ID: {cluster_info.get('cluster_id', 'Unknown')}")
            print(f"   Broker Count: {cluster_info.get('broker_count', 'Unknown')}")
            print(f"   Topic Count: {cluster_info.get('topic_count', 'Unknown')}")
            
            if 'brokers' in cluster_info:
                print("   Brokers:")
                for broker in cluster_info['brokers'][:3]:  # Show first 3 brokers
                    print(f"     - ID {broker['id']}: {broker['host']}:{broker['port']}")
            
            return True
        else:
            print("⚠️  No cluster information available (cluster may not be running)")
            return False
            
    except Exception as e:
        print(f"❌ Exception getting cluster info: {e}")
        return False


async def cleanup_test_topics():
    """Clean up test topics."""
    
    print("\n🧹 Cleaning Up Test Topics")
    
    try:
        topic_service = await get_topic_service()
        
        # Topics to clean up
        test_topics = [
            "topic-mgmt-test-1",
            "bulk-test-1",
            "bulk-test-2", 
            "bulk-test-3"
        ]
        
        print(f"   Deleting {len(test_topics)} test topics...")
        results = await topic_service.bulk_delete_topics("test-cluster", test_topics, "test-user")
        
        successful = sum(1 for result in results.values() if result.success)
        
        print(f"✅ Cleanup completed: {successful} topics deleted")
        
        for topic_name, result in results.items():
            if result.success:
                print(f"   ✅ Deleted {topic_name}")
            else:
                print(f"   ⚠️  {topic_name}: {result.message}")
        
        return True
        
    except Exception as e:
        print(f"❌ Exception during cleanup: {e}")
        return False


async def main():
    """Run all topic management tests."""
    
    print("🧪 Topic Management Service Tests")
    print("=" * 50)
    
    try:
        # Setup test cluster
        if not await setup_test_cluster():
            print("❌ Failed to setup test cluster")
            return False
        
        # Check if user wants to run tests that require a running cluster
        response = input("\n❓ Do you have a running Kafka cluster at localhost:9092? [y/N]: ")
        if response.lower() not in ['y', 'yes']:
            print("ℹ️  Skipping tests that require a running cluster")
            print("💡 Start a cluster with: python scripts/create_local_cluster.py")
            return True
        
        # Run tests
        tests = [
            ("Topic Creation", test_topic_creation),
            ("Topic Listing", test_topic_listing),
            ("Topic Description", test_topic_description),
            ("Config Update", test_topic_config_update),
            ("Bulk Operations", test_bulk_operations),
            ("Cluster Info", test_cluster_info)
        ]
        
        results = []
        for test_name, test_func in tests:
            try:
                result = await test_func()
                results.append((test_name, result))
            except Exception as e:
                print(f"❌ {test_name} failed with exception: {e}")
                results.append((test_name, False))
        
        # Cleanup
        await cleanup_test_topics()
        
        # Summary
        print("\n📊 Test Results Summary")
        print("-" * 30)
        
        passed = 0
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} {test_name}")
            if result:
                passed += 1
        
        print(f"\nTotal: {passed}/{len(results)} tests passed")
        
        if passed == len(results):
            print("🎉 All tests passed!")
            return True
        else:
            print("⚠️  Some tests failed")
            return False
        
    except KeyboardInterrupt:
        print("\n👋 Tests interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Tests failed with exception: {e}")
        logger.exception("Test failed")
        return False
    
    finally:
        # Cleanup services
        try:
            from kafka_ops_agent.services.topic_management import close_topic_service
            from kafka_ops_agent.clients.kafka_client import close_client_manager
            
            close_topic_service()
            close_client_manager()
            print("📦 Services closed")
        except Exception as e:
            print(f"⚠️  Warning: Failed to close services: {e}")


if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        sys.exit(1)