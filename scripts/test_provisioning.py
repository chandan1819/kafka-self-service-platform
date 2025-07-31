#!/usr/bin/env python3
"""Script to test the provisioning service with local setup."""

import sys
import asyncio
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.services.provisioning import ProvisioningService
from kafka_ops_agent.storage.factory import StorageFactory
from kafka_ops_agent.logging_config import setup_logging
from kafka_ops_agent.config import config

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


async def test_provisioning_workflow():
    """Test the complete provisioning workflow."""
    
    print("🧪 Testing Kafka Provisioning Service")
    
    # Initialize storage
    print("📦 Initializing storage...")
    try:
        metadata_store, audit_store = await StorageFactory.create_stores()
        print("✅ Storage initialized")
    except Exception as e:
        print(f"❌ Failed to initialize storage: {e}")
        return False
    
    # Initialize provisioning service
    print("🚀 Initializing provisioning service...")
    try:
        provisioning_service = ProvisioningService(metadata_store, audit_store)
        print("✅ Provisioning service initialized")
    except Exception as e:
        print(f"❌ Failed to initialize provisioning service: {e}")
        return False
    
    # Test parameters
    instance_id = "test-provisioning-cluster"
    parameters = {
        'cluster_size': 1,
        'replication_factor': 1,
        'partition_count': 3,
        'retention_hours': 24,
        'runtime_provider': 'docker',
        'custom_properties': {
            'auto.create.topics.enable': 'true',
            'delete.topic.enable': 'true'
        }
    }
    
    try:
        # Test provisioning
        print(f"⏳ Provisioning cluster {instance_id}...")
        result = await provisioning_service.provision_cluster(
            instance_id=instance_id,
            service_id="kafka-service",
            plan_id="basic",
            organization_guid="test-org",
            space_guid="test-space",
            parameters=parameters,
            user_id="test-user"
        )
        
        if result.status.value == "succeeded":
            print("✅ Cluster provisioned successfully!")
            print(f"📊 Instance ID: {result.instance_id}")
            
            if result.connection_info:
                print("🔗 Connection Information:")
                print(f"   Bootstrap Servers: {result.connection_info['bootstrap_servers']}")
                print(f"   Zookeeper: {result.connection_info['zookeeper_connect']}")
        else:
            print(f"❌ Provisioning failed: {result.error_message}")
            return False
        
        # Test status check
        print("\n📊 Checking cluster status...")
        status = await provisioning_service.get_cluster_status(instance_id)
        print(f"   Status: {status.value if status else 'Unknown'}")
        
        # Test connection info
        print("\n🔗 Getting connection information...")
        connection_info = await provisioning_service.get_connection_info(instance_id)
        if connection_info:
            print(f"   Bootstrap Servers: {connection_info['bootstrap_servers']}")
            print(f"   Zookeeper: {connection_info['zookeeper_connect']}")
        else:
            print("   No connection info available")
        
        # Test health check
        print("\n💚 Performing health check...")
        health = await provisioning_service.health_check(instance_id)
        print(f"   Health: {'Healthy' if health else 'Unhealthy'}")
        
        # Test listing instances
        print("\n📋 Listing all instances...")
        instances = await provisioning_service.list_instances()
        print(f"   Found {len(instances)} instances:")
        for instance in instances:
            print(f"   - {instance.instance_id}: {instance.status.value} ({instance.runtime_provider.value})")
        
        # Wait for user input before cleanup
        input("\n⏸️  Press Enter to deprovision the cluster...")
        
        # Test deprovisioning
        print(f"\n🛑 Deprovisioning cluster {instance_id}...")
        result = await provisioning_service.deprovision_cluster(instance_id, "test-user")
        
        if result.status.value == "succeeded":
            print("✅ Cluster deprovisioned successfully!")
        else:
            print(f"❌ Deprovisioning failed: {result.error_message}")
            return False
        
        print("\n🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        logger.exception("Test failed")
        return False
    
    finally:
        # Cleanup storage
        try:
            await metadata_store.close()
            print("📦 Storage connections closed")
        except Exception as e:
            print(f"⚠️  Warning: Failed to close storage: {e}")


async def test_audit_logs():
    """Test audit logging functionality."""
    
    print("\n📝 Testing audit logs...")
    
    try:
        metadata_store, audit_store = await StorageFactory.create_stores()
        
        # Get audit logs for the test instance
        logs = await audit_store.get_audit_logs("test-provisioning-cluster")
        
        print(f"📋 Found {len(logs)} audit log entries:")
        for log in logs:
            print(f"   - {log['timestamp']}: {log['operation']} by {log['user_id']}")
            if log['details']:
                print(f"     Details: {log['details']}")
        
        await metadata_store.close()
        
    except Exception as e:
        print(f"❌ Failed to retrieve audit logs: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test provisioning service")
    parser.add_argument("--audit-only", action="store_true", help="Only show audit logs")
    
    args = parser.parse_args()
    
    if args.audit_only:
        asyncio.run(test_audit_logs())
    else:
        success = asyncio.run(test_provisioning_workflow())
        if not success:
            sys.exit(1)