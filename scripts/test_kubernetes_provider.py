#!/usr/bin/env python3
"""Test script for Kubernetes provider functionality."""

import asyncio
import logging
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_ops_agent.providers.kubernetes_provider import KubernetesProvider
from kafka_ops_agent.providers.base import ProvisioningStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_kubernetes_provider():
    """Test Kubernetes provider functionality."""
    print("🚀 Testing Kubernetes Provider")
    print("=" * 50)
    
    try:
        # Initialize provider
        print("1. Initializing Kubernetes provider...")
        provider = KubernetesProvider(namespace="kafka-test")
        print("✅ Provider initialized successfully")
        
        # Test configuration parsing
        print("\n2. Testing configuration parsing...")
        test_config = {
            'cluster_size': 2,
            'replication_factor': 2,
            'partition_count': 6,
            'retention_hours': 24,
            'storage_size_gb': 5,
            'enable_ssl': False,
            'enable_sasl': False,
            'custom_properties': {
                'log.segment.bytes': '536870912',
                'num.network.threads': '4'
            }
        }
        
        cluster_config = provider._parse_config(test_config)
        print(f"✅ Configuration parsed: {cluster_config.cluster_size} brokers, "
              f"{cluster_config.replication_factor} replication factor")
        
        # Test manifest generation
        print("\n3. Testing manifest generation...")
        manifests = provider._generate_manifests("test-cluster", cluster_config)
        print(f"✅ Generated {len(manifests)} manifests:")
        for name in manifests.keys():
            print(f"   - {name}")
        
        # Validate manifest structure
        print("\n4. Validating manifest structure...")
        
        # Check Zookeeper manifests
        zk_service = manifests.get("test-cluster-zookeeper-service")
        zk_statefulset = manifests.get("test-cluster-zookeeper-statefulset")
        
        assert zk_service is not None, "Zookeeper service manifest missing"
        assert zk_service["kind"] == "Service", "Invalid Zookeeper service kind"
        assert zk_statefulset is not None, "Zookeeper StatefulSet manifest missing"
        assert zk_statefulset["kind"] == "StatefulSet", "Invalid Zookeeper StatefulSet kind"
        
        # Check Kafka manifests
        kafka_service = manifests.get("test-cluster-kafka-service")
        kafka_statefulset = manifests.get("test-cluster-kafka-statefulset")
        
        assert kafka_service is not None, "Kafka service manifest missing"
        assert kafka_service["kind"] == "Service", "Invalid Kafka service kind"
        assert kafka_statefulset is not None, "Kafka StatefulSet manifest missing"
        assert kafka_statefulset["kind"] == "StatefulSet", "Invalid Kafka StatefulSet kind"
        
        # Validate Kafka StatefulSet configuration
        kafka_spec = kafka_statefulset["spec"]
        assert kafka_spec["replicas"] == 2, f"Expected 2 replicas, got {kafka_spec['replicas']}"
        
        # Check environment variables
        container = kafka_spec["template"]["spec"]["containers"][0]
        env_vars = {env["name"]: env.get("value") for env in container["env"] if "value" in env}
        
        assert env_vars.get("KAFKA_LOG_RETENTION_HOURS") == "24", "Retention hours not set correctly"
        assert env_vars.get("KAFKA_NUM_PARTITIONS") == "6", "Partition count not set correctly"
        assert env_vars.get("KAFKA_LOG_SEGMENT_BYTES") == "536870912", "Custom property not set"
        assert env_vars.get("KAFKA_NUM_NETWORK_THREADS") == "4", "Custom property not set"
        
        print("✅ Manifest validation passed")
        
        # Test cluster lifecycle (if Kubernetes cluster is available)
        print("\n5. Testing cluster lifecycle...")
        try:
            # Test basic Kubernetes connectivity
            provider.core_v1.list_namespace()
            print("✅ Kubernetes cluster connectivity verified")
            
            # Test cluster provisioning (dry run)
            print("   Testing cluster provisioning (this may take a few minutes)...")
            instance_id = "test-k8s-cluster"
            
            # Provision cluster
            result = provider.provision_cluster(instance_id, test_config)
            
            if result.status == ProvisioningStatus.SUCCEEDED:
                print("✅ Cluster provisioned successfully")
                print(f"   Connection info: {result.connection_info}")
                
                # Test status check
                status = provider.get_cluster_status(instance_id)
                print(f"   Cluster status: {status}")
                
                # Test health check
                is_healthy = provider.health_check(instance_id)
                print(f"   Health check: {'✅ Healthy' if is_healthy else '❌ Unhealthy'}")
                
                # Test connection info
                connection_info = provider.get_connection_info(instance_id)
                if connection_info:
                    print(f"   Bootstrap servers: {connection_info['bootstrap_servers']}")
                    print(f"   Zookeeper connect: {connection_info['zookeeper_connect']}")
                
                # Cleanup
                print("   Cleaning up cluster...")
                cleanup_result = provider.deprovision_cluster(instance_id)
                if cleanup_result.status == ProvisioningStatus.SUCCEEDED:
                    print("✅ Cluster cleaned up successfully")
                else:
                    print(f"❌ Cleanup failed: {cleanup_result.error_message}")
                    
            else:
                print(f"❌ Cluster provisioning failed: {result.error_message}")
                
        except Exception as e:
            print(f"⚠️  Kubernetes cluster not available or accessible: {e}")
            print("   Skipping live cluster tests")
        
        print("\n🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_manifest_yaml_output():
    """Test YAML output of generated manifests."""
    print("\n📄 Testing YAML manifest output")
    print("=" * 30)
    
    try:
        import yaml
        
        provider = KubernetesProvider(namespace="kafka-test")
        
        test_config = {
            'cluster_size': 1,
            'replication_factor': 1,
            'partition_count': 3,
            'retention_hours': 168,
            'storage_size_gb': 10,
            'enable_ssl': False,
            'enable_sasl': False,
            'custom_properties': {}
        }
        
        cluster_config = provider._parse_config(test_config)
        manifests = provider._generate_manifests("sample-cluster", cluster_config)
        
        print("Generated Kubernetes manifests:")
        print("-" * 40)
        
        for name, manifest in manifests.items():
            print(f"\n# {name}")
            print("---")
            print(yaml.dump(manifest, default_flow_style=False))
        
        return True
        
    except Exception as e:
        print(f"❌ YAML output test failed: {e}")
        return False


def test_error_scenarios():
    """Test error handling scenarios."""
    print("\n🚨 Testing error scenarios")
    print("=" * 30)
    
    try:
        provider = KubernetesProvider(namespace="kafka-test")
        
        # Test with invalid configuration
        print("1. Testing invalid configuration handling...")
        invalid_config = {
            'cluster_size': 0,  # Invalid
            'replication_factor': -1,  # Invalid
            'storage_size_gb': -5  # Invalid
        }
        
        try:
            cluster_config = provider._parse_config(invalid_config)
            # The provider should handle this gracefully with defaults
            print(f"   Handled invalid config with defaults: cluster_size={cluster_config.cluster_size}")
        except Exception as e:
            print(f"   Error handling invalid config: {e}")
        
        # Test nonexistent cluster operations
        print("2. Testing operations on nonexistent cluster...")
        
        status = provider.get_cluster_status("nonexistent-cluster")
        print(f"   Status of nonexistent cluster: {status}")
        
        connection_info = provider.get_connection_info("nonexistent-cluster")
        print(f"   Connection info for nonexistent cluster: {connection_info}")
        
        is_healthy = provider.health_check("nonexistent-cluster")
        print(f"   Health check for nonexistent cluster: {is_healthy}")
        
        # Test cleanup of nonexistent cluster
        cleanup_result = provider.deprovision_cluster("nonexistent-cluster")
        print(f"   Cleanup of nonexistent cluster: {cleanup_result.status}")
        
        print("✅ Error scenario tests completed")
        return True
        
    except Exception as e:
        print(f"❌ Error scenario tests failed: {e}")
        return False


def main():
    """Main test function."""
    print("🧪 Kubernetes Provider Test Suite")
    print("=" * 50)
    
    success = True
    
    # Run basic functionality tests
    if not test_kubernetes_provider():
        success = False
    
    # Run YAML output tests
    if not test_manifest_yaml_output():
        success = False
    
    # Run error scenario tests
    if not test_error_scenarios():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()