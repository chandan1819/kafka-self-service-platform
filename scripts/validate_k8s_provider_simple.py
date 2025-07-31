#!/usr/bin/env python3
"""Simple validation script for Kubernetes provider."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def validate_kubernetes_provider():
    """Validate Kubernetes provider implementation."""
    print("🔍 Validating Kubernetes Provider Implementation")
    print("=" * 50)
    
    try:
        # Test direct imports to avoid docker dependency
        print("1. Testing direct imports...")
        
        # Import base classes directly
        sys.path.insert(0, str(Path(__file__).parent.parent / "kafka_ops_agent"))
        
        from providers.base import (
            RuntimeProvider, ProvisioningResult, DeprovisioningResult, ProvisioningStatus
        )
        print("   ✅ Base provider classes imported")
        
        from models.cluster import ClusterConfig, ConnectionInfo
        print("   ✅ Model classes imported")
        
        # Import Kubernetes provider directly
        from providers.kubernetes_provider import KubernetesProvider
        print("   ✅ KubernetesProvider imported successfully")
        
        # Test class inheritance
        assert issubclass(KubernetesProvider, RuntimeProvider), "KubernetesProvider doesn't inherit from RuntimeProvider"
        print("   ✅ Proper inheritance verified")
        
        # Test required methods exist
        required_methods = [
            'provision_cluster',
            'deprovision_cluster', 
            'get_cluster_status',
            'get_connection_info',
            'health_check'
        ]
        
        for method in required_methods:
            assert hasattr(KubernetesProvider, method), f"Method {method} not found"
        print("   ✅ All required methods present")
        
        print("\n2. Testing configuration parsing...")
        
        # Create a mock provider instance to test parsing
        class MockKubernetesProvider(KubernetesProvider):
            def __init__(self):
                # Skip actual Kubernetes initialization
                self.namespace = "test"
        
        test_config = {
            'cluster_size': 3,
            'replication_factor': 2,
            'partition_count': 6,
            'retention_hours': 168,
            'storage_size_gb': 20,
            'enable_ssl': False,
            'enable_sasl': False,
            'custom_properties': {
                'log.segment.bytes': '1073741824'
            }
        }
        
        mock_provider = MockKubernetesProvider()
        cluster_config = mock_provider._parse_config(test_config)
        
        assert isinstance(cluster_config, ClusterConfig)
        assert cluster_config.cluster_size == 3
        assert cluster_config.replication_factor == 2
        assert cluster_config.custom_properties['log.segment.bytes'] == '1073741824'
        print("   ✅ Configuration parsing works correctly")
        
        print("\n3. Testing manifest generation...")
        
        # Test manifest generation
        manifests = mock_provider._generate_manifests("test-cluster", cluster_config)
        
        assert isinstance(manifests, dict)
        assert len(manifests) > 0
        print(f"   ✅ Generated {len(manifests)} manifests")
        
        # Validate manifest structure
        expected_manifests = [
            "test-cluster-zookeeper-service",
            "test-cluster-zookeeper-statefulset", 
            "test-cluster-kafka-service",
            "test-cluster-kafka-statefulset"
        ]
        
        for expected in expected_manifests:
            assert expected in manifests, f"Expected manifest {expected} not found"
        print("   ✅ All expected manifests generated")
        
        # Validate manifest content
        kafka_sts = manifests["test-cluster-kafka-statefulset"]
        assert kafka_sts["kind"] == "StatefulSet"
        assert kafka_sts["spec"]["replicas"] == 3
        print("   ✅ Manifest content validation passed")
        
        # Test Zookeeper manifests
        zk_service = manifests["test-cluster-zookeeper-service"]
        zk_sts = manifests["test-cluster-zookeeper-statefulset"]
        
        assert zk_service["kind"] == "Service"
        assert zk_sts["kind"] == "StatefulSet"
        assert zk_sts["spec"]["replicas"] == 1  # Single ZK instance
        print("   ✅ Zookeeper manifests validated")
        
        # Test Kafka manifests
        kafka_service = manifests["test-cluster-kafka-service"]
        kafka_sts = manifests["test-cluster-kafka-statefulset"]
        
        assert kafka_service["kind"] == "Service"
        assert kafka_sts["kind"] == "StatefulSet"
        
        # Check environment variables
        container = kafka_sts["spec"]["template"]["spec"]["containers"][0]
        env_vars = {env["name"]: env.get("value") for env in container["env"] if "value" in env}
        
        assert env_vars.get("KAFKA_LOG_RETENTION_HOURS") == "168"
        assert env_vars.get("KAFKA_NUM_PARTITIONS") == "6"
        assert env_vars.get("KAFKA_LOG_SEGMENT_BYTES") == "1073741824"
        print("   ✅ Kafka configuration environment variables validated")
        
        print("\n4. Testing resource specifications...")
        
        # Check resource limits and requests
        kafka_container = kafka_sts["spec"]["template"]["spec"]["containers"][0]
        resources = kafka_container["resources"]
        
        assert "requests" in resources
        assert "limits" in resources
        assert resources["requests"]["memory"] == "1Gi"
        assert resources["requests"]["cpu"] == "500m"
        assert resources["limits"]["memory"] == "2Gi"
        assert resources["limits"]["cpu"] == "1000m"
        print("   ✅ Resource specifications validated")
        
        # Check health probes
        assert "readinessProbe" in kafka_container
        assert "livenessProbe" in kafka_container
        assert kafka_container["readinessProbe"]["tcpSocket"]["port"] == 9092
        assert kafka_container["livenessProbe"]["tcpSocket"]["port"] == 9092
        print("   ✅ Health probes validated")
        
        # Check volume claim templates
        volume_claims = kafka_sts["spec"]["volumeClaimTemplates"]
        assert len(volume_claims) == 1
        assert volume_claims[0]["metadata"]["name"] == "kafka-data"
        assert volume_claims[0]["spec"]["resources"]["requests"]["storage"] == "20Gi"
        print("   ✅ Volume claim templates validated")
        
        print("\n5. Testing different cluster sizes...")
        
        # Test single-node cluster
        single_node_config = test_config.copy()
        single_node_config['cluster_size'] = 1
        single_cluster_config = mock_provider._parse_config(single_node_config)
        single_manifests = mock_provider._generate_manifests("single-cluster", single_cluster_config)
        
        single_kafka_sts = single_manifests["single-cluster-kafka-statefulset"]
        assert single_kafka_sts["spec"]["replicas"] == 1
        print("   ✅ Single-node cluster configuration validated")
        
        # Test large cluster
        large_config = test_config.copy()
        large_config['cluster_size'] = 5
        large_cluster_config = mock_provider._parse_config(large_config)
        large_manifests = mock_provider._generate_manifests("large-cluster", large_cluster_config)
        
        large_kafka_sts = large_manifests["large-cluster-kafka-statefulset"]
        assert large_kafka_sts["spec"]["replicas"] == 5
        print("   ✅ Large cluster configuration validated")
        
        print("\n🎉 All validation tests passed!")
        print("\nImplementation Summary:")
        print("- ✅ Kubernetes provider class implemented")
        print("- ✅ All required methods present")
        print("- ✅ Configuration parsing working")
        print("- ✅ Manifest generation functional")
        print("- ✅ Resource specifications correct")
        print("- ✅ Health probes configured")
        print("- ✅ Volume persistence configured")
        print("- ✅ Supports different cluster sizes")
        print("- ✅ Ready for integration testing")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_sample_manifests():
    """Show sample generated manifests."""
    print("\n📄 Sample Generated Manifests")
    print("=" * 30)
    
    try:
        import json
        
        # Import directly
        sys.path.insert(0, str(Path(__file__).parent.parent / "kafka_ops_agent"))
        
        from providers.kubernetes_provider import KubernetesProvider
        from models.cluster import ClusterConfig
        
        class MockKubernetesProvider(KubernetesProvider):
            def __init__(self):
                self.namespace = "kafka-demo"
        
        mock_provider = MockKubernetesProvider()
        
        test_config = {
            'cluster_size': 2,
            'replication_factor': 2,
            'partition_count': 6,
            'retention_hours': 72,
            'storage_size_gb': 15,
            'enable_ssl': False,
            'enable_sasl': False,
            'custom_properties': {
                'log.segment.bytes': '268435456',
                'num.network.threads': '8'
            }
        }
        
        cluster_config = mock_provider._parse_config(test_config)
        manifests = mock_provider._generate_manifests("demo-cluster", cluster_config)
        
        print("Generated Kubernetes manifests:")
        print("-" * 40)
        
        for name, manifest in manifests.items():
            print(f"\n# {name}")
            print("---")
            print(json.dumps(manifest, indent=2))
        
        print("\n✅ Sample manifests generated successfully")
        return True
        
    except Exception as e:
        print(f"❌ Sample manifest generation failed: {e}")
        return False


def main():
    """Main validation function."""
    print("🧪 Kubernetes Provider Simple Validation")
    print("=" * 50)
    
    success = True
    
    # Run validation tests
    if not validate_kubernetes_provider():
        success = False
    
    # Show sample manifests
    if not show_sample_manifests():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All validations passed!")
        print("\nNext steps:")
        print("1. Install kubernetes client library: pip install kubernetes")
        print("2. Run integration tests with actual Kubernetes cluster")
        print("3. Test with different cluster configurations")
        print("4. Integrate with provisioning service")
        sys.exit(0)
    else:
        print("❌ Some validations failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()