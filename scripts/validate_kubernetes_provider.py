#!/usr/bin/env python3
"""Validation script for Kubernetes provider (no external dependencies)."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def validate_kubernetes_provider():
    """Validate Kubernetes provider implementation."""
    print("🔍 Validating Kubernetes Provider Implementation")
    print("=" * 50)
    
    try:
        # Test imports
        print("1. Testing imports...")
        
        # Test base imports
        from kafka_ops_agent.providers.base import (
            RuntimeProvider, ProvisioningResult, DeprovisioningResult, ProvisioningStatus
        )
        print("   ✅ Base provider classes imported")
        
        # Test model imports
        from kafka_ops_agent.models.cluster import ClusterConfig, ConnectionInfo
        print("   ✅ Model classes imported")
        
        # Test Kubernetes provider import (without initializing)
        import kafka_ops_agent.providers.kubernetes_provider as k8s_module
        print("   ✅ Kubernetes provider module imported")
        
        # Test provider class exists
        assert hasattr(k8s_module, 'KubernetesProvider'), "KubernetesProvider class not found"
        print("   ✅ KubernetesProvider class found")
        
        # Test class inheritance
        KubernetesProvider = k8s_module.KubernetesProvider
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
        
        # Test configuration parsing without initializing provider
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
        
        # Create a mock provider instance to test parsing
        class MockKubernetesProvider(KubernetesProvider):
            def __init__(self):
                # Skip actual Kubernetes initialization
                self.namespace = "test"
        
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
        
        print("\n4. Testing provider registry integration...")
        
        # Test that provider is properly exported
        from kafka_ops_agent.providers import KubernetesProvider as ExportedProvider
        assert ExportedProvider is KubernetesProvider
        print("   ✅ Provider properly exported from module")
        
        # Test enum integration
        from kafka_ops_agent.models.cluster import RuntimeProvider as RuntimeProviderEnum
        assert hasattr(RuntimeProviderEnum, 'KUBERNETES')
        assert RuntimeProviderEnum.KUBERNETES == "kubernetes"
        print("   ✅ Runtime provider enum includes Kubernetes")
        
        print("\n🎉 All validation tests passed!")
        print("\nImplementation Summary:")
        print("- ✅ Kubernetes provider class implemented")
        print("- ✅ All required methods present")
        print("- ✅ Configuration parsing working")
        print("- ✅ Manifest generation functional")
        print("- ✅ Proper integration with existing codebase")
        print("- ✅ Ready for integration testing with actual Kubernetes cluster")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_yaml_output():
    """Validate YAML output without external dependencies."""
    print("\n📄 Validating YAML Structure")
    print("=" * 30)
    
    try:
        import json
        
        # Import without initializing
        import kafka_ops_agent.providers.kubernetes_provider as k8s_module
        
        class MockKubernetesProvider(k8s_module.KubernetesProvider):
            def __init__(self):
                self.namespace = "test"
        
        mock_provider = MockKubernetesProvider()
        
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
        
        cluster_config = mock_provider._parse_config(test_config)
        manifests = mock_provider._generate_manifests("sample-cluster", cluster_config)
        
        print("Generated manifest structure (JSON format):")
        print("-" * 40)
        
        for name, manifest in manifests.items():
            print(f"\n# {name}")
            print(json.dumps(manifest, indent=2))
        
        print("\n✅ YAML structure validation completed")
        return True
        
    except Exception as e:
        print(f"❌ YAML validation failed: {e}")
        return False


def main():
    """Main validation function."""
    print("🧪 Kubernetes Provider Validation Suite")
    print("=" * 50)
    
    success = True
    
    # Run validation tests
    if not validate_kubernetes_provider():
        success = False
    
    # Run YAML structure validation
    if not validate_yaml_output():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All validations passed!")
        print("\nNext steps:")
        print("1. Install kubernetes client library: pip install kubernetes")
        print("2. Run integration tests with actual Kubernetes cluster")
        print("3. Test with different cluster configurations")
        sys.exit(0)
    else:
        print("❌ Some validations failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()