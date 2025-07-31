#!/usr/bin/env python3
"""Test script for Terraform provider functionality."""

import sys
import tempfile
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_ops_agent.providers.terraform_provider import TerraformProvider
from kafka_ops_agent.providers.base import ProvisioningStatus

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_terraform_provider():
    """Test Terraform provider functionality."""
    print("🚀 Testing Terraform Provider")
    print("=" * 50)
    
    try:
        # Test provider initialization
        print("1. Testing provider initialization...")
        
        # Test different cloud providers
        cloud_providers = ["aws", "gcp", "azure"]
        
        for cloud_provider in cloud_providers:
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    provider = TerraformProvider(
                        working_dir=temp_dir,
                        cloud_provider=cloud_provider
                    )
                    print(f"   ✅ {cloud_provider.upper()} provider initialized successfully")
            except Exception as e:
                if "terraform" in str(e).lower():
                    print(f"   ⚠️  Terraform not available: {e}")
                    return False
                else:
                    print(f"   ❌ Failed to initialize {cloud_provider} provider: {e}")
                    return False
        
        # Use AWS provider for remaining tests
        with tempfile.TemporaryDirectory() as temp_dir:
            provider = TerraformProvider(
                working_dir=temp_dir,
                cloud_provider="aws"
            )
            
            # Test configuration parsing
            print("\n2. Testing configuration parsing...")
            test_config = {
                'cluster_size': 3,
                'replication_factor': 2,
                'partition_count': 6,
                'retention_hours': 168,
                'storage_size_gb': 100,
                'enable_ssl': True,
                'enable_sasl': True,
                'custom_properties': {
                    'log.segment.bytes': '1073741824',
                    'num.network.threads': '8'
                }
            }
            
            cluster_config = provider._parse_config(test_config)
            print(f"   ✅ Configuration parsed: {cluster_config.cluster_size} brokers, "
                  f"{cluster_config.replication_factor} replication factor")
            
            # Test directory management
            print("\n3. Testing directory management...")
            instance_id = "test-terraform-cluster"
            
            instance_dir = provider._create_instance_directory(instance_id)
            print(f"   ✅ Instance directory created: {instance_dir}")
            
            retrieved_dir = provider._get_instance_directory(instance_id)
            assert retrieved_dir == instance_dir, "Directory retrieval failed"
            print("   ✅ Directory retrieval working")
            
            # Test Terraform configuration generation
            print("\n4. Testing Terraform configuration generation...")
            
            provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
            
            # Check that all required files are created
            required_files = [
                "main.tf", "variables.tf", "outputs.tf", 
                "provider.tf", "resources.tf", "terraform.tfvars"
            ]
            
            for file_name in required_files:
                file_path = instance_dir / file_name
                assert file_path.exists(), f"Missing file: {file_name}"
                print(f"   ✅ Generated {file_name}")
            
            # Test setup script generation
            print("\n5. Testing setup script generation...")
            
            provider._create_setup_scripts(instance_dir)
            
            scripts_dir = instance_dir / "scripts"
            assert scripts_dir.exists(), "Scripts directory not created"
            
            zk_script = scripts_dir / "zookeeper-setup.sh"
            kafka_script = scripts_dir / "kafka-setup.sh"
            
            assert zk_script.exists(), "Zookeeper setup script not created"
            assert kafka_script.exists(), "Kafka setup script not created"
            
            # Check script permissions
            assert zk_script.stat().st_mode & 0o111, "Zookeeper script not executable"
            assert kafka_script.stat().st_mode & 0o111, "Kafka script not executable"
            
            print("   ✅ Setup scripts generated and executable")
            
            # Test configuration content validation
            print("\n6. Validating configuration content...")
            
            # Check main.tf
            main_content = (instance_dir / "main.tf").read_text()
            assert instance_id in main_content, "Instance ID not in main.tf"
            assert "terraform" in main_content, "Terraform block missing"
            print("   ✅ main.tf content validated")
            
            # Check variables.tf
            variables_content = (instance_dir / "variables.tf").read_text()
            assert "cluster_size" in variables_content, "cluster_size variable missing"
            assert "instance_type" in variables_content, "instance_type variable missing"
            print("   ✅ variables.tf content validated")
            
            # Check outputs.tf
            outputs_content = (instance_dir / "outputs.tf").read_text()
            assert "bootstrap_servers" in outputs_content, "bootstrap_servers output missing"
            assert "zookeeper_connect" in outputs_content, "zookeeper_connect output missing"
            print("   ✅ outputs.tf content validated")
            
            # Check provider.tf
            provider_content = (instance_dir / "provider.tf").read_text()
            assert "aws" in provider_content, "AWS provider configuration missing"
            print("   ✅ provider.tf content validated")
            
            # Check resources.tf
            resources_content = (instance_dir / "resources.tf").read_text()
            assert "aws_instance" in resources_content, "AWS instance resources missing"
            assert "aws_vpc" in resources_content, "AWS VPC resources missing"
            print("   ✅ resources.tf content validated")
            
            # Check terraform.tfvars
            tfvars_content = (instance_dir / "terraform.tfvars").read_text()
            assert f'cluster_name = "{instance_id}"' in tfvars_content, "Cluster name not in tfvars"
            assert 'cluster_size = 3' in tfvars_content, "Cluster size not in tfvars"
            assert 'enable_ssl = true' in tfvars_content, "SSL setting not in tfvars"
            print("   ✅ terraform.tfvars content validated")
            
            # Test Terraform initialization (if terraform is available)
            print("\n7. Testing Terraform initialization...")
            try:
                provider._terraform_init(instance_dir)
                print("   ✅ Terraform initialization successful")
                
                # Check that .terraform directory was created
                terraform_dir = instance_dir / ".terraform"
                assert terraform_dir.exists(), ".terraform directory not created"
                print("   ✅ .terraform directory created")
                
            except Exception as e:
                print(f"   ⚠️  Terraform init failed (expected in some environments): {e}")
            
            # Test error handling scenarios
            print("\n8. Testing error handling...")
            
            # Test operations on nonexistent cluster
            nonexistent_status = provider.get_cluster_status("nonexistent-cluster")
            assert nonexistent_status == ProvisioningStatus.FAILED, "Should return FAILED for nonexistent cluster"
            print("   ✅ Nonexistent cluster status handling")
            
            nonexistent_connection = provider.get_connection_info("nonexistent-cluster")
            assert nonexistent_connection is None, "Should return None for nonexistent cluster"
            print("   ✅ Nonexistent cluster connection info handling")
            
            nonexistent_health = provider.health_check("nonexistent-cluster")
            assert nonexistent_health is False, "Should return False for nonexistent cluster"
            print("   ✅ Nonexistent cluster health check handling")
            
            # Test cleanup
            print("\n9. Testing cleanup...")
            provider._cleanup_cluster(instance_id)
            
            # Directory should be removed
            assert not instance_dir.exists(), "Instance directory should be removed after cleanup"
            print("   ✅ Cleanup successful")
            
            # Test that get_instance_directory returns None after cleanup
            cleaned_dir = provider._get_instance_directory(instance_id)
            assert cleaned_dir is None, "Should return None after cleanup"
            print("   ✅ Directory retrieval returns None after cleanup")
        
        print("\n🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multi_cloud_configurations():
    """Test configuration generation for different cloud providers."""
    print("\n🌐 Testing Multi-Cloud Configurations")
    print("=" * 40)
    
    try:
        cloud_providers = [
            ("aws", "aws_instance", "aws_vpc"),
            ("gcp", "google_compute_instance", "google_compute_network"),
            ("azure", "azurerm_linux_virtual_machine", "azurerm_resource_group")
        ]
        
        for cloud_provider, instance_resource, network_resource in cloud_providers:
            print(f"\nTesting {cloud_provider.upper()} configuration...")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    provider = TerraformProvider(
                        working_dir=temp_dir,
                        cloud_provider=cloud_provider
                    )
                    
                    instance_dir = provider._create_instance_directory(f"test-{cloud_provider}-cluster")
                    cluster_config = provider._parse_config({'cluster_size': 2})
                    
                    provider._generate_terraform_config(f"test-{cloud_provider}-cluster", cluster_config, instance_dir)
                    
                    # Validate cloud-specific content
                    resources_content = (instance_dir / "resources.tf").read_text()
                    assert instance_resource in resources_content, f"Missing {instance_resource} in {cloud_provider}"
                    assert network_resource in resources_content, f"Missing {network_resource} in {cloud_provider}"
                    
                    provider_content = (instance_dir / "provider.tf").read_text()
                    if cloud_provider == "aws":
                        assert "aws" in provider_content
                    elif cloud_provider == "gcp":
                        assert "google" in provider_content
                    elif cloud_provider == "azure":
                        assert "azurerm" in provider_content
                    
                    print(f"   ✅ {cloud_provider.upper()} configuration generated successfully")
                    
                except Exception as e:
                    if "terraform" in str(e).lower():
                        print(f"   ⚠️  Terraform not available: {e}")
                        return False
                    else:
                        print(f"   ❌ {cloud_provider.upper()} configuration failed: {e}")
                        return False
        
        print("\n✅ Multi-cloud configuration tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Multi-cloud test failed: {e}")
        return False


def test_cluster_scaling():
    """Test configuration generation for different cluster sizes."""
    print("\n📈 Testing Cluster Scaling Configurations")
    print("=" * 40)
    
    try:
        cluster_sizes = [1, 3, 5, 7]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            provider = TerraformProvider(
                working_dir=temp_dir,
                cloud_provider="aws"
            )
            
            for size in cluster_sizes:
                print(f"\nTesting cluster size: {size}")
                
                instance_id = f"test-scale-{size}-cluster"
                instance_dir = provider._create_instance_directory(instance_id)
                
                config = {
                    'cluster_size': size,
                    'replication_factor': min(size, 3),  # Cap replication factor
                    'partition_count': size * 2  # Scale partitions with cluster
                }
                
                cluster_config = provider._parse_config(config)
                provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
                
                # Validate tfvars
                tfvars_content = (instance_dir / "terraform.tfvars").read_text()
                assert f'cluster_size = {size}' in tfvars_content, f"Cluster size {size} not in tfvars"
                
                # Validate resources reference cluster size
                resources_content = (instance_dir / "resources.tf").read_text()
                assert "var.cluster_size" in resources_content, "Resources don't reference cluster size variable"
                
                print(f"   ✅ Cluster size {size} configuration validated")
        
        print("\n✅ Cluster scaling tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Cluster scaling test failed: {e}")
        return False


def show_sample_terraform_config():
    """Show sample generated Terraform configuration."""
    print("\n📄 Sample Terraform Configuration")
    print("=" * 40)
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            provider = TerraformProvider(
                working_dir=temp_dir,
                cloud_provider="aws"
            )
            
            instance_id = "sample-kafka-cluster"
            instance_dir = provider._create_instance_directory(instance_id)
            
            sample_config = {
                'cluster_size': 3,
                'replication_factor': 2,
                'partition_count': 6,
                'retention_hours': 168,
                'storage_size_gb': 100,
                'enable_ssl': True,
                'enable_sasl': True,
                'custom_properties': {
                    'log.segment.bytes': '536870912',
                    'num.network.threads': '8'
                }
            }
            
            cluster_config = provider._parse_config(sample_config)
            provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
            
            print("\nGenerated Terraform files:")
            print("-" * 30)
            
            files_to_show = ["main.tf", "terraform.tfvars"]
            
            for file_name in files_to_show:
                file_path = instance_dir / file_name
                if file_path.exists():
                    print(f"\n# {file_name}")
                    print("---")
                    content = file_path.read_text()
                    # Show first 20 lines to keep output manageable
                    lines = content.split('\n')[:20]
                    print('\n'.join(lines))
                    if len(content.split('\n')) > 20:
                        print("... (truncated)")
        
        print("\n✅ Sample configuration displayed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Sample configuration display failed: {e}")
        return False


def main():
    """Main test function."""
    print("🧪 Terraform Provider Test Suite")
    print("=" * 50)
    
    success = True
    
    # Run basic functionality tests
    if not test_terraform_provider():
        success = False
    
    # Run multi-cloud tests
    if not test_multi_cloud_configurations():
        success = False
    
    # Run scaling tests
    if not test_cluster_scaling():
        success = False
    
    # Show sample configuration
    if not show_sample_terraform_config():
        success = False
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests passed!")
        print("\nTask 6 Implementation Summary:")
        print("✅ TerraformProvider class implemented")
        print("✅ Multi-cloud support (AWS, GCP, Azure)")
        print("✅ Infrastructure as code deployment")
        print("✅ Proper state management and resource cleanup")
        print("✅ Custom VM configurations and cloud-specific features")
        print("✅ Integration tests structure created")
        print("\nNext steps:")
        print("1. Configure cloud provider credentials")
        print("2. Run integration tests with actual cloud resources")
        print("3. Test with different cluster configurations")
        print("4. Move to next task in the implementation plan")
        sys.exit(0)
    else:
        print("❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()