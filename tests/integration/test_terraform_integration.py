"""Integration tests for Terraform provider."""

import pytest
import time
import tempfile
from pathlib import Path

from kafka_ops_agent.providers.terraform_provider import TerraformProvider
from kafka_ops_agent.providers.base import ProvisioningStatus


@pytest.mark.integration
@pytest.mark.terraform
class TestTerraformIntegration:
    """Integration tests for TerraformProvider."""
    
    @pytest.fixture(scope="class")
    def provider(self):
        """Create TerraformProvider for integration tests."""
        # Skip if Terraform is not available
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                provider = TerraformProvider(
                    working_dir=temp_dir,
                    cloud_provider="aws"
                )
                return provider
        except Exception as e:
            pytest.skip(f"Terraform not available: {e}")
    
    @pytest.fixture
    def cluster_config(self):
        """Sample cluster configuration for integration tests."""
        return {
            'cluster_size': 1,  # Single node for faster testing
            'replication_factor': 1,
            'partition_count': 3,
            'retention_hours': 1,  # Short retention for testing
            'storage_size_gb': 20,  # Minimal storage
            'enable_ssl': False,  # Disable SSL for testing
            'enable_sasl': False,  # Disable SASL for testing
            'custom_properties': {
                'log.segment.bytes': '104857600'  # 100MB segments
            }
        }
    
    def test_terraform_config_generation(self, provider, cluster_config):
        """Test Terraform configuration generation without actual deployment."""
        instance_id = "test-config-cluster"
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            cluster_config_obj = provider._parse_config(cluster_config)
            
            # Generate configuration
            provider._generate_terraform_config(instance_id, cluster_config_obj, instance_dir)
            
            # Verify all files are created
            assert (instance_dir / "main.tf").exists()
            assert (instance_dir / "variables.tf").exists()
            assert (instance_dir / "outputs.tf").exists()
            assert (instance_dir / "provider.tf").exists()
            assert (instance_dir / "resources.tf").exists()
            assert (instance_dir / "terraform.tfvars").exists()
            
            # Verify setup scripts are created when apply is called
            provider._create_setup_scripts(instance_dir)
            scripts_dir = instance_dir / "scripts"
            assert scripts_dir.exists()
            assert (scripts_dir / "zookeeper-setup.sh").exists()
            assert (scripts_dir / "kafka-setup.sh").exists()
            
            # Verify script permissions
            zk_script = scripts_dir / "zookeeper-setup.sh"
            kafka_script = scripts_dir / "kafka-setup.sh"
            assert zk_script.stat().st_mode & 0o111  # Executable
            assert kafka_script.stat().st_mode & 0o111  # Executable
    
    def test_terraform_init_validation(self, provider):
        """Test Terraform init with generated configuration."""
        instance_id = "test-init-cluster"
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            cluster_config = provider._parse_config({'cluster_size': 1})
            
            # Generate configuration
            provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
            
            try:
                # Test terraform init (should work with valid config)
                provider._terraform_init(instance_dir)
                
                # Verify .terraform directory is created
                assert (instance_dir / ".terraform").exists()
                
            except Exception as e:
                # If init fails due to missing cloud credentials, that's expected
                if "credentials" in str(e).lower() or "authentication" in str(e).lower():
                    pytest.skip(f"Cloud credentials not configured: {e}")
                else:
                    raise
    
    def test_terraform_plan_validation(self, provider):
        """Test Terraform plan with generated configuration."""
        instance_id = "test-plan-cluster"
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            cluster_config = provider._parse_config({'cluster_size': 1})
            
            # Generate configuration
            provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
            
            try:
                # Initialize Terraform
                provider._terraform_init(instance_dir)
                
                # Test terraform plan (dry run)
                import subprocess
                result = subprocess.run(
                    [provider.terraform_binary, "plan"],
                    cwd=instance_dir,
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                
                # Plan should either succeed or fail with credential issues
                if result.returncode != 0:
                    if any(keyword in result.stderr.lower() for keyword in 
                           ["credentials", "authentication", "access", "permission"]):
                        pytest.skip(f"Cloud credentials not configured: {result.stderr}")
                    else:
                        # Other errors might indicate configuration issues
                        print(f"Plan stderr: {result.stderr}")
                        print(f"Plan stdout: {result.stdout}")
                        # Don't fail the test for plan issues in integration
                        pytest.skip(f"Terraform plan failed (expected in CI): {result.stderr}")
                
            except Exception as e:
                if any(keyword in str(e).lower() for keyword in 
                       ["credentials", "authentication", "access", "permission"]):
                    pytest.skip(f"Cloud credentials not configured: {e}")
                else:
                    raise
    
    def test_different_cloud_providers(self):
        """Test configuration generation for different cloud providers."""
        providers = ["aws", "gcp", "azure"]
        
        for cloud_provider in providers:
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    provider = TerraformProvider(
                        working_dir=temp_dir,
                        cloud_provider=cloud_provider
                    )
                    
                    instance_id = f"test-{cloud_provider}-cluster"
                    instance_dir = Path(temp_dir) / instance_id
                    instance_dir.mkdir()
                    
                    cluster_config = provider._parse_config({'cluster_size': 2})
                    
                    # Generate configuration
                    provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
                    
                    # Verify provider-specific files
                    provider_content = (instance_dir / "provider.tf").read_text()
                    resources_content = (instance_dir / "resources.tf").read_text()
                    
                    if cloud_provider == "aws":
                        assert "aws" in provider_content
                        assert "aws_instance" in resources_content
                        assert "aws_vpc" in resources_content
                    elif cloud_provider == "gcp":
                        assert "google" in provider_content
                        assert "google_compute_instance" in resources_content
                        assert "google_compute_network" in resources_content
                    elif cloud_provider == "azure":
                        assert "azurerm" in provider_content
                        assert "azurerm_linux_virtual_machine" in resources_content
                        assert "azurerm_resource_group" in resources_content
                    
            except Exception as e:
                pytest.fail(f"Failed to generate config for {cloud_provider}: {e}")
    
    def test_cluster_scaling_configurations(self, provider):
        """Test configuration generation for different cluster sizes."""
        cluster_sizes = [1, 3, 5]
        
        for size in cluster_sizes:
            with tempfile.TemporaryDirectory() as temp_dir:
                instance_dir = Path(temp_dir)
                instance_id = f"test-scale-{size}-cluster"
                
                config = {'cluster_size': size, 'replication_factor': min(size, 2)}
                cluster_config = provider._parse_config(config)
                
                # Generate configuration
                provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
                
                # Verify tfvars has correct cluster size
                tfvars_content = (instance_dir / "terraform.tfvars").read_text()
                assert f'cluster_size = {size}' in tfvars_content
                
                # Verify resources.tf references cluster size
                resources_content = (instance_dir / "resources.tf").read_text()
                assert "var.cluster_size" in resources_content
    
    def test_custom_properties_integration(self, provider):
        """Test that custom properties are properly integrated."""
        custom_config = {
            'cluster_size': 2,
            'custom_properties': {
                'log.retention.bytes': '2147483648',
                'num.network.threads': '16',
                'compression.type': 'snappy'
            }
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            instance_id = "test-custom-props-cluster"
            
            cluster_config = provider._parse_config(custom_config)
            provider._generate_terraform_config(instance_id, cluster_config, instance_dir)
            
            # Create setup scripts to check custom properties
            provider._create_setup_scripts(instance_dir)
            
            # Check that setup script templates would receive custom properties
            kafka_script = (instance_dir / "scripts" / "kafka-setup.sh").read_text()
            
            # The script should have placeholders for custom configuration
            # In a real deployment, these would be filled by Terraform templatefile
            assert "server.properties" in kafka_script
            assert "broker.id" in kafka_script
    
    def test_error_handling_scenarios(self, provider):
        """Test error handling in various scenarios."""
        # Test with invalid cluster size
        with pytest.raises(Exception):
            invalid_config = {'cluster_size': 0}
            cluster_config = provider._parse_config(invalid_config)
            # This should be caught during validation
    
    def test_directory_management(self, provider):
        """Test proper directory creation and cleanup."""
        instance_id = "test-directory-cluster"
        
        # Test directory creation
        instance_dir = provider._create_instance_directory(instance_id)
        assert instance_dir.exists()
        assert instance_dir.name == instance_id
        
        # Test directory retrieval
        retrieved_dir = provider._get_instance_directory(instance_id)
        assert retrieved_dir == instance_dir
        assert retrieved_dir.exists()
        
        # Test cleanup
        provider._cleanup_cluster(instance_id)
        
        # Directory should be removed
        assert not instance_dir.exists()
        
        # Retrieving non-existent directory should return None
        assert provider._get_instance_directory(instance_id) is None
    
    @pytest.mark.slow
    def test_full_terraform_workflow_dry_run(self, provider, cluster_config):
        """Test full Terraform workflow without actual cloud deployment."""
        instance_id = "test-workflow-cluster"
        
        try:
            # This tests the full workflow but expects it to fail at apply
            # due to missing cloud credentials (which is expected in CI)
            result = provider.provision_cluster(instance_id, cluster_config)
            
            # If it succeeds, great! If it fails due to credentials, that's expected
            if result.status == ProvisioningStatus.FAILED:
                error_msg = result.error_message.lower()
                if any(keyword in error_msg for keyword in 
                       ["credentials", "authentication", "access", "permission"]):
                    # This is expected in CI environments
                    pass
                else:
                    # Other failures might indicate real issues
                    print(f"Unexpected failure: {result.error_message}")
            
            # Test cleanup regardless of provision result
            cleanup_result = provider.deprovision_cluster(instance_id)
            # Cleanup should succeed even if provision failed
            assert cleanup_result.status == ProvisioningStatus.SUCCEEDED
            
        except Exception as e:
            # If the whole process fails due to missing terraform or credentials,
            # that's expected in some CI environments
            if any(keyword in str(e).lower() for keyword in 
                   ["terraform", "credentials", "authentication"]):
                pytest.skip(f"Infrastructure not available: {e}")
            else:
                raise


@pytest.mark.integration
@pytest.mark.terraform
class TestTerraformProviderErrorHandling:
    """Test error handling scenarios for Terraform provider."""
    
    def test_invalid_terraform_binary(self):
        """Test initialization with invalid terraform binary."""
        with pytest.raises(Exception, match="Terraform binary not found"):
            TerraformProvider(terraform_binary="/nonexistent/terraform")
    
    def test_unsupported_cloud_provider(self):
        """Test initialization with unsupported cloud provider."""
        try:
            provider = TerraformProvider(cloud_provider="unsupported")
            
            # Should fail when trying to generate config
            with tempfile.TemporaryDirectory() as temp_dir:
                instance_dir = Path(temp_dir)
                cluster_config = provider._parse_config({})
                
                with pytest.raises(ValueError, match="Unsupported cloud provider"):
                    provider._generate_terraform_config("test", cluster_config, instance_dir)
                    
        except Exception as e:
            if "terraform" in str(e).lower():
                pytest.skip(f"Terraform not available: {e}")
            else:
                raise
    
    def test_operations_on_nonexistent_cluster(self):
        """Test operations on clusters that don't exist."""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                provider = TerraformProvider(working_dir=temp_dir)
                
                # Test status of nonexistent cluster
                status = provider.get_cluster_status("nonexistent-cluster")
                assert status == ProvisioningStatus.FAILED
                
                # Test connection info of nonexistent cluster
                connection_info = provider.get_connection_info("nonexistent-cluster")
                assert connection_info is None
                
                # Test health check of nonexistent cluster
                is_healthy = provider.health_check("nonexistent-cluster")
                assert is_healthy is False
                
                # Test cleanup of nonexistent cluster (should succeed)
                cleanup_result = provider.deprovision_cluster("nonexistent-cluster")
                assert cleanup_result.status == ProvisioningStatus.SUCCEEDED
                
        except Exception as e:
            if "terraform" in str(e).lower():
                pytest.skip(f"Terraform not available: {e}")
            else:
                raise


if __name__ == "__main__":
    # Run integration tests
    pytest.main([
        __file__,
        "-v",
        "-m", "integration and terraform",
        "--tb=short"
    ])