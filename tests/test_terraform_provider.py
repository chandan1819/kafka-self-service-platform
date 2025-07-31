"""Tests for Terraform runtime provider."""

import pytest
import json
import tempfile
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from kafka_ops_agent.providers.terraform_provider import TerraformProvider
from kafka_ops_agent.providers.base import ProvisioningStatus
from kafka_ops_agent.models.cluster import ClusterConfig


class TestTerraformProvider:
    """Test cases for TerraformProvider."""
    
    @pytest.fixture
    def mock_subprocess(self):
        """Mock subprocess calls."""
        with patch('kafka_ops_agent.providers.terraform_provider.subprocess') as mock_subprocess:
            # Mock terraform version check
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "Terraform v1.5.0"
            mock_result.stderr = ""
            mock_subprocess.run.return_value = mock_result
            yield mock_subprocess
    
    @pytest.fixture
    def provider(self, mock_subprocess):
        """Create TerraformProvider instance with mocked subprocess."""
        return TerraformProvider(cloud_provider="aws")
    
    @pytest.fixture
    def sample_config(self):
        """Sample cluster configuration."""
        return {
            'cluster_size': 3,
            'replication_factor': 2,
            'partition_count': 6,
            'retention_hours': 168,
            'storage_size_gb': 100,
            'enable_ssl': True,
            'enable_sasl': True,
            'custom_properties': {
                'log.segment.bytes': '1073741824'
            }
        }
    
    def test_init_success(self, mock_subprocess):
        """Test successful provider initialization."""
        provider = TerraformProvider(cloud_provider="aws")
        
        assert provider.cloud_provider == "aws"
        assert provider.terraform_binary == "terraform"
        mock_subprocess.run.assert_called_once()
    
    def test_init_terraform_not_found(self, mock_subprocess):
        """Test initialization when terraform binary is not found."""
        mock_subprocess.run.side_effect = FileNotFoundError("terraform not found")
        
        with pytest.raises(Exception, match="Terraform binary not found"):
            TerraformProvider()
    
    def test_init_terraform_not_working(self, mock_subprocess):
        """Test initialization when terraform is not working."""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "terraform: command not found"
        mock_subprocess.run.return_value = mock_result
        
        with pytest.raises(Exception, match="Terraform not found or not working"):
            TerraformProvider()
    
    def test_parse_config(self, provider, sample_config):
        """Test configuration parsing."""
        cluster_config = provider._parse_config(sample_config)
        
        assert isinstance(cluster_config, ClusterConfig)
        assert cluster_config.cluster_size == 3
        assert cluster_config.replication_factor == 2
        assert cluster_config.partition_count == 6
        assert cluster_config.retention_hours == 168
        assert cluster_config.storage_size_gb == 100
        assert cluster_config.enable_ssl is True
        assert cluster_config.enable_sasl is True
        assert cluster_config.custom_properties['log.segment.bytes'] == '1073741824'
    
    def test_parse_config_defaults(self, provider):
        """Test configuration parsing with defaults."""
        cluster_config = provider._parse_config({})
        
        assert cluster_config.cluster_size == 3  # Cloud default
        assert cluster_config.replication_factor == 2
        assert cluster_config.storage_size_gb == 100  # Cloud default
        assert cluster_config.enable_ssl is True  # Cloud default
        assert cluster_config.enable_sasl is True  # Cloud default
    
    def test_create_instance_directory(self, provider):
        """Test instance directory creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            provider.working_dir = Path(temp_dir)
            
            instance_dir = provider._create_instance_directory("test-cluster")
            
            assert instance_dir.exists()
            assert instance_dir.name == "test-cluster"
            assert instance_dir.parent == Path(temp_dir)
    
    def test_create_instance_directory_temp(self, provider):
        """Test instance directory creation in temp directory."""
        provider.working_dir = None
        
        instance_dir = provider._create_instance_directory("test-cluster")
        
        assert instance_dir.exists()
        assert instance_dir.name == "test-cluster"
        assert "kafka-terraform" in str(instance_dir)
    
    def test_generate_terraform_config_aws(self, provider, sample_config):
        """Test Terraform configuration generation for AWS."""
        cluster_config = provider._parse_config(sample_config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            provider._generate_terraform_config("test-cluster", cluster_config, instance_dir)
            
            # Check that all required files are created
            assert (instance_dir / "main.tf").exists()
            assert (instance_dir / "variables.tf").exists()
            assert (instance_dir / "outputs.tf").exists()
            assert (instance_dir / "provider.tf").exists()
            assert (instance_dir / "resources.tf").exists()
            assert (instance_dir / "terraform.tfvars").exists()
            
            # Check main.tf content
            main_content = (instance_dir / "main.tf").read_text()
            assert "test-cluster" in main_content
            assert "terraform" in main_content
            assert "required_providers" in main_content
            
            # Check tfvars content
            tfvars_content = (instance_dir / "terraform.tfvars").read_text()
            assert 'cluster_name = "test-cluster"' in tfvars_content
            assert 'cluster_size = 3' in tfvars_content
            assert 'enable_ssl = true' in tfvars_content
    
    def test_generate_terraform_config_gcp(self, mock_subprocess, sample_config):
        """Test Terraform configuration generation for GCP."""
        provider = TerraformProvider(cloud_provider="gcp")
        cluster_config = provider._parse_config(sample_config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            provider._generate_terraform_config("test-cluster", cluster_config, instance_dir)
            
            # Check provider.tf content
            provider_content = (instance_dir / "provider.tf").read_text()
            assert "google" in provider_content
            assert "gcp_project" in provider_content
            
            # Check outputs.tf content
            outputs_content = (instance_dir / "outputs.tf").read_text()
            assert "google_compute_instance" in outputs_content
    
    def test_generate_terraform_config_azure(self, mock_subprocess, sample_config):
        """Test Terraform configuration generation for Azure."""
        provider = TerraformProvider(cloud_provider="azure")
        cluster_config = provider._parse_config(sample_config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            provider._generate_terraform_config("test-cluster", cluster_config, instance_dir)
            
            # Check provider.tf content
            provider_content = (instance_dir / "provider.tf").read_text()
            assert "azurerm" in provider_content
            assert "azure_location" in provider_content
            
            # Check outputs.tf content
            outputs_content = (instance_dir / "outputs.tf").read_text()
            assert "azurerm_linux_virtual_machine" in outputs_content
    
    def test_generate_terraform_config_unsupported_provider(self, mock_subprocess, sample_config):
        """Test error handling for unsupported cloud provider."""
        provider = TerraformProvider(cloud_provider="unsupported")
        cluster_config = provider._parse_config(sample_config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            with pytest.raises(ValueError, match="Unsupported cloud provider"):
                provider._generate_terraform_config("test-cluster", cluster_config, instance_dir)
    
    def test_create_setup_scripts(self, provider):
        """Test setup script creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            provider._create_setup_scripts(instance_dir)
            
            scripts_dir = instance_dir / "scripts"
            assert scripts_dir.exists()
            assert (scripts_dir / "zookeeper-setup.sh").exists()
            assert (scripts_dir / "kafka-setup.sh").exists()
            
            # Check script content
            zk_script = (scripts_dir / "zookeeper-setup.sh").read_text()
            assert "zookeeper" in zk_script.lower()
            assert "systemctl" in zk_script
            
            kafka_script = (scripts_dir / "kafka-setup.sh").read_text()
            assert "kafka" in kafka_script.lower()
            assert "broker.id" in kafka_script
    
    def test_terraform_init_success(self, provider, mock_subprocess):
        """Test successful Terraform initialization."""
        mock_subprocess.run.return_value.returncode = 0
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            provider._terraform_init(instance_dir)
            
            # Check that terraform init was called
            calls = mock_subprocess.run.call_args_list
            init_call = next((call for call in calls if "init" in call[0][0]), None)
            assert init_call is not None
    
    def test_terraform_init_failure(self, provider, mock_subprocess):
        """Test Terraform initialization failure."""
        # First call (version check) succeeds, second call (init) fails
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=1, stderr="Init failed")  # init failure
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            with pytest.raises(Exception, match="Terraform init failed"):
                provider._terraform_init(instance_dir)
    
    def test_terraform_apply_success(self, provider, mock_subprocess):
        """Test successful Terraform apply."""
        # Mock successful plan and apply
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout="Plan successful"),    # plan
            Mock(returncode=0, stdout="Apply successful")    # apply
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            provider._terraform_apply("test-cluster", instance_dir)
            
            # Check that plan and apply were called
            calls = mock_subprocess.run.call_args_list
            plan_call = next((call for call in calls if "plan" in call[0][0]), None)
            apply_call = next((call for call in calls if "apply" in call[0][0]), None)
            
            assert plan_call is not None
            assert apply_call is not None
    
    def test_terraform_apply_plan_failure(self, provider, mock_subprocess):
        """Test Terraform apply with plan failure."""
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=1, stderr="Plan failed")        # plan failure
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            with pytest.raises(Exception, match="Terraform plan failed"):
                provider._terraform_apply("test-cluster", instance_dir)
    
    def test_terraform_apply_apply_failure(self, provider, mock_subprocess):
        """Test Terraform apply with apply failure."""
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout="Plan successful"),    # plan success
            Mock(returncode=1, stderr="Apply failed")        # apply failure
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            with pytest.raises(Exception, match="Terraform apply failed"):
                provider._terraform_apply("test-cluster", instance_dir)
    
    def test_get_terraform_outputs_success(self, provider, mock_subprocess):
        """Test successful Terraform outputs retrieval."""
        outputs_json = {
            "bootstrap_servers": {"value": ["10.0.1.10:9092", "10.0.1.11:9092"]},
            "zookeeper_connect": {"value": "10.0.1.20:2181"}
        }
        
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout=json.dumps(outputs_json))  # outputs
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            connection_info = provider._get_terraform_outputs(instance_dir)
            
            assert connection_info is not None
            assert connection_info.bootstrap_servers == ["10.0.1.10:9092", "10.0.1.11:9092"]
            assert connection_info.zookeeper_connect == "10.0.1.20:2181"
    
    def test_get_terraform_outputs_failure(self, provider, mock_subprocess):
        """Test Terraform outputs retrieval failure."""
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=1, stderr="No outputs")         # outputs failure
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            connection_info = provider._get_terraform_outputs(instance_dir)
            
            assert connection_info is None
    
    def test_get_terraform_outputs_missing_data(self, provider, mock_subprocess):
        """Test Terraform outputs with missing required data."""
        outputs_json = {
            "bootstrap_servers": {"value": []},  # Empty
            "zookeeper_connect": {"value": ""}   # Empty
        }
        
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout=json.dumps(outputs_json))  # outputs
        ]
        
        with tempfile.TemporaryDirectory() as temp_dir:
            instance_dir = Path(temp_dir)
            
            connection_info = provider._get_terraform_outputs(instance_dir)
            
            assert connection_info is None
    
    def test_provision_cluster_success(self, provider, mock_subprocess, sample_config):
        """Test successful cluster provisioning."""
        outputs_json = {
            "bootstrap_servers": {"value": ["10.0.1.10:9092"]},
            "zookeeper_connect": {"value": "10.0.1.20:2181"}
        }
        
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout="Init successful"),   # init
            Mock(returncode=0, stdout="Plan successful"),   # plan
            Mock(returncode=0, stdout="Apply successful"),  # apply
            Mock(returncode=0, stdout=json.dumps(outputs_json))  # outputs
        ]
        
        with patch.object(provider, '_create_instance_directory') as mock_create_dir:
            with tempfile.TemporaryDirectory() as temp_dir:
                instance_dir = Path(temp_dir)
                mock_create_dir.return_value = instance_dir
                
                result = provider.provision_cluster("test-cluster", sample_config)
                
                assert result.status == ProvisioningStatus.SUCCEEDED
                assert result.instance_id == "test-cluster"
                assert result.connection_info is not None
    
    def test_provision_cluster_failure(self, provider, mock_subprocess, sample_config):
        """Test cluster provisioning failure."""
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=1, stderr="Init failed")        # init failure
        ]
        
        with patch.object(provider, '_create_instance_directory') as mock_create_dir:
            with patch.object(provider, '_cleanup_cluster') as mock_cleanup:
                with tempfile.TemporaryDirectory() as temp_dir:
                    instance_dir = Path(temp_dir)
                    mock_create_dir.return_value = instance_dir
                    
                    result = provider.provision_cluster("test-cluster", sample_config)
                    
                    assert result.status == ProvisioningStatus.FAILED
                    assert result.instance_id == "test-cluster"
                    assert "Init failed" in result.error_message
                    mock_cleanup.assert_called_once_with("test-cluster")
    
    def test_deprovision_cluster_success(self, provider):
        """Test successful cluster deprovisioning."""
        with patch.object(provider, '_cleanup_cluster') as mock_cleanup:
            result = provider.deprovision_cluster("test-cluster")
            
            assert result.status == ProvisioningStatus.SUCCEEDED
            assert result.instance_id == "test-cluster"
            mock_cleanup.assert_called_once_with("test-cluster")
    
    def test_deprovision_cluster_failure(self, provider):
        """Test cluster deprovisioning failure."""
        with patch.object(provider, '_cleanup_cluster') as mock_cleanup:
            mock_cleanup.side_effect = Exception("Cleanup failed")
            
            result = provider.deprovision_cluster("test-cluster")
            
            assert result.status == ProvisioningStatus.FAILED
            assert result.instance_id == "test-cluster"
            assert "Cleanup failed" in result.error_message
    
    def test_get_cluster_status_succeeded(self, provider, mock_subprocess):
        """Test cluster status when cluster is healthy."""
        outputs_json = {
            "bootstrap_servers": {"value": ["10.0.1.10:9092"]},
            "zookeeper_connect": {"value": "10.0.1.20:2181"}
        }
        
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout='{"resources": []}'),  # state
            Mock(returncode=0, stdout=json.dumps(outputs_json))  # outputs
        ]
        
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            with tempfile.TemporaryDirectory() as temp_dir:
                instance_dir = Path(temp_dir)
                mock_get_dir.return_value = instance_dir
                
                status = provider.get_cluster_status("test-cluster")
                
                assert status == ProvisioningStatus.SUCCEEDED
    
    def test_get_cluster_status_failed_no_directory(self, provider):
        """Test cluster status when instance directory doesn't exist."""
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            mock_get_dir.return_value = None
            
            status = provider.get_cluster_status("test-cluster")
            
            assert status == ProvisioningStatus.FAILED
    
    def test_get_connection_info_success(self, provider, mock_subprocess):
        """Test successful connection info retrieval."""
        outputs_json = {
            "bootstrap_servers": {"value": ["10.0.1.10:9092"]},
            "zookeeper_connect": {"value": "10.0.1.20:2181"}
        }
        
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout=json.dumps(outputs_json))  # outputs
        ]
        
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            with tempfile.TemporaryDirectory() as temp_dir:
                instance_dir = Path(temp_dir)
                mock_get_dir.return_value = instance_dir
                
                connection_info = provider.get_connection_info("test-cluster")
                
                assert connection_info is not None
                assert connection_info["bootstrap_servers"] == ["10.0.1.10:9092"]
                assert connection_info["zookeeper_connect"] == "10.0.1.20:2181"
    
    def test_get_connection_info_no_directory(self, provider):
        """Test connection info when instance directory doesn't exist."""
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            mock_get_dir.return_value = None
            
            connection_info = provider.get_connection_info("test-cluster")
            
            assert connection_info is None
    
    def test_health_check_healthy(self, provider, mock_subprocess):
        """Test health check for healthy cluster."""
        outputs_json = {
            "bootstrap_servers": {"value": ["10.0.1.10:9092"]},
            "zookeeper_connect": {"value": "10.0.1.20:2181"}
        }
        
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout=json.dumps(outputs_json))  # outputs
        ]
        
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            with tempfile.TemporaryDirectory() as temp_dir:
                instance_dir = Path(temp_dir)
                mock_get_dir.return_value = instance_dir
                
                is_healthy = provider.health_check("test-cluster")
                
                assert is_healthy is True
    
    def test_health_check_unhealthy(self, provider):
        """Test health check for unhealthy cluster."""
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            mock_get_dir.return_value = None
            
            is_healthy = provider.health_check("test-cluster")
            
            assert is_healthy is False
    
    def test_cleanup_cluster_success(self, provider, mock_subprocess):
        """Test successful cluster cleanup."""
        mock_subprocess.run.side_effect = [
            Mock(returncode=0, stdout="Terraform v1.5.0"),  # version check
            Mock(returncode=0, stdout="Destroy successful")  # destroy
        ]
        
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            with patch('kafka_ops_agent.providers.terraform_provider.shutil') as mock_shutil:
                with tempfile.TemporaryDirectory() as temp_dir:
                    instance_dir = Path(temp_dir)
                    mock_get_dir.return_value = instance_dir
                    
                    provider._cleanup_cluster("test-cluster")
                    
                    mock_shutil.rmtree.assert_called_once_with(instance_dir)
    
    def test_cleanup_cluster_no_directory(self, provider):
        """Test cluster cleanup when directory doesn't exist."""
        with patch.object(provider, '_get_instance_directory') as mock_get_dir:
            mock_get_dir.return_value = None
            
            # Should not raise exception
            provider._cleanup_cluster("test-cluster")


if __name__ == "__main__":
    pytest.main([__file__])