"""Terraform runtime provider for Kafka clusters."""

import logging
import time
import json
import subprocess
import tempfile
import shutil
from typing import Dict, Any, Optional, List
from pathlib import Path

from kafka_ops_agent.providers.base import (
    RuntimeProvider, 
    ProvisioningResult, 
    DeprovisioningResult, 
    ProvisioningStatus
)
from kafka_ops_agent.models.cluster import ClusterConfig, ConnectionInfo

logger = logging.getLogger(__name__)


class TerraformProvider(RuntimeProvider):
    """Terraform-based Kafka cluster provider for cloud deployments."""
    
    def __init__(self, 
                 terraform_binary: str = "terraform",
                 working_dir: Optional[str] = None,
                 cloud_provider: str = "aws"):
        """Initialize Terraform provider.
        
        Args:
            terraform_binary: Path to terraform binary
            working_dir: Working directory for Terraform files (None for temp dir)
            cloud_provider: Target cloud provider (aws, gcp, azure)
        """
        self.terraform_binary = terraform_binary
        self.working_dir = Path(working_dir) if working_dir else None
        self.cloud_provider = cloud_provider.lower()
        
        # Validate terraform installation
        try:
            result = subprocess.run(
                [self.terraform_binary, "version"],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                raise Exception(f"Terraform not found or not working: {result.stderr}")
            
            logger.info(f"Terraform provider initialized for {self.cloud_provider}")
            logger.info(f"Terraform version: {result.stdout.strip()}")
            
        except FileNotFoundError:
            raise Exception(f"Terraform binary not found at: {self.terraform_binary}")
        except Exception as e:
            logger.error(f"Failed to initialize Terraform provider: {e}")
            raise
    
    def provision_cluster(self, instance_id: str, config: Dict[str, Any]) -> ProvisioningResult:
        """Provision a new Kafka cluster using Terraform."""
        try:
            logger.info(f"Starting Terraform provisioning for cluster {instance_id}")
            
            # Parse configuration
            cluster_config = self._parse_config(config)
            
            # Create working directory for this instance
            instance_dir = self._create_instance_directory(instance_id)
            
            # Generate Terraform configuration
            self._generate_terraform_config(instance_id, cluster_config, instance_dir)
            
            # Initialize Terraform
            self._terraform_init(instance_dir)
            
            # Plan and apply Terraform configuration
            self._terraform_apply(instance_id, instance_dir)
            
            # Get connection information from Terraform outputs
            connection_info = self._get_terraform_outputs(instance_dir)
            
            logger.info(f"Successfully provisioned Terraform cluster {instance_id}")
            
            return ProvisioningResult(
                status=ProvisioningStatus.SUCCEEDED,
                instance_id=instance_id,
                connection_info=connection_info.__dict__ if connection_info else None
            )
            
        except Exception as e:
            logger.error(f"Failed to provision Terraform cluster {instance_id}: {e}")
            # Cleanup on failure
            try:
                self._cleanup_cluster(instance_id)
            except Exception as cleanup_error:
                logger.warning(f"Cleanup failed: {cleanup_error}")
            
            return ProvisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=str(e)
            )
    
    def deprovision_cluster(self, instance_id: str) -> DeprovisioningResult:
        """Deprovision an existing Kafka cluster."""
        try:
            logger.info(f"Starting Terraform deprovisioning for cluster {instance_id}")
            
            self._cleanup_cluster(instance_id)
            
            logger.info(f"Successfully deprovisioned Terraform cluster {instance_id}")
            
            return DeprovisioningResult(
                status=ProvisioningStatus.SUCCEEDED,
                instance_id=instance_id
            )
            
        except Exception as e:
            logger.error(f"Failed to deprovision Terraform cluster {instance_id}: {e}")
            
            return DeprovisioningResult(
                status=ProvisioningStatus.FAILED,
                instance_id=instance_id,
                error_message=str(e)
            )
    
    def get_cluster_status(self, instance_id: str) -> ProvisioningStatus:
        """Get the current status of a Terraform-managed cluster."""
        try:
            instance_dir = self._get_instance_directory(instance_id)
            
            if not instance_dir or not instance_dir.exists():
                return ProvisioningStatus.FAILED
            
            # Check Terraform state
            state_info = self._get_terraform_state(instance_dir)
            
            if not state_info:
                return ProvisioningStatus.FAILED
            
            # Check if resources are created and healthy
            if self._check_cluster_health(instance_dir):
                return ProvisioningStatus.SUCCEEDED
            else:
                return ProvisioningStatus.IN_PROGRESS
                
        except Exception as e:
            logger.error(f"Failed to get status for Terraform cluster {instance_id}: {e}")
            return ProvisioningStatus.FAILED
    
    def get_connection_info(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get connection information for a Terraform-managed cluster."""
        try:
            instance_dir = self._get_instance_directory(instance_id)
            
            if not instance_dir or not instance_dir.exists():
                return None
            
            connection_info = self._get_terraform_outputs(instance_dir)
            
            return connection_info.__dict__ if connection_info else None
            
        except Exception as e:
            logger.error(f"Failed to get connection info for Terraform cluster {instance_id}: {e}")
            return None
    
    def health_check(self, instance_id: str) -> bool:
        """Check if a Terraform-managed cluster is healthy and accessible."""
        try:
            instance_dir = self._get_instance_directory(instance_id)
            
            if not instance_dir or not instance_dir.exists():
                return False
            
            return self._check_cluster_health(instance_dir)
            
        except Exception as e:
            logger.error(f"Health check failed for Terraform cluster {instance_id}: {e}")
            return False    

    def _parse_config(self, config: Dict[str, Any]) -> ClusterConfig:
        """Parse configuration into ClusterConfig object."""
        return ClusterConfig(
            cluster_size=config.get('cluster_size', 3),  # Default to 3 for cloud
            replication_factor=config.get('replication_factor', 2),
            partition_count=config.get('partition_count', 6),
            retention_hours=config.get('retention_hours', 168),
            storage_size_gb=config.get('storage_size_gb', 100),  # Larger default for cloud
            enable_ssl=config.get('enable_ssl', True),  # Enable SSL by default for cloud
            enable_sasl=config.get('enable_sasl', True),  # Enable SASL by default for cloud
            custom_properties=config.get('custom_properties', {})
        )
    
    def _create_instance_directory(self, instance_id: str) -> Path:
        """Create working directory for Terraform instance."""
        if self.working_dir:
            instance_dir = self.working_dir / instance_id
        else:
            # Use system temp directory
            temp_base = Path(tempfile.gettempdir()) / "kafka-terraform"
            temp_base.mkdir(exist_ok=True)
            instance_dir = temp_base / instance_id
        
        instance_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created instance directory: {instance_dir}")
        
        return instance_dir
    
    def _get_instance_directory(self, instance_id: str) -> Optional[Path]:
        """Get the working directory for a Terraform instance."""
        if self.working_dir:
            instance_dir = self.working_dir / instance_id
        else:
            temp_base = Path(tempfile.gettempdir()) / "kafka-terraform"
            instance_dir = temp_base / instance_id
        
        return instance_dir if instance_dir.exists() else None
    
    def _generate_terraform_config(self, instance_id: str, config: ClusterConfig, instance_dir: Path):
        """Generate Terraform configuration files."""
        # Generate main configuration
        main_tf = self._generate_main_tf(instance_id, config)
        
        # Generate variables
        variables_tf = self._generate_variables_tf()
        
        # Generate outputs
        outputs_tf = self._generate_outputs_tf()
        
        # Generate provider-specific configuration
        if self.cloud_provider == "aws":
            provider_tf = self._generate_aws_provider_tf()
            resources_tf = self._generate_aws_resources_tf(instance_id, config)
        elif self.cloud_provider == "gcp":
            provider_tf = self._generate_gcp_provider_tf()
            resources_tf = self._generate_gcp_resources_tf(instance_id, config)
        elif self.cloud_provider == "azure":
            provider_tf = self._generate_azure_provider_tf()
            resources_tf = self._generate_azure_resources_tf(instance_id, config)
        else:
            raise ValueError(f"Unsupported cloud provider: {self.cloud_provider}")
        
        # Write files
        (instance_dir / "main.tf").write_text(main_tf)
        (instance_dir / "variables.tf").write_text(variables_tf)
        (instance_dir / "outputs.tf").write_text(outputs_tf)
        (instance_dir / "provider.tf").write_text(provider_tf)
        (instance_dir / "resources.tf").write_text(resources_tf)
        
        # Generate terraform.tfvars
        tfvars = self._generate_tfvars(instance_id, config)
        (instance_dir / "terraform.tfvars").write_text(tfvars)
        
        logger.info(f"Generated Terraform configuration in {instance_dir}")
    
    def _generate_main_tf(self, instance_id: str, config: ClusterConfig) -> str:
        """Generate main Terraform configuration."""
        return f'''# Kafka cluster: {instance_id}
# Generated by Kafka Ops Agent

terraform {{
  required_version = ">= 1.0"
  
  required_providers {{
    {self._get_provider_requirements()}
  }}
}}

locals {{
  cluster_name = var.cluster_name
  environment = var.environment
  
  common_tags = {{
    Name        = "${{local.cluster_name}}"
    Environment = "${{local.environment}}"
    ManagedBy   = "kafka-ops-agent"
    ClusterSize = "{config.cluster_size}"
  }}
}}
'''
    
    def _generate_variables_tf(self) -> str:
        """Generate Terraform variables."""
        return '''variable "cluster_name" {
  description = "Name of the Kafka cluster"
  type        = string
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "cluster_size" {
  description = "Number of Kafka brokers"
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "Instance type for Kafka brokers"
  type        = string
}

variable "storage_size_gb" {
  description = "Storage size in GB per broker"
  type        = number
  default     = 100
}

variable "enable_ssl" {
  description = "Enable SSL encryption"
  type        = bool
  default     = true
}

variable "enable_sasl" {
  description = "Enable SASL authentication"
  type        = bool
  default     = true
}

variable "retention_hours" {
  description = "Default log retention in hours"
  type        = number
  default     = 168
}

variable "partition_count" {
  description = "Default partition count for topics"
  type        = number
  default     = 6
}

variable "replication_factor" {
  description = "Default replication factor"
  type        = number
  default     = 2
}
'''
    
    def _generate_outputs_tf(self) -> str:
        """Generate Terraform outputs."""
        if self.cloud_provider == "aws":
            return '''output "bootstrap_servers" {
  description = "Kafka bootstrap servers"
  value       = [for instance in aws_instance.kafka : "${instance.private_ip}:9092"]
}

output "zookeeper_connect" {
  description = "Zookeeper connection string"
  value       = join(",", [for instance in aws_instance.zookeeper : "${instance.private_ip}:2181"])
}

output "cluster_id" {
  description = "Kafka cluster ID"
  value       = var.cluster_name
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.kafka_vpc.id
}

output "security_group_id" {
  description = "Security group ID"
  value       = aws_security_group.kafka_sg.id
}

output "broker_instance_ids" {
  description = "Kafka broker instance IDs"
  value       = aws_instance.kafka[*].id
}

output "zookeeper_instance_ids" {
  description = "Zookeeper instance IDs"
  value       = aws_instance.zookeeper[*].id
}
'''
        elif self.cloud_provider == "gcp":
            return '''output "bootstrap_servers" {
  description = "Kafka bootstrap servers"
  value       = [for instance in google_compute_instance.kafka : "${instance.network_interface[0].network_ip}:9092"]
}

output "zookeeper_connect" {
  description = "Zookeeper connection string"
  value       = join(",", [for instance in google_compute_instance.zookeeper : "${instance.network_interface[0].network_ip}:2181"])
}

output "cluster_id" {
  description = "Kafka cluster ID"
  value       = var.cluster_name
}

output "network_name" {
  description = "VPC network name"
  value       = google_compute_network.kafka_network.name
}

output "broker_instance_names" {
  description = "Kafka broker instance names"
  value       = google_compute_instance.kafka[*].name
}

output "zookeeper_instance_names" {
  description = "Zookeeper instance names"
  value       = google_compute_instance.zookeeper[*].name
}
'''
        else:  # Azure
            return '''output "bootstrap_servers" {
  description = "Kafka bootstrap servers"
  value       = [for instance in azurerm_linux_virtual_machine.kafka : "${instance.private_ip_address}:9092"]
}

output "zookeeper_connect" {
  description = "Zookeeper connection string"
  value       = join(",", [for instance in azurerm_linux_virtual_machine.zookeeper : "${instance.private_ip_address}:2181"])
}

output "cluster_id" {
  description = "Kafka cluster ID"
  value       = var.cluster_name
}

output "resource_group_name" {
  description = "Resource group name"
  value       = azurerm_resource_group.kafka_rg.name
}

output "virtual_network_name" {
  description = "Virtual network name"
  value       = azurerm_virtual_network.kafka_vnet.name
}

output "broker_vm_names" {
  description = "Kafka broker VM names"
  value       = azurerm_linux_virtual_machine.kafka[*].name
}

output "zookeeper_vm_names" {
  description = "Zookeeper VM names"
  value       = azurerm_linux_virtual_machine.zookeeper[*].name
}
'''
    
    def _get_provider_requirements(self) -> str:
        """Get provider requirements based on cloud provider."""
        if self.cloud_provider == "aws":
            return '''aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }'''
        elif self.cloud_provider == "gcp":
            return '''google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }'''
        else:  # Azure
            return '''azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }'''
    
    def _generate_aws_provider_tf(self) -> str:
        """Generate AWS provider configuration."""
        return '''provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "availability_zones" {
  description = "Availability zones"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}
'''
    
    def _generate_gcp_provider_tf(self) -> str:
        """Generate GCP provider configuration."""
        return '''provider "google" {
  project = var.gcp_project
  region  = var.gcp_region
}

variable "gcp_project" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcp_zones" {
  description = "GCP zones"
  type        = list(string)
  default     = ["us-central1-a", "us-central1-b", "us-central1-c"]
}
'''
    
    def _generate_azure_provider_tf(self) -> str:
        """Generate Azure provider configuration."""
        return '''provider "azurerm" {
  features {}
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "East US"
}
''' 
   
    def _generate_aws_resources_tf(self, instance_id: str, config: ClusterConfig) -> str:
        """Generate AWS-specific Terraform resources."""
        return f'''# AWS Resources for Kafka cluster: {instance_id}

# VPC
resource "aws_vpc" "kafka_vpc" {{
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-vpc"
  }})
}}

# Internet Gateway
resource "aws_internet_gateway" "kafka_igw" {{
  vpc_id = aws_vpc.kafka_vpc.id
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-igw"
  }})
}}

# Subnets
resource "aws_subnet" "kafka_subnet" {{
  count             = min(var.cluster_size, length(var.availability_zones))
  vpc_id            = aws_vpc.kafka_vpc.id
  cidr_block        = "10.0.${{count.index + 1}}.0/24"
  availability_zone = var.availability_zones[count.index]
  
  map_public_ip_on_launch = true
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-subnet-${{count.index + 1}}"
  }})
}}

# Route Table
resource "aws_route_table" "kafka_rt" {{
  vpc_id = aws_vpc.kafka_vpc.id
  
  route {{
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.kafka_igw.id
  }}
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-rt"
  }})
}}

# Route Table Association
resource "aws_route_table_association" "kafka_rta" {{
  count          = length(aws_subnet.kafka_subnet)
  subnet_id      = aws_subnet.kafka_subnet[count.index].id
  route_table_id = aws_route_table.kafka_rt.id
}}

# Security Group
resource "aws_security_group" "kafka_sg" {{
  name_prefix = "${{local.cluster_name}}-sg"
  vpc_id      = aws_vpc.kafka_vpc.id
  
  # Kafka broker port
  ingress {{
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.kafka_vpc.cidr_block]
  }}
  
  # Zookeeper port
  ingress {{
    from_port   = 2181
    to_port     = 2181
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.kafka_vpc.cidr_block]
  }}
  
  # Zookeeper peer ports
  ingress {{
    from_port   = 2888
    to_port     = 3888
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.kafka_vpc.cidr_block]
  }}
  
  # SSH access
  ingress {{
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  
  # All outbound traffic
  egress {{
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-sg"
  }})
}}

# Key Pair
resource "aws_key_pair" "kafka_key" {{
  key_name   = "${{local.cluster_name}}-key"
  public_key = file("~/.ssh/id_rsa.pub")
  
  tags = local.common_tags
}}

# Zookeeper Instances
resource "aws_instance" "zookeeper" {{
  count                  = 1  # Single ZK for simplicity
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name              = aws_key_pair.kafka_key.key_name
  vpc_security_group_ids = [aws_security_group.kafka_sg.id]
  subnet_id             = aws_subnet.kafka_subnet[count.index % length(aws_subnet.kafka_subnet)].id
  
  root_block_device {{
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }}
  
  user_data = templatefile("${{path.module}}/scripts/zookeeper-setup.sh", {{
    zk_id = count.index + 1
  }})
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-zookeeper-${{count.index + 1}}"
    Role = "zookeeper"
  }})
}}

# Kafka Broker Instances
resource "aws_instance" "kafka" {{
  count                  = var.cluster_size
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  key_name              = aws_key_pair.kafka_key.key_name
  vpc_security_group_ids = [aws_security_group.kafka_sg.id]
  subnet_id             = aws_subnet.kafka_subnet[count.index % length(aws_subnet.kafka_subnet)].id
  
  root_block_device {{
    volume_type = "gp3"
    volume_size = var.storage_size_gb
    encrypted   = true
  }}
  
  user_data = templatefile("${{path.module}}/scripts/kafka-setup.sh", {{
    broker_id           = count.index + 1
    zookeeper_connect   = join(",", [for zk in aws_instance.zookeeper : "${{zk.private_ip}}:2181"])
    retention_hours     = var.retention_hours
    partition_count     = var.partition_count
    replication_factor  = var.replication_factor
    enable_ssl          = var.enable_ssl
    enable_sasl         = var.enable_sasl
  }})
  
  tags = merge(local.common_tags, {{
    Name = "${{local.cluster_name}}-kafka-${{count.index + 1}}"
    Role = "kafka-broker"
  }})
}}

# Data source for Ubuntu AMI
data "aws_ami" "ubuntu" {{
  most_recent = true
  owners      = ["099720109477"] # Canonical
  
  filter {{
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-22.04-amd64-server-*"]
  }}
  
  filter {{
    name   = "virtualization-type"
    values = ["hvm"]
  }}
}}
'''
    
    def _generate_gcp_resources_tf(self, instance_id: str, config: ClusterConfig) -> str:
        """Generate GCP-specific Terraform resources."""
        return f'''# GCP Resources for Kafka cluster: {instance_id}

# VPC Network
resource "google_compute_network" "kafka_network" {{
  name                    = "${{local.cluster_name}}-network"
  auto_create_subnetworks = false
}}

# Subnet
resource "google_compute_subnetwork" "kafka_subnet" {{
  name          = "${{local.cluster_name}}-subnet"
  ip_cidr_range = "10.0.0.0/16"
  region        = var.gcp_region
  network       = google_compute_network.kafka_network.id
}}

# Firewall Rules
resource "google_compute_firewall" "kafka_firewall" {{
  name    = "${{local.cluster_name}}-firewall"
  network = google_compute_network.kafka_network.name
  
  allow {{
    protocol = "tcp"
    ports    = ["22", "2181", "2888", "3888", "9092"]
  }}
  
  source_ranges = ["10.0.0.0/16"]
  target_tags   = ["kafka-cluster"]
}}

# External firewall for SSH
resource "google_compute_firewall" "kafka_ssh" {{
  name    = "${{local.cluster_name}}-ssh"
  network = google_compute_network.kafka_network.name
  
  allow {{
    protocol = "tcp"
    ports    = ["22"]
  }}
  
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka-cluster"]
}}

# Zookeeper Instances
resource "google_compute_instance" "zookeeper" {{
  count        = 1
  name         = "${{local.cluster_name}}-zookeeper-${{count.index + 1}}"
  machine_type = var.instance_type
  zone         = var.gcp_zones[count.index % length(var.gcp_zones)]
  
  boot_disk {{
    initialize_params {{
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 20
      type  = "pd-ssd"
    }}
  }}
  
  network_interface {{
    network    = google_compute_network.kafka_network.id
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    
    access_config {{
      // Ephemeral public IP
    }}
  }}
  
  metadata_startup_script = templatefile("${{path.module}}/scripts/zookeeper-setup.sh", {{
    zk_id = count.index + 1
  }})
  
  tags = ["kafka-cluster", "zookeeper"]
  
  labels = {{
    environment = var.environment
    role        = "zookeeper"
    managed-by  = "kafka-ops-agent"
  }}
}}

# Kafka Broker Instances
resource "google_compute_instance" "kafka" {{
  count        = var.cluster_size
  name         = "${{local.cluster_name}}-kafka-${{count.index + 1}}"
  machine_type = var.instance_type
  zone         = var.gcp_zones[count.index % length(var.gcp_zones)]
  
  boot_disk {{
    initialize_params {{
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.storage_size_gb
      type  = "pd-ssd"
    }}
  }}
  
  network_interface {{
    network    = google_compute_network.kafka_network.id
    subnetwork = google_compute_subnetwork.kafka_subnet.id
    
    access_config {{
      // Ephemeral public IP
    }}
  }}
  
  metadata_startup_script = templatefile("${{path.module}}/scripts/kafka-setup.sh", {{
    broker_id           = count.index + 1
    zookeeper_connect   = join(",", [for zk in google_compute_instance.zookeeper : "${{zk.network_interface[0].network_ip}}:2181"])
    retention_hours     = var.retention_hours
    partition_count     = var.partition_count
    replication_factor  = var.replication_factor
    enable_ssl          = var.enable_ssl
    enable_sasl         = var.enable_sasl
  }})
  
  tags = ["kafka-cluster", "kafka-broker"]
  
  labels = {{
    environment = var.environment
    role        = "kafka-broker"
    managed-by  = "kafka-ops-agent"
  }}
}}
'''
    
    def _generate_azure_resources_tf(self, instance_id: str, config: ClusterConfig) -> str:
        """Generate Azure-specific Terraform resources."""
        return f'''# Azure Resources for Kafka cluster: {instance_id}

# Resource Group
resource "azurerm_resource_group" "kafka_rg" {{
  name     = "${{local.cluster_name}}-rg"
  location = var.azure_location
  
  tags = local.common_tags
}}

# Virtual Network
resource "azurerm_virtual_network" "kafka_vnet" {{
  name                = "${{local.cluster_name}}-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.kafka_rg.location
  resource_group_name = azurerm_resource_group.kafka_rg.name
  
  tags = local.common_tags
}}

# Subnet
resource "azurerm_subnet" "kafka_subnet" {{
  name                 = "${{local.cluster_name}}-subnet"
  resource_group_name  = azurerm_resource_group.kafka_rg.name
  virtual_network_name = azurerm_virtual_network.kafka_vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}}

# Network Security Group
resource "azurerm_network_security_group" "kafka_nsg" {{
  name                = "${{local.cluster_name}}-nsg"
  location            = azurerm_resource_group.kafka_rg.location
  resource_group_name = azurerm_resource_group.kafka_rg.name
  
  security_rule {{
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }}
  
  security_rule {{
    name                       = "Kafka"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "9092"
    source_address_prefix      = "10.0.0.0/16"
    destination_address_prefix = "*"
  }}
  
  security_rule {{
    name                       = "Zookeeper"
    priority                   = 1003
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["2181", "2888", "3888"]
    source_address_prefix      = "10.0.0.0/16"
    destination_address_prefix = "*"
  }}
  
  tags = local.common_tags
}}

# Public IPs for VMs
resource "azurerm_public_ip" "kafka_public_ip" {{
  count               = var.cluster_size + 1  # +1 for Zookeeper
  name                = "${{local.cluster_name}}-public-ip-${{count.index + 1}}"
  location            = azurerm_resource_group.kafka_rg.location
  resource_group_name = azurerm_resource_group.kafka_rg.name
  allocation_method   = "Static"
  
  tags = local.common_tags
}}

# Network Interfaces for Zookeeper
resource "azurerm_network_interface" "zookeeper_nic" {{
  count               = 1
  name                = "${{local.cluster_name}}-zookeeper-nic-${{count.index + 1}}"
  location            = azurerm_resource_group.kafka_rg.location
  resource_group_name = azurerm_resource_group.kafka_rg.name
  
  ip_configuration {{
    name                          = "internal"
    subnet_id                     = azurerm_subnet.kafka_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.kafka_public_ip[count.index].id
  }}
  
  tags = local.common_tags
}}

# Network Interfaces for Kafka
resource "azurerm_network_interface" "kafka_nic" {{
  count               = var.cluster_size
  name                = "${{local.cluster_name}}-kafka-nic-${{count.index + 1}}"
  location            = azurerm_resource_group.kafka_rg.location
  resource_group_name = azurerm_resource_group.kafka_rg.name
  
  ip_configuration {{
    name                          = "internal"
    subnet_id                     = azurerm_subnet.kafka_subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.kafka_public_ip[count.index + 1].id
  }}
  
  tags = local.common_tags
}}

# Associate NSG with NICs
resource "azurerm_network_interface_security_group_association" "zookeeper_nsg_assoc" {{
  count                     = 1
  network_interface_id      = azurerm_network_interface.zookeeper_nic[count.index].id
  network_security_group_id = azurerm_network_security_group.kafka_nsg.id
}}

resource "azurerm_network_interface_security_group_association" "kafka_nsg_assoc" {{
  count                     = var.cluster_size
  network_interface_id      = azurerm_network_interface.kafka_nic[count.index].id
  network_security_group_id = azurerm_network_security_group.kafka_nsg.id
}}

# Zookeeper Virtual Machines
resource "azurerm_linux_virtual_machine" "zookeeper" {{
  count               = 1
  name                = "${{local.cluster_name}}-zookeeper-${{count.index + 1}}"
  resource_group_name = azurerm_resource_group.kafka_rg.name
  location            = azurerm_resource_group.kafka_rg.location
  size                = var.instance_type
  admin_username      = "adminuser"
  
  disable_password_authentication = true
  
  network_interface_ids = [
    azurerm_network_interface.zookeeper_nic[count.index].id,
  ]
  
  admin_ssh_key {{
    username   = "adminuser"
    public_key = file("~/.ssh/id_rsa.pub")
  }}
  
  os_disk {{
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }}
  
  source_image_reference {{
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }}
  
  custom_data = base64encode(templatefile("${{path.module}}/scripts/zookeeper-setup.sh", {{
    zk_id = count.index + 1
  }}))
  
  tags = merge(local.common_tags, {{
    Role = "zookeeper"
  }})
}}

# Kafka Virtual Machines
resource "azurerm_linux_virtual_machine" "kafka" {{
  count               = var.cluster_size
  name                = "${{local.cluster_name}}-kafka-${{count.index + 1}}"
  resource_group_name = azurerm_resource_group.kafka_rg.name
  location            = azurerm_resource_group.kafka_rg.location
  size                = var.instance_type
  admin_username      = "adminuser"
  
  disable_password_authentication = true
  
  network_interface_ids = [
    azurerm_network_interface.kafka_nic[count.index].id,
  ]
  
  admin_ssh_key {{
    username   = "adminuser"
    public_key = file("~/.ssh/id_rsa.pub")
  }}
  
  os_disk {{
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = var.storage_size_gb
  }}
  
  source_image_reference {{
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }}
  
  custom_data = base64encode(templatefile("${{path.module}}/scripts/kafka-setup.sh", {{
    broker_id           = count.index + 1
    zookeeper_connect   = join(",", [for zk in azurerm_linux_virtual_machine.zookeeper : "${{zk.private_ip_address}}:2181"])
    retention_hours     = var.retention_hours
    partition_count     = var.partition_count
    replication_factor  = var.replication_factor
    enable_ssl          = var.enable_ssl
    enable_sasl         = var.enable_sasl
  }}))
  
  tags = merge(local.common_tags, {{
    Role = "kafka-broker"
  }})
}}
'''   
 
    def _generate_tfvars(self, instance_id: str, config: ClusterConfig) -> str:
        """Generate terraform.tfvars file."""
        instance_type_map = {
            "aws": "t3.medium",
            "gcp": "e2-standard-2", 
            "azure": "Standard_B2s"
        }
        
        tfvars = f'''cluster_name = "{instance_id}"
environment = "dev"
cluster_size = {config.cluster_size}
instance_type = "{instance_type_map.get(self.cloud_provider, 't3.medium')}"
storage_size_gb = {config.storage_size_gb}
enable_ssl = {str(config.enable_ssl).lower()}
enable_sasl = {str(config.enable_sasl).lower()}
retention_hours = {config.retention_hours}
partition_count = {config.partition_count}
replication_factor = {config.replication_factor}
'''
        
        # Add cloud-specific variables
        if self.cloud_provider == "aws":
            tfvars += '''aws_region = "us-west-2"
availability_zones = ["us-west-2a", "us-west-2b", "us-west-2c"]
'''
        elif self.cloud_provider == "gcp":
            tfvars += '''gcp_project = "your-gcp-project"
gcp_region = "us-central1"
gcp_zones = ["us-central1-a", "us-central1-b", "us-central1-c"]
'''
        elif self.cloud_provider == "azure":
            tfvars += '''azure_location = "East US"
'''
        
        return tfvars
    
    def _create_setup_scripts(self, instance_dir: Path):
        """Create setup scripts for Kafka and Zookeeper."""
        scripts_dir = instance_dir / "scripts"
        scripts_dir.mkdir(exist_ok=True)
        
        # Zookeeper setup script
        zk_script = '''#!/bin/bash
set -e

# Update system
apt-get update
apt-get install -y openjdk-11-jdk wget

# Create kafka user
useradd -m -s /bin/bash kafka

# Download and install Zookeeper (part of Kafka)
cd /opt
wget https://downloads.apache.org/kafka/2.8.2/kafka_2.13-2.8.2.tgz
tar -xzf kafka_2.13-2.8.2.tgz
mv kafka_2.13-2.8.2 kafka
chown -R kafka:kafka kafka

# Configure Zookeeper
cat > /opt/kafka/config/zookeeper.properties << EOF
dataDir=/var/lib/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
server.${zk_id}=0.0.0.0:2888:3888
EOF

# Create data directory
mkdir -p /var/lib/zookeeper
echo "${zk_id}" > /var/lib/zookeeper/myid
chown -R kafka:kafka /var/lib/zookeeper

# Create systemd service
cat > /etc/systemd/system/zookeeper.service << EOF
[Unit]
Description=Apache Zookeeper
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Start Zookeeper
systemctl daemon-reload
systemctl enable zookeeper
systemctl start zookeeper
'''
        
        # Kafka setup script
        kafka_script = '''#!/bin/bash
set -e

# Update system
apt-get update
apt-get install -y openjdk-11-jdk wget

# Create kafka user
useradd -m -s /bin/bash kafka

# Download and install Kafka
cd /opt
wget https://downloads.apache.org/kafka/2.8.2/kafka_2.13-2.8.2.tgz
tar -xzf kafka_2.13-2.8.2.tgz
mv kafka_2.13-2.8.2 kafka
chown -R kafka:kafka kafka

# Configure Kafka
cat > /opt/kafka/config/server.properties << EOF
broker.id=${broker_id}
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4):9092
zookeeper.connect=${zookeeper_connect}
log.dirs=/var/lib/kafka-logs
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.retention.hours=${retention_hours}
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
num.partitions=${partition_count}
default.replication.factor=${replication_factor}
min.insync.replicas=1
unclean.leader.election.enable=false
delete.topic.enable=true
auto.create.topics.enable=true
EOF

# Create log directory
mkdir -p /var/lib/kafka-logs
chown -R kafka:kafka /var/lib/kafka-logs

# Create systemd service
cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka
After=network.target zookeeper.service

[Service]
Type=simple
User=kafka
Group=kafka
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Start Kafka
systemctl daemon-reload
systemctl enable kafka
systemctl start kafka
'''
        
        (scripts_dir / "zookeeper-setup.sh").write_text(zk_script)
        (scripts_dir / "kafka-setup.sh").write_text(kafka_script)
        
        # Make scripts executable
        (scripts_dir / "zookeeper-setup.sh").chmod(0o755)
        (scripts_dir / "kafka-setup.sh").chmod(0o755)
    
    def _terraform_init(self, instance_dir: Path):
        """Initialize Terraform in the instance directory."""
        logger.info(f"Initializing Terraform in {instance_dir}")
        
        result = subprocess.run(
            [self.terraform_binary, "init"],
            cwd=instance_dir,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            raise Exception(f"Terraform init failed: {result.stderr}")
        
        logger.info("Terraform initialized successfully")
    
    def _terraform_apply(self, instance_id: str, instance_dir: Path):
        """Apply Terraform configuration."""
        logger.info(f"Applying Terraform configuration for {instance_id}")
        
        # Create setup scripts
        self._create_setup_scripts(instance_dir)
        
        # Run terraform plan first
        plan_result = subprocess.run(
            [self.terraform_binary, "plan", "-out=tfplan"],
            cwd=instance_dir,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if plan_result.returncode != 0:
            raise Exception(f"Terraform plan failed: {plan_result.stderr}")
        
        # Apply the plan
        apply_result = subprocess.run(
            [self.terraform_binary, "apply", "-auto-approve", "tfplan"],
            cwd=instance_dir,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout for apply
        )
        
        if apply_result.returncode != 0:
            raise Exception(f"Terraform apply failed: {apply_result.stderr}")
        
        logger.info(f"Terraform apply completed for {instance_id}")
    
    def _terraform_destroy(self, instance_dir: Path):
        """Destroy Terraform-managed resources."""
        logger.info(f"Destroying Terraform resources in {instance_dir}")
        
        result = subprocess.run(
            [self.terraform_binary, "destroy", "-auto-approve"],
            cwd=instance_dir,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )
        
        if result.returncode != 0:
            logger.warning(f"Terraform destroy had issues: {result.stderr}")
            # Don't raise exception as partial cleanup is better than none
        
        logger.info("Terraform destroy completed")
    
    def _get_terraform_outputs(self, instance_dir: Path) -> Optional[ConnectionInfo]:
        """Get Terraform outputs and convert to ConnectionInfo."""
        try:
            result = subprocess.run(
                [self.terraform_binary, "output", "-json"],
                cwd=instance_dir,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                logger.error(f"Failed to get Terraform outputs: {result.stderr}")
                return None
            
            outputs = json.loads(result.stdout)
            
            bootstrap_servers = outputs.get("bootstrap_servers", {}).get("value", [])
            zookeeper_connect = outputs.get("zookeeper_connect", {}).get("value", "")
            
            if not bootstrap_servers or not zookeeper_connect:
                logger.warning("Missing required outputs from Terraform")
                return None
            
            return ConnectionInfo(
                bootstrap_servers=bootstrap_servers,
                zookeeper_connect=zookeeper_connect
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Terraform outputs: {e}")
            return None
    
    def _get_terraform_state(self, instance_dir: Path) -> Optional[Dict[str, Any]]:
        """Get Terraform state information."""
        try:
            result = subprocess.run(
                [self.terraform_binary, "show", "-json"],
                cwd=instance_dir,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                return None
            
            return json.loads(result.stdout)
            
        except Exception as e:
            logger.error(f"Failed to get Terraform state: {e}")
            return None
    
    def _check_cluster_health(self, instance_dir: Path) -> bool:
        """Check if the Terraform-managed cluster is healthy."""
        try:
            # Get connection info
            connection_info = self._get_terraform_outputs(instance_dir)
            
            if not connection_info:
                return False
            
            # Basic connectivity check could be implemented here
            # For now, just check if we have valid connection info
            return (
                len(connection_info.bootstrap_servers) > 0 and
                connection_info.zookeeper_connect is not None
            )
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def _cleanup_cluster(self, instance_id: str):
        """Clean up all Terraform resources for a cluster."""
        try:
            instance_dir = self._get_instance_directory(instance_id)
            
            if not instance_dir or not instance_dir.exists():
                logger.info(f"No Terraform directory found for {instance_id}")
                return
            
            # Destroy Terraform resources
            self._terraform_destroy(instance_dir)
            
            # Remove the instance directory
            shutil.rmtree(instance_dir)
            logger.info(f"Removed Terraform directory: {instance_dir}")
            
        except Exception as e:
            logger.error(f"Error during cleanup of Terraform cluster {instance_id}: {e}")
            raise