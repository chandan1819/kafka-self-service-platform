"""Configuration templates for different deployment scenarios."""

from typing import Dict, Any
from enum import Enum

from kafka_ops_agent.config.config_manager import ApplicationConfig


class DeploymentScenario(str, Enum):
    """Deployment scenarios."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    DOCKER_LOCAL = "docker_local"
    KUBERNETES = "kubernetes"
    CLOUD_AWS = "cloud_aws"
    CLOUD_GCP = "cloud_gcp"
    CLOUD_AZURE = "cloud_azure"


class ConfigurationTemplates:
    """Provides configuration templates for different deployment scenarios."""
    
    @staticmethod
    def get_development_config() -> Dict[str, Any]:
        """Get development configuration template."""
        return {
            "name": "Kafka Ops Agent",
            "version": "1.0.0",
            "environment": "development",
            "database": {
                "type": "sqlite",
                "sqlite_file": "data/dev_kafka_ops_agent.db",
                "connection_pool_size": 5
            },
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "security_protocol": "PLAINTEXT",
                "client_id": "kafka-ops-agent-dev",
                "request_timeout_ms": 30000,
                "admin_timeout_ms": 60000
            },
            "api": {
                "host": "127.0.0.1",
                "port": 8080,
                "debug": True,
                "workers": 1,
                "cors_enabled": True,
                "cors_origins": ["*"],
                "rate_limit_enabled": False
            },
            "logging": {
                "level": "DEBUG",
                "format": "text",
                "file_enabled": True,
                "file_path": "logs/dev_kafka_ops_agent.log",
                "console_enabled": True,
                "audit_enabled": True,
                "aggregation_enabled": False
            },
            "providers": {
                "default_provider": "docker",
                "docker_enabled": True,
                "docker_network": "kafka-ops-dev-network",
                "kubernetes_enabled": False,
                "terraform_enabled": False
            },
            "cleanup": {
                "enabled": True,
                "topic_cleanup_enabled": True,
                "topic_cleanup_schedule": "0 */6 * * *",  # Every 6 hours
                "topic_max_age_hours": 24,
                "cluster_cleanup_enabled": True,
                "metadata_cleanup_enabled": False
            },
            "features": {
                "osb_api": True,
                "topic_management": True,
                "cleanup_operations": True,
                "scheduler": True,
                "audit_logging": True
            }
        }
    
    @staticmethod
    def get_testing_config() -> Dict[str, Any]:
        """Get testing configuration template."""
        return {
            "name": "Kafka Ops Agent Test",
            "version": "1.0.0",
            "environment": "testing",
            "database": {
                "type": "sqlite",
                "sqlite_file": ":memory:",  # In-memory database for tests
                "connection_pool_size": 2
            },
            "kafka": {
                "bootstrap_servers": ["localhost:9093"],  # Different port for tests
                "security_protocol": "PLAINTEXT",
                "client_id": "kafka-ops-agent-test",
                "request_timeout_ms": 10000,  # Shorter timeout for tests
                "admin_timeout_ms": 30000
            },
            "api": {
                "host": "127.0.0.1",
                "port": 8081,  # Different port for tests
                "debug": True,
                "workers": 1,
                "cors_enabled": True,
                "rate_limit_enabled": False,
                "request_timeout": 10
            },
            "logging": {
                "level": "WARNING",  # Less verbose for tests
                "format": "json",
                "file_enabled": False,  # No file logging in tests
                "console_enabled": False,  # No console logging in tests
                "audit_enabled": False,  # No audit logging in tests
                "aggregation_enabled": False
            },
            "providers": {
                "default_provider": "docker",
                "docker_enabled": True,
                "docker_network": "kafka-ops-test-network",
                "kubernetes_enabled": False,
                "terraform_enabled": False
            },
            "cleanup": {
                "enabled": False,  # Disable cleanup in tests
                "topic_cleanup_enabled": False,
                "cluster_cleanup_enabled": False,
                "metadata_cleanup_enabled": False
            },
            "features": {
                "osb_api": True,
                "topic_management": True,
                "cleanup_operations": False,  # Disable for tests
                "scheduler": False,  # Disable for tests
                "audit_logging": False
            }
        }
    
    @staticmethod
    def get_production_config() -> Dict[str, Any]:
        """Get production configuration template."""
        return {
            "name": "Kafka Ops Agent",
            "version": "1.0.0",
            "environment": "production",
            "database": {
                "type": "postgresql",
                "host": "postgres.internal",
                "port": 5432,
                "database": "kafka_ops_agent",
                "username": "${DB_USERNAME}",  # Environment variable placeholder
                "password": "${DB_PASSWORD}",  # Environment variable placeholder
                "ssl_mode": "require",
                "connection_pool_size": 20,
                "connection_timeout": 30
            },
            "kafka": {
                "bootstrap_servers": ["kafka-1.internal:9092", "kafka-2.internal:9092", "kafka-3.internal:9092"],
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "${KAFKA_USERNAME}",
                "sasl_password": "${KAFKA_PASSWORD}",
                "ssl_ca_location": "/etc/ssl/certs/kafka-ca.pem",
                "client_id": "kafka-ops-agent-prod",
                "request_timeout_ms": 30000,
                "admin_timeout_ms": 120000,
                "max_retries": 5
            },
            "api": {
                "host": "0.0.0.0",
                "port": 8080,
                "debug": False,
                "workers": 4,
                "cors_enabled": False,  # Disable CORS in production
                "rate_limit_enabled": True,
                "rate_limit_requests": 1000,  # Higher limit for production
                "rate_limit_window": 60,
                "max_request_size": 2097152,  # 2MB
                "request_timeout": 60
            },
            "logging": {
                "level": "INFO",
                "format": "json",
                "file_enabled": True,
                "file_path": "/var/log/kafka-ops-agent/app.log",
                "file_max_size": 104857600,  # 100MB
                "file_backup_count": 10,
                "console_enabled": False,  # No console logging in production
                "audit_enabled": True,
                "audit_file_path": "/var/log/kafka-ops-agent/audit.log",
                "aggregation_enabled": True,
                "aggregation_interval": 3600
            },
            "providers": {
                "default_provider": "kubernetes",
                "docker_enabled": False,
                "kubernetes_enabled": True,
                "kubernetes_namespace": "kafka-ops",
                "terraform_enabled": True,
                "terraform_state_backend": "s3"
            },
            "cleanup": {
                "enabled": True,
                "topic_cleanup_enabled": True,
                "topic_cleanup_schedule": "0 2 * * *",  # Daily at 2 AM
                "topic_max_age_hours": 168,  # 7 days
                "cluster_cleanup_enabled": True,
                "cluster_cleanup_schedule": "0 3 * * *",  # Daily at 3 AM
                "cluster_max_age_hours": 72,  # 3 days
                "metadata_cleanup_enabled": True,
                "metadata_cleanup_schedule": "0 4 * * 0",  # Weekly on Sunday at 4 AM
                "metadata_max_age_days": 90
            },
            "features": {
                "osb_api": True,
                "topic_management": True,
                "cleanup_operations": True,
                "scheduler": True,
                "audit_logging": True
            }
        }
    
    @staticmethod
    def get_kubernetes_config() -> Dict[str, Any]:
        """Get Kubernetes deployment configuration template."""
        base_config = ConfigurationTemplates.get_production_config()
        
        # Kubernetes-specific overrides
        kubernetes_overrides = {
            "database": {
                "host": "postgres-service.kafka-ops.svc.cluster.local",
                "port": 5432
            },
            "kafka": {
                "bootstrap_servers": [
                    "kafka-service.kafka-ops.svc.cluster.local:9092"
                ]
            },
            "api": {
                "host": "0.0.0.0",
                "port": 8080
            },
            "logging": {
                "file_enabled": False,  # Use stdout/stderr in Kubernetes
                "console_enabled": True,
                "audit_file_path": "/dev/stdout"  # Audit to stdout
            },
            "providers": {
                "default_provider": "kubernetes",
                "kubernetes_enabled": True,
                "kubernetes_namespace": "kafka-ops",
                "kubernetes_config_path": "/var/run/secrets/kubernetes.io/serviceaccount"
            }
        }
        
        return ConfigurationTemplates._merge_configs(base_config, kubernetes_overrides)
    
    @staticmethod
    def get_docker_local_config() -> Dict[str, Any]:
        """Get Docker local deployment configuration template."""
        base_config = ConfigurationTemplates.get_development_config()
        
        # Docker-specific overrides
        docker_overrides = {
            "database": {
                "type": "postgresql",
                "host": "postgres",  # Docker service name
                "port": 5432,
                "database": "kafka_ops_agent",
                "username": "kafka_ops",
                "password": "kafka_ops_password"
            },
            "kafka": {
                "bootstrap_servers": ["kafka:9092"]  # Docker service name
            },
            "api": {
                "host": "0.0.0.0",
                "port": 8080
            },
            "logging": {
                "level": "INFO",
                "console_enabled": True,
                "file_enabled": False  # Use stdout in Docker
            },
            "providers": {
                "docker_enabled": True,
                "docker_host": "unix:///var/run/docker.sock",
                "docker_network": "kafka-ops-network"
            }
        }
        
        return ConfigurationTemplates._merge_configs(base_config, docker_overrides)
    
    @staticmethod
    def get_cloud_aws_config() -> Dict[str, Any]:
        """Get AWS cloud deployment configuration template."""
        base_config = ConfigurationTemplates.get_production_config()
        
        # AWS-specific overrides
        aws_overrides = {
            "database": {
                "host": "${RDS_ENDPOINT}",
                "port": 5432,
                "ssl_mode": "require"
            },
            "kafka": {
                "bootstrap_servers": ["${MSK_BOOTSTRAP_SERVERS}"],
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "AWS_MSK_IAM"
            },
            "logging": {
                "file_path": "/var/log/kafka-ops-agent/app.log",
                "aggregation_enabled": True
            },
            "providers": {
                "default_provider": "terraform",
                "terraform_enabled": True,
                "terraform_state_backend": "s3",
                "kubernetes_enabled": True  # EKS support
            }
        }
        
        return ConfigurationTemplates._merge_configs(base_config, aws_overrides)
    
    @staticmethod
    def get_cloud_gcp_config() -> Dict[str, Any]:
        """Get GCP cloud deployment configuration template."""
        base_config = ConfigurationTemplates.get_production_config()
        
        # GCP-specific overrides
        gcp_overrides = {
            "database": {
                "host": "${CLOUD_SQL_ENDPOINT}",
                "port": 5432,
                "ssl_mode": "require"
            },
            "kafka": {
                "bootstrap_servers": ["${KAFKA_BOOTSTRAP_SERVERS}"],
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN"
            },
            "logging": {
                "format": "json",  # Better for Cloud Logging
                "aggregation_enabled": True
            },
            "providers": {
                "default_provider": "terraform",
                "terraform_enabled": True,
                "terraform_state_backend": "gcs",
                "kubernetes_enabled": True  # GKE support
            }
        }
        
        return ConfigurationTemplates._merge_configs(base_config, gcp_overrides)
    
    @staticmethod
    def get_cloud_azure_config() -> Dict[str, Any]:
        """Get Azure cloud deployment configuration template."""
        base_config = ConfigurationTemplates.get_production_config()
        
        # Azure-specific overrides
        azure_overrides = {
            "database": {
                "host": "${AZURE_POSTGRES_ENDPOINT}",
                "port": 5432,
                "ssl_mode": "require"
            },
            "kafka": {
                "bootstrap_servers": ["${EVENT_HUBS_ENDPOINT}"],
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN"
            },
            "logging": {
                "format": "json",
                "aggregation_enabled": True
            },
            "providers": {
                "default_provider": "terraform",
                "terraform_enabled": True,
                "terraform_state_backend": "azurerm",
                "kubernetes_enabled": True  # AKS support
            }
        }
        
        return ConfigurationTemplates._merge_configs(base_config, azure_overrides)
    
    @staticmethod
    def get_template(scenario: DeploymentScenario) -> Dict[str, Any]:
        """Get configuration template for deployment scenario.
        
        Args:
            scenario: Deployment scenario
            
        Returns:
            Configuration template
        """
        template_map = {
            DeploymentScenario.DEVELOPMENT: ConfigurationTemplates.get_development_config,
            DeploymentScenario.TESTING: ConfigurationTemplates.get_testing_config,
            DeploymentScenario.PRODUCTION: ConfigurationTemplates.get_production_config,
            DeploymentScenario.DOCKER_LOCAL: ConfigurationTemplates.get_docker_local_config,
            DeploymentScenario.KUBERNETES: ConfigurationTemplates.get_kubernetes_config,
            DeploymentScenario.CLOUD_AWS: ConfigurationTemplates.get_cloud_aws_config,
            DeploymentScenario.CLOUD_GCP: ConfigurationTemplates.get_cloud_gcp_config,
            DeploymentScenario.CLOUD_AZURE: ConfigurationTemplates.get_cloud_azure_config,
        }
        
        if scenario not in template_map:
            raise ValueError(f"Unknown deployment scenario: {scenario}")
        
        return template_map[scenario]()
    
    @staticmethod
    def _merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Merge configuration dictionaries recursively.
        
        Args:
            base: Base configuration
            override: Override configuration
            
        Returns:
            Merged configuration
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigurationTemplates._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    @staticmethod
    def list_available_templates() -> List[str]:
        """List available configuration templates.
        
        Returns:
            List of available template names
        """
        return [scenario.value for scenario in DeploymentScenario]
    
    @staticmethod
    def get_template_description(scenario: DeploymentScenario) -> str:
        """Get description of a configuration template.
        
        Args:
            scenario: Deployment scenario
            
        Returns:
            Template description
        """
        descriptions = {
            DeploymentScenario.DEVELOPMENT: "Local development with SQLite and debug logging",
            DeploymentScenario.TESTING: "Testing environment with in-memory database and minimal logging",
            DeploymentScenario.PRODUCTION: "Production environment with PostgreSQL and comprehensive logging",
            DeploymentScenario.DOCKER_LOCAL: "Local Docker deployment with PostgreSQL container",
            DeploymentScenario.KUBERNETES: "Kubernetes deployment with service discovery",
            DeploymentScenario.CLOUD_AWS: "AWS cloud deployment with RDS and MSK",
            DeploymentScenario.CLOUD_GCP: "Google Cloud deployment with Cloud SQL and Kafka",
            DeploymentScenario.CLOUD_AZURE: "Azure cloud deployment with PostgreSQL and Event Hubs",
        }
        
        return descriptions.get(scenario, "No description available")


def generate_config_file(scenario: DeploymentScenario, output_path: str, format: str = "yaml"):
    """Generate configuration file for deployment scenario.
    
    Args:
        scenario: Deployment scenario
        output_path: Output file path
        format: Output format (yaml or json)
    """
    import json
    import yaml
    from pathlib import Path
    
    config_data = ConfigurationTemplates.get_template(scenario)
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w') as f:
        if format.lower() == 'json':
            json.dump(config_data, f, indent=2)
        else:  # Default to YAML
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
    
    print(f"Generated {scenario.value} configuration: {output_path}")


if __name__ == "__main__":
    # Generate sample configuration files
    import sys
    from pathlib import Path
    
    # Create config directory
    config_dir = Path("config/templates")
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate templates for all scenarios
    for scenario in DeploymentScenario:
        output_path = config_dir / f"{scenario.value}.yaml"
        generate_config_file(scenario, str(output_path), "yaml")
    
    print(f"Generated {len(DeploymentScenario)} configuration templates in {config_dir}")