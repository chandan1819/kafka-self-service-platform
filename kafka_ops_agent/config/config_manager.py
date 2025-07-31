"""Comprehensive configuration management system for Kafka Ops Agent."""

import os
import json
import yaml
from typing import Dict, Any, Optional, List, Union, Type
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
import logging
from pydantic import BaseModel, ValidationError, Field

from kafka_ops_agent.exceptions import ConfigurationError, ValidationError as CustomValidationError


class ConfigSource(str, Enum):
    """Configuration sources in order of precedence."""
    ENVIRONMENT = "environment"
    CLI_ARGS = "cli_args"
    CONFIG_FILE = "config_file"
    DEFAULTS = "defaults"


class ConfigFormat(str, Enum):
    """Supported configuration file formats."""
    JSON = "json"
    YAML = "yaml"
    TOML = "toml"


@dataclass
class ConfigMetadata:
    """Metadata about a configuration value."""
    source: ConfigSource
    file_path: Optional[str] = None
    env_var: Optional[str] = None
    description: Optional[str] = None
    sensitive: bool = False
    validation_rules: Optional[Dict[str, Any]] = None


class BaseConfig(BaseModel):
    """Base configuration model with validation."""
    
    class Config:
        """Pydantic configuration."""
        extra = "forbid"  # Forbid extra fields
        validate_assignment = True
        use_enum_values = True


class DatabaseConfig(BaseConfig):
    """Database configuration."""
    
    type: str = Field(default="sqlite", description="Database type (sqlite, postgresql)")
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(default="kafka_ops_agent", description="Database name")
    username: Optional[str] = Field(default=None, description="Database username")
    password: Optional[str] = Field(default=None, description="Database password")
    ssl_mode: str = Field(default="prefer", description="SSL mode for PostgreSQL")
    connection_pool_size: int = Field(default=10, ge=1, le=100, description="Connection pool size")
    connection_timeout: int = Field(default=30, ge=1, description="Connection timeout in seconds")
    
    # SQLite specific
    sqlite_file: str = Field(default="data/kafka_ops_agent.db", description="SQLite database file path")
    
    def get_connection_string(self) -> str:
        """Get database connection string."""
        if self.type == "sqlite":
            return f"sqlite:///{self.sqlite_file}"
        elif self.type == "postgresql":
            auth = ""
            if self.username:
                auth = f"{self.username}"
                if self.password:
                    auth += f":{self.password}"
                auth += "@"
            
            return f"postgresql://{auth}{self.host}:{self.port}/{self.database}?sslmode={self.ssl_mode}"
        else:
            raise ConfigurationError(f"Unsupported database type: {self.type}")


class KafkaConfig(BaseConfig):
    """Kafka client configuration."""
    
    bootstrap_servers: List[str] = Field(default=["localhost:9092"], description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism")
    sasl_username: Optional[str] = Field(default=None, description="SASL username")
    sasl_password: Optional[str] = Field(default=None, description="SASL password")
    ssl_ca_location: Optional[str] = Field(default=None, description="SSL CA certificate location")
    ssl_certificate_location: Optional[str] = Field(default=None, description="SSL certificate location")
    ssl_key_location: Optional[str] = Field(default=None, description="SSL key location")
    ssl_key_password: Optional[str] = Field(default=None, description="SSL key password")
    
    # Client settings
    client_id: str = Field(default="kafka-ops-agent", description="Kafka client ID")
    request_timeout_ms: int = Field(default=30000, ge=1000, description="Request timeout in milliseconds")
    retry_backoff_ms: int = Field(default=100, ge=1, description="Retry backoff in milliseconds")
    max_retries: int = Field(default=3, ge=0, description="Maximum number of retries")
    
    # Admin client settings
    admin_timeout_ms: int = Field(default=60000, ge=1000, description="Admin operation timeout")


class APIConfig(BaseConfig):
    """API server configuration."""
    
    host: str = Field(default="0.0.0.0", description="API server host")
    port: int = Field(default=8080, ge=1, le=65535, description="API server port")
    debug: bool = Field(default=False, description="Enable debug mode")
    workers: int = Field(default=4, ge=1, description="Number of worker processes")
    
    # Security
    api_key_header: str = Field(default="X-API-Key", description="API key header name")
    cors_enabled: bool = Field(default=True, description="Enable CORS")
    cors_origins: List[str] = Field(default=["*"], description="Allowed CORS origins")
    
    # Rate limiting
    rate_limit_enabled: bool = Field(default=True, description="Enable rate limiting")
    rate_limit_requests: int = Field(default=100, ge=1, description="Requests per minute")
    rate_limit_window: int = Field(default=60, ge=1, description="Rate limit window in seconds")
    
    # Request/Response
    max_request_size: int = Field(default=1024*1024, ge=1024, description="Maximum request size in bytes")
    request_timeout: int = Field(default=30, ge=1, description="Request timeout in seconds")


class LoggingConfig(BaseConfig):
    """Logging configuration."""
    
    level: str = Field(default="INFO", description="Log level")
    format: str = Field(default="json", description="Log format (json, text)")
    
    # File logging
    file_enabled: bool = Field(default=True, description="Enable file logging")
    file_path: str = Field(default="logs/kafka_ops_agent.log", description="Log file path")
    file_max_size: int = Field(default=100*1024*1024, ge=1024, description="Max log file size in bytes")
    file_backup_count: int = Field(default=5, ge=1, description="Number of backup log files")
    
    # Console logging
    console_enabled: bool = Field(default=True, description="Enable console logging")
    
    # Audit logging
    audit_enabled: bool = Field(default=True, description="Enable audit logging")
    audit_file_path: str = Field(default="logs/audit.log", description="Audit log file path")
    
    # Log aggregation
    aggregation_enabled: bool = Field(default=False, description="Enable log aggregation")
    aggregation_interval: int = Field(default=3600, ge=60, description="Aggregation interval in seconds")


class ProviderConfig(BaseConfig):
    """Provider configuration."""
    
    default_provider: str = Field(default="docker", description="Default runtime provider")
    
    # Docker provider
    docker_enabled: bool = Field(default=True, description="Enable Docker provider")
    docker_host: Optional[str] = Field(default=None, description="Docker host URL")
    docker_network: str = Field(default="kafka-ops-network", description="Docker network name")
    
    # Kubernetes provider
    kubernetes_enabled: bool = Field(default=False, description="Enable Kubernetes provider")
    kubernetes_namespace: str = Field(default="kafka-ops", description="Kubernetes namespace")
    kubernetes_config_path: Optional[str] = Field(default=None, description="Kubernetes config file path")
    
    # Terraform provider
    terraform_enabled: bool = Field(default=False, description="Enable Terraform provider")
    terraform_binary_path: str = Field(default="terraform", description="Terraform binary path")
    terraform_state_backend: str = Field(default="local", description="Terraform state backend")


class CleanupConfig(BaseConfig):
    """Cleanup and maintenance configuration."""
    
    enabled: bool = Field(default=True, description="Enable cleanup operations")
    
    # Topic cleanup
    topic_cleanup_enabled: bool = Field(default=True, description="Enable topic cleanup")
    topic_cleanup_schedule: str = Field(default="0 2 * * *", description="Topic cleanup cron schedule")
    topic_max_age_hours: int = Field(default=168, ge=1, description="Max topic age in hours")
    
    # Cluster cleanup
    cluster_cleanup_enabled: bool = Field(default=True, description="Enable cluster cleanup")
    cluster_cleanup_schedule: str = Field(default="0 3 * * *", description="Cluster cleanup cron schedule")
    cluster_max_age_hours: int = Field(default=72, ge=1, description="Max failed cluster age in hours")
    
    # Metadata cleanup
    metadata_cleanup_enabled: bool = Field(default=True, description="Enable metadata cleanup")
    metadata_cleanup_schedule: str = Field(default="0 4 * * 0", description="Metadata cleanup cron schedule")
    metadata_max_age_days: int = Field(default=30, ge=1, description="Max metadata age in days")


class ApplicationConfig(BaseConfig):
    """Main application configuration."""
    
    # Application info
    name: str = Field(default="Kafka Ops Agent", description="Application name")
    version: str = Field(default="1.0.0", description="Application version")
    environment: str = Field(default="development", description="Environment (development, staging, production)")
    
    # Component configurations
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    providers: ProviderConfig = Field(default_factory=ProviderConfig)
    cleanup: CleanupConfig = Field(default_factory=CleanupConfig)
    
    # Feature flags
    features: Dict[str, bool] = Field(default_factory=lambda: {
        "osb_api": True,
        "topic_management": True,
        "cleanup_operations": True,
        "scheduler": True,
        "audit_logging": True
    })


class ConfigurationManager:
    """Manages application configuration from multiple sources."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration manager.
        
        Args:
            config_file: Optional path to configuration file
        """
        self.logger = logging.getLogger('kafka_ops_agent.config')
        self.config_file = config_file
        self.metadata: Dict[str, ConfigMetadata] = {}
        self._config: Optional[ApplicationConfig] = None
        
        # Environment variable mappings
        self.env_mappings = {
            "KAFKA_OPS_AGENT_DB_TYPE": "database.type",
            "KAFKA_OPS_AGENT_DB_HOST": "database.host",
            "KAFKA_OPS_AGENT_DB_PORT": "database.port",
            "KAFKA_OPS_AGENT_DB_NAME": "database.database",
            "KAFKA_OPS_AGENT_DB_USER": "database.username",
            "KAFKA_OPS_AGENT_DB_PASSWORD": "database.password",
            "KAFKA_OPS_AGENT_KAFKA_SERVERS": "kafka.bootstrap_servers",
            "KAFKA_OPS_AGENT_KAFKA_SECURITY": "kafka.security_protocol",
            "KAFKA_OPS_AGENT_API_HOST": "api.host",
            "KAFKA_OPS_AGENT_API_PORT": "api.port",
            "KAFKA_OPS_AGENT_LOG_LEVEL": "logging.level",
            "KAFKA_OPS_AGENT_ENVIRONMENT": "environment",
        }
    
    def load_configuration(self) -> ApplicationConfig:
        """Load configuration from all sources.
        
        Returns:
            Loaded and validated configuration
        """
        # Start with defaults
        config_data = {}
        
        # Load from file if specified
        if self.config_file:
            file_data = self._load_from_file(self.config_file)
            config_data = self._merge_config(config_data, file_data)
        
        # Override with environment variables
        env_data = self._load_from_environment()
        config_data = self._merge_config(config_data, env_data)
        
        # Validate and create configuration object
        try:
            self._config = ApplicationConfig(**config_data)
            self.logger.info("Configuration loaded successfully")
            return self._config
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}")
    
    def _load_from_file(self, file_path: str) -> Dict[str, Any]:
        """Load configuration from file.
        
        Args:
            file_path: Path to configuration file
            
        Returns:
            Configuration data from file
        """
        config_path = Path(file_path)
        
        if not config_path.exists():
            self.logger.warning(f"Configuration file not found: {file_path}")
            return {}
        
        try:
            with open(config_path, 'r') as f:
                if config_path.suffix.lower() == '.json':
                    data = json.load(f)
                elif config_path.suffix.lower() in ['.yaml', '.yml']:
                    data = yaml.safe_load(f)
                else:
                    raise ConfigurationError(f"Unsupported configuration file format: {config_path.suffix}")
            
            self.logger.info(f"Loaded configuration from file: {file_path}")
            
            # Record metadata
            self._record_metadata(data, ConfigSource.CONFIG_FILE, file_path=file_path)
            
            return data
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration file {file_path}: {e}")
    
    def _load_from_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables.
        
        Returns:
            Configuration data from environment
        """
        env_data = {}
        
        for env_var, config_path in self.env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Convert value to appropriate type
                converted_value = self._convert_env_value(value)
                
                # Set nested configuration value
                self._set_nested_value(env_data, config_path, converted_value)
                
                # Record metadata
                self._record_metadata({config_path: converted_value}, ConfigSource.ENVIRONMENT, env_var=env_var)
                
                self.logger.debug(f"Loaded from environment: {env_var} -> {config_path}")
        
        return env_data
    
    def _convert_env_value(self, value: str) -> Union[str, int, bool, List[str]]:
        """Convert environment variable value to appropriate type.
        
        Args:
            value: Environment variable value
            
        Returns:
            Converted value
        """
        # Boolean values
        if value.lower() in ['true', 'yes', '1', 'on']:
            return True
        elif value.lower() in ['false', 'no', '0', 'off']:
            return False
        
        # Integer values
        try:
            return int(value)
        except ValueError:
            pass
        
        # List values (comma-separated)
        if ',' in value:
            return [item.strip() for item in value.split(',')]
        
        # String value
        return value
    
    def _set_nested_value(self, data: Dict[str, Any], path: str, value: Any):
        """Set nested dictionary value using dot notation.
        
        Args:
            data: Dictionary to update
            path: Dot-separated path (e.g., 'database.host')
            value: Value to set
        """
        keys = path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def _merge_config(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Merge configuration dictionaries.
        
        Args:
            base: Base configuration
            override: Override configuration
            
        Returns:
            Merged configuration
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _record_metadata(self, data: Dict[str, Any], source: ConfigSource, 
                        file_path: Optional[str] = None, env_var: Optional[str] = None):
        """Record metadata about configuration values.
        
        Args:
            data: Configuration data
            source: Configuration source
            file_path: Optional file path
            env_var: Optional environment variable name
        """
        def record_recursive(obj: Dict[str, Any], prefix: str = ""):
            for key, value in obj.items():
                full_key = f"{prefix}.{key}" if prefix else key
                
                if isinstance(value, dict):
                    record_recursive(value, full_key)
                else:
                    self.metadata[full_key] = ConfigMetadata(
                        source=source,
                        file_path=file_path,
                        env_var=env_var
                    )
        
        record_recursive(data)
    
    def get_config(self) -> ApplicationConfig:
        """Get current configuration.
        
        Returns:
            Current configuration
        """
        if self._config is None:
            return self.load_configuration()
        return self._config
    
    def reload_configuration(self) -> ApplicationConfig:
        """Reload configuration from all sources.
        
        Returns:
            Reloaded configuration
        """
        self._config = None
        self.metadata.clear()
        return self.load_configuration()
    
    def get_config_metadata(self, key: str) -> Optional[ConfigMetadata]:
        """Get metadata for a configuration key.
        
        Args:
            key: Configuration key (dot notation)
            
        Returns:
            Configuration metadata or None
        """
        return self.metadata.get(key)
    
    def validate_configuration(self) -> List[str]:
        """Validate current configuration.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        config = self.get_config()
        
        # Custom validation rules
        try:
            # Database validation
            if config.database.type == "postgresql":
                if not config.database.username:
                    errors.append("PostgreSQL database requires username")
                if not config.database.password:
                    errors.append("PostgreSQL database requires password")
            
            # Kafka validation
            if not config.kafka.bootstrap_servers:
                errors.append("Kafka bootstrap servers cannot be empty")
            
            # API validation
            if config.api.port < 1024 and os.getuid() != 0:
                errors.append("API port < 1024 requires root privileges")
            
            # Provider validation
            if config.providers.default_provider not in ["docker", "kubernetes", "terraform"]:
                errors.append(f"Invalid default provider: {config.providers.default_provider}")
            
        except Exception as e:
            errors.append(f"Configuration validation error: {e}")
        
        return errors
    
    def export_configuration(self, format: ConfigFormat = ConfigFormat.JSON, 
                           include_sensitive: bool = False) -> str:
        """Export configuration to string.
        
        Args:
            format: Export format
            include_sensitive: Whether to include sensitive values
            
        Returns:
            Configuration as string
        """
        config = self.get_config()
        config_dict = config.dict()
        
        # Remove sensitive values if requested
        if not include_sensitive:
            config_dict = self._remove_sensitive_values(config_dict)
        
        if format == ConfigFormat.JSON:
            return json.dumps(config_dict, indent=2, default=str)
        elif format == ConfigFormat.YAML:
            return yaml.dump(config_dict, default_flow_style=False)
        else:
            raise ConfigurationError(f"Unsupported export format: {format}")
    
    def _remove_sensitive_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive values from configuration.
        
        Args:
            data: Configuration data
            
        Returns:
            Configuration with sensitive values masked
        """
        sensitive_keys = [
            'password', 'secret', 'key', 'token', 'credential',
            'sasl_password', 'ssl_key_password'
        ]
        
        def mask_recursive(obj):
            if isinstance(obj, dict):
                return {
                    key: "***MASKED***" if any(sensitive in key.lower() for sensitive in sensitive_keys)
                    else mask_recursive(value)
                    for key, value in obj.items()
                }
            elif isinstance(obj, list):
                return [mask_recursive(item) for item in obj]
            else:
                return obj
        
        return mask_recursive(data)


# Global configuration manager instance
_config_manager: Optional[ConfigurationManager] = None


def get_config_manager(config_file: Optional[str] = None) -> ConfigurationManager:
    """Get global configuration manager instance.
    
    Args:
        config_file: Optional configuration file path
        
    Returns:
        Configuration manager instance
    """
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigurationManager(config_file)
    
    return _config_manager


def get_config() -> ApplicationConfig:
    """Get current application configuration.
    
    Returns:
        Application configuration
    """
    return get_config_manager().get_config()


def reload_config() -> ApplicationConfig:
    """Reload application configuration.
    
    Returns:
        Reloaded configuration
    """
    return get_config_manager().reload_configuration()