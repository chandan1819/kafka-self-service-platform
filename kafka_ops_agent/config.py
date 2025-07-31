"""Configuration management for KafkaOpsAgent."""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database configuration."""
    type: str = "sqlite"
    host: str = "localhost"
    port: int = 5432
    database: str = "kafka_ops_agent"
    username: str = ""
    password: str = ""
    sqlite_path: str = "kafka_ops_agent.db"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class APIConfig:
    """API configuration."""
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False
    api_key: Optional[str] = None
    enable_cors: bool = True


@dataclass
class ProviderConfig:
    """Provider-specific configuration."""
    default_provider: str = "docker"
    docker_config: Dict[str, Any] = field(default_factory=dict)
    kubernetes_config: Dict[str, Any] = field(default_factory=dict)
    terraform_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KafkaConfig:
    """Kafka-specific configuration."""
    default_partitions: int = 3
    default_replication_factor: int = 1
    default_retention_hours: int = 168  # 7 days
    client_timeout_ms: int = 30000
    connection_pool_size: int = 10


@dataclass
class Config:
    """Main configuration class."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    api: APIConfig = field(default_factory=APIConfig)
    providers: ProviderConfig = field(default_factory=ProviderConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        config = cls()
        
        # Database config
        config.database.type = os.getenv('DB_TYPE', config.database.type)
        config.database.host = os.getenv('DB_HOST', config.database.host)
        config.database.port = int(os.getenv('DB_PORT', str(config.database.port)))
        config.database.database = os.getenv('DB_NAME', config.database.database)
        config.database.username = os.getenv('DB_USER', config.database.username)
        config.database.password = os.getenv('DB_PASSWORD', config.database.password)
        config.database.sqlite_path = os.getenv('SQLITE_PATH', config.database.sqlite_path)
        
        # API config
        config.api.host = os.getenv('API_HOST', config.api.host)
        config.api.port = int(os.getenv('API_PORT', str(config.api.port)))
        config.api.debug = os.getenv('API_DEBUG', 'false').lower() == 'true'
        config.api.api_key = os.getenv('API_KEY')
        
        # Logging config
        config.logging.level = os.getenv('LOG_LEVEL', config.logging.level)
        config.logging.file_path = os.getenv('LOG_FILE_PATH')
        
        # Provider config
        config.providers.default_provider = os.getenv('DEFAULT_PROVIDER', config.providers.default_provider)
        
        return config


# Global configuration instance
config = Config.from_env()