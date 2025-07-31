"""Tests for configuration management system."""

import pytest
import tempfile
import json
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from kafka_ops_agent.config.config_manager import (
    ConfigurationManager, ApplicationConfig, DatabaseConfig, 
    KafkaConfig, APIConfig, LoggingConfig, ProviderConfig, CleanupConfig
)
from kafka_ops_agent.config.templates import ConfigurationTemplates, DeploymentScenario
from kafka_ops_agent.config.runtime_config import RuntimeConfigurationManager, ConfigChangeEvent
from kafka_ops_agent.exceptions import ConfigurationError


class TestConfigurationManager:
    """Test cases for ConfigurationManager."""
    
    @pytest.fixture
    def temp_config_file(self):
        """Create temporary configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'name': 'Test App',
                'environment': 'testing',
                'database': {
                    'type': 'sqlite',
                    'sqlite_file': 'test.db'
                },
                'api': {
                    'host': '127.0.0.1',
                    'port': 8081
                },
                'logging': {
                    'level': 'DEBUG'
                }
            }
            yaml.dump(config_data, f)
            yield f.name
        
        # Cleanup
        Path(f.name).unlink(missing_ok=True)
    
    def test_load_configuration_defaults(self):
        """Test loading configuration with defaults."""
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration()
        
        assert isinstance(config, ApplicationConfig)
        assert config.name == "Kafka Ops Agent"
        assert config.environment == "development"
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.kafka, KafkaConfig)
        assert isinstance(config.api, APIConfig)
        assert isinstance(config.logging, LoggingConfig)
        assert isinstance(config.providers, ProviderConfig)
        assert isinstance(config.cleanup, CleanupConfig)
    
    def test_load_configuration_from_file(self, temp_config_file):
        """Test loading configuration from file."""
        config_manager = ConfigurationManager(temp_config_file)
        config = config_manager.load_configuration()
        
        assert config.name == "Test App"
        assert config.environment == "testing"
        assert config.database.type == "sqlite"
        assert config.database.sqlite_file == "test.db"
        assert config.api.host == "127.0.0.1"
        assert config.api.port == 8081
        assert config.logging.level == "DEBUG"
    
    def test_load_configuration_from_environment(self):
        """Test loading configuration from environment variables."""
        with patch.dict('os.environ', {
            'KAFKA_OPS_AGENT_ENVIRONMENT': 'production',
            'KAFKA_OPS_AGENT_DB_TYPE': 'postgresql',
            'KAFKA_OPS_AGENT_DB_HOST': 'db.example.com',
            'KAFKA_OPS_AGENT_API_PORT': '9090',
            'KAFKA_OPS_AGENT_LOG_LEVEL': 'ERROR'
        }):
            config_manager = ConfigurationManager()
            config = config_manager.load_configuration()
            
            assert config.environment == "production"
            assert config.database.type == "postgresql"
            assert config.database.host == "db.example.com"
            assert config.api.port == 9090
            assert config.logging.level == "ERROR"
    
    def test_environment_override_file(self, temp_config_file):
        """Test that environment variables override file configuration."""
        with patch.dict('os.environ', {
            'KAFKA_OPS_AGENT_ENVIRONMENT': 'production',
            'KAFKA_OPS_AGENT_API_PORT': '9090'
        }):
            config_manager = ConfigurationManager(temp_config_file)
            config = config_manager.load_configuration()
            
            # Environment should override file
            assert config.environment == "production"  # From env
            assert config.api.port == 9090  # From env
            
            # File values should still be present
            assert config.name == "Test App"  # From file
            assert config.database.sqlite_file == "test.db"  # From file
    
    def test_configuration_validation(self):
        """Test configuration validation."""
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration()
        
        errors = config_manager.validate_configuration()
        assert isinstance(errors, list)
        # Should be valid by default
        assert len(errors) == 0
    
    def test_export_configuration(self):
        """Test configuration export."""
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration()
        
        # Test JSON export
        json_export = config_manager.export_configuration(format='json')
        assert isinstance(json_export, str)
        json_data = json.loads(json_export)
        assert json_data['name'] == config.name
        
        # Test YAML export
        yaml_export = config_manager.export_configuration(format='yaml')
        assert isinstance(yaml_export, str)
        yaml_data = yaml.safe_load(yaml_export)
        assert yaml_data['name'] == config.name
    
    def test_export_configuration_mask_sensitive(self):
        """Test configuration export with sensitive values masked."""
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration()
        
        # Set a sensitive value
        config.database.password = "secret123"
        config_manager._config = config
        
        # Export without sensitive values
        export_data = config_manager.export_configuration(include_sensitive=False)
        assert "***MASKED***" in export_data
        assert "secret123" not in export_data
        
        # Export with sensitive values
        export_data = config_manager.export_configuration(include_sensitive=True)
        assert "secret123" in export_data
    
    def test_get_config_metadata(self, temp_config_file):
        """Test getting configuration metadata."""
        config_manager = ConfigurationManager(temp_config_file)
        config = config_manager.load_configuration()
        
        # Check metadata for file-loaded value
        metadata = config_manager.get_config_metadata('name')
        assert metadata is not None
        assert metadata.source.value == 'config_file'
        assert metadata.file_path == temp_config_file
    
    def test_reload_configuration(self, temp_config_file):
        """Test configuration reloading."""
        config_manager = ConfigurationManager(temp_config_file)
        config1 = config_manager.load_configuration()
        
        # Modify the file
        with open(temp_config_file, 'w') as f:
            modified_data = {
                'name': 'Modified App',
                'environment': 'staging'
            }
            yaml.dump(modified_data, f)
        
        # Reload configuration
        config2 = config_manager.reload_configuration()
        
        assert config2.name == "Modified App"
        assert config2.environment == "staging"
        assert config1.name != config2.name


class TestConfigurationTemplates:
    """Test cases for ConfigurationTemplates."""
    
    def test_get_development_config(self):
        """Test development configuration template."""
        config = ConfigurationTemplates.get_development_config()
        
        assert config['environment'] == 'development'
        assert config['database']['type'] == 'sqlite'
        assert config['api']['debug'] is True
        assert config['logging']['level'] == 'DEBUG'
        assert config['providers']['default_provider'] == 'docker'
    
    def test_get_production_config(self):
        """Test production configuration template."""
        config = ConfigurationTemplates.get_production_config()
        
        assert config['environment'] == 'production'
        assert config['database']['type'] == 'postgresql'
        assert config['api']['debug'] is False
        assert config['logging']['level'] == 'INFO'
        assert config['providers']['default_provider'] == 'kubernetes'
    
    def test_get_testing_config(self):
        """Test testing configuration template."""
        config = ConfigurationTemplates.get_testing_config()
        
        assert config['environment'] == 'testing'
        assert config['database']['sqlite_file'] == ':memory:'
        assert config['logging']['file_enabled'] is False
        assert config['cleanup']['enabled'] is False
    
    def test_get_kubernetes_config(self):
        """Test Kubernetes configuration template."""
        config = ConfigurationTemplates.get_kubernetes_config()
        
        assert config['environment'] == 'production'
        assert config['providers']['kubernetes_enabled'] is True
        assert config['logging']['console_enabled'] is True
        assert 'svc.cluster.local' in config['database']['host']
    
    def test_get_cloud_aws_config(self):
        """Test AWS cloud configuration template."""
        config = ConfigurationTemplates.get_cloud_aws_config()
        
        assert config['providers']['terraform_enabled'] is True
        assert config['providers']['terraform_state_backend'] == 's3'
        assert '${RDS_ENDPOINT}' in config['database']['host']
        assert '${MSK_BOOTSTRAP_SERVERS}' in str(config['kafka']['bootstrap_servers'])
    
    def test_get_template_by_scenario(self):
        """Test getting template by deployment scenario."""
        for scenario in DeploymentScenario:
            config = ConfigurationTemplates.get_template(scenario)
            assert isinstance(config, dict)
            assert 'environment' in config
            assert 'database' in config
            assert 'api' in config
    
    def test_list_available_templates(self):
        """Test listing available templates."""
        templates = ConfigurationTemplates.list_available_templates()
        
        assert isinstance(templates, list)
        assert len(templates) > 0
        assert 'development' in templates
        assert 'production' in templates
        assert 'kubernetes' in templates
    
    def test_get_template_description(self):
        """Test getting template descriptions."""
        for scenario in DeploymentScenario:
            description = ConfigurationTemplates.get_template_description(scenario)
            assert isinstance(description, str)
            assert len(description) > 0


class TestRuntimeConfigurationManager:
    """Test cases for RuntimeConfigurationManager."""
    
    @pytest.fixture
    def config_manager(self):
        """Create configuration manager for testing."""
        return ConfigurationManager()
    
    @pytest.fixture
    def runtime_manager(self, config_manager):
        """Create runtime configuration manager for testing."""
        return RuntimeConfigurationManager(config_manager)
    
    def test_initialization(self, runtime_manager):
        """Test runtime manager initialization."""
        assert runtime_manager.config_manager is not None
        assert runtime_manager.change_handler is not None
        assert runtime_manager.file_watcher is not None
        assert runtime_manager._running is False
    
    def test_update_configuration(self, runtime_manager):
        """Test runtime configuration updates."""
        # Get initial configuration
        initial_config = runtime_manager.config_manager.get_config()
        initial_level = initial_config.logging.level
        
        # Update configuration
        updates = {'logging.level': 'ERROR'}
        updated_config = runtime_manager.update_configuration(updates)
        
        assert updated_config.logging.level == 'ERROR'
        assert updated_config.logging.level != initial_level
    
    def test_configuration_change_handler(self, runtime_manager):
        """Test configuration change handler."""
        callback_called = False
        change_event = None
        
        def test_callback(event: ConfigChangeEvent):
            nonlocal callback_called, change_event
            callback_called = True
            change_event = event
        
        # Register callback
        runtime_manager.register_change_handler(test_callback)
        
        # Update configuration
        updates = {'api.port': 9999}
        runtime_manager.update_configuration(updates)
        
        # Check callback was called
        assert callback_called
        assert change_event is not None
        assert 'api.port' in change_event.changed_keys
        assert change_event.new_values['api.port'] == 9999
    
    def test_get_configuration_status(self, runtime_manager):
        """Test getting configuration status."""
        status = runtime_manager.get_configuration_status()
        
        assert isinstance(status, dict)
        assert 'environment' in status
        assert 'version' in status
        assert 'watcher_running' in status
        assert 'validation_errors' in status
        assert 'config_sources' in status
    
    def test_start_stop_watching(self, runtime_manager):
        """Test starting and stopping file watching."""
        # Initially not running
        assert not runtime_manager._running
        
        # Start watching (will fail without valid paths, but that's OK for test)
        try:
            runtime_manager.start_watching([])
        except:
            pass  # Expected to fail in test environment
        
        # Stop watching
        runtime_manager.stop_watching()
        assert not runtime_manager._running
    
    def test_nested_value_operations(self, runtime_manager):
        """Test nested value get/set operations."""
        test_dict = {
            'level1': {
                'level2': {
                    'value': 'test'
                }
            }
        }
        
        # Test getting nested value
        value = runtime_manager._get_nested_value(test_dict, 'level1.level2.value')
        assert value == 'test'
        
        # Test setting nested value
        runtime_manager._set_nested_value(test_dict, 'level1.level2.new_value', 'new')
        assert test_dict['level1']['level2']['new_value'] == 'new'
        
        # Test getting non-existent value
        value = runtime_manager._get_nested_value(test_dict, 'non.existent.path')
        assert value is None


class TestConfigChangeEvent:
    """Test cases for ConfigChangeEvent."""
    
    def test_config_change_event_creation(self):
        """Test creating configuration change event."""
        from datetime import datetime
        
        event = ConfigChangeEvent(
            timestamp=datetime.utcnow(),
            changed_keys=['api.port', 'logging.level'],
            old_values={'api.port': 8080, 'logging.level': 'INFO'},
            new_values={'api.port': 9090, 'logging.level': 'DEBUG'},
            source='test'
        )
        
        assert len(event.changed_keys) == 2
        assert 'api.port' in event.changed_keys
        assert event.old_values['api.port'] == 8080
        assert event.new_values['api.port'] == 9090
        assert event.source == 'test'


class TestConfigurationIntegration:
    """Integration tests for configuration management."""
    
    def test_full_configuration_workflow(self):
        """Test complete configuration management workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_file = Path(temp_dir) / 'config.yaml'
            
            # Create initial configuration file
            initial_config = {
                'name': 'Integration Test',
                'environment': 'testing',
                'api': {'port': 8080},
                'logging': {'level': 'INFO'}
            }
            
            with open(config_file, 'w') as f:
                yaml.dump(initial_config, f)
            
            # Initialize configuration manager
            config_manager = ConfigurationManager(str(config_file))
            config = config_manager.load_configuration()
            
            # Verify initial configuration
            assert config.name == 'Integration Test'
            assert config.api.port == 8080
            
            # Initialize runtime manager
            runtime_manager = RuntimeConfigurationManager(config_manager)
            
            # Update configuration at runtime
            updates = {
                'api.port': 9090,
                'logging.level': 'DEBUG'
            }
            updated_config = runtime_manager.update_configuration(updates)
            
            # Verify updates
            assert updated_config.api.port == 9090
            assert updated_config.logging.level == 'DEBUG'
            
            # Verify configuration status
            status = runtime_manager.get_configuration_status()
            assert status['environment'] == 'testing'
            assert len(status['validation_errors']) == 0
    
    def test_template_to_config_conversion(self):
        """Test converting template to actual configuration."""
        # Get development template
        template_data = ConfigurationTemplates.get_development_config()
        
        # Create configuration from template
        config = ApplicationConfig(**template_data)
        
        # Verify configuration is valid
        assert config.environment == 'development'
        assert config.database.type == 'sqlite'
        assert config.api.debug is True
        
        # Verify all required fields are present
        assert hasattr(config, 'database')
        assert hasattr(config, 'kafka')
        assert hasattr(config, 'api')
        assert hasattr(config, 'logging')
        assert hasattr(config, 'providers')
        assert hasattr(config, 'cleanup')


if __name__ == "__main__":
    pytest.main([__file__])