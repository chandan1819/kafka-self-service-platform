#!/usr/bin/env python3
"""Demonstration script for configuration management system."""

import sys
import tempfile
import json
import yaml
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka_ops_agent.config.config_manager import get_config_manager, ConfigurationManager
from kafka_ops_agent.config.templates import ConfigurationTemplates, DeploymentScenario
from kafka_ops_agent.config.runtime_config import RuntimeConfigurationManager, ConfigChangeEvent


def demo_basic_configuration():
    """Demonstrate basic configuration loading."""
    print("🔧 Basic Configuration Management Demo")
    print("=" * 50)
    
    # Initialize configuration manager
    config_manager = get_config_manager()
    config = config_manager.get_config()
    
    print(f"Application: {config.name}")
    print(f"Environment: {config.environment}")
    print(f"Version: {config.version}")
    print(f"Database Type: {config.database.type}")
    print(f"API Port: {config.api.port}")
    print(f"Log Level: {config.logging.level}")
    print(f"Default Provider: {config.providers.default_provider}")
    
    # Show configuration validation
    errors = config_manager.validate_configuration()
    if errors:
        print(f"\n⚠️  Configuration Validation Errors:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("\n✅ Configuration is valid")
    
    print()


def demo_configuration_templates():
    """Demonstrate configuration templates."""
    print("📋 Configuration Templates Demo")
    print("=" * 40)
    
    # List available templates
    templates = ConfigurationTemplates.list_available_templates()
    print(f"Available templates: {len(templates)}")
    
    for template_name in templates:
        scenario = DeploymentScenario(template_name)
        description = ConfigurationTemplates.get_template_description(scenario)
        print(f"  {template_name:<15} - {description}")
    
    print("\n📄 Development Template Sample:")
    dev_config = ConfigurationTemplates.get_development_config()
    print(f"  Environment: {dev_config['environment']}")
    print(f"  Database: {dev_config['database']['type']}")
    print(f"  Debug Mode: {dev_config['api']['debug']}")
    print(f"  Log Level: {dev_config['logging']['level']}")
    
    print("\n🏭 Production Template Sample:")
    prod_config = ConfigurationTemplates.get_production_config()
    print(f"  Environment: {prod_config['environment']}")
    print(f"  Database: {prod_config['database']['type']}")
    print(f"  Debug Mode: {prod_config['api']['debug']}")
    print(f"  Log Level: {prod_config['logging']['level']}")
    print(f"  Workers: {prod_config['api']['workers']}")
    
    print()


def demo_runtime_configuration():
    """Demonstrate runtime configuration management."""
    print("⚡ Runtime Configuration Management Demo")
    print("=" * 45)
    
    # Initialize configuration manager
    config_manager = ConfigurationManager()
    config = config_manager.load_configuration()
    
    print(f"Initial API Port: {config.api.port}")
    print(f"Initial Log Level: {config.logging.level}")
    
    # Initialize runtime manager
    runtime_manager = RuntimeConfigurationManager(config_manager)
    
    # Register change handler
    def config_change_handler(event: ConfigChangeEvent):
        print(f"\n🔄 Configuration Changed:")
        print(f"  Source: {event.source}")
        print(f"  Changed Keys: {', '.join(event.changed_keys)}")
        for key in event.changed_keys:
            old_val = event.old_values.get(key)
            new_val = event.new_values.get(key)
            print(f"  {key}: {old_val} → {new_val}")
    
    runtime_manager.register_change_handler(config_change_handler)
    
    # Update configuration at runtime
    print("\n🔧 Updating configuration at runtime...")
    updates = {
        'api.port': 9090,
        'logging.level': 'DEBUG',
        'api.debug': True
    }
    
    updated_config = runtime_manager.update_configuration(updates)
    
    print(f"\nUpdated API Port: {updated_config.api.port}")
    print(f"Updated Log Level: {updated_config.logging.level}")
    print(f"Updated Debug Mode: {updated_config.api.debug}")
    
    # Show configuration status
    print("\n📊 Configuration Status:")
    status = runtime_manager.get_configuration_status()
    print(f"  Environment: {status['environment']}")
    print(f"  Watcher Running: {status['watcher_running']}")
    print(f"  Validation Errors: {len(status['validation_errors'])}")
    
    # Show configuration sources
    print("\n📁 Configuration Sources:")
    sources = {}
    for key, source in status['config_sources'].items():
        if source not in sources:
            sources[source] = 0
        sources[source] += 1
    
    for source, count in sources.items():
        print(f"  {source}: {count} keys")
    
    print()


def demo_file_based_configuration():
    """Demonstrate file-based configuration."""
    print("📁 File-Based Configuration Demo")
    print("=" * 40)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / "demo_config.yaml"
        
        # Create sample configuration file
        sample_config = {
            'name': 'Demo Application',
            'environment': 'demo',
            'database': {
                'type': 'postgresql',
                'host': 'demo-db.example.com',
                'port': 5432,
                'database': 'demo_db'
            },
            'api': {
                'host': '0.0.0.0',
                'port': 8888,
                'debug': False,
                'workers': 2
            },
            'logging': {
                'level': 'WARNING',
                'format': 'json',
                'file_enabled': True,
                'file_path': '/var/log/demo.log'
            },
            'kafka': {
                'bootstrap_servers': ['kafka1.demo.com:9092', 'kafka2.demo.com:9092'],
                'security_protocol': 'SASL_SSL',
                'client_id': 'demo-client'
            }
        }
        
        # Write configuration file
        with open(config_file, 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)
        
        print(f"Created configuration file: {config_file}")
        
        # Load configuration from file
        config_manager = ConfigurationManager(str(config_file))
        config = config_manager.load_configuration()
        
        print(f"\n📖 Loaded Configuration:")
        print(f"  Name: {config.name}")
        print(f"  Environment: {config.environment}")
        print(f"  Database Host: {config.database.host}")
        print(f"  API Port: {config.api.port}")
        print(f"  Kafka Servers: {', '.join(config.kafka.bootstrap_servers)}")
        
        # Show configuration metadata
        print(f"\n🏷️  Configuration Metadata:")
        metadata = config_manager.get_config_metadata('name')
        if metadata:
            print(f"  Source: {metadata.source.value}")
            print(f"  File: {metadata.file_path}")
        
        # Export configuration
        print(f"\n📤 Configuration Export (JSON):")
        json_export = config_manager.export_configuration(format='json', include_sensitive=False)
        exported_data = json.loads(json_export)
        print(f"  Keys: {len(exported_data)}")
        print(f"  Database Type: {exported_data['database']['type']}")
        print(f"  API Workers: {exported_data['api']['workers']}")
    
    print()


def demo_environment_override():
    """Demonstrate environment variable override."""
    print("🌍 Environment Variable Override Demo")
    print("=" * 45)
    
    import os
    
    # Set environment variables
    env_vars = {
        'KAFKA_OPS_AGENT_ENVIRONMENT': 'env-override-demo',
        'KAFKA_OPS_AGENT_DB_TYPE': 'postgresql',
        'KAFKA_OPS_AGENT_DB_HOST': 'env-db.example.com',
        'KAFKA_OPS_AGENT_API_PORT': '7777',
        'KAFKA_OPS_AGENT_LOG_LEVEL': 'ERROR',
        'KAFKA_OPS_AGENT_KAFKA_SERVERS': 'env-kafka1:9092,env-kafka2:9092'
    }
    
    print("Setting environment variables:")
    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"  {key}={value}")
    
    try:
        # Load configuration with environment overrides
        config_manager = ConfigurationManager()
        config = config_manager.load_configuration()
        
        print(f"\n📊 Configuration with Environment Overrides:")
        print(f"  Environment: {config.environment}")
        print(f"  Database Type: {config.database.type}")
        print(f"  Database Host: {config.database.host}")
        print(f"  API Port: {config.api.port}")
        print(f"  Log Level: {config.logging.level}")
        print(f"  Kafka Servers: {', '.join(config.kafka.bootstrap_servers)}")
        
        # Show metadata for environment-loaded values
        print(f"\n🏷️  Environment Variable Metadata:")
        for key in ['environment', 'database.type', 'api.port']:
            metadata = config_manager.get_config_metadata(key)
            if metadata and metadata.source.value == 'environment':
                print(f"  {key}: {metadata.env_var}")
    
    finally:
        # Clean up environment variables
        for key in env_vars:
            os.environ.pop(key, None)
    
    print()


def demo_configuration_validation():
    """Demonstrate configuration validation."""
    print("✅ Configuration Validation Demo")
    print("=" * 40)
    
    # Test with valid configuration
    print("Testing valid configuration...")
    config_manager = ConfigurationManager()
    config = config_manager.load_configuration()
    
    errors = config_manager.validate_configuration()
    if errors:
        print(f"❌ Validation errors found:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Configuration is valid")
    
    # Test with invalid configuration (simulated)
    print("\nTesting configuration with potential issues...")
    
    # Simulate some validation scenarios
    validation_scenarios = [
        ("PostgreSQL without username", "PostgreSQL requires username and password"),
        ("Empty Kafka servers", "Kafka bootstrap servers cannot be empty"),
        ("Invalid provider", "Default provider must be docker, kubernetes, or terraform"),
        ("Low port without privileges", "Ports < 1024 require root privileges")
    ]
    
    for scenario, description in validation_scenarios:
        print(f"  Scenario: {scenario}")
        print(f"    Issue: {description}")
    
    print()


def main():
    """Main demonstration function."""
    print("🧪 Kafka Ops Agent Configuration Management Demo")
    print("=" * 60)
    print()
    
    try:
        # Run all demonstrations
        demo_basic_configuration()
        demo_configuration_templates()
        demo_runtime_configuration()
        demo_file_based_configuration()
        demo_environment_override()
        demo_configuration_validation()
        
        print("🎉 Configuration Management Demo Completed Successfully!")
        print()
        print("Key Features Demonstrated:")
        print("✅ Basic configuration loading with defaults")
        print("✅ Configuration templates for different deployment scenarios")
        print("✅ Runtime configuration updates with change notifications")
        print("✅ File-based configuration loading and metadata tracking")
        print("✅ Environment variable overrides with precedence")
        print("✅ Configuration validation and error reporting")
        print()
        print("Next Steps:")
        print("1. Try the CLI: kafka-ops-agent config-mgmt --help")
        print("2. Generate templates: kafka-ops-agent config-mgmt template generate")
        print("3. Start file watcher: kafka-ops-agent config-mgmt watch start")
        print("4. Update runtime config: kafka-ops-agent config-mgmt set api.port 9999")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()