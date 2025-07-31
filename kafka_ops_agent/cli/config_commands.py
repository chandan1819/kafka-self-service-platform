"""CLI commands for configuration management."""

import click
import json
import yaml
from typing import Dict, Any
from pathlib import Path
from tabulate import tabulate

from kafka_ops_agent.config.config_manager import get_config_manager, ConfigFormat
from kafka_ops_agent.config.templates import ConfigurationTemplates, DeploymentScenario
from kafka_ops_agent.config.runtime_config import get_runtime_config_manager
from kafka_ops_agent.exceptions import ConfigurationError


@click.group()
def config_mgmt():
    """Configuration management commands."""
    pass


@config_mgmt.command()
@click.option('--format', 'output_format', type=click.Choice(['table', 'json', 'yaml']), 
              default='table', help='Output format')
@click.option('--include-sensitive', is_flag=True, 
              help='Include sensitive values (passwords, keys)')
def show(output_format: str, include_sensitive: bool):
    """Show current configuration."""
    try:
        config_manager = get_config_manager()
        config = config_manager.get_config()
        
        if output_format == 'table':
            _show_config_table(config.dict(), include_sensitive)
        elif output_format == 'json':
            config_str = config_manager.export_configuration(
                ConfigFormat.JSON, include_sensitive
            )
            click.echo(config_str)
        elif output_format == 'yaml':
            config_str = config_manager.export_configuration(
                ConfigFormat.YAML, include_sensitive
            )
            click.echo(config_str)
            
    except Exception as e:
        click.echo(f"Error showing configuration: {e}", err=True)
        raise click.Abort()


@config_mgmt.command()
@click.argument('key')
def get(key: str):
    """Get a specific configuration value."""
    try:
        config_manager = get_config_manager()
        config = config_manager.get_config()
        config_dict = config.dict()
        
        value = _get_nested_value(config_dict, key)
        if value is None:
            click.echo(f"Configuration key '{key}' not found", err=True)
            raise click.Abort()
        
        # Format output based on value type
        if isinstance(value, (dict, list)):
            click.echo(json.dumps(value, indent=2))
        else:
            click.echo(str(value))
            
        # Show metadata if available
        metadata = config_manager.get_config_metadata(key)
        if metadata:
            click.echo(f"\nSource: {metadata.source.value}", err=True)
            if metadata.env_var:
                click.echo(f"Environment Variable: {metadata.env_var}", err=True)
            if metadata.file_path:
                click.echo(f"File: {metadata.file_path}", err=True)
                
    except Exception as e:
        click.echo(f"Error getting configuration: {e}", err=True)
        raise click.Abort()


@config_mgmt.command()
@click.argument('key')
@click.argument('value')
@click.option('--persist', is_flag=True, help='Persist the change to configuration file')
def set(key: str, value: str, persist: bool):
    """Set a configuration value at runtime."""
    try:
        # Convert value to appropriate type
        converted_value = _convert_value(value)
        
        runtime_manager = get_runtime_config_manager()
        updated_config = runtime_manager.update_configuration(
            {key: converted_value}, persist
        )
        
        click.echo(f"Configuration updated: {key} = {converted_value}")
        
        if persist:
            click.echo("Change persisted to configuration file")
        else:
            click.echo("Change is temporary (not persisted)")
            
    except Exception as e:
        click.echo(f"Error setting configuration: {e}", err=True)
        raise click.Abort()


@config_mgmt.command()
def validate():
    """Validate current configuration."""
    try:
        config_manager = get_config_manager()
        errors = config_manager.validate_configuration()
        
        if not errors:
            click.echo("✅ Configuration is valid")
        else:
            click.echo("❌ Configuration validation failed:")
            for error in errors:
                click.echo(f"  - {error}")
            raise click.Abort()
            
    except Exception as e:
        click.echo(f"Error validating configuration: {e}", err=True)
        raise click.Abort()


@config_mgmt.command()
def reload():
    """Reload configuration from all sources."""
    try:
        config_manager = get_config_manager()
        config = config_manager.reload_configuration()
        
        click.echo("✅ Configuration reloaded successfully")
        click.echo(f"Environment: {config.environment}")
        click.echo(f"Version: {config.version}")
        
        # Validate after reload
        errors = config_manager.validate_configuration()
        if errors:
            click.echo("\n⚠️  Configuration validation warnings:")
            for error in errors:
                click.echo(f"  - {error}")
                
    except Exception as e:
        click.echo(f"Error reloading configuration: {e}", err=True)
        raise click.Abort()


@config_mgmt.command()
def status():
    """Show configuration status and metadata."""
    try:
        runtime_manager = get_runtime_config_manager()
        status_info = runtime_manager.get_configuration_status()
        
        click.echo("Configuration Status:")
        click.echo(f"  Environment: {status_info['environment']}")
        click.echo(f"  Version: {status_info['version']}")
        click.echo(f"  File Watcher: {'Running' if status_info['watcher_running'] else 'Stopped'}")
        
        if status_info['validation_errors']:
            click.echo(f"  Validation Errors: {len(status_info['validation_errors'])}")
        else:
            click.echo("  Validation: ✅ Valid")
        
        # Show configuration sources
        click.echo("\nConfiguration Sources:")
        sources = {}
        for key, source in status_info['config_sources'].items():
            if source not in sources:
                sources[source] = []
            sources[source].append(key)
        
        for source, keys in sources.items():
            click.echo(f"  {source}: {len(keys)} keys")
            
    except Exception as e:
        click.echo(f"Error getting configuration status: {e}", err=True)
        raise click.Abort()


@config_mgmt.group()
def template():
    """Configuration template commands."""
    pass


@template.command('list')
def list_templates():
    """List available configuration templates."""
    templates = ConfigurationTemplates.list_available_templates()
    
    click.echo("Available Configuration Templates:")
    click.echo()
    
    for template_name in templates:
        scenario = DeploymentScenario(template_name)
        description = ConfigurationTemplates.get_template_description(scenario)
        click.echo(f"  {template_name:<15} - {description}")


@template.command()
@click.argument('scenario', type=click.Choice([s.value for s in DeploymentScenario]))
@click.option('--format', 'output_format', type=click.Choice(['json', 'yaml']), 
              default='yaml', help='Output format')
def show(scenario: str, output_format: str):
    """Show configuration template for deployment scenario."""
    try:
        deployment_scenario = DeploymentScenario(scenario)
        template_data = ConfigurationTemplates.get_template(deployment_scenario)
        
        if output_format == 'json':
            click.echo(json.dumps(template_data, indent=2))
        else:
            click.echo(yaml.dump(template_data, default_flow_style=False, indent=2))
            
    except Exception as e:
        click.echo(f"Error showing template: {e}", err=True)
        raise click.Abort()


@template.command()
@click.argument('scenario', type=click.Choice([s.value for s in DeploymentScenario]))
@click.argument('output_path', type=click.Path())
@click.option('--format', 'output_format', type=click.Choice(['json', 'yaml']), 
              default='yaml', help='Output format')
@click.option('--force', is_flag=True, help='Overwrite existing file')
def generate(scenario: str, output_path: str, output_format: str, force: bool):
    """Generate configuration file from template."""
    try:
        output_file = Path(output_path)
        
        # Check if file exists
        if output_file.exists() and not force:
            click.echo(f"File already exists: {output_path}")
            click.echo("Use --force to overwrite")
            raise click.Abort()
        
        # Generate configuration
        deployment_scenario = DeploymentScenario(scenario)
        template_data = ConfigurationTemplates.get_template(deployment_scenario)
        
        # Create output directory
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Write configuration file
        with open(output_file, 'w') as f:
            if output_format == 'json':
                json.dump(template_data, f, indent=2)
            else:
                yaml.dump(template_data, f, default_flow_style=False, indent=2)
        
        click.echo(f"✅ Generated {scenario} configuration: {output_path}")
        
    except Exception as e:
        click.echo(f"Error generating configuration: {e}", err=True)
        raise click.Abort()


@config_mgmt.group()
def watch():
    """Configuration file watching commands."""
    pass


@watch.command()
@click.option('--paths', multiple=True, help='Additional paths to watch')
def start(paths):
    """Start configuration file watcher."""
    try:
        from kafka_ops_agent.config.runtime_config import start_config_watcher
        
        watch_paths = list(paths) if paths else None
        start_config_watcher(watch_paths)
        
        click.echo("✅ Configuration file watcher started")
        if watch_paths:
            click.echo(f"Watching paths: {', '.join(watch_paths)}")
        
    except Exception as e:
        click.echo(f"Error starting configuration watcher: {e}", err=True)
        raise click.Abort()


@watch.command()
def stop():
    """Stop configuration file watcher."""
    try:
        from kafka_ops_agent.config.runtime_config import stop_config_watcher
        
        stop_config_watcher()
        click.echo("✅ Configuration file watcher stopped")
        
    except Exception as e:
        click.echo(f"Error stopping configuration watcher: {e}", err=True)
        raise click.Abort()


def _show_config_table(config_dict: Dict[str, Any], include_sensitive: bool, prefix: str = ""):
    """Show configuration as a table.
    
    Args:
        config_dict: Configuration dictionary
        include_sensitive: Whether to include sensitive values
        prefix: Key prefix for nested values
    """
    rows = []
    sensitive_keys = ['password', 'secret', 'key', 'token', 'credential']
    
    def add_rows(obj: Dict[str, Any], current_prefix: str = ""):
        for key, value in obj.items():
            full_key = f"{current_prefix}.{key}" if current_prefix else key
            
            if isinstance(value, dict):
                add_rows(value, full_key)
            else:
                # Check if value is sensitive
                is_sensitive = any(sensitive in key.lower() for sensitive in sensitive_keys)
                
                if is_sensitive and not include_sensitive:
                    display_value = "***MASKED***"
                else:
                    display_value = str(value)
                
                rows.append([full_key, display_value, "🔒" if is_sensitive else ""])
    
    add_rows(config_dict, prefix)
    
    headers = ["Configuration Key", "Value", "Sensitive"]
    click.echo(tabulate(rows, headers=headers, tablefmt="grid"))


def _get_nested_value(data: Dict[str, Any], path: str) -> Any:
    """Get nested dictionary value using dot notation.
    
    Args:
        data: Dictionary to read from
        path: Dot-separated path
        
    Returns:
        Value at path or None if not found
    """
    keys = path.split('.')
    current = data
    
    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError):
        return None


def _convert_value(value: str) -> Any:
    """Convert string value to appropriate type.
    
    Args:
        value: String value to convert
        
    Returns:
        Converted value
    """
    # Boolean values
    if value.lower() in ['true', 'yes', '1', 'on']:
        return True
    elif value.lower() in ['false', 'no', '0', 'off']:
        return False
    
    # Try JSON parsing for complex values
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        pass
    
    # Try integer conversion
    try:
        return int(value)
    except ValueError:
        pass
    
    # Try float conversion
    try:
        return float(value)
    except ValueError:
        pass
    
    # Return as string
    return value


if __name__ == "__main__":
    config_mgmt()