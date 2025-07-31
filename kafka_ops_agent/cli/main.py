"""Main CLI entry point for Kafka Ops Agent."""

import click
import asyncio
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.cli.topic_commands import topic_cli
from kafka_ops_agent.cli.cluster_commands import cluster_cli
from kafka_ops_agent.cli.cleanup_commands import cleanup_cli
from kafka_ops_agent.cli.config_commands import config_mgmt
from kafka_ops_agent.cli.config import load_cli_config, save_cli_config
from kafka_ops_agent.logging_config import setup_logging


@click.group()
@click.option('--config-file', '-c', default='~/.kafka-ops-agent/config.json', 
              help='Configuration file path')
@click.option('--cluster-id', '-k', help='Kafka cluster ID')
@click.option('--bootstrap-servers', '-b', help='Kafka bootstrap servers (comma-separated)')
@click.option('--user-id', '-u', help='User ID for audit logging')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config_file, cluster_id, bootstrap_servers, user_id, verbose):
    """Kafka Ops Agent CLI - Manage Kafka clusters and topics."""
    
    # Setup logging
    if verbose:
        setup_logging()
    
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    # Load configuration
    config_path = Path(config_file).expanduser()
    cli_config = load_cli_config(config_path)
    
    # Override config with command line options
    if cluster_id:
        cli_config['cluster_id'] = cluster_id
    if bootstrap_servers:
        cli_config['bootstrap_servers'] = bootstrap_servers.split(',')
    if user_id:
        cli_config['user_id'] = user_id
    
    # Store config in context
    ctx.obj['config'] = cli_config
    ctx.obj['config_path'] = config_path


@cli.command()
@click.option('--cluster-id', '-k', required=True, help='Kafka cluster ID')
@click.option('--bootstrap-servers', '-b', required=True, 
              help='Kafka bootstrap servers (comma-separated)')
@click.option('--zookeeper-connect', '-z', help='Zookeeper connection string')
@click.option('--user-id', '-u', help='Default user ID')
@click.pass_context
def configure(ctx, cluster_id, bootstrap_servers, zookeeper_connect, user_id):
    """Configure CLI with cluster connection details."""
    
    config = {
        'cluster_id': cluster_id,
        'bootstrap_servers': bootstrap_servers.split(','),
        'user_id': user_id or 'cli-user'
    }
    
    if zookeeper_connect:
        config['zookeeper_connect'] = zookeeper_connect
    
    # Save configuration
    config_path = ctx.obj['config_path']
    save_cli_config(config_path, config)
    
    click.echo(f"✅ Configuration saved to {config_path}")
    click.echo(f"   Cluster ID: {cluster_id}")
    click.echo(f"   Bootstrap Servers: {', '.join(bootstrap_servers.split(','))}")
    if zookeeper_connect:
        click.echo(f"   Zookeeper: {zookeeper_connect}")


@cli.command()
@click.pass_context
def config(ctx):
    """Show current configuration."""
    
    config = ctx.obj['config']
    config_path = ctx.obj['config_path']
    
    click.echo(f"Configuration file: {config_path}")
    click.echo("Current configuration:")
    click.echo(json.dumps(config, indent=2))


@cli.command()
@click.pass_context
def version(ctx):
    """Show version information."""
    
    click.echo("Kafka Ops Agent CLI")
    click.echo("Version: 0.1.0")
    click.echo("Author: KafkaOpsAgent")


# Add command groups
cli.add_command(topic_cli, name='topic')
cli.add_command(cluster_cli, name='cluster')
cli.add_command(cleanup_cli, name='cleanup')
cli.add_command(config_mgmt, name='config-mgmt')


def main():
    """Main entry point."""
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\n👋 Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()