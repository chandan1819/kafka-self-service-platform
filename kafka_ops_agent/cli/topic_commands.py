"""CLI commands for topic management."""

import click
import asyncio
import json
import sys
from typing import Dict, Any, List
from tabulate import tabulate

from kafka_ops_agent.cli.config import get_cluster_config
from kafka_ops_agent.services.topic_management import get_topic_service
from kafka_ops_agent.clients.kafka_client import get_client_manager
from kafka_ops_agent.models.cluster import ConnectionInfo
from kafka_ops_agent.models.topic import TopicConfig


@click.group()
def topic_cli():
    """Topic management commands."""
    pass


@topic_cli.command('list')
@click.option('--include-internal', is_flag=True, help='Include internal topics')
@click.option('--format', '-f', type=click.Choice(['table', 'json']), default='table',
              help='Output format')
@click.pass_context
def list_topics(ctx, include_internal, format):
    """List topics in the cluster."""
    
    async def _list_topics():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Get topic service and list topics
            topic_service = await get_topic_service()
            topics = await topic_service.list_topics(
                cluster_config['cluster_id'], 
                include_internal, 
                cluster_config['user_id']
            )
            
            if format == 'json':
                topic_data = [
                    {
                        'name': topic.name,
                        'partitions': topic.partitions,
                        'replication_factor': topic.replication_factor,
                        'configs': topic.configs
                    }
                    for topic in topics
                ]
                click.echo(json.dumps(topic_data, indent=2))
            else:
                if not topics:
                    click.echo("No topics found.")
                    return
                
                # Format as table
                headers = ['Topic Name', 'Partitions', 'Replication Factor']
                rows = [
                    [topic.name, topic.partitions, topic.replication_factor]
                    for topic in topics
                ]
                
                click.echo(f"Topics in cluster '{cluster_config['cluster_id']}':")
                click.echo(tabulate(rows, headers=headers, tablefmt='grid'))
                click.echo(f"\nTotal: {len(topics)} topics")
            
        except Exception as e:
            click.echo(f"❌ Failed to list topics: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_list_topics())


@topic_cli.command('create')
@click.argument('topic_name')
@click.option('--partitions', '-p', type=int, default=3, help='Number of partitions')
@click.option('--replication-factor', '-r', type=int, default=1, help='Replication factor')
@click.option('--retention-hours', type=int, default=168, help='Retention time in hours')
@click.option('--cleanup-policy', type=click.Choice(['delete', 'compact', 'compact,delete']), 
              default='delete', help='Cleanup policy')
@click.option('--compression-type', type=click.Choice(['none', 'gzip', 'snappy', 'lz4', 'zstd']), 
              default='none', help='Compression type')
@click.option('--config', '-c', multiple=True, help='Custom config in key=value format')
@click.pass_context
def create_topic(ctx, topic_name, partitions, replication_factor, retention_hours, 
                cleanup_policy, compression_type, config):
    """Create a new topic."""
    
    async def _create_topic():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Parse custom configs
            custom_configs = {}
            for cfg in config:
                if '=' not in cfg:
                    click.echo(f"❌ Invalid config format: {cfg}. Use key=value format.", err=True)
                    raise click.Abort()
                key, value = cfg.split('=', 1)
                custom_configs[key] = value
            
            # Create topic config
            topic_config = TopicConfig(
                name=topic_name,
                partitions=partitions,
                replication_factor=replication_factor,
                retention_ms=retention_hours * 3600 * 1000,  # Convert hours to milliseconds
                cleanup_policy=cleanup_policy,
                compression_type=compression_type,
                custom_configs=custom_configs
            )
            
            # Create topic
            topic_service = await get_topic_service()
            result = await topic_service.create_topic(
                cluster_config['cluster_id'], 
                topic_config, 
                cluster_config['user_id']
            )
            
            if result.success:
                click.echo(f"✅ Topic '{topic_name}' created successfully")
                if result.details:
                    click.echo(f"   Partitions: {result.details.get('partitions', partitions)}")
                    click.echo(f"   Replication Factor: {result.details.get('replication_factor', replication_factor)}")
            else:
                click.echo(f"❌ Failed to create topic: {result.message}", err=True)
                raise click.Abort()
            
        except Exception as e:
            click.echo(f"❌ Failed to create topic: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_create_topic())


@topic_cli.command('describe')
@click.argument('topic_name')
@click.option('--format', '-f', type=click.Choice(['table', 'json']), default='table',
              help='Output format')
@click.pass_context
def describe_topic(ctx, topic_name, format):
    """Describe a topic in detail."""
    
    async def _describe_topic():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Describe topic
            topic_service = await get_topic_service()
            topic_details = await topic_service.describe_topic(
                cluster_config['cluster_id'], 
                topic_name, 
                cluster_config['user_id']
            )
            
            if not topic_details:
                click.echo(f"❌ Topic '{topic_name}' not found", err=True)
                raise click.Abort()
            
            if format == 'json':
                topic_data = {
                    'name': topic_details.name,
                    'partitions': topic_details.partitions,
                    'replication_factor': topic_details.replication_factor,
                    'configs': topic_details.configs,
                    'partition_details': topic_details.partition_details,
                    'total_messages': topic_details.total_messages,
                    'total_size_bytes': topic_details.total_size_bytes
                }
                click.echo(json.dumps(topic_data, indent=2))
            else:
                click.echo(f"Topic: {topic_details.name}")
                click.echo(f"Partitions: {topic_details.partitions}")
                click.echo(f"Replication Factor: {topic_details.replication_factor}")
                
                if topic_details.total_messages is not None:
                    click.echo(f"Total Messages: {topic_details.total_messages:,}")
                
                if topic_details.total_size_bytes is not None:
                    size_mb = topic_details.total_size_bytes / (1024 * 1024)
                    click.echo(f"Total Size: {size_mb:.2f} MB")
                
                # Show configurations
                if topic_details.configs:
                    click.echo("\nConfigurations:")
                    config_rows = [[k, v] for k, v in topic_details.configs.items()]
                    click.echo(tabulate(config_rows, headers=['Key', 'Value'], tablefmt='grid'))
                
                # Show partition details
                if topic_details.partition_details:
                    click.echo("\nPartition Details:")
                    partition_rows = [
                        [p['partition'], p['leader'], ','.join(map(str, p['replicas'])), 
                         ','.join(map(str, p['isr']))]
                        for p in topic_details.partition_details
                    ]
                    headers = ['Partition', 'Leader', 'Replicas', 'ISR']
                    click.echo(tabulate(partition_rows, headers=headers, tablefmt='grid'))
            
        except Exception as e:
            click.echo(f"❌ Failed to describe topic: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_describe_topic())


@topic_cli.command('update-config')
@click.argument('topic_name')
@click.option('--config', '-c', multiple=True, required=True, 
              help='Config to update in key=value format')
@click.pass_context
def update_topic_config(ctx, topic_name, config):
    """Update topic configuration."""
    
    async def _update_config():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Parse configs
            configs = {}
            for cfg in config:
                if '=' not in cfg:
                    click.echo(f"❌ Invalid config format: {cfg}. Use key=value format.", err=True)
                    raise click.Abort()
                key, value = cfg.split('=', 1)
                configs[key] = value
            
            # Update config
            topic_service = await get_topic_service()
            result = await topic_service.update_topic_config(
                cluster_config['cluster_id'], 
                topic_name, 
                configs, 
                cluster_config['user_id']
            )
            
            if result.success:
                click.echo(f"✅ Topic '{topic_name}' configuration updated")
                if result.details and 'updated_configs' in result.details:
                    click.echo("Updated configurations:")
                    for key, value in result.details['updated_configs'].items():
                        click.echo(f"   {key}: {value}")
            else:
                click.echo(f"❌ Failed to update configuration: {result.message}", err=True)
                raise click.Abort()
            
        except Exception as e:
            click.echo(f"❌ Failed to update topic configuration: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_update_config())


@topic_cli.command('delete')
@click.argument('topic_name')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def delete_topic(ctx, topic_name, force):
    """Delete a topic."""
    
    async def _delete_topic():
        try:
            # Confirmation prompt
            if not force:
                if not click.confirm(f"Are you sure you want to delete topic '{topic_name}'?"):
                    click.echo("Operation cancelled.")
                    return
            
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Delete topic
            topic_service = await get_topic_service()
            result = await topic_service.delete_topic(
                cluster_config['cluster_id'], 
                topic_name, 
                cluster_config['user_id']
            )
            
            if result.success:
                click.echo(f"✅ Topic '{topic_name}' deleted successfully")
            else:
                click.echo(f"❌ Failed to delete topic: {result.message}", err=True)
                raise click.Abort()
            
        except Exception as e:
            click.echo(f"❌ Failed to delete topic: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_delete_topic())


@topic_cli.command('purge')
@click.argument('topic_name')
@click.option('--retention-ms', type=int, default=1000, 
              help='Temporary retention in milliseconds for purging')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def purge_topic(ctx, topic_name, retention_ms, force):
    """Purge messages from a topic."""
    
    async def _purge_topic():
        try:
            # Confirmation prompt
            if not force:
                if not click.confirm(f"Are you sure you want to purge all messages from topic '{topic_name}'?"):
                    click.echo("Operation cancelled.")
                    return
            
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Purge topic
            topic_service = await get_topic_service()
            result = await topic_service.purge_topic(
                cluster_config['cluster_id'], 
                topic_name, 
                retention_ms,
                cluster_config['user_id']
            )
            
            if result.success:
                click.echo(f"✅ Topic '{topic_name}' purged successfully")
                if result.details:
                    click.echo(f"   Retention used: {result.details.get('retention_ms', retention_ms)} ms")
            else:
                click.echo(f"❌ Failed to purge topic: {result.message}", err=True)
                raise click.Abort()
            
        except Exception as e:
            click.echo(f"❌ Failed to purge topic: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_purge_topic())


@topic_cli.command('bulk-create')
@click.option('--file', '-f', type=click.File('r'), required=True,
              help='JSON file with topic configurations')
@click.pass_context
def bulk_create_topics(ctx, file):
    """Create multiple topics from JSON file."""
    
    async def _bulk_create():
        try:
            # Load topic configurations
            try:
                topics_data = json.load(file)
            except json.JSONDecodeError as e:
                click.echo(f"❌ Invalid JSON file: {e}", err=True)
                raise click.Abort()
            
            if not isinstance(topics_data, list):
                click.echo("❌ JSON file must contain an array of topic configurations", err=True)
                raise click.Abort()
            
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Create topic configs
            topic_configs = []
            for topic_data in topics_data:
                try:
                    topic_config = TopicConfig(**topic_data)
                    topic_configs.append(topic_config)
                except Exception as e:
                    click.echo(f"❌ Invalid topic configuration: {e}", err=True)
                    raise click.Abort()
            
            # Bulk create topics
            topic_service = await get_topic_service()
            results = await topic_service.bulk_create_topics(
                cluster_config['cluster_id'], 
                topic_configs, 
                cluster_config['user_id']
            )
            
            # Display results
            successful = []
            failed = []
            
            for topic_name, result in results.items():
                if result.success:
                    successful.append(topic_name)
                else:
                    failed.append((topic_name, result.message))
            
            click.echo(f"Bulk create completed:")
            click.echo(f"✅ Successful: {len(successful)}")
            click.echo(f"❌ Failed: {len(failed)}")
            
            if successful:
                click.echo("\nSuccessfully created topics:")
                for topic_name in successful:
                    click.echo(f"   ✅ {topic_name}")
            
            if failed:
                click.echo("\nFailed to create topics:")
                for topic_name, error in failed:
                    click.echo(f"   ❌ {topic_name}: {error}")
            
        except Exception as e:
            click.echo(f"❌ Failed to bulk create topics: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_bulk_create())


@topic_cli.command('search')
@click.argument('pattern')
@click.option('--regex', is_flag=True, help='Use regex pattern matching')
@click.option('--include-internal', is_flag=True, help='Include internal topics')
@click.option('--format', '-f', type=click.Choice(['table', 'json']), default='table',
              help='Output format')
@click.pass_context
def search_topics(ctx, pattern, regex, include_internal, format):
    """Search for topics by name pattern."""
    
    async def _search_topics():
        try:
            import re
            
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Get all topics
            topic_service = await get_topic_service()
            all_topics = await topic_service.list_topics(
                cluster_config['cluster_id'], 
                include_internal, 
                cluster_config['user_id']
            )
            
            # Filter topics by pattern
            if regex:
                try:
                    pattern_re = re.compile(pattern)
                    matching_topics = [t for t in all_topics if pattern_re.search(t.name)]
                except re.error as e:
                    click.echo(f"❌ Invalid regex pattern: {e}", err=True)
                    raise click.Abort()
            else:
                # Simple substring matching
                matching_topics = [t for t in all_topics if pattern.lower() in t.name.lower()]
            
            if format == 'json':
                topic_data = [
                    {
                        'name': topic.name,
                        'partitions': topic.partitions,
                        'replication_factor': topic.replication_factor,
                        'configs': topic.configs
                    }
                    for topic in matching_topics
                ]
                click.echo(json.dumps(topic_data, indent=2))
            else:
                if not matching_topics:
                    click.echo(f"No topics found matching pattern: {pattern}")
                    return
                
                # Format as table
                headers = ['Topic Name', 'Partitions', 'Replication Factor']
                rows = [
                    [topic.name, topic.partitions, topic.replication_factor]
                    for topic in matching_topics
                ]
                
                click.echo(f"Topics matching '{pattern}' in cluster '{cluster_config['cluster_id']}':")
                click.echo(tabulate(rows, headers=headers, tablefmt='grid'))
                click.echo(f"\nFound: {len(matching_topics)} topics")
            
        except Exception as e:
            click.echo(f"❌ Failed to search topics: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_search_topics())


@topic_cli.command('copy-config')
@click.argument('source_topic')
@click.argument('target_topic')
@click.option('--exclude', multiple=True, help='Config keys to exclude from copying')
@click.option('--dry-run', is_flag=True, help='Show what would be copied without applying')
@click.pass_context
def copy_topic_config(ctx, source_topic, target_topic, exclude, dry_run):
    """Copy configuration from one topic to another."""
    
    async def _copy_config():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Get source topic details
            topic_service = await get_topic_service()
            source_details = await topic_service.describe_topic(
                cluster_config['cluster_id'], 
                source_topic, 
                cluster_config['user_id']
            )
            
            if not source_details:
                click.echo(f"❌ Source topic '{source_topic}' not found", err=True)
                raise click.Abort()
            
            # Check target topic exists
            target_details = await topic_service.describe_topic(
                cluster_config['cluster_id'], 
                target_topic, 
                cluster_config['user_id']
            )
            
            if not target_details:
                click.echo(f"❌ Target topic '{target_topic}' not found", err=True)
                raise click.Abort()
            
            # Filter configs to copy
            configs_to_copy = {}
            excluded_keys = set(exclude)
            
            for key, value in source_details.configs.items():
                if key not in excluded_keys:
                    configs_to_copy[key] = value
            
            if not configs_to_copy:
                click.echo("No configurations to copy after exclusions")
                return
            
            if dry_run:
                click.echo(f"Would copy the following configurations from '{source_topic}' to '{target_topic}':")
                for key, value in configs_to_copy.items():
                    click.echo(f"   {key}: {value}")
                return
            
            # Apply configurations
            result = await topic_service.update_topic_config(
                cluster_config['cluster_id'], 
                target_topic, 
                configs_to_copy, 
                cluster_config['user_id']
            )
            
            if result.success:
                click.echo(f"✅ Copied {len(configs_to_copy)} configurations from '{source_topic}' to '{target_topic}'")
                click.echo("Copied configurations:")
                for key, value in configs_to_copy.items():
                    click.echo(f"   {key}: {value}")
            else:
                click.echo(f"❌ Failed to copy configuration: {result.message}", err=True)
                raise click.Abort()
            
        except Exception as e:
            click.echo(f"❌ Failed to copy topic configuration: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_copy_config())


@topic_cli.command('validate')
@click.argument('topic_name')
@click.pass_context
def validate_topic(ctx, topic_name):
    """Validate topic configuration and health."""
    
    async def _validate_topic():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Get topic details
            topic_service = await get_topic_service()
            topic_details = await topic_service.describe_topic(
                cluster_config['cluster_id'], 
                topic_name, 
                cluster_config['user_id']
            )
            
            if not topic_details:
                click.echo(f"❌ Topic '{topic_name}' not found", err=True)
                raise click.Abort()
            
            # Validation checks
            issues = []
            warnings = []
            
            # Check replication factor
            if topic_details.replication_factor < 2:
                warnings.append("Replication factor is less than 2 - consider increasing for fault tolerance")
            
            # Check partition count
            if topic_details.partitions == 1:
                warnings.append("Topic has only 1 partition - consider increasing for better parallelism")
            
            # Check retention settings
            if 'retention.ms' in topic_details.configs:
                retention_ms = int(topic_details.configs['retention.ms'])
                retention_hours = retention_ms / (1000 * 3600)
                if retention_hours < 1:
                    warnings.append(f"Very short retention time: {retention_hours:.1f} hours")
                elif retention_hours > 8760:  # 1 year
                    warnings.append(f"Very long retention time: {retention_hours:.1f} hours")
            
            # Check partition details for issues
            if topic_details.partition_details:
                for partition in topic_details.partition_details:
                    if partition['leader'] == -1:
                        issues.append(f"Partition {partition['partition']} has no leader")
                    
                    if len(partition['isr']) < topic_details.replication_factor:
                        issues.append(f"Partition {partition['partition']} has under-replicated ISR")
            
            # Display results
            click.echo(f"Validation results for topic '{topic_name}':")
            click.echo(f"   Partitions: {topic_details.partitions}")
            click.echo(f"   Replication Factor: {topic_details.replication_factor}")
            
            if not issues and not warnings:
                click.echo("✅ Topic configuration looks healthy")
            else:
                if issues:
                    click.echo(f"\n❌ Issues found ({len(issues)}):")
                    for issue in issues:
                        click.echo(f"   • {issue}")
                
                if warnings:
                    click.echo(f"\n⚠️  Warnings ({len(warnings)}):")
                    for warning in warnings:
                        click.echo(f"   • {warning}")
            
        except Exception as e:
            click.echo(f"❌ Failed to validate topic: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_validate_topic())


@topic_cli.command('export')
@click.option('--output', '-o', type=click.File('w'), default='-',
              help='Output file (default: stdout)')
@click.option('--include-internal', is_flag=True, help='Include internal topics')
@click.option('--pattern', help='Only export topics matching pattern')
@click.pass_context
def export_topics(ctx, output, include_internal, pattern):
    """Export topic configurations to JSON."""
    
    async def _export_topics():
        try:
            import re
            
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Register cluster
            client_manager = get_client_manager()
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Get topics
            topic_service = await get_topic_service()
            topics = await topic_service.list_topics(
                cluster_config['cluster_id'], 
                include_internal, 
                cluster_config['user_id']
            )
            
            # Filter by pattern if provided
            if pattern:
                try:
                    pattern_re = re.compile(pattern)
                    topics = [t for t in topics if pattern_re.search(t.name)]
                except re.error as e:
                    click.echo(f"❌ Invalid regex pattern: {e}", err=True)
                    raise click.Abort()
            
            # Export topic configurations
            export_data = []
            for topic in topics:
                # Get detailed configuration
                topic_details = await topic_service.describe_topic(
                    cluster_config['cluster_id'], 
                    topic.name, 
                    cluster_config['user_id']
                )
                
                if topic_details:
                    topic_config = {
                        'name': topic_details.name,
                        'partitions': topic_details.partitions,
                        'replication_factor': topic_details.replication_factor,
                        'custom_configs': topic_details.configs
                    }
                    
                    # Convert retention.ms to retention_ms for TopicConfig compatibility
                    if 'retention.ms' in topic_config['custom_configs']:
                        topic_config['retention_ms'] = int(topic_config['custom_configs']['retention.ms'])
                        del topic_config['custom_configs']['retention.ms']
                    
                    # Extract common configs
                    if 'cleanup.policy' in topic_config['custom_configs']:
                        topic_config['cleanup_policy'] = topic_config['custom_configs']['cleanup.policy']
                        del topic_config['custom_configs']['cleanup.policy']
                    
                    if 'compression.type' in topic_config['custom_configs']:
                        topic_config['compression_type'] = topic_config['custom_configs']['compression.type']
                        del topic_config['custom_configs']['compression.type']
                    
                    export_data.append(topic_config)
            
            # Write to output
            json.dump(export_data, output, indent=2)
            
            if output != sys.stdout:
                click.echo(f"✅ Exported {len(export_data)} topic configurations")
            
        except Exception as e:
            click.echo(f"❌ Failed to export topics: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_export_topics())