"""CLI commands for cluster management."""

import click
import asyncio
import json
from tabulate import tabulate

from kafka_ops_agent.cli.config import get_cluster_config
from kafka_ops_agent.services.topic_management import get_topic_service
from kafka_ops_agent.clients.kafka_client import get_client_manager
from kafka_ops_agent.models.cluster import ConnectionInfo


@click.group()
def cluster_cli():
    """Cluster management commands."""
    pass


@cluster_cli.command('info')
@click.option('--format', '-f', type=click.Choice(['table', 'json']), default='table',
              help='Output format')
@click.pass_context
def cluster_info(ctx, format):
    """Get cluster information."""
    
    async def _cluster_info():
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
            
            # Get cluster info
            topic_service = await get_topic_service()
            cluster_info = await topic_service.get_cluster_info(cluster_config['cluster_id'])
            
            if not cluster_info:
                click.echo(f"❌ Cluster '{cluster_config['cluster_id']}' is not available", err=True)
                raise click.Abort()
            
            if format == 'json':
                click.echo(json.dumps(cluster_info, indent=2))
            else:
                click.echo(f"Cluster Information: {cluster_config['cluster_id']}")
                click.echo(f"Cluster ID: {cluster_info.get('cluster_id', 'Unknown')}")
                click.echo(f"Broker Count: {cluster_info.get('broker_count', 'Unknown')}")
                click.echo(f"Topic Count: {cluster_info.get('topic_count', 'Unknown')}")
                
                if 'controller_id' in cluster_info:
                    click.echo(f"Controller ID: {cluster_info['controller_id']}")
                
                # Show brokers
                if 'brokers' in cluster_info and cluster_info['brokers']:
                    click.echo("\nBrokers:")
                    broker_rows = [
                        [broker['id'], broker['host'], broker['port']]
                        for broker in cluster_info['brokers']
                    ]
                    headers = ['Broker ID', 'Host', 'Port']
                    click.echo(tabulate(broker_rows, headers=headers, tablefmt='grid'))
            
        except Exception as e:
            click.echo(f"❌ Failed to get cluster info: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_cluster_info())


@cluster_cli.command('health')
@click.pass_context
def cluster_health(ctx):
    """Check cluster health."""
    
    async def _cluster_health():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Get client manager
            client_manager = get_client_manager()
            
            # Register cluster
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            
            # Get connection and check health
            connection = client_manager.get_connection(cluster_config['cluster_id'])
            if not connection:
                click.echo(f"❌ Failed to connect to cluster '{cluster_config['cluster_id']}'", err=True)
                raise click.Abort()
            
            # Perform health check
            is_healthy = connection.health_check()
            
            if is_healthy:
                click.echo(f"✅ Cluster '{cluster_config['cluster_id']}' is healthy")
                
                # Get connection stats
                stats = connection.get_stats()
                click.echo(f"   Connection age: {stats['age_seconds']:.1f} seconds")
                click.echo(f"   Use count: {stats['use_count']}")
                click.echo(f"   Last used: {stats['idle_seconds']:.1f} seconds ago")
            else:
                click.echo(f"❌ Cluster '{cluster_config['cluster_id']}' is unhealthy", err=True)
                raise click.Abort()
            
        except Exception as e:
            click.echo(f"❌ Failed to check cluster health: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_cluster_health())


@cluster_cli.command('stats')
@click.pass_context
def cluster_stats(ctx):
    """Show cluster connection statistics."""
    
    async def _cluster_stats():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            # Get client manager stats
            client_manager = get_client_manager()
            manager_stats = client_manager.get_stats()
            
            click.echo("Client Manager Statistics:")
            click.echo(f"Total Connections: {manager_stats['total_connections']}")
            click.echo(f"Max Connections: {manager_stats['max_connections']}")
            click.echo(f"Pool Utilization: {manager_stats['pool_utilization']:.1%}")
            click.echo(f"Registered Clusters: {manager_stats['registered_clusters']}")
            
            if manager_stats['connections']:
                click.echo("\nConnection Details:")
                connection_rows = []
                for conn in manager_stats['connections']:
                    connection_rows.append([
                        conn['connection_id'],
                        'Healthy' if conn['is_healthy'] else 'Unhealthy',
                        conn['use_count'],
                        f"{conn['age_seconds']:.1f}s",
                        f"{conn['idle_seconds']:.1f}s"
                    ])
                
                headers = ['Connection ID', 'Health', 'Use Count', 'Age', 'Idle']
                click.echo(tabulate(connection_rows, headers=headers, tablefmt='grid'))
            
        except Exception as e:
            click.echo(f"❌ Failed to get cluster stats: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_cluster_stats())


@cluster_cli.command('test-connection')
@click.pass_context
def test_connection(ctx):
    """Test connection to the cluster."""
    
    async def _test_connection():
        try:
            # Get cluster config
            cluster_config = get_cluster_config(ctx.obj['config'])
            
            click.echo(f"Testing connection to cluster '{cluster_config['cluster_id']}'...")
            click.echo(f"Bootstrap servers: {', '.join(cluster_config['bootstrap_servers'])}")
            
            # Get client manager
            client_manager = get_client_manager()
            
            # Register cluster
            connection_info = ConnectionInfo(
                bootstrap_servers=cluster_config['bootstrap_servers'],
                zookeeper_connect=cluster_config['zookeeper_connect']
            )
            
            success = client_manager.register_cluster(cluster_config['cluster_id'], connection_info)
            if not success:
                click.echo("❌ Failed to register cluster", err=True)
                raise click.Abort()
            
            click.echo("✅ Cluster registered successfully")
            
            # Get connection
            connection = client_manager.get_connection(cluster_config['cluster_id'])
            if not connection:
                click.echo("❌ Failed to get connection", err=True)
                raise click.Abort()
            
            click.echo("✅ Connection obtained successfully")
            
            # Test health
            is_healthy = connection.health_check()
            if is_healthy:
                click.echo("✅ Health check passed")
            else:
                click.echo("❌ Health check failed", err=True)
                raise click.Abort()
            
            # Try to get cluster info
            topic_service = await get_topic_service()
            cluster_info = await topic_service.get_cluster_info(cluster_config['cluster_id'])
            
            if cluster_info:
                click.echo("✅ Cluster info retrieved successfully")
                click.echo(f"   Brokers: {cluster_info.get('broker_count', 'Unknown')}")
                click.echo(f"   Topics: {cluster_info.get('topic_count', 'Unknown')}")
            else:
                click.echo("⚠️  Could not retrieve cluster info")
            
            click.echo(f"\n🎉 Connection test completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ Connection test failed: {e}", err=True)
            raise click.Abort()
    
    asyncio.run(_test_connection())