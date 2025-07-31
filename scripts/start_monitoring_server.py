#!/usr/bin/env python3
"""Start the monitoring server for health checks and metrics."""

import sys
import os
import asyncio
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.monitoring.endpoints import (
    create_monitoring_app, setup_default_health_checks, setup_default_alerts
)
from kafka_ops_agent.monitoring.health_checks import (
    get_health_check_manager, DatabaseHealthCheck, KafkaHealthCheck, ServiceHealthCheck
)
from kafka_ops_agent.monitoring.metrics import get_metrics_collector
from kafka_ops_agent.logging_config import setup_logging


async def setup_kafka_health_check():
    """Set up Kafka health check if available."""
    try:
        from kafka_ops_agent.clients.kafka_client import KafkaClientManager
        from kafka_ops_agent.config import get_config
        
        config = get_config()
        kafka_config = config.get('kafka', {})
        
        if kafka_config.get('bootstrap_servers'):
            async def create_kafka_client():
                manager = KafkaClientManager(kafka_config)
                return await manager.get_admin_client()
            
            health_manager = get_health_check_manager()
            kafka_check = KafkaHealthCheck(create_kafka_client)
            health_manager.register_health_check(kafka_check)
            
            print("✓ Kafka health check registered")
        else:
            print("⚠ Kafka configuration not found, skipping Kafka health check")
            
    except ImportError as e:
        print(f"⚠ Kafka client not available: {e}")
    except Exception as e:
        print(f"⚠ Failed to set up Kafka health check: {e}")


async def setup_database_health_check():
    """Set up database health check if available."""
    try:
        from kafka_ops_agent.storage.factory import create_metadata_store
        from kafka_ops_agent.config import get_config
        
        config = get_config()
        
        async def get_db_connection():
            store = await create_metadata_store(config)
            return store.connection
        
        health_manager = get_health_check_manager()
        db_check = DatabaseHealthCheck(get_db_connection)
        health_manager.register_health_check(db_check)
        
        print("✓ Database health check registered")
        
    except ImportError as e:
        print(f"⚠ Database client not available: {e}")
    except Exception as e:
        print(f"⚠ Failed to set up database health check: {e}")


async def setup_service_health_checks():
    """Set up service health checks."""
    try:
        health_manager = get_health_check_manager()
        
        # Add provisioning service health check
        async def check_provisioning_service():
            try:
                from kafka_ops_agent.services.provisioning import ProvisioningService
                # Simple check - just verify the service can be imported and initialized
                service = ProvisioningService()
                return True, {'status': 'available'}
            except Exception as e:
                return False, {'error': str(e)}
        
        provisioning_check = ServiceHealthCheck("provisioning", check_provisioning_service)
        health_manager.register_health_check(provisioning_check)
        
        # Add topic management service health check
        async def check_topic_service():
            try:
                from kafka_ops_agent.services.topic_management import TopicManagementService
                # Simple check - just verify the service can be imported
                return True, {'status': 'available'}
            except Exception as e:
                return False, {'error': str(e)}
        
        topic_check = ServiceHealthCheck("topic_management", check_topic_service)
        health_manager.register_health_check(topic_check)
        
        print("✓ Service health checks registered")
        
    except Exception as e:
        print(f"⚠ Failed to set up service health checks: {e}")


def simulate_metrics():
    """Simulate some metrics for demonstration."""
    import random
    import time
    
    metrics_collector = get_metrics_collector()
    
    # Simulate some request metrics
    metrics_collector.increment_counter("kafka_ops_requests_total", random.randint(1, 10))
    metrics_collector.set_gauge("kafka_ops_active_connections", random.randint(5, 50))
    metrics_collector.set_gauge("kafka_ops_topics_total", random.randint(10, 100))
    metrics_collector.set_gauge("kafka_ops_clusters_total", random.randint(1, 5))
    
    # Simulate request duration
    metrics_collector.observe_histogram("kafka_ops_request_duration_seconds", random.uniform(0.1, 2.0))
    
    # Occasionally simulate errors
    if random.random() < 0.1:  # 10% chance
        metrics_collector.increment_counter("kafka_ops_errors_total", 1)
    
    print(f"📊 Simulated metrics at {time.strftime('%H:%M:%S')}")


async def main():
    """Main function to start the monitoring server."""
    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("🚀 Starting Kafka Ops Agent Monitoring Server")
    print("=" * 50)
    
    # Create the monitoring app
    app = create_monitoring_app("kafka-ops-monitoring")
    
    # Set up default health checks
    print("\n📋 Setting up health checks...")
    setup_default_health_checks()
    
    # Set up additional health checks
    await setup_database_health_check()
    await setup_kafka_health_check()
    await setup_service_health_checks()
    
    # Set up alerts
    print("\n🚨 Setting up alerts...")
    alert_manager = setup_default_alerts()
    
    # Start alert manager
    await alert_manager.start(check_interval=30)  # Check every 30 seconds
    print("✓ Alert manager started")
    
    # Simulate some initial metrics
    print("\n📊 Generating initial metrics...")
    simulate_metrics()
    
    # Print available endpoints
    print("\n🌐 Monitoring server endpoints:")
    print("  Health Checks:")
    print("    GET  http://localhost:8080/health          - Overall health status")
    print("    GET  http://localhost:8080/health/ready    - Readiness probe (K8s)")
    print("    GET  http://localhost:8080/health/live     - Liveness probe (K8s)")
    print("    GET  http://localhost:8080/health/checks   - List all health checks")
    print("    GET  http://localhost:8080/health/history  - Health check history")
    print()
    print("  Metrics:")
    print("    GET  http://localhost:8080/metrics         - Current metrics (JSON)")
    print("    GET  http://localhost:8080/metrics/prometheus - Prometheus format")
    print("    GET  http://localhost:8080/metrics/history - Metrics history")
    print()
    print("  Alerts:")
    print("    GET  http://localhost:8080/alerts          - Current alerts")
    print("    GET  http://localhost:8080/alerts/history  - Alert history")
    print()
    print("  System:")
    print("    GET  http://localhost:8080/status          - Overall system status")
    print("    GET  http://localhost:8080/info            - System information")
    
    print(f"\n🎯 Server starting on http://localhost:8080")
    print("Press Ctrl+C to stop the server")
    
    try:
        # Start metrics simulation in background
        async def metrics_simulator():
            while True:
                await asyncio.sleep(10)  # Every 10 seconds
                simulate_metrics()
        
        # Start the metrics simulator
        metrics_task = asyncio.create_task(metrics_simulator())
        
        # Run the Flask app (this blocks)
        app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)
        
    except KeyboardInterrupt:
        print("\n🛑 Shutting down monitoring server...")
        
        # Stop alert manager
        await alert_manager.stop()
        print("✓ Alert manager stopped")
        
        # Cancel metrics simulator
        if 'metrics_task' in locals():
            metrics_task.cancel()
        
        print("✓ Monitoring server stopped")
    
    except Exception as e:
        logger.error(f"Error running monitoring server: {e}")
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit_code = asyncio.run(main())