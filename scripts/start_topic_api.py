#!/usr/bin/env python3
"""Script to start the topic management REST API server."""

import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.api.topic_management import create_topic_management_app
from kafka_ops_agent.logging_config import setup_logging
from kafka_ops_agent.config import config

def main():
    """Start the topic management API server."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Use port + 1 for topic management API
    api_port = config.api.port + 1
    
    print("🚀 Starting Topic Management API Server")
    print(f"📡 Host: {config.api.host}")
    print(f"🔌 Port: {api_port}")
    print(f"🗄️  Database: {config.database.type}")
    print(f"🐳 Default Provider: {config.providers.default_provider}")
    print()
    print("📋 Available Endpoints:")
    print(f"   POST   http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics")
    print(f"   GET    http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics")
    print(f"   GET    http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics/{{topic_name}}")
    print(f"   PUT    http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics/{{topic_name}}/config")
    print(f"   DELETE http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics/{{topic_name}}")
    print(f"   POST   http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics/{{topic_name}}/purge")
    print(f"   POST   http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/topics/bulk")
    print(f"   GET    http://{config.api.host}:{api_port}/api/v1/clusters/{{cluster_id}}/info")
    print(f"   GET    http://{config.api.host}:{api_port}/api/v1/health")
    print()
    
    logger.info("Starting Topic Management API server...")
    logger.info(f"Server configuration: host={config.api.host}, port={api_port}")
    
    try:
        app = create_topic_management_app()
        app.run(
            host=config.api.host,
            port=api_port,
            debug=config.api.debug
        )
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
        logger.info("Server stopped by user")
    except Exception as e:
        print(f"❌ Server failed to start: {e}")
        logger.error(f"Server failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()