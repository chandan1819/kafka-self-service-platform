#!/usr/bin/env python3
"""Script to start the Open Service Broker API server."""

import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.api.service_broker import run_server
from kafka_ops_agent.logging_config import setup_logging
from kafka_ops_agent.config import config

def main():
    """Start the API server."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("🚀 Starting Kafka Ops Agent API Server")
    print(f"📡 Host: {config.api.host}")
    print(f"🔌 Port: {config.api.port}")
    print(f"🔐 Auth: {'Enabled' if config.api.api_key else 'Disabled'}")
    print(f"🗄️  Database: {config.database.type}")
    print(f"🐳 Default Provider: {config.providers.default_provider}")
    print()
    
    logger.info("Starting Kafka Ops Agent API server...")
    logger.info(f"Server configuration: host={config.api.host}, port={config.api.port}")
    
    try:
        run_server()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
        logger.info("Server stopped by user")
    except Exception as e:
        print(f"❌ Server failed to start: {e}")
        logger.error(f"Server failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()