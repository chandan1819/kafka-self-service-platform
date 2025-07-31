#!/usr/bin/env python3
"""Script to stop the local Kafka cluster."""

import sys
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.providers.docker_provider import DockerProvider
from kafka_ops_agent.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def stop_local_cluster():
    """Stop the local Kafka cluster."""
    
    print("🛑 Stopping local Kafka cluster...")
    
    # Initialize Docker provider
    try:
        provider = DockerProvider()
        print("✅ Docker provider initialized")
    except Exception as e:
        print(f"❌ Failed to initialize Docker provider: {e}")
        return False
    
    # Deprovision cluster
    instance_id = "local-kafka-dev"
    print(f"⏳ Deprovisioning cluster {instance_id}...")
    
    result = provider.deprovision_cluster(instance_id)
    
    if result.status.value == "succeeded":
        print("✅ Cluster stopped and cleaned up successfully!")
        return True
    else:
        print(f"❌ Failed to stop cluster: {result.error_message}")
        return False


if __name__ == "__main__":
    stop_local_cluster()