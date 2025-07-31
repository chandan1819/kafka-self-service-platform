"""CLI configuration management."""

import json
import os
from pathlib import Path
from typing import Dict, Any


def load_cli_config(config_path: Path) -> Dict[str, Any]:
    """Load CLI configuration from file."""
    
    default_config = {
        'cluster_id': None,
        'bootstrap_servers': ['localhost:9092'],
        'zookeeper_connect': 'localhost:2181',
        'user_id': 'cli-user'
    }
    
    if not config_path.exists():
        return default_config
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Merge with defaults
        merged_config = default_config.copy()
        merged_config.update(config)
        
        return merged_config
        
    except Exception as e:
        print(f"Warning: Failed to load config from {config_path}: {e}")
        return default_config


def save_cli_config(config_path: Path, config: Dict[str, Any]) -> None:
    """Save CLI configuration to file."""
    
    # Ensure directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        raise Exception(f"Failed to save config to {config_path}: {e}")


def get_cluster_config(ctx_config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract cluster configuration from CLI context."""
    
    cluster_id = ctx_config.get('cluster_id')
    if not cluster_id:
        raise Exception("Cluster ID not configured. Use 'kafka-ops configure' or --cluster-id option.")
    
    bootstrap_servers = ctx_config.get('bootstrap_servers', ['localhost:9092'])
    if isinstance(bootstrap_servers, str):
        bootstrap_servers = bootstrap_servers.split(',')
    
    return {
        'cluster_id': cluster_id,
        'bootstrap_servers': bootstrap_servers,
        'zookeeper_connect': ctx_config.get('zookeeper_connect', 'localhost:2181'),
        'user_id': ctx_config.get('user_id', 'cli-user')
    }


def get_current_config() -> Dict[str, Any]:
    """Get current CLI configuration from default location."""
    default_config_path = Path.home() / '.kafka-ops-agent' / 'config.json'
    return load_cli_config(default_config_path)