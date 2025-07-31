#!/usr/bin/env python3
"""
Simple start script for Kafka Self-Service Platform.
Compatible with Python 3.9+
"""

import sys
import os
import subprocess
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    """Main function to start the platform."""
    print("🚀 Kafka Self-Service Platform - Simple Start")
    print("=" * 50)
    
    # Check Python version
    python_version = sys.version_info
    print(f"Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version < (3, 9):
        print("❌ Python 3.9+ is required")
        return 1
    
    # Set environment variables
    env = os.environ.copy()
    env.update({
        "API_HOST": "0.0.0.0",
        "API_PORT": "8000",
        "MONITORING_HOST": "0.0.0.0", 
        "MONITORING_PORT": "8080",
        "LOG_LEVEL": "INFO",
        "API_KEY": "admin-secret-key",
        "JWT_SECRET": "jwt-secret-key-for-development",
        "PYTHONPATH": str(project_root),
        "DATABASE_URL": "sqlite:///./data/kafka_ops.db"
    })
    
    # Create data directory
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)
    
    print("📦 Installing basic dependencies...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "flask", "pydantic", "requests"], 
                      check=True, capture_output=True)
        print("✅ Basic dependencies installed")
    except subprocess.CalledProcessError:
        print("⚠️  Could not install dependencies, continuing anyway...")
    
    print("🚀 Starting API server...")
    print("API Server:      http://localhost:8000")
    print("Monitoring:      http://localhost:8080") 
    print("API Key:         admin-secret-key")
    print()
    print("Press Ctrl+C to stop")
    print()
    
    try:
        # Try to start the API server
        process = subprocess.Popen(
            [sys.executable, "-c", """
import sys
sys.path.insert(0, '.')

# Simple Flask app for demonstration
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/health')
def health():
    return jsonify({
        'status': 'healthy',
        'message': 'Kafka Ops Platform is running',
        'version': '1.0.0'
    })

@app.route('/v2/catalog')
def catalog():
    if request.headers.get('X-API-Key') != 'admin-secret-key':
        return jsonify({'error': 'Unauthorized'}), 401
    
    return jsonify({
        'services': [{
            'id': 'kafka-cluster',
            'name': 'kafka-cluster',
            'description': 'Kafka Cluster Service',
            'plans': [{
                'id': 'small',
                'name': 'Small',
                'description': 'Small Kafka cluster'
            }]
        }]
    })

@app.route('/api/v1/topics', methods=['GET', 'POST'])
def topics():
    if request.headers.get('X-API-Key') != 'admin-secret-key':
        return jsonify({'error': 'Unauthorized'}), 401
    
    if request.method == 'GET':
        return jsonify({
            'topics': [
                {'name': 'demo-topic', 'partitions': 3, 'replication_factor': 1}
            ]
        })
    else:
        data = request.get_json()
        return jsonify({
            'status': 'success',
            'message': f'Topic {data.get("name", "unknown")} created',
            'topic': data
        })

if __name__ == '__main__':
    print('Starting simple Kafka Ops Platform...')
    app.run(host='0.0.0.0', port=8000, debug=False)
"""],
            env=env,
            cwd=project_root
        )
        
        # Wait for process
        process.wait()
        
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down...")
        if 'process' in locals():
            process.terminate()
        print("✅ Server stopped")
    
    return 0

if __name__ == "__main__":
    exit(main())