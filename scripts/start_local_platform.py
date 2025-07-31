#!/usr/bin/env python3
"""
Start the Kafka Self-Service Platform locally without Docker.
This script sets up a minimal environment for testing and development.
"""

import sys
import os
import subprocess
import time
import signal
from pathlib import Path
from threading import Thread

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 11):
        print("❌ Python 3.11+ is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True

def install_dependencies():
    """Install required dependencies."""
    print("📦 Installing dependencies...")
    
    try:
        # Install main dependencies
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        
        # Install package in development mode
        subprocess.run([sys.executable, "-m", "pip", "install", "-e", "."], 
                      check=True, capture_output=True)
        
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def setup_sqlite_database():
    """Set up SQLite database for local development."""
    print("🗄️  Setting up SQLite database...")
    
    try:
        # Create data directory
        data_dir = project_root / "data"
        data_dir.mkdir(exist_ok=True)
        
        # Set database URL environment variable
        os.environ["DATABASE_URL"] = f"sqlite:///{data_dir}/kafka_ops.db"
        
        print("✅ SQLite database configured")
        return True
    except Exception as e:
        print(f"❌ Failed to setup database: {e}")
        return False

def start_api_server():
    """Start the API server."""
    print("🚀 Starting API server on http://localhost:8000...")
    
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
        "PYTHONPATH": str(project_root)
    })
    
    try:
        # Start the API server
        process = subprocess.Popen(
            [sys.executable, "-m", "kafka_ops_agent.api"],
            env=env,
            cwd=project_root
        )
        
        return process
    except Exception as e:
        print(f"❌ Failed to start API server: {e}")
        return None

def wait_for_server(url="http://localhost:8080/health", timeout=30):
    """Wait for server to be ready."""
    import requests
    
    print("⏳ Waiting for server to be ready...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print("✅ Server is ready!")
                return True
        except requests.RequestException:
            pass
        
        time.sleep(2)
    
    print("❌ Server did not start within timeout")
    return False

def show_usage_info():
    """Show usage information."""
    print("\n🎉 Kafka Self-Service Platform is running!")
    print("=" * 50)
    print("API Server:      http://localhost:8000")
    print("Monitoring:      http://localhost:8080")
    print("Health Check:    http://localhost:8080/health")
    print("API Key:         admin-secret-key")
    print()
    print("Quick Test Commands:")
    print("# Check health")
    print("curl http://localhost:8080/health")
    print()
    print("# Get service catalog")
    print("curl -H 'X-API-Key: admin-secret-key' http://localhost:8000/v2/catalog")
    print()
    print("# Create a topic")
    print("curl -X POST -H 'Content-Type: application/json' -H 'X-API-Key: admin-secret-key' \\")
    print("  -d '{\"name\":\"test-topic\",\"partitions\":1,\"replication_factor\":1}' \\")
    print("  http://localhost:8000/api/v1/topics")
    print()
    print("Press Ctrl+C to stop the server")

def main():
    """Main function."""
    print("🚀 Kafka Self-Service Platform - Local Setup")
    print("=" * 50)
    
    # Check prerequisites
    if not check_python_version():
        return 1
    
    # Install dependencies
    if not install_dependencies():
        return 1
    
    # Setup database
    if not setup_sqlite_database():
        return 1
    
    # Start API server
    api_process = start_api_server()
    if not api_process:
        return 1
    
    try:
        # Wait for server to be ready
        if not wait_for_server():
            api_process.terminate()
            return 1
        
        # Show usage information
        show_usage_info()
        
        # Wait for process to complete or user interrupt
        api_process.wait()
        
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down...")
        api_process.terminate()
        api_process.wait()
        print("✅ Server stopped")
    
    return 0

if __name__ == "__main__":
    exit(main())