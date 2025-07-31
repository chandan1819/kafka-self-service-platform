#!/usr/bin/env python3
"""
Check if Kafka clusters are running in different environments.
"""

import subprocess
import sys
import socket
import requests
from pathlib import Path

def check_port(host, port):
    """Check if a port is open."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def check_docker_kafka():
    """Check for Kafka running in Docker."""
    print("🐳 Checking Docker Kafka...")
    
    try:
        # Check if Docker is available
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ Docker not available")
            return False
        
        # Check for running Kafka containers
        result = subprocess.run(['docker', 'ps', '--filter', 'name=kafka', '--format', 'table {{.Names}}\t{{.Status}}'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:  # More than just header
                print("   ✅ Found Docker Kafka containers:")
                for line in lines[1:]:  # Skip header
                    print(f"      {line}")
                return True
        
        print("   ❌ No Docker Kafka containers found")
        return False
        
    except Exception as e:
        print(f"   ❌ Error checking Docker: {e}")
        return False

def check_local_kafka():
    """Check for locally running Kafka."""
    print("💻 Checking Local Kafka...")
    
    # Common Kafka ports
    kafka_ports = [9092, 9093, 9094]
    zookeeper_ports = [2181, 2182, 2183]
    
    kafka_running = False
    zk_running = False
    
    for port in kafka_ports:
        if check_port('localhost', port):
            print(f"   ✅ Kafka broker found on port {port}")
            kafka_running = True
    
    for port in zookeeper_ports:
        if check_port('localhost', port):
            print(f"   ✅ Zookeeper found on port {port}")
            zk_running = True
    
    if not kafka_running:
        print("   ❌ No local Kafka brokers found")
    
    if not zk_running:
        print("   ❌ No local Zookeeper found")
    
    return kafka_running

def check_kubernetes_kafka():
    """Check for Kafka running in Kubernetes."""
    print("☸️  Checking Kubernetes Kafka...")
    
    try:
        # Check if kubectl is available
        result = subprocess.run(['kubectl', 'version', '--client'], capture_output=True, text=True)
        if result.returncode != 0:
            print("   ❌ kubectl not available")
            return False
        
        # Check for Kafka pods
        result = subprocess.run(['kubectl', 'get', 'pods', '-A', '-l', 'app=kafka'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:  # More than just header
                print("   ✅ Found Kubernetes Kafka pods:")
                for line in lines[1:]:  # Skip header
                    print(f"      {line}")
                return True
        
        # Also check for Confluent Platform
        result = subprocess.run(['kubectl', 'get', 'pods', '-A', '-l', 'app=cp-kafka'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:
                print("   ✅ Found Confluent Kafka pods:")
                for line in lines[1:]:
                    print(f"      {line}")
                return True
        
        print("   ❌ No Kubernetes Kafka pods found")
        return False
        
    except Exception as e:
        print(f"   ❌ Error checking Kubernetes: {e}")
        return False

def check_kafka_ops_platform():
    """Check if Kafka Ops Platform is running."""
    print("🚀 Checking Kafka Ops Platform...")
    
    # Check if our platform is running
    if check_port('localhost', 8000):
        try:
            response = requests.get('http://localhost:8000/health', timeout=5)
            if response.status_code == 200:
                print("   ✅ Kafka Ops Platform API is running on port 8000")
                
                # Check if monitoring is also running
                if check_port('localhost', 8080):
                    print("   ✅ Kafka Ops Platform monitoring is running on port 8080")
                
                return True
        except:
            pass
    
    print("   ❌ Kafka Ops Platform not running")
    return False

def check_confluent_cloud():
    """Check for Confluent Cloud configuration."""
    print("☁️  Checking Confluent Cloud...")
    
    # Check for common Confluent Cloud config files
    home = Path.home()
    confluent_configs = [
        home / '.confluent' / 'config',
        home / '.ccloud' / 'config',
        Path.cwd() / 'confluent.properties'
    ]
    
    for config_path in confluent_configs:
        if config_path.exists():
            print(f"   ✅ Found Confluent config: {config_path}")
            return True
    
    print("   ❌ No Confluent Cloud configuration found")
    return False

def provide_recommendations(results):
    """Provide recommendations based on what was found."""
    print("\n💡 Recommendations:")
    print("=" * 50)
    
    if not any(results.values()):
        print("❌ No Kafka clusters found running!")
        print("\n🚀 To get started quickly:")
        print("1. Start the Kafka Ops Platform:")
        print("   python3 scripts/simple_start.py")
        print("\n2. Or install Docker and run:")
        print("   brew install --cask docker")
        print("   docker run -d --name kafka -p 9092:9092 apache/kafka:latest")
        print("\n3. Or use Confluent Cloud (managed Kafka)")
        
    else:
        print("✅ Found running Kafka infrastructure!")
        
        if results['kafka_ops_platform']:
            print("\n🎯 Your Kafka Ops Platform is ready!")
            print("   API: http://localhost:8000")
            print("   Health: http://localhost:8000/health")
            print("   Try: curl -H 'X-API-Key: admin-secret-key' http://localhost:8000/v2/catalog")
        
        if results['docker_kafka']:
            print("\n🐳 Docker Kafka is running!")
            print("   You can connect to: localhost:9092")
            print("   Try: docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list")
        
        if results['local_kafka']:
            print("\n💻 Local Kafka is running!")
            print("   You can connect to: localhost:9092")
            print("   Try: kafka-topics --bootstrap-server localhost:9092 --list")
        
        if results['kubernetes_kafka']:
            print("\n☸️  Kubernetes Kafka is running!")
            print("   Try: kubectl get svc -A | grep kafka")

def main():
    """Main function."""
    print("🔍 Kafka Cluster Status Check")
    print("=" * 40)
    
    results = {
        'kafka_ops_platform': check_kafka_ops_platform(),
        'docker_kafka': check_docker_kafka(),
        'local_kafka': check_local_kafka(),
        'kubernetes_kafka': check_kubernetes_kafka(),
        'confluent_cloud': check_confluent_cloud()
    }
    
    print(f"\n📊 Summary:")
    print("=" * 20)
    for service, running in results.items():
        status = "✅ Running" if running else "❌ Not Found"
        service_name = service.replace('_', ' ').title()
        print(f"{service_name:<20} {status}")
    
    provide_recommendations(results)
    
    return 0

if __name__ == "__main__":
    exit(main())