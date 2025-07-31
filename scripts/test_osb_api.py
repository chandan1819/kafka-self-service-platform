#!/usr/bin/env python3
"""Script to test the Open Service Broker API endpoints."""

import sys
import requests
import json
import time
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.config import config

# API configuration
BASE_URL = f"http://{config.api.host}:{config.api.port}"
HEADERS = {
    'Content-Type': 'application/json',
    'X-Broker-API-Version': '2.16'
}

if config.api.api_key:
    HEADERS['Authorization'] = f'Bearer {config.api.api_key}'


def test_catalog():
    """Test the catalog endpoint."""
    print("📋 Testing catalog endpoint...")
    
    try:
        response = requests.get(f"{BASE_URL}/v2/catalog", headers=HEADERS)
        
        if response.status_code == 200:
            catalog = response.json()
            print("✅ Catalog retrieved successfully")
            print(f"   Services: {len(catalog['services'])}")
            
            for service in catalog['services']:
                print(f"   - {service['name']}: {len(service['plans'])} plans")
                for plan in service['plans']:
                    print(f"     * {plan['name']}: {plan['description']}")
            
            return True
        else:
            print(f"❌ Catalog request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Catalog request exception: {e}")
        return False


def test_provision_instance(instance_id: str):
    """Test provisioning a service instance."""
    print(f"⏳ Testing provision instance: {instance_id}")
    
    provision_data = {
        "service_id": "kafka-service",
        "plan_id": "basic",
        "organization_guid": "test-org-123",
        "space_guid": "test-space-456",
        "parameters": {
            "cluster_size": 1,
            "replication_factor": 1,
            "retention_hours": 24,
            "custom_properties": {
                "auto.create.topics.enable": "true"
            }
        }
    }
    
    try:
        response = requests.put(
            f"{BASE_URL}/v2/service_instances/{instance_id}",
            headers=HEADERS,
            json=provision_data
        )
        
        if response.status_code in [201, 202]:
            print("✅ Instance provisioned successfully")
            if response.status_code == 202:
                print("   Status: Asynchronous operation in progress")
            else:
                print("   Status: Synchronous operation completed")
            
            return True
        elif response.status_code == 409:
            print("⚠️  Instance already exists")
            return True
        else:
            print(f"❌ Provision request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Provision request exception: {e}")
        return False


def test_last_operation(instance_id: str):
    """Test getting last operation status."""
    print(f"📊 Testing last operation: {instance_id}")
    
    try:
        response = requests.get(
            f"{BASE_URL}/v2/service_instances/{instance_id}/last_operation",
            headers=HEADERS,
            params={
                "service_id": "kafka-service",
                "plan_id": "basic"
            }
        )
        
        if response.status_code == 200:
            operation = response.json()
            print(f"✅ Operation status: {operation['state']}")
            print(f"   Description: {operation.get('description', 'N/A')}")
            return operation['state']
        elif response.status_code == 410:
            print("⚠️  Instance does not exist")
            return "gone"
        else:
            print(f"❌ Last operation request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Last operation request exception: {e}")
        return None


def test_deprovision_instance(instance_id: str):
    """Test deprovisioning a service instance."""
    print(f"🛑 Testing deprovision instance: {instance_id}")
    
    try:
        response = requests.delete(
            f"{BASE_URL}/v2/service_instances/{instance_id}",
            headers=HEADERS,
            params={
                "service_id": "kafka-service",
                "plan_id": "basic"
            }
        )
        
        if response.status_code in [200, 202]:
            print("✅ Instance deprovisioned successfully")
            if response.status_code == 202:
                print("   Status: Asynchronous operation in progress")
            else:
                print("   Status: Synchronous operation completed")
            
            return True
        elif response.status_code == 410:
            print("⚠️  Instance does not exist")
            return True
        else:
            print(f"❌ Deprovision request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Deprovision request exception: {e}")
        return False


def test_health_check():
    """Test the health check endpoint."""
    print("💚 Testing health check...")
    
    try:
        response = requests.get(f"{BASE_URL}/health")
        
        if response.status_code == 200:
            health = response.json()
            print(f"✅ Service is healthy: {health['status']}")
            print(f"   Service: {health['service']}")
            print(f"   Version: {health['version']}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Health check exception: {e}")
        return False


def wait_for_operation_complete(instance_id: str, max_wait: int = 300):
    """Wait for an operation to complete."""
    print(f"⏳ Waiting for operation to complete (max {max_wait}s)...")
    
    start_time = time.time()
    while time.time() - start_time < max_wait:
        state = test_last_operation(instance_id)
        
        if state == "succeeded":
            print("✅ Operation completed successfully")
            return True
        elif state == "failed":
            print("❌ Operation failed")
            return False
        elif state == "gone":
            print("⚠️  Instance no longer exists")
            return True
        elif state == "in progress":
            print("   Still in progress...")
            time.sleep(10)
        else:
            print(f"   Unknown state: {state}")
            time.sleep(5)
    
    print("⏰ Operation timed out")
    return False


def main():
    """Run the complete API test suite."""
    print("🧪 Testing Open Service Broker API")
    print(f"🔗 Base URL: {BASE_URL}")
    print()
    
    # Test health check first
    if not test_health_check():
        print("❌ Health check failed - server may not be running")
        print("💡 Start the server with: python scripts/start_api_server.py")
        return False
    
    print()
    
    # Test catalog
    if not test_catalog():
        return False
    
    print()
    
    # Test provisioning workflow
    instance_id = "test-osb-instance"
    
    # Provision instance
    if not test_provision_instance(instance_id):
        return False
    
    print()
    
    # Wait for provisioning to complete
    if not wait_for_operation_complete(instance_id):
        return False
    
    print()
    
    # Wait a bit for user to see the cluster
    print("⏸️  Cluster is running. Waiting 5 seconds before cleanup...")
    time.sleep(5)
    
    # Deprovision instance
    if not test_deprovision_instance(instance_id):
        return False
    
    print()
    
    # Wait for deprovisioning to complete
    if not wait_for_operation_complete(instance_id):
        return False
    
    print()
    print("🎉 All API tests completed successfully!")
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)