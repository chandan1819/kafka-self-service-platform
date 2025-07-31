#!/usr/bin/env python3
"""Test script for cleanup operations API."""

import asyncio
import json
import time
import requests
from typing import Dict, Any

# API base URL
BASE_URL = "http://localhost:8083/api/v1/cleanup"


def test_topic_cleanup():
    """Test topic cleanup endpoint."""
    print("🧹 Testing topic cleanup...")
    
    # Test topic cleanup request
    cleanup_request = {
        "cluster_id": "local-kafka-dev",
        "max_age_hours": 1,
        "retention_pattern": "test",
        "dry_run": True,
        "user_id": "test-user"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/topics", json=cleanup_request)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code in [200, 202]:
            result = response.json()
            print(f"✅ Topic cleanup initiated:")
            print(f"   Execution ID: {result['execution_id']}")
            print(f"   Status: {result['status']}")
            print(f"   Cluster: {result['cluster_id']}")
            
            if result.get('result'):
                print(f"   Result: {json.dumps(result['result'], indent=2)}")
            
            return result['execution_id']
        else:
            print(f"❌ Topic cleanup failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error testing topic cleanup: {e}")
        return None


def test_cluster_cleanup():
    """Test cluster cleanup endpoint."""
    print("\n🧹 Testing cluster cleanup...")
    
    # Test cluster cleanup request
    cleanup_request = {
        "max_age_hours": 24,
        "dry_run": True,
        "user_id": "test-user"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/clusters", json=cleanup_request)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code in [200, 202]:
            result = response.json()
            print(f"✅ Cluster cleanup initiated:")
            print(f"   Execution ID: {result['execution_id']}")
            print(f"   Status: {result['status']}")
            
            if result.get('result'):
                print(f"   Result: {json.dumps(result['result'], indent=2)}")
            
            return result['execution_id']
        else:
            print(f"❌ Cluster cleanup failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error testing cluster cleanup: {e}")
        return None


def test_metadata_cleanup():
    """Test metadata cleanup endpoint."""
    print("\n🧹 Testing metadata cleanup...")
    
    # Test metadata cleanup request
    cleanup_request = {
        "max_age_days": 7,
        "dry_run": True,
        "user_id": "test-user"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/metadata", json=cleanup_request)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code in [200, 202]:
            result = response.json()
            print(f"✅ Metadata cleanup initiated:")
            print(f"   Execution ID: {result['execution_id']}")
            print(f"   Status: {result['status']}")
            
            if result.get('result'):
                print(f"   Result: {json.dumps(result['result'], indent=2)}")
            
            return result['execution_id']
        else:
            print(f"❌ Metadata cleanup failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error testing metadata cleanup: {e}")
        return None


def test_cleanup_status(execution_id: str):
    """Test cleanup status endpoint."""
    print(f"\n📊 Testing cleanup status for {execution_id}...")
    
    try:
        response = requests.get(f"{BASE_URL}/status/{execution_id}")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Cleanup status retrieved:")
            print(f"   Execution ID: {result['execution_id']}")
            print(f"   Type: {result['cleanup_type']}")
            print(f"   Status: {result['status']}")
            print(f"   Started: {result['started_at']}")
            
            if result.get('completed_at'):
                print(f"   Completed: {result['completed_at']}")
            
            if result.get('result'):
                print(f"   Result: {json.dumps(result['result'], indent=2)}")
            
            if result.get('error_message'):
                print(f"   Error: {result['error_message']}")
                
        else:
            print(f"❌ Failed to get cleanup status: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing cleanup status: {e}")


def test_cleanup_overview():
    """Test cleanup overview endpoint."""
    print("\n📊 Testing cleanup overview...")
    
    try:
        response = requests.get(f"{BASE_URL}/status")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Cleanup overview retrieved:")
            
            print(f"   Active cleanups: {len(result['active_cleanups'])}")
            for cleanup_key, cleanup_info in result['active_cleanups'].items():
                print(f"     - {cleanup_key}: {cleanup_info['type']}")
            
            print(f"   Recent operations: {len(result['recent_operations'])}")
            for operation in result['recent_operations'][:3]:  # Show first 3
                print(f"     - {operation['execution_id']}: {operation['status']}")
            
            print(f"   Scheduler stats:")
            stats = result['scheduler_stats']
            print(f"     - Running: {stats['scheduler_running']}")
            print(f"     - Total tasks: {stats['total_tasks']}")
            print(f"     - Enabled tasks: {stats['enabled_tasks']}")
            print(f"     - Running tasks: {stats['running_tasks']}")
            
        else:
            print(f"❌ Failed to get cleanup overview: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing cleanup overview: {e}")


def test_cleanup_logs(execution_id: str):
    """Test cleanup logs endpoint."""
    print(f"\n📝 Testing cleanup logs for {execution_id}...")
    
    try:
        response = requests.get(f"{BASE_URL}/logs/{execution_id}")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Cleanup logs retrieved:")
            print(f"   Execution ID: {result['execution_id']}")
            print(f"   Log count: {result['log_count']}")
            
            if result['logs']:
                print("   Logs:")
                for i, log_entry in enumerate(result['logs'][:5]):  # Show first 5 logs
                    print(f"     {i+1}. {log_entry}")
                
                if len(result['logs']) > 5:
                    print(f"     ... and {len(result['logs']) - 5} more logs")
            else:
                print("   No logs available")
                
        else:
            print(f"❌ Failed to get cleanup logs: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing cleanup logs: {e}")


def test_validation_errors():
    """Test validation error handling."""
    print("\n🔍 Testing validation errors...")
    
    # Test invalid topic cleanup request
    invalid_request = {
        "cluster_id": "",  # Empty cluster ID
        "max_age_hours": -1,  # Invalid age
        "retention_pattern": "invalid"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/topics", json=invalid_request)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 400:
            result = response.json()
            print(f"✅ Validation error handled correctly:")
            print(f"   Error: {result['error']}")
            print(f"   Details: {result.get('details', [])}")
        else:
            print(f"❌ Expected validation error, got: {response.text}")
            
    except Exception as e:
        print(f"❌ Error testing validation: {e}")


def test_conflict_detection():
    """Test cleanup conflict detection."""
    print("\n⚠️  Testing conflict detection...")
    
    # First, start a cleanup operation
    cleanup_request = {
        "cluster_id": "test-cluster",
        "max_age_hours": 1,
        "retention_pattern": "test",
        "dry_run": True,
        "user_id": "test-user"
    }
    
    try:
        # Start first cleanup
        response1 = requests.post(f"{BASE_URL}/topics", json=cleanup_request)
        
        if response1.status_code in [200, 202]:
            print("✅ First cleanup started")
            
            # Try to start another cleanup for the same cluster immediately
            response2 = requests.post(f"{BASE_URL}/topics", json=cleanup_request)
            
            if response2.status_code == 409:
                result = response2.json()
                print(f"✅ Conflict detected correctly:")
                print(f"   Error: {result['error']}")
                print(f"   Message: {result['message']}")
            else:
                print(f"❌ Expected conflict error, got: {response2.status_code}")
        else:
            print(f"❌ Failed to start first cleanup: {response1.text}")
            
    except Exception as e:
        print(f"❌ Error testing conflict detection: {e}")


def main():
    """Run all cleanup API tests."""
    print("🚀 Starting Cleanup Operations API Tests")
    print("=" * 50)
    
    # Test basic cleanup operations
    topic_execution_id = test_topic_cleanup()
    cluster_execution_id = test_cluster_cleanup()
    metadata_execution_id = test_metadata_cleanup()
    
    # Wait a moment for operations to complete
    time.sleep(2)
    
    # Test status and logs endpoints
    if topic_execution_id:
        test_cleanup_status(topic_execution_id)
        test_cleanup_logs(topic_execution_id)
    
    # Test overview
    test_cleanup_overview()
    
    # Test error handling
    test_validation_errors()
    test_conflict_detection()
    
    print("\n" + "=" * 50)
    print("🎉 Cleanup API tests completed!")
    print("\nTo start the cleanup API server, run:")
    print("python kafka_ops_agent/api/cleanup_operations.py")


if __name__ == "__main__":
    main()