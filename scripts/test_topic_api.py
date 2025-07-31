#!/usr/bin/env python3
"""Script to test the topic management REST API."""

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
BASE_URL = f"http://{config.api.host}:{config.api.port + 1}/api/v1"
HEADERS = {
    'Content-Type': 'application/json',
    'X-User-ID': 'test-user'
}

CLUSTER_ID = "test-cluster"


def test_health_check():
    """Test the health check endpoint."""
    print("💚 Testing health check...")
    
    try:
        response = requests.get(f"{BASE_URL}/health")
        
        if response.status_code == 200:
            health = response.json()
            print(f"✅ API is healthy: {health['status']}")
            print(f"   Service: {health['service']}")
            print(f"   Version: {health['version']}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Health check exception: {e}")
        return False


def test_cluster_info():
    """Test getting cluster information."""
    print(f"📊 Testing cluster info for {CLUSTER_ID}...")
    
    try:
        response = requests.get(f"{BASE_URL}/clusters/{CLUSTER_ID}/info", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Cluster info retrieved:")
            info = data.get('info', {})
            print(f"   Cluster ID: {info.get('cluster_id', 'Unknown')}")
            print(f"   Broker Count: {info.get('broker_count', 'Unknown')}")
            print(f"   Topic Count: {info.get('topic_count', 'Unknown')}")
            return True
        elif response.status_code == 404:
            print("⚠️  Cluster not available (expected if no cluster running)")
            return True
        else:
            print(f"❌ Cluster info request failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Cluster info exception: {e}")
        return False


def test_list_topics():
    """Test listing topics."""
    print(f"📋 Testing topic listing for {CLUSTER_ID}...")
    
    try:
        response = requests.get(f"{BASE_URL}/clusters/{CLUSTER_ID}/topics", headers=HEADERS)
        
        if response.status_code == 200:
            data = response.json()
            topics = data.get('topics', [])
            print(f"✅ Found {len(topics)} topics:")
            for topic in topics[:5]:  # Show first 5 topics
                print(f"   - {topic['name']}: {topic['partitions']} partitions, RF={topic['replication_factor']}")
            if len(topics) > 5:
                print(f"   ... and {len(topics) - 5} more topics")
            return True
        else:
            print(f"⚠️  Topic listing failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Topic listing exception: {e}")
        return False


def test_create_topic():
    """Test creating a topic."""
    print(f"📝 Testing topic creation...")
    
    topic_data = {
        'name': 'api-test-topic',
        'partitions': 3,
        'replication_factor': 1,
        'retention_ms': 3600000,  # 1 hour
        'cleanup_policy': 'delete',
        'compression_type': 'snappy',
        'custom_configs': {
            'max.message.bytes': '2097152'  # 2MB
        }
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/clusters/{CLUSTER_ID}/topics",
            headers=HEADERS,
            json=topic_data
        )
        
        if response.status_code == 201:
            data = response.json()
            print("✅ Topic created successfully:")
            print(f"   Name: {data['topic']['name']}")
            print(f"   Message: {data['message']}")
            return True
        elif response.status_code == 409:
            print("⚠️  Topic already exists")
            return True
        else:
            print(f"❌ Topic creation failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Topic creation exception: {e}")
        return False


def test_describe_topic():
    """Test describing a topic."""
    print(f"📖 Testing topic description...")
    
    topic_name = 'api-test-topic'
    
    try:
        response = requests.get(
            f"{BASE_URL}/clusters/{CLUSTER_ID}/topics/{topic_name}",
            headers=HEADERS
        )
        
        if response.status_code == 200:
            data = response.json()
            topic = data['topic']
            print("✅ Topic described successfully:")
            print(f"   Name: {topic['name']}")
            print(f"   Partitions: {topic['partitions']}")
            print(f"   Replication Factor: {topic['replication_factor']}")
            print(f"   Configurations: {len(topic.get('configs', {}))}")
            return True
        elif response.status_code == 404:
            print(f"⚠️  Topic {topic_name} not found")
            return False
        else:
            print(f"❌ Topic description failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Topic description exception: {e}")
        return False


def test_update_topic_config():
    """Test updating topic configuration."""
    print(f"⚙️  Testing topic configuration update...")
    
    topic_name = 'api-test-topic'
    config_data = {
        'configs': {
            'retention.ms': '7200000',  # 2 hours
            'max.message.bytes': '4194304'  # 4MB
        }
    }
    
    try:
        response = requests.put(
            f"{BASE_URL}/clusters/{CLUSTER_ID}/topics/{topic_name}/config",
            headers=HEADERS,
            json=config_data
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Topic configuration updated:")
            print(f"   Topic: {data['topic_name']}")
            print(f"   Message: {data['message']}")
            return True
        else:
            print(f"❌ Config update failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Config update exception: {e}")
        return False


def test_bulk_operations():
    """Test bulk topic operations."""
    print(f"🔄 Testing bulk operations...")
    
    # Test bulk create
    bulk_create_data = {
        'operation': 'create',
        'topics': [
            {
                'name': 'bulk-api-test-1',
                'partitions': 2,
                'replication_factor': 1
            },
            {
                'name': 'bulk-api-test-2',
                'partitions': 4,
                'replication_factor': 1
            }
        ]
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/clusters/{CLUSTER_ID}/topics/bulk",
            headers=HEADERS,
            json=bulk_create_data
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Bulk create completed:")
            print(f"   Total: {data['total']}")
            print(f"   Successful: {data['successful']}")
            print(f"   Failed: {data['failed']}")
            
            for result in data['results']['successful']:
                print(f"   ✅ {result['topic_name']}")
            
            for result in data['results']['failed']:
                print(f"   ❌ {result['topic_name']}: {result['message']}")
            
            return data['successful'] > 0
        else:
            print(f"❌ Bulk create failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Bulk operations exception: {e}")
        return False


def test_purge_topic():
    """Test topic purging."""
    print(f"🧹 Testing topic purge...")
    
    topic_name = 'api-test-topic'
    purge_data = {
        'retention_ms': 2000
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/clusters/{CLUSTER_ID}/topics/{topic_name}/purge",
            headers=HEADERS,
            json=purge_data
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Topic purged successfully:")
            print(f"   Topic: {data['topic_name']}")
            print(f"   Message: {data['message']}")
            return True
        else:
            print(f"❌ Topic purge failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Topic purge exception: {e}")
        return False


def test_cleanup():
    """Clean up test topics."""
    print(f"🧹 Cleaning up test topics...")
    
    # Bulk delete test topics
    bulk_delete_data = {
        'operation': 'delete',
        'topic_names': [
            'api-test-topic',
            'bulk-api-test-1',
            'bulk-api-test-2'
        ]
    }
    
    try:
        response = requests.post(
            f"{BASE_URL}/clusters/{CLUSTER_ID}/topics/bulk",
            headers=HEADERS,
            json=bulk_delete_data
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Cleanup completed:")
            print(f"   Successful deletions: {data['successful']}")
            
            for result in data['results']['successful']:
                print(f"   ✅ Deleted {result['topic_name']}")
            
            return True
        else:
            print(f"⚠️  Cleanup had issues: {response.status_code}")
            return True  # Don't fail the test for cleanup issues
            
    except Exception as e:
        print(f"⚠️  Cleanup exception: {e}")
        return True  # Don't fail the test for cleanup issues


def main():
    """Run all API tests."""
    print("🧪 Testing Topic Management REST API")
    print(f"🔗 Base URL: {BASE_URL}")
    print(f"🏷️  Cluster ID: {CLUSTER_ID}")
    print()
    
    # Test health check first
    if not test_health_check():
        print("❌ Health check failed - server may not be running")
        print("💡 Start the server with: python scripts/start_topic_api.py")
        return False
    
    print()
    
    # Check if user wants to run tests that require a running cluster
    response = input("❓ Do you have a running Kafka cluster? [y/N]: ")
    if response.lower() not in ['y', 'yes']:
        print("ℹ️  Skipping tests that require a running cluster")
        print("💡 Start a cluster with: python scripts/create_local_cluster.py")
        return True
    
    # Run all tests
    tests = [
        ("Cluster Info", test_cluster_info),
        ("List Topics", test_list_topics),
        ("Create Topic", test_create_topic),
        ("Describe Topic", test_describe_topic),
        ("Update Config", test_update_topic_config),
        ("Bulk Operations", test_bulk_operations),
        ("Purge Topic", test_purge_topic)
    ]
    
    results = []
    for test_name, test_func in tests:
        print()
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Cleanup
    print()
    test_cleanup()
    
    # Summary
    print("\n📊 Test Results Summary")
    print("-" * 30)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed!")
        return True
    else:
        print("⚠️  Some tests failed")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)