#!/usr/bin/env python3
"""Test script for CLI commands."""

import subprocess
import json
import tempfile
import sys
import os
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_cli_command(args, input_data=None):
    """Run a CLI command and return the result."""
    cmd = ['python', '-m', 'kafka_ops_agent.cli.main'] + args
    
    try:
        result = subprocess.run(
            cmd,
            input=input_data,
            text=True,
            capture_output=True,
            timeout=30
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)


def test_cli_help():
    """Test CLI help commands."""
    print("🔍 Testing CLI help commands...")
    
    # Test main help
    returncode, stdout, stderr = run_cli_command(['--help'])
    if returncode == 0:
        print("✅ Main help command works")
        assert "Kafka Ops Agent CLI" in stdout
    else:
        print(f"❌ Main help failed: {stderr}")
        return False
    
    # Test topic help
    returncode, stdout, stderr = run_cli_command(['topic', '--help'])
    if returncode == 0:
        print("✅ Topic help command works")
        assert "Topic management commands" in stdout
    else:
        print(f"❌ Topic help failed: {stderr}")
        return False
    
    # Test cleanup help
    returncode, stdout, stderr = run_cli_command(['cleanup', '--help'])
    if returncode == 0:
        print("✅ Cleanup help command works")
        assert "Cleanup operations commands" in stdout
    else:
        print(f"❌ Cleanup help failed: {stderr}")
        return False
    
    return True


def test_cli_configuration():
    """Test CLI configuration commands."""
    print("\n🔧 Testing CLI configuration...")
    
    # Create temporary config directory
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / 'config.json'
        
        # Test configure command
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(config_file),
            'configure',
            '--cluster-id', 'test-cluster',
            '--bootstrap-servers', 'localhost:9092',
            '--user-id', 'test-user'
        ])
        
        if returncode == 0:
            print("✅ Configure command works")
            assert "Configuration saved" in stdout
            
            # Verify config file was created
            if config_file.exists():
                print("✅ Config file created successfully")
                
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    assert config['cluster_id'] == 'test-cluster'
                    assert config['bootstrap_servers'] == ['localhost:9092']
                    assert config['user_id'] == 'test-user'
                    print("✅ Config file contains correct values")
            else:
                print("❌ Config file was not created")
                return False
        else:
            print(f"❌ Configure command failed: {stderr}")
            return False
        
        # Test config display command
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(config_file),
            'config'
        ])
        
        if returncode == 0:
            print("✅ Config display command works")
            assert 'test-cluster' in stdout
        else:
            print(f"❌ Config display failed: {stderr}")
            return False
    
    return True


def test_topic_commands_without_kafka():
    """Test topic commands that should fail gracefully without Kafka."""
    print("\n📋 Testing topic commands (expected to fail without Kafka)...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / 'config.json'
        
        # Create config
        config = {
            'cluster_id': 'test-cluster',
            'bootstrap_servers': ['localhost:9092'],
            'user_id': 'test-user'
        }
        
        with open(config_file, 'w') as f:
            json.dump(config, f)
        
        # Test list topics (should fail gracefully)
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(config_file),
            'topic', 'list'
        ])
        
        # We expect this to fail since Kafka is not running
        if returncode != 0:
            print("✅ Topic list fails gracefully without Kafka")
            assert "Failed to list topics" in stderr or "Failed to list topics" in stdout
        else:
            print("⚠️  Topic list succeeded unexpectedly (Kafka might be running)")
        
        # Test topic creation (should fail gracefully)
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(config_file),
            'topic', 'create', 'test-topic',
            '--partitions', '3'
        ])
        
        if returncode != 0:
            print("✅ Topic create fails gracefully without Kafka")
        else:
            print("⚠️  Topic create succeeded unexpectedly")
    
    return True


def test_cleanup_commands():
    """Test cleanup commands."""
    print("\n🧹 Testing cleanup commands...")
    
    # Test cleanup help
    returncode, stdout, stderr = run_cli_command(['cleanup', '--help'])
    if returncode == 0:
        print("✅ Cleanup help works")
        assert "topic" in stdout
        assert "cluster" in stdout
        assert "metadata" in stdout
    else:
        print(f"❌ Cleanup help failed: {stderr}")
        return False
    
    # Test cleanup stats (should work without Kafka)
    returncode, stdout, stderr = run_cli_command(['cleanup', 'stats'])
    if returncode == 0:
        print("✅ Cleanup stats command works")
        assert "Scheduler Statistics" in stdout
    else:
        print(f"❌ Cleanup stats failed: {stderr}")
        return False
    
    return True


def test_bulk_topic_operations():
    """Test bulk topic operations."""
    print("\n📦 Testing bulk topic operations...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config_file = Path(temp_dir) / 'config.json'
        topics_file = Path(temp_dir) / 'topics.json'
        
        # Create config
        config = {
            'cluster_id': 'test-cluster',
            'bootstrap_servers': ['localhost:9092'],
            'user_id': 'test-user'
        }
        
        with open(config_file, 'w') as f:
            json.dump(config, f)
        
        # Create topics configuration
        topics_config = [
            {
                "name": "bulk-topic-1",
                "partitions": 3,
                "replication_factor": 1,
                "retention_ms": 604800000
            },
            {
                "name": "bulk-topic-2",
                "partitions": 6,
                "replication_factor": 1,
                "retention_ms": 2592000000
            }
        ]
        
        with open(topics_file, 'w') as f:
            json.dump(topics_config, f)
        
        # Test bulk create (should fail gracefully without Kafka)
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(config_file),
            'topic', 'bulk-create',
            '--file', str(topics_file)
        ])
        
        if returncode != 0:
            print("✅ Bulk create fails gracefully without Kafka")
        else:
            print("⚠️  Bulk create succeeded unexpectedly")
        
        # Test with invalid JSON file
        invalid_json_file = Path(temp_dir) / 'invalid.json'
        with open(invalid_json_file, 'w') as f:
            f.write("invalid json")
        
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(config_file),
            'topic', 'bulk-create',
            '--file', str(invalid_json_file)
        ])
        
        if returncode != 0:
            print("✅ Bulk create handles invalid JSON correctly")
            assert "Invalid JSON file" in stderr or "Invalid JSON file" in stdout
        else:
            print("❌ Bulk create should have failed with invalid JSON")
            return False
    
    return True


def test_version_command():
    """Test version command."""
    print("\n📋 Testing version command...")
    
    returncode, stdout, stderr = run_cli_command(['version'])
    if returncode == 0:
        print("✅ Version command works")
        assert "Kafka Ops Agent CLI" in stdout
        assert "Version:" in stdout
    else:
        print(f"❌ Version command failed: {stderr}")
        return False
    
    return True


def test_error_handling():
    """Test error handling scenarios."""
    print("\n⚠️  Testing error handling...")
    
    # Test with missing cluster configuration
    with tempfile.TemporaryDirectory() as temp_dir:
        empty_config_file = Path(temp_dir) / 'empty_config.json'
        
        with open(empty_config_file, 'w') as f:
            json.dump({}, f)
        
        returncode, stdout, stderr = run_cli_command([
            '--config-file', str(empty_config_file),
            'topic', 'list'
        ])
        
        if returncode != 0:
            print("✅ Handles missing cluster configuration correctly")
            assert "Cluster ID not configured" in stderr or "Cluster ID not configured" in stdout
        else:
            print("❌ Should have failed with missing cluster configuration")
            return False
    
    # Test with invalid command
    returncode, stdout, stderr = run_cli_command(['invalid-command'])
    if returncode != 0:
        print("✅ Handles invalid commands correctly")
    else:
        print("❌ Should have failed with invalid command")
        return False
    
    return True


def main():
    """Run all CLI tests."""
    print("🚀 Starting CLI Command Tests")
    print("=" * 50)
    
    tests = [
        ("CLI Help Commands", test_cli_help),
        ("CLI Configuration", test_cli_configuration),
        ("Topic Commands", test_topic_commands_without_kafka),
        ("Cleanup Commands", test_cleanup_commands),
        ("Bulk Operations", test_bulk_topic_operations),
        ("Version Command", test_version_command),
        ("Error Handling", test_error_handling),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*50}")
            print(f"Running test: {test_name}")
            print(f"{'='*50}")
            
            if test_func():
                print(f"✅ {test_name} PASSED")
                passed += 1
            else:
                print(f"❌ {test_name} FAILED")
                failed += 1
                
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {e}")
            failed += 1
    
    print(f"\n{'='*50}")
    print(f"Test Results: {passed} passed, {failed} failed")
    print(f"{'='*50}")
    
    if failed == 0:
        print("🎉 All CLI tests passed!")
        return 0
    else:
        print(f"💥 {failed} tests failed!")
        return 1


if __name__ == '__main__':
    sys.exit(main())