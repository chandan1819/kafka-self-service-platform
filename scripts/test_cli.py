#!/usr/bin/env python3
"""Script to test the CLI interface."""

import sys
import subprocess
import tempfile
import json
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_cli_command(cmd_args, input_data=None):
    """Run a CLI command and return the result."""
    
    # Use python -m to run the CLI
    full_cmd = [sys.executable, '-m', 'kafka_ops_agent.cli.main'] + cmd_args
    
    try:
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            input=input_data,
            cwd=project_root
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return -1, "", str(e)


def test_cli_help():
    """Test CLI help commands."""
    print("📋 Testing CLI help commands...")
    
    # Test main help
    returncode, stdout, stderr = run_cli_command(['--help'])
    if returncode == 0 and 'Kafka Ops Agent CLI' in stdout:
        print("✅ Main help command works")
    else:
        print(f"❌ Main help failed: {stderr}")
        return False
    
    # Test topic help
    returncode, stdout, stderr = run_cli_command(['topic', '--help'])
    if returncode == 0 and 'Topic management commands' in stdout:
        print("✅ Topic help command works")
    else:
        print(f"❌ Topic help failed: {stderr}")
        return False
    
    # Test cluster help
    returncode, stdout, stderr = run_cli_command(['cluster', '--help'])
    if returncode == 0 and 'Cluster management commands' in stdout:
        print("✅ Cluster help command works")
    else:
        print(f"❌ Cluster help failed: {stderr}")
        return False
    
    return True


def test_cli_version():
    """Test version command."""
    print("📋 Testing version command...")
    
    returncode, stdout, stderr = run_cli_command(['version'])
    if returncode == 0 and 'Kafka Ops Agent CLI' in stdout and 'Version: 0.1.0' in stdout:
        print("✅ Version command works")
        return True
    else:
        print(f"❌ Version command failed: {stderr}")
        return False


def test_cli_configuration():
    """Test configuration commands."""
    print("📋 Testing configuration commands...")
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config_file = f.name
    
    try:
        # Test configure command
        returncode, stdout, stderr = run_cli_command([
            '--config-file', config_file,
            'configure',
            '--cluster-id', 'test-cluster',
            '--bootstrap-servers', 'localhost:9092,localhost:9093',
            '--zookeeper-connect', 'localhost:2181',
            '--user-id', 'test-user'
        ])
        
        if returncode == 0 and 'Configuration saved' in stdout:
            print("✅ Configure command works")
        else:
            print(f"❌ Configure command failed: {stderr}")
            return False
        
        # Test config show command
        returncode, stdout, stderr = run_cli_command([
            '--config-file', config_file,
            'config'
        ])
        
        if returncode == 0 and 'test-cluster' in stdout:
            print("✅ Config show command works")
        else:
            print(f"❌ Config show command failed: {stderr}")
            return False
        
        return True
        
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)


def test_topic_commands_without_cluster():
    """Test topic commands without a running cluster."""
    print("📋 Testing topic commands (without cluster)...")
    
    # Create temporary config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {
            'cluster_id': 'test-cluster',
            'bootstrap_servers': ['localhost:9092'],
            'zookeeper_connect': 'localhost:2181',
            'user_id': 'test-user'
        }
        json.dump(config, f)
        config_file = f.name
    
    try:
        # Test topic list (should fail gracefully without cluster)
        returncode, stdout, stderr = run_cli_command([
            '--config-file', config_file,
            'topic', 'list'
        ])
        
        # Should fail but with a meaningful error message
        if returncode != 0:
            print("✅ Topic list fails gracefully without cluster")
        else:
            print("⚠️  Topic list succeeded unexpectedly (cluster might be running)")
        
        # Test topic create help
        returncode, stdout, stderr = run_cli_command([
            'topic', 'create', '--help'
        ])
        
        if returncode == 0 and 'Create a new topic' in stdout:
            print("✅ Topic create help works")
        else:
            print(f"❌ Topic create help failed: {stderr}")
            return False
        
        return True
        
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)


def test_cluster_commands_without_cluster():
    """Test cluster commands without a running cluster."""
    print("📋 Testing cluster commands (without cluster)...")
    
    # Create temporary config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {
            'cluster_id': 'test-cluster',
            'bootstrap_servers': ['localhost:9092'],
            'zookeeper_connect': 'localhost:2181',
            'user_id': 'test-user'
        }
        json.dump(config, f)
        config_file = f.name
    
    try:
        # Test cluster info (should fail gracefully without cluster)
        returncode, stdout, stderr = run_cli_command([
            '--config-file', config_file,
            'cluster', 'info'
        ])
        
        # Should fail but with a meaningful error message
        if returncode != 0:
            print("✅ Cluster info fails gracefully without cluster")
        else:
            print("⚠️  Cluster info succeeded unexpectedly (cluster might be running)")
        
        # Test cluster help
        returncode, stdout, stderr = run_cli_command([
            'cluster', '--help'
        ])
        
        if returncode == 0 and 'Cluster management commands' in stdout:
            print("✅ Cluster help works")
        else:
            print(f"❌ Cluster help failed: {stderr}")
            return False
        
        return True
        
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)


def test_bulk_create_format():
    """Test bulk create JSON format validation."""
    print("📋 Testing bulk create format validation...")
    
    # Create temporary config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config = {
            'cluster_id': 'test-cluster',
            'bootstrap_servers': ['localhost:9092'],
            'user_id': 'test-user'
        }
        json.dump(config, f)
        config_file = f.name
    
    # Create sample topics file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        topics = [
            {
                'name': 'test-topic-1',
                'partitions': 3,
                'replication_factor': 1
            },
            {
                'name': 'test-topic-2',
                'partitions': 6,
                'replication_factor': 1,
                'retention_ms': 3600000
            }
        ]
        json.dump(topics, f)
        topics_file = f.name
    
    try:
        # Test bulk create help
        returncode, stdout, stderr = run_cli_command([
            'topic', 'bulk-create', '--help'
        ])
        
        if returncode == 0 and 'JSON file with topic configurations' in stdout:
            print("✅ Bulk create help works")
        else:
            print(f"❌ Bulk create help failed: {stderr}")
            return False
        
        # Test bulk create with file (will fail without cluster but should validate format)
        returncode, stdout, stderr = run_cli_command([
            '--config-file', config_file,
            'topic', 'bulk-create',
            '--file', topics_file
        ])
        
        # Should fail due to no cluster, but not due to format issues
        if returncode != 0 and 'Invalid JSON' not in stderr:
            print("✅ Bulk create validates JSON format correctly")
        else:
            print(f"⚠️  Bulk create format validation unclear: {stderr}")
        
        return True
        
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)
        Path(topics_file).unlink(missing_ok=True)


def main():
    """Run all CLI tests."""
    print("🧪 Testing Kafka Ops Agent CLI")
    print("=" * 50)
    
    tests = [
        ("CLI Help Commands", test_cli_help),
        ("Version Command", test_cli_version),
        ("Configuration", test_cli_configuration),
        ("Topic Commands", test_topic_commands_without_cluster),
        ("Cluster Commands", test_cluster_commands_without_cluster),
        ("Bulk Create Format", test_bulk_create_format)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
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
        print("\n🎉 All CLI tests passed!")
        print("\n💡 To test with a running cluster:")
        print("   1. Start a cluster: python scripts/create_local_cluster.py")
        print("   2. Configure CLI: kafka-ops-agent configure --cluster-id local-kafka-dev --bootstrap-servers localhost:9092")
        print("   3. Test commands: kafka-ops-agent topic list")
        return True
    else:
        print("\n⚠️  Some CLI tests failed")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)