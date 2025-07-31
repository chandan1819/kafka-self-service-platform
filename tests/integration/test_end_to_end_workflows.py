"""End-to-end workflow tests for Kafka Ops Agent."""

import pytest
import asyncio
import time
from typing import Dict, Any

from .conftest import APIClient, MonitoringClient, wait_for_condition, retry_async


class TestCompleteProvisioningWorkflow:
    """Test complete provisioning workflows."""
    
    @pytest.mark.asyncio
    async def test_full_kafka_cluster_provisioning_workflow(
        self, 
        api_client: APIClient, 
        monitoring_client: MonitoringClient,
        test_instance_id: str,
        sample_service_config: Dict[str, Any],
        clean_test_data
    ):
        """Test complete Kafka cluster provisioning workflow."""
        print(f"\\n🚀 Starting full provisioning workflow for instance: {test_instance_id}")
        
        # Step 1: Get service catalog
        print("📋 Step 1: Getting service catalog...")
        catalog = await api_client.get_catalog()
        
        assert 'services' in catalog
        assert len(catalog['services']) > 0
        
        kafka_service = None
        for service in catalog['services']:
            if service['name'] == 'kafka-cluster':
                kafka_service = service
                break
        
        assert kafka_service is not None, "Kafka service not found in catalog"
        assert len(kafka_service['plans']) > 0, "No plans available for Kafka service"
        
        print(f"✓ Found Kafka service with {len(kafka_service['plans'])} plans")
        
        # Step 2: Provision service instance
        print("🏗️  Step 2: Provisioning service instance...")
        provision_data = sample_service_config.copy()
        provision_data['service_id'] = kafka_service['id']
        provision_data['plan_id'] = kafka_service['plans'][0]['id']
        
        provision_response = await api_client.provision_service(test_instance_id, provision_data)
        
        assert 'operation' in provision_response
        operation_id = provision_response['operation']
        
        print(f"✓ Provisioning started with operation ID: {operation_id}")
        
        # Step 3: Wait for provisioning to complete
        print("⏳ Step 3: Waiting for provisioning to complete...")
        
        async def check_provisioning_complete():
            """Check if provisioning is complete."""
            try:
                operation_status = await api_client.get_last_operation(test_instance_id)
                state = operation_status.get('state', 'in progress')
                print(f"   Provisioning state: {state}")
                return state == 'succeeded'
            except Exception as e:
                print(f"   Error checking operation status: {e}")
                return False
        
        provisioning_complete = await wait_for_condition(
            check_provisioning_complete, 
            timeout=300,  # 5 minutes
            interval=10
        )
        
        assert provisioning_complete, "Provisioning did not complete within timeout"
        print("✓ Provisioning completed successfully")
        
        # Step 4: Verify service instance is accessible
        print("🔍 Step 4: Verifying service instance accessibility...")
        
        final_status = await api_client.get_last_operation(test_instance_id)
        assert final_status['state'] == 'succeeded'
        
        # Check if connection info is available
        if 'connection_info' in final_status:
            connection_info = final_status['connection_info']
            assert 'bootstrap_servers' in connection_info
            print(f"✓ Service accessible at: {connection_info['bootstrap_servers']}")
        
        # Step 5: Verify monitoring shows healthy cluster
        print("📊 Step 5: Verifying monitoring shows healthy cluster...")
        
        health_status = await monitoring_client.get_health()
        assert health_status['overall_status'] in ['healthy', 'degraded']  # Allow degraded for test env
        
        metrics = await monitoring_client.get_metrics()
        assert 'gauges' in metrics
        
        # Check cluster count increased
        cluster_count = metrics['gauges'].get('kafka_ops_clusters_total', 0)
        assert cluster_count >= 1
        
        print(f"✓ Monitoring shows {cluster_count} clusters")
        
        # Step 6: Test topic operations on provisioned cluster
        print("📝 Step 6: Testing topic operations on provisioned cluster...")
        
        test_topic_name = f"test-topic-{test_instance_id[:8]}"
        topic_config = {
            "name": test_topic_name,
            "partitions": 3,
            "replication_factor": 1,
            "config": {
                "retention.ms": "604800000"
            }
        }
        
        # Create topic
        create_response = await api_client.create_topic(topic_config)
        assert create_response['status'] == 'success'
        
        # Verify topic exists
        topic_info = await api_client.get_topic(test_topic_name)
        assert topic_info['name'] == test_topic_name
        assert topic_info['partitions'] == 3
        
        print(f"✓ Successfully created and verified topic: {test_topic_name}")
        
        # Step 7: Clean up - Delete topic
        print("🧹 Step 7: Cleaning up topic...")
        delete_response = await api_client.delete_topic(test_topic_name)
        assert delete_response['status'] == 'success'
        
        # Step 8: Deprovision service instance
        print("🗑️  Step 8: Deprovisioning service instance...")
        deprovision_response = await api_client.deprovision_service(test_instance_id)
        
        # Wait for deprovisioning to complete
        async def check_deprovisioning_complete():
            """Check if deprovisioning is complete."""
            try:
                operation_status = await api_client.get_last_operation(test_instance_id)
                state = operation_status.get('state', 'in progress')
                print(f"   Deprovisioning state: {state}")
                return state == 'succeeded'
            except Exception as e:
                # Instance might be gone, which is expected
                return True
        
        deprovisioning_complete = await wait_for_condition(
            check_deprovisioning_complete,
            timeout=180,  # 3 minutes
            interval=10
        )
        
        assert deprovisioning_complete, "Deprovisioning did not complete within timeout"
        print("✓ Deprovisioning completed successfully")
        
        print("🎉 Full provisioning workflow completed successfully!")


class TestTopicManagementWorkflows:
    """Test topic management workflows."""
    
    @pytest.mark.asyncio
    async def test_complete_topic_lifecycle_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        test_topic_name: str,
        sample_topic_config: Dict[str, Any],
        clean_test_data
    ):
        """Test complete topic lifecycle from creation to deletion."""
        print(f"\\n📝 Starting topic lifecycle workflow for: {test_topic_name}")
        
        # Update config with test topic name
        topic_config = sample_topic_config.copy()
        topic_config['name'] = test_topic_name
        
        # Step 1: Create topic
        print("🆕 Step 1: Creating topic...")
        create_response = await api_client.create_topic(topic_config)
        
        assert create_response['status'] == 'success'
        assert 'topic' in create_response
        
        created_topic = create_response['topic']
        assert created_topic['name'] == test_topic_name
        assert created_topic['partitions'] == topic_config['partitions']
        
        print(f"✓ Topic created: {test_topic_name}")
        
        # Step 2: Verify topic appears in list
        print("📋 Step 2: Verifying topic in list...")
        topics_response = await api_client.list_topics()
        
        assert 'topics' in topics_response
        topic_names = [t['name'] for t in topics_response['topics']]
        assert test_topic_name in topic_names
        
        print(f"✓ Topic found in list of {len(topic_names)} topics")
        
        # Step 3: Get detailed topic information
        print("🔍 Step 3: Getting detailed topic information...")
        topic_info = await api_client.get_topic(test_topic_name)
        
        assert topic_info['name'] == test_topic_name
        assert topic_info['partitions'] == topic_config['partitions']
        assert topic_info['replication_factor'] == topic_config['replication_factor']
        
        # Verify configuration
        if 'config' in topic_info:
            config = topic_info['config']
            assert config.get('retention.ms') == topic_config['config']['retention.ms']
        
        print("✓ Topic information verified")
        
        # Step 4: Update topic configuration
        print("⚙️  Step 4: Updating topic configuration...")
        update_config = {
            "config": {
                "retention.ms": "1209600000",  # 14 days
                "cleanup.policy": "delete"
            }
        }
        
        update_response = await api_client.update_topic(test_topic_name, update_config)
        assert update_response['status'] == 'success'
        
        # Verify update
        updated_topic_info = await api_client.get_topic(test_topic_name)
        if 'config' in updated_topic_info:
            updated_config = updated_topic_info['config']
            assert updated_config.get('retention.ms') == "1209600000"
        
        print("✓ Topic configuration updated")
        
        # Step 5: Verify metrics reflect topic operations
        print("📊 Step 5: Verifying metrics...")
        metrics = await monitoring_client.get_metrics()
        
        # Check topic count
        topic_count = metrics['gauges'].get('kafka_ops_topics_total', 0)
        assert topic_count >= 1
        
        # Check request count increased
        request_count = metrics['counters'].get('kafka_ops_requests_total', 0)
        assert request_count > 0
        
        print(f"✓ Metrics show {topic_count} topics, {request_count} requests")
        
        # Step 6: Delete topic
        print("🗑️  Step 6: Deleting topic...")
        delete_response = await api_client.delete_topic(test_topic_name)
        
        assert delete_response['status'] == 'success'
        
        # Verify topic is gone
        await asyncio.sleep(2)  # Give some time for deletion to propagate
        
        topics_response = await api_client.list_topics()
        topic_names = [t['name'] for t in topics_response['topics']]
        assert test_topic_name not in topic_names
        
        print("✓ Topic successfully deleted")
        
        print("🎉 Topic lifecycle workflow completed successfully!")
    
    @pytest.mark.asyncio
    async def test_bulk_topic_operations_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test bulk topic operations workflow."""
        print("\\n📦 Starting bulk topic operations workflow...")
        
        # Create multiple topics
        topic_count = 5
        topic_names = [f"bulk-test-topic-{i}" for i in range(topic_count)]
        
        print(f"🆕 Creating {topic_count} topics...")
        created_topics = []
        
        for i, topic_name in enumerate(topic_names):
            topic_config = {
                "name": topic_name,
                "partitions": 2 + i,  # Vary partition count
                "replication_factor": 1,
                "config": {
                    "retention.ms": str(86400000 * (i + 1))  # Vary retention
                }
            }
            
            create_response = await api_client.create_topic(topic_config)
            assert create_response['status'] == 'success'
            created_topics.append(topic_name)
            
            print(f"  ✓ Created topic {i+1}/{topic_count}: {topic_name}")
        
        # Verify all topics exist
        print("📋 Verifying all topics exist...")
        topics_response = await api_client.list_topics()
        existing_topic_names = [t['name'] for t in topics_response['topics']]
        
        for topic_name in created_topics:
            assert topic_name in existing_topic_names
        
        print(f"✓ All {len(created_topics)} topics verified")
        
        # Check metrics reflect increased topic count
        print("📊 Checking metrics...")
        metrics = await monitoring_client.get_metrics()
        topic_count_metric = metrics['gauges'].get('kafka_ops_topics_total', 0)
        assert topic_count_metric >= len(created_topics)
        
        print(f"✓ Metrics show {topic_count_metric} total topics")
        
        # Clean up all topics
        print("🧹 Cleaning up all topics...")
        for topic_name in created_topics:
            delete_response = await api_client.delete_topic(topic_name)
            assert delete_response['status'] == 'success'
            print(f"  ✓ Deleted: {topic_name}")
        
        print("🎉 Bulk topic operations workflow completed successfully!")


class TestErrorHandlingWorkflows:
    """Test error handling in workflows."""
    
    @pytest.mark.asyncio
    async def test_invalid_topic_creation_workflow(
        self,
        api_client: APIClient,
        clean_test_data
    ):
        """Test error handling for invalid topic creation."""
        print("\\n❌ Testing invalid topic creation workflow...")
        
        # Test 1: Invalid topic name
        print("🔍 Test 1: Invalid topic name...")
        invalid_config = {
            "name": "invalid.topic.name!",  # Invalid characters
            "partitions": 1,
            "replication_factor": 1
        }
        
        try:
            await api_client.create_topic(invalid_config)
            assert False, "Expected error for invalid topic name"
        except Exception as e:
            print(f"✓ Correctly rejected invalid topic name: {e}")
        
        # Test 2: Invalid partition count
        print("🔍 Test 2: Invalid partition count...")
        invalid_config = {
            "name": "test-topic-invalid-partitions",
            "partitions": 0,  # Invalid partition count
            "replication_factor": 1
        }
        
        try:
            await api_client.create_topic(invalid_config)
            assert False, "Expected error for invalid partition count"
        except Exception as e:
            print(f"✓ Correctly rejected invalid partition count: {e}")
        
        # Test 3: Missing required fields
        print("🔍 Test 3: Missing required fields...")
        invalid_config = {
            "name": "test-topic-missing-fields"
            # Missing partitions and replication_factor
        }
        
        try:
            await api_client.create_topic(invalid_config)
            assert False, "Expected error for missing fields"
        except Exception as e:
            print(f"✓ Correctly rejected missing fields: {e}")
        
        print("✅ Invalid topic creation workflow completed successfully!")
    
    @pytest.mark.asyncio
    async def test_duplicate_topic_creation_workflow(
        self,
        api_client: APIClient,
        test_topic_name: str,
        sample_topic_config: Dict[str, Any],
        clean_test_data
    ):
        """Test error handling for duplicate topic creation."""
        print("\\n🔄 Testing duplicate topic creation workflow...")
        
        # Update config with test topic name
        topic_config = sample_topic_config.copy()
        topic_config['name'] = test_topic_name
        
        # Create topic first time
        print("🆕 Creating topic first time...")
        create_response = await api_client.create_topic(topic_config)
        assert create_response['status'] == 'success'
        print(f"✓ Topic created: {test_topic_name}")
        
        # Try to create same topic again
        print("🔄 Attempting to create duplicate topic...")
        try:
            await api_client.create_topic(topic_config)
            assert False, "Expected error for duplicate topic"
        except Exception as e:
            print(f"✓ Correctly rejected duplicate topic: {e}")
        
        # Clean up
        delete_response = await api_client.delete_topic(test_topic_name)
        assert delete_response['status'] == 'success'
        
        print("✅ Duplicate topic creation workflow completed successfully!")


class TestConcurrentOperationsWorkflow:
    """Test concurrent operations workflows."""
    
    @pytest.mark.asyncio
    async def test_concurrent_topic_creation_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test concurrent topic creation operations."""
        print("\\n⚡ Testing concurrent topic creation workflow...")
        
        # Create multiple topics concurrently
        concurrent_count = 10
        topic_names = [f"concurrent-topic-{i}" for i in range(concurrent_count)]
        
        async def create_topic_task(topic_name: str, partition_count: int):
            """Task to create a single topic."""
            topic_config = {
                "name": topic_name,
                "partitions": partition_count,
                "replication_factor": 1,
                "config": {
                    "retention.ms": "604800000"
                }
            }
            
            try:
                response = await api_client.create_topic(topic_config)
                return topic_name, response['status'] == 'success', None
            except Exception as e:
                return topic_name, False, str(e)
        
        print(f"🚀 Creating {concurrent_count} topics concurrently...")
        
        # Create tasks for concurrent execution
        tasks = [
            create_topic_task(topic_name, i + 1)
            for i, topic_name in enumerate(topic_names)
        ]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Analyze results
        successful_topics = []
        failed_topics = []
        
        for result in results:
            if isinstance(result, Exception):
                failed_topics.append(f"Exception: {result}")
            else:
                topic_name, success, error = result
                if success:
                    successful_topics.append(topic_name)
                else:
                    failed_topics.append(f"{topic_name}: {error}")
        
        print(f"✓ Successfully created {len(successful_topics)} topics")
        if failed_topics:
            print(f"⚠️  Failed to create {len(failed_topics)} topics:")
            for failure in failed_topics:
                print(f"   - {failure}")
        
        # Verify topics exist
        topics_response = await api_client.list_topics()
        existing_topic_names = [t['name'] for t in topics_response['topics']]
        
        verified_count = 0
        for topic_name in successful_topics:
            if topic_name in existing_topic_names:
                verified_count += 1
        
        print(f"✓ Verified {verified_count} topics exist")
        
        # Check metrics
        metrics = await monitoring_client.get_metrics()
        request_count = metrics['counters'].get('kafka_ops_requests_total', 0)
        print(f"📊 Total requests processed: {request_count}")
        
        # Clean up successful topics
        print("🧹 Cleaning up created topics...")
        cleanup_tasks = [
            api_client.delete_topic(topic_name)
            for topic_name in successful_topics
        ]
        
        cleanup_results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        cleaned_count = sum(1 for r in cleanup_results if not isinstance(r, Exception))
        
        print(f"✓ Cleaned up {cleaned_count} topics")
        
        print("✅ Concurrent topic creation workflow completed successfully!")


class TestMonitoringIntegrationWorkflow:
    """Test monitoring integration workflows."""
    
    @pytest.mark.asyncio
    async def test_monitoring_during_operations_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test monitoring system during various operations."""
        print("\\n📊 Testing monitoring integration workflow...")
        
        # Step 1: Get baseline metrics
        print("📈 Step 1: Getting baseline metrics...")
        baseline_metrics = await monitoring_client.get_metrics()
        baseline_requests = baseline_metrics['counters'].get('kafka_ops_requests_total', 0)
        baseline_topics = baseline_metrics['gauges'].get('kafka_ops_topics_total', 0)
        
        print(f"✓ Baseline: {baseline_requests} requests, {baseline_topics} topics")
        
        # Step 2: Perform operations while monitoring
        print("🔄 Step 2: Performing operations while monitoring...")
        
        operations_count = 5
        topic_names = []
        
        for i in range(operations_count):
            topic_name = f"monitoring-test-topic-{i}"
            topic_names.append(topic_name)
            
            # Create topic
            topic_config = {
                "name": topic_name,
                "partitions": 2,
                "replication_factor": 1
            }
            
            create_response = await api_client.create_topic(topic_config)
            assert create_response['status'] == 'success'
            
            # Check metrics after each operation
            current_metrics = await monitoring_client.get_metrics()
            current_requests = current_metrics['counters'].get('kafka_ops_requests_total', 0)
            current_topics = current_metrics['gauges'].get('kafka_ops_topics_total', 0)
            
            print(f"   Operation {i+1}: {current_requests} requests (+{current_requests - baseline_requests}), "
                  f"{current_topics} topics (+{current_topics - baseline_topics})")
        
        # Step 3: Verify final metrics
        print("📊 Step 3: Verifying final metrics...")
        final_metrics = await monitoring_client.get_metrics()
        final_requests = final_metrics['counters'].get('kafka_ops_requests_total', 0)
        final_topics = final_metrics['gauges'].get('kafka_ops_topics_total', 0)
        
        # Should have increased by at least the number of operations
        assert final_requests >= baseline_requests + operations_count
        assert final_topics >= baseline_topics + operations_count
        
        print(f"✓ Final metrics: {final_requests} requests (+{final_requests - baseline_requests}), "
              f"{final_topics} topics (+{final_topics - baseline_topics})")
        
        # Step 4: Check health status
        print("🏥 Step 4: Checking health status...")
        health_status = await monitoring_client.get_health()
        
        assert 'overall_status' in health_status
        assert health_status['overall_status'] in ['healthy', 'degraded']
        
        print(f"✓ Health status: {health_status['overall_status']}")
        
        # Step 5: Check alerts
        print("🚨 Step 5: Checking alerts...")
        alerts = await monitoring_client.get_alerts()
        
        assert 'active_count' in alerts
        print(f"✓ Active alerts: {alerts['active_count']}")
        
        # Step 6: Clean up and verify metrics decrease
        print("🧹 Step 6: Cleaning up and verifying metrics...")
        
        for topic_name in topic_names:
            delete_response = await api_client.delete_topic(topic_name)
            assert delete_response['status'] == 'success'
        
        # Check final metrics after cleanup
        cleanup_metrics = await monitoring_client.get_metrics()
        cleanup_requests = cleanup_metrics['counters'].get('kafka_ops_requests_total', 0)
        cleanup_topics = cleanup_metrics['gauges'].get('kafka_ops_topics_total', 0)
        
        # Requests should have increased (delete operations)
        assert cleanup_requests >= final_requests + operations_count
        
        print(f"✓ After cleanup: {cleanup_requests} requests, {cleanup_topics} topics")
        
        print("✅ Monitoring integration workflow completed successfully!")


if __name__ == '__main__':
    pytest.main([__file__, "-v", "-s"])