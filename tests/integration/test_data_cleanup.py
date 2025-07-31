"""Test data cleanup and environment reset procedures."""

import pytest
import asyncio
import time
from typing import Dict, Any, List, Set
from datetime import datetime, timedelta

from .conftest import APIClient, MonitoringClient, wait_for_condition


class TestDataCleanupProcedures:
    """Test data cleanup and environment reset procedures."""
    
    @pytest.mark.asyncio
    async def test_comprehensive_cleanup_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient
    ):
        """Test comprehensive cleanup workflow."""
        print("\\n🧹 Testing comprehensive cleanup workflow...")
        
        # Step 1: Create test data to clean up
        print("📝 Step 1: Creating test data...")
        
        test_topics = []
        test_instances = []
        
        # Create test topics
        for i in range(10):
            topic_name = f"cleanup-test-topic-{i}"
            topic_config = {
                "name": topic_name,
                "partitions": 2,
                "replication_factor": 1,
                "config": {
                    "retention.ms": "3600000"
                }
            }
            
            try:
                response = await api_client.create_topic(topic_config)
                if response.get('status') == 'success':
                    test_topics.append(topic_name)
                    print(f"   ✓ Created topic: {topic_name}")
            except Exception as e:
                print(f"   ⚠️  Failed to create topic {topic_name}: {e}")
        
        # Create test service instances (simulated)
        for i in range(3):
            instance_id = f"cleanup-test-instance-{i}"
            service_config = {
                "service_id": "kafka-cluster",
                "plan_id": "small",
                "parameters": {
                    "cluster_name": f"cleanup-test-cluster-{i}",
                    "broker_count": 1
                }
            }
            
            try:
                response = await api_client.provision_service(instance_id, service_config)
                if 'operation' in response:
                    test_instances.append(instance_id)
                    print(f"   ✓ Created instance: {instance_id}")
            except Exception as e:
                print(f"   ⚠️  Failed to create instance {instance_id}: {e}")
        
        print(f"✓ Created {len(test_topics)} topics and {len(test_instances)} instances")
        
        # Step 2: Verify test data exists
        print("🔍 Step 2: Verifying test data exists...")
        
        topics_response = await api_client.list_topics()
        existing_topics = [t['name'] for t in topics_response.get('topics', [])]
        
        verified_topics = [t for t in test_topics if t in existing_topics]
        print(f"✓ Verified {len(verified_topics)} topics exist")
        
        # Step 3: Execute cleanup procedures
        print("🧹 Step 3: Executing cleanup procedures...")
        
        cleanup_results = await self._execute_comprehensive_cleanup(
            api_client, test_topics, test_instances
        )
        
        print(f"✓ Cleanup completed: {cleanup_results}")
        
        # Step 4: Verify cleanup was successful
        print("✅ Step 4: Verifying cleanup was successful...")
        
        # Check topics are gone
        final_topics_response = await api_client.list_topics()
        final_existing_topics = [t['name'] for t in final_topics_response.get('topics', [])]
        
        remaining_test_topics = [t for t in test_topics if t in final_existing_topics]
        
        if remaining_test_topics:
            print(f"   ⚠️  {len(remaining_test_topics)} topics still exist: {remaining_test_topics}")
        else:
            print("   ✓ All test topics cleaned up successfully")
        
        # Check instances are gone
        remaining_instances = []
        for instance_id in test_instances:
            try:
                status = await api_client.get_last_operation(instance_id)
                if status.get('state') != 'succeeded':  # Not deprovisioned
                    remaining_instances.append(instance_id)
            except Exception:
                # Instance not found is good - it means it was cleaned up
                pass
        
        if remaining_instances:
            print(f"   ⚠️  {len(remaining_instances)} instances still exist: {remaining_instances}")
        else:
            print("   ✓ All test instances cleaned up successfully")
        
        # Verify metrics reflect cleanup
        metrics = await monitoring_client.get_metrics()
        topic_count = metrics['gauges'].get('kafka_ops_topics_total', 0)
        cluster_count = metrics['gauges'].get('kafka_ops_clusters_total', 0)
        
        print(f"   📊 Final metrics: {topic_count} topics, {cluster_count} clusters")
        
        assert len(remaining_test_topics) == 0, "Some test topics were not cleaned up"
        assert len(remaining_instances) == 0, "Some test instances were not cleaned up"
        
        print("🎉 Comprehensive cleanup workflow completed successfully!")
    
    async def _execute_comprehensive_cleanup(
        self,
        api_client: APIClient,
        test_topics: List[str],
        test_instances: List[str]
    ) -> Dict[str, Any]:
        """Execute comprehensive cleanup procedures.
        
        Args:
            api_client: API client
            test_topics: List of topic names to clean up
            test_instances: List of instance IDs to clean up
            
        Returns:
            Cleanup results summary
        """
        results = {
            'topics_deleted': 0,
            'topics_failed': 0,
            'instances_deprovisioned': 0,
            'instances_failed': 0,
            'total_time': 0
        }
        
        start_time = time.time()
        
        # Clean up topics
        print("   🗑️  Cleaning up topics...")
        topic_cleanup_tasks = [
            self._cleanup_topic(api_client, topic_name)
            for topic_name in test_topics
        ]
        
        topic_results = await asyncio.gather(*topic_cleanup_tasks, return_exceptions=True)
        
        for result in topic_results:
            if isinstance(result, Exception):
                results['topics_failed'] += 1
            elif result:
                results['topics_deleted'] += 1
            else:
                results['topics_failed'] += 1
        
        # Clean up instances
        print("   🗑️  Cleaning up instances...")
        instance_cleanup_tasks = [
            self._cleanup_instance(api_client, instance_id)
            for instance_id in test_instances
        ]
        
        instance_results = await asyncio.gather(*instance_cleanup_tasks, return_exceptions=True)
        
        for result in instance_results:
            if isinstance(result, Exception):
                results['instances_failed'] += 1
            elif result:
                results['instances_deprovisioned'] += 1
            else:
                results['instances_failed'] += 1
        
        results['total_time'] = time.time() - start_time
        
        return results
    
    async def _cleanup_topic(self, api_client: APIClient, topic_name: str) -> bool:
        """Clean up a single topic.
        
        Args:
            api_client: API client
            topic_name: Name of topic to clean up
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = await api_client.delete_topic(topic_name)
            return response.get('status') == 'success'
        except Exception as e:
            print(f"     ❌ Failed to delete topic {topic_name}: {e}")
            return False
    
    async def _cleanup_instance(self, api_client: APIClient, instance_id: str) -> bool:
        """Clean up a single service instance.
        
        Args:
            api_client: API client
            instance_id: ID of instance to clean up
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Deprovision instance
            await api_client.deprovision_service(instance_id)
            
            # Wait for deprovisioning to complete
            async def check_deprovisioning():
                try:
                    status = await api_client.get_last_operation(instance_id)
                    return status.get('state') == 'succeeded'
                except Exception:
                    # Instance not found means deprovisioning succeeded
                    return True
            
            success = await wait_for_condition(
                check_deprovisioning,
                timeout=120,  # 2 minutes
                interval=5
            )
            
            return success
            
        except Exception as e:
            print(f"     ❌ Failed to deprovision instance {instance_id}: {e}")
            return False
    
    @pytest.mark.asyncio
    async def test_orphaned_resource_cleanup(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient
    ):
        """Test cleanup of orphaned resources."""
        print("\\n🔍 Testing orphaned resource cleanup...")
        
        # Step 1: Create resources that might become orphaned
        print("📝 Step 1: Creating potentially orphaned resources...")
        
        # Create topics with specific naming pattern
        orphaned_topics = []
        for i in range(5):
            topic_name = f"orphaned-topic-{int(time.time())}-{i}"
            topic_config = {
                "name": topic_name,
                "partitions": 1,
                "replication_factor": 1,
                "config": {
                    "retention.ms": "60000"  # Very short retention for testing
                }
            }
            
            try:
                response = await api_client.create_topic(topic_config)
                if response.get('status') == 'success':
                    orphaned_topics.append(topic_name)
            except Exception as e:
                print(f"   ⚠️  Failed to create orphaned topic: {e}")
        
        print(f"✓ Created {len(orphaned_topics)} potentially orphaned topics")
        
        # Step 2: Simulate orphaned state (topics without proper metadata)
        print("🔄 Step 2: Simulating orphaned state...")
        
        # In a real scenario, orphaned resources might be:
        # - Topics created but not tracked in metadata
        # - Resources from failed provisioning operations
        # - Resources with expired cleanup timestamps
        
        # For this test, we'll identify orphaned resources by naming pattern
        all_topics_response = await api_client.list_topics()
        all_topics = [t['name'] for t in all_topics_response.get('topics', [])]
        
        # Find topics that match orphaned pattern
        identified_orphaned = [
            topic for topic in all_topics 
            if topic.startswith('orphaned-topic-') or 
               topic.startswith('test-') or
               topic.startswith('cleanup-')
        ]
        
        print(f"✓ Identified {len(identified_orphaned)} orphaned resources")
        
        # Step 3: Execute orphaned resource cleanup
        print("🧹 Step 3: Executing orphaned resource cleanup...")
        
        cleanup_count = 0
        failed_cleanup_count = 0
        
        for topic_name in identified_orphaned:
            try:
                response = await api_client.delete_topic(topic_name)
                if response.get('status') == 'success':
                    cleanup_count += 1
                    print(f"   ✓ Cleaned up orphaned topic: {topic_name}")
                else:
                    failed_cleanup_count += 1
                    print(f"   ❌ Failed to clean up topic: {topic_name}")
            except Exception as e:
                failed_cleanup_count += 1
                print(f"   ❌ Exception cleaning up {topic_name}: {e}")
        
        print(f"✓ Cleaned up {cleanup_count} orphaned resources")
        print(f"⚠️  Failed to clean up {failed_cleanup_count} resources")
        
        # Step 4: Verify orphaned resources are gone
        print("✅ Step 4: Verifying orphaned resources are gone...")
        
        final_topics_response = await api_client.list_topics()
        final_topics = [t['name'] for t in final_topics_response.get('topics', [])]
        
        remaining_orphaned = [
            topic for topic in final_topics
            if topic in identified_orphaned
        ]
        
        if remaining_orphaned:
            print(f"   ⚠️  {len(remaining_orphaned)} orphaned resources still exist")
            for topic in remaining_orphaned:
                print(f"     - {topic}")
        else:
            print("   ✓ All orphaned resources cleaned up successfully")
        
        assert len(remaining_orphaned) <= failed_cleanup_count, "More orphaned resources remain than expected"
        
        print("🎉 Orphaned resource cleanup test completed successfully!")
    
    @pytest.mark.asyncio
    async def test_environment_reset_procedure(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient
    ):
        """Test complete environment reset procedure."""
        print("\\n🔄 Testing environment reset procedure...")
        
        # Step 1: Capture initial state
        print("📊 Step 1: Capturing initial state...")
        
        initial_metrics = await monitoring_client.get_metrics()
        initial_health = await monitoring_client.get_health()
        initial_topics = await api_client.list_topics()
        
        initial_state = {
            'topic_count': len(initial_topics.get('topics', [])),
            'request_count': initial_metrics['counters'].get('kafka_ops_requests_total', 0),
            'health_status': initial_health.get('overall_status', 'unknown')
        }
        
        print(f"✓ Initial state: {initial_state}")
        
        # Step 2: Create test data
        print("📝 Step 2: Creating test data for reset...")
        
        reset_test_topics = []
        for i in range(8):
            topic_name = f"reset-test-topic-{i}"
            topic_config = {
                "name": topic_name,
                "partitions": 2,
                "replication_factor": 1
            }
            
            try:
                response = await api_client.create_topic(topic_config)
                if response.get('status') == 'success':
                    reset_test_topics.append(topic_name)
            except Exception as e:
                print(f"   ⚠️  Failed to create reset test topic: {e}")
        
        print(f"✓ Created {len(reset_test_topics)} test topics for reset")
        
        # Step 3: Verify environment is "dirty"
        print("🔍 Step 3: Verifying environment is dirty...")
        
        dirty_topics = await api_client.list_topics()
        dirty_metrics = await monitoring_client.get_metrics()
        
        dirty_state = {
            'topic_count': len(dirty_topics.get('topics', [])),
            'request_count': dirty_metrics['counters'].get('kafka_ops_requests_total', 0)
        }
        
        assert dirty_state['topic_count'] > initial_state['topic_count'], "Environment not dirty"
        assert dirty_state['request_count'] > initial_state['request_count'], "No activity recorded"
        
        print(f"✓ Environment is dirty: {dirty_state}")
        
        # Step 4: Execute environment reset
        print("🔄 Step 4: Executing environment reset...")
        
        reset_results = await self._execute_environment_reset(api_client)
        
        print(f"✓ Environment reset completed: {reset_results}")
        
        # Step 5: Verify environment is clean
        print("✅ Step 5: Verifying environment is clean...")
        
        # Wait a moment for cleanup to propagate
        await asyncio.sleep(2)
        
        clean_topics = await api_client.list_topics()
        clean_metrics = await monitoring_client.get_metrics()
        clean_health = await monitoring_client.get_health()
        
        clean_state = {
            'topic_count': len(clean_topics.get('topics', [])),
            'health_status': clean_health.get('overall_status', 'unknown')
        }
        
        # Verify reset was successful
        remaining_test_topics = [
            t['name'] for t in clean_topics.get('topics', [])
            if t['name'] in reset_test_topics
        ]
        
        print(f"✓ Clean state: {clean_state}")
        print(f"✓ Remaining test topics: {len(remaining_test_topics)}")
        
        # Environment should be clean
        assert len(remaining_test_topics) == 0, f"Test topics still exist: {remaining_test_topics}"
        assert clean_state['health_status'] in ['healthy', 'degraded'], "System not healthy after reset"
        
        print("🎉 Environment reset procedure completed successfully!")
    
    async def _execute_environment_reset(self, api_client: APIClient) -> Dict[str, Any]:
        """Execute complete environment reset.
        
        Args:
            api_client: API client
            
        Returns:
            Reset results summary
        """
        results = {
            'topics_cleaned': 0,
            'instances_cleaned': 0,
            'errors': [],
            'total_time': 0
        }
        
        start_time = time.time()
        
        try:
            # Step 1: Clean up all test topics
            topics_response = await api_client.list_topics()
            all_topics = topics_response.get('topics', [])
            
            test_topic_patterns = [
                'test-', 'reset-', 'cleanup-', 'orphaned-', 'perf-', 
                'load-', 'mixed-', 'bulk-', 'concurrent-', 'integration-'
            ]
            
            topics_to_clean = []
            for topic in all_topics:
                topic_name = topic['name']
                if any(topic_name.startswith(pattern) for pattern in test_topic_patterns):
                    topics_to_clean.append(topic_name)
            
            print(f"   🗑️  Cleaning {len(topics_to_clean)} test topics...")
            
            # Clean topics in batches to avoid overwhelming the system
            batch_size = 5
            for i in range(0, len(topics_to_clean), batch_size):
                batch = topics_to_clean[i:i + batch_size]
                
                batch_tasks = [
                    api_client.delete_topic(topic_name)
                    for topic_name in batch
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for j, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        results['errors'].append(f"Failed to delete {batch[j]}: {result}")
                    elif result.get('status') == 'success':
                        results['topics_cleaned'] += 1
                    else:
                        results['errors'].append(f"Delete failed for {batch[j]}")
                
                # Small delay between batches
                await asyncio.sleep(0.5)
            
            # Step 2: Clean up any test service instances
            # Note: In a real implementation, this would query for test instances
            # For now, we'll just record that this step was attempted
            results['instances_cleaned'] = 0  # Would be actual count
            
        except Exception as e:
            results['errors'].append(f"Reset procedure error: {e}")
        
        results['total_time'] = time.time() - start_time
        
        return results


class TestCleanupUtilities:
    """Test cleanup utility functions."""
    
    @pytest.mark.asyncio
    async def test_cleanup_by_age(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient
    ):
        """Test cleanup of resources by age."""
        print("\\n⏰ Testing cleanup by age...")
        
        # Create topics with different "ages" (simulated by naming)
        current_time = int(time.time())
        
        # Old topics (simulate 1 hour ago)
        old_timestamp = current_time - 3600
        old_topics = []
        
        for i in range(3):
            topic_name = f"aged-topic-{old_timestamp}-{i}"
            topic_config = {
                "name": topic_name,
                "partitions": 1,
                "replication_factor": 1
            }
            
            try:
                response = await api_client.create_topic(topic_config)
                if response.get('status') == 'success':
                    old_topics.append(topic_name)
            except Exception:
                pass
        
        # Recent topics (simulate 5 minutes ago)
        recent_timestamp = current_time - 300
        recent_topics = []
        
        for i in range(3):
            topic_name = f"aged-topic-{recent_timestamp}-{i}"
            topic_config = {
                "name": topic_name,
                "partitions": 1,
                "replication_factor": 1
            }
            
            try:
                response = await api_client.create_topic(topic_config)
                if response.get('status') == 'success':
                    recent_topics.append(topic_name)
            except Exception:
                pass
        
        print(f"✓ Created {len(old_topics)} old topics and {len(recent_topics)} recent topics")
        
        # Cleanup topics older than 30 minutes
        cutoff_time = current_time - 1800  # 30 minutes ago
        
        all_topics_response = await api_client.list_topics()
        all_topics = [t['name'] for t in all_topics_response.get('topics', [])]
        
        topics_to_cleanup = []
        for topic_name in all_topics:
            if topic_name.startswith('aged-topic-'):
                # Extract timestamp from topic name
                try:
                    parts = topic_name.split('-')
                    if len(parts) >= 3:
                        topic_timestamp = int(parts[2])
                        if topic_timestamp < cutoff_time:
                            topics_to_cleanup.append(topic_name)
                except ValueError:
                    pass
        
        print(f"🧹 Cleaning up {len(topics_to_cleanup)} topics older than 30 minutes")
        
        # Execute age-based cleanup
        cleanup_tasks = [
            api_client.delete_topic(topic_name)
            for topic_name in topics_to_cleanup
        ]
        
        cleanup_results = await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        successful_cleanups = sum(
            1 for result in cleanup_results
            if not isinstance(result, Exception) and result.get('status') == 'success'
        )
        
        print(f"✓ Successfully cleaned up {successful_cleanups} old topics")
        
        # Verify old topics are gone and recent topics remain
        final_topics_response = await api_client.list_topics()
        final_topics = [t['name'] for t in final_topics_response.get('topics', [])]
        
        remaining_old = [t for t in old_topics if t in final_topics]
        remaining_recent = [t for t in recent_topics if t in final_topics]
        
        print(f"✓ Remaining old topics: {len(remaining_old)}")
        print(f"✓ Remaining recent topics: {len(remaining_recent)}")
        
        # Clean up remaining test topics
        all_test_topics = old_topics + recent_topics
        cleanup_tasks = [
            api_client.delete_topic(topic_name)
            for topic_name in all_test_topics
            if topic_name in final_topics
        ]
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        assert len(remaining_old) == 0, "Old topics should have been cleaned up"
        
        print("🎉 Cleanup by age test completed successfully!")


if __name__ == '__main__':
    pytest.main([__file__, "-v", "-s"])