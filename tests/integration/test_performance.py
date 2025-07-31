"""Performance tests for concurrent operations."""

import pytest
import asyncio
import time
import statistics
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor

from .conftest import APIClient, MonitoringClient, wait_for_condition


class TestConcurrentOperationsPerformance:
    """Test performance under concurrent operations."""
    
    @pytest.mark.asyncio
    async def test_concurrent_topic_creation_performance(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test performance of concurrent topic creation."""
        print("\\n⚡ Testing concurrent topic creation performance...")
        
        # Test parameters
        concurrent_levels = [1, 5, 10, 20]
        topics_per_level = 10
        
        performance_results = {}
        
        for concurrency in concurrent_levels:
            print(f"\\n🔄 Testing concurrency level: {concurrency}")
            
            # Generate topic names
            topic_names = [
                f"perf-test-c{concurrency}-{i}" 
                for i in range(topics_per_level)
            ]
            
            # Prepare topic configurations
            topic_configs = []
            for topic_name in topic_names:
                config = {
                    "name": topic_name,
                    "partitions": 2,
                    "replication_factor": 1,
                    "config": {
                        "retention.ms": "3600000"
                    }
                }
                topic_configs.append(config)
            
            # Measure creation performance
            start_time = time.time()
            
            # Create topics in batches based on concurrency level
            created_topics = []
            failed_topics = []
            
            for i in range(0, len(topic_configs), concurrency):
                batch = topic_configs[i:i + concurrency]
                
                # Create batch concurrently
                batch_tasks = [
                    self._create_topic_with_timing(api_client, config)
                    for config in batch
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        failed_topics.append(str(result))
                    else:
                        topic_name, success, duration = result
                        if success:
                            created_topics.append((topic_name, duration))
                        else:
                            failed_topics.append(topic_name)
            
            total_time = time.time() - start_time
            
            # Calculate performance metrics
            success_count = len(created_topics)
            failure_count = len(failed_topics)
            success_rate = success_count / topics_per_level if topics_per_level > 0 else 0
            
            if created_topics:
                durations = [duration for _, duration in created_topics]
                avg_duration = statistics.mean(durations)
                p95_duration = statistics.quantiles(durations, n=20)[18] if len(durations) > 1 else durations[0]
                throughput = success_count / total_time
            else:
                avg_duration = 0
                p95_duration = 0
                throughput = 0
            
            performance_results[concurrency] = {
                'total_time': total_time,
                'success_count': success_count,
                'failure_count': failure_count,
                'success_rate': success_rate,
                'avg_duration': avg_duration,
                'p95_duration': p95_duration,
                'throughput': throughput
            }
            
            print(f"   ✓ Success rate: {success_rate:.2%}")
            print(f"   ✓ Average duration: {avg_duration:.3f}s")
            print(f"   ✓ P95 duration: {p95_duration:.3f}s")
            print(f"   ✓ Throughput: {throughput:.2f} topics/sec")
            
            # Clean up created topics
            cleanup_tasks = [
                api_client.delete_topic(topic_name)
                for topic_name, _ in created_topics
            ]
            
            if cleanup_tasks:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                print(f"   🧹 Cleaned up {len(cleanup_tasks)} topics")
        
        # Analyze performance trends
        print("\\n📊 Performance Analysis:")
        
        for concurrency, results in performance_results.items():
            print(f"Concurrency {concurrency:2d}: "
                  f"{results['success_rate']:6.2%} success, "
                  f"{results['throughput']:6.2f} topics/sec, "
                  f"{results['avg_duration']:6.3f}s avg")
        
        # Verify performance requirements
        for concurrency, results in performance_results.items():
            # Success rate should be high
            assert results['success_rate'] >= 0.8, f"Low success rate at concurrency {concurrency}"
            
            # Average duration should be reasonable
            assert results['avg_duration'] < 10.0, f"High latency at concurrency {concurrency}"
            
            # P95 should not be too high
            assert results['p95_duration'] < 20.0, f"High P95 latency at concurrency {concurrency}"
        
        print("✅ Concurrent topic creation performance test completed!")
    
    async def _create_topic_with_timing(
        self, 
        api_client: APIClient, 
        topic_config: Dict[str, Any]
    ) -> Tuple[str, bool, float]:
        """Create a topic and measure timing.
        
        Returns:
            Tuple of (topic_name, success, duration)
        """
        topic_name = topic_config['name']
        start_time = time.time()
        
        try:
            response = await api_client.create_topic(topic_config)
            duration = time.time() - start_time
            success = response.get('status') == 'success'
            return topic_name, success, duration
        except Exception as e:
            duration = time.time() - start_time
            return topic_name, False, duration
    
    @pytest.mark.asyncio
    async def test_mixed_operations_performance(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test performance of mixed operations (create, read, update, delete)."""
        print("\\n🔄 Testing mixed operations performance...")
        
        # Test parameters
        operation_count = 50
        concurrency = 10
        
        # Pre-create some topics for read/update/delete operations
        base_topics = []
        for i in range(20):
            topic_name = f"mixed-ops-base-{i}"
            topic_config = {
                "name": topic_name,
                "partitions": 2,
                "replication_factor": 1
            }
            
            try:
                await api_client.create_topic(topic_config)
                base_topics.append(topic_name)
            except Exception as e:
                print(f"Failed to create base topic {topic_name}: {e}")
        
        print(f"✓ Created {len(base_topics)} base topics")
        
        # Define mixed operations
        operations = []
        
        # 40% create operations
        for i in range(int(operation_count * 0.4)):
            operations.append(('create', f"mixed-ops-new-{i}"))
        
        # 30% read operations
        for i in range(int(operation_count * 0.3)):
            if base_topics:
                topic_name = base_topics[i % len(base_topics)]
                operations.append(('read', topic_name))
        
        # 20% update operations
        for i in range(int(operation_count * 0.2)):
            if base_topics:
                topic_name = base_topics[i % len(base_topics)]
                operations.append(('update', topic_name))
        
        # 10% delete operations (we'll recreate them)
        for i in range(int(operation_count * 0.1)):
            operations.append(('delete', f"mixed-ops-delete-{i}"))
        
        # Pre-create topics for delete operations
        delete_topics = [op[1] for op in operations if op[0] == 'delete']
        for topic_name in delete_topics:
            topic_config = {
                "name": topic_name,
                "partitions": 1,
                "replication_factor": 1
            }
            try:
                await api_client.create_topic(topic_config)
            except Exception:
                pass
        
        print(f"✓ Prepared {len(operations)} mixed operations")
        
        # Execute operations concurrently
        start_time = time.time()
        
        # Split operations into batches
        operation_results = []
        
        for i in range(0, len(operations), concurrency):
            batch = operations[i:i + concurrency]
            
            batch_tasks = [
                self._execute_operation_with_timing(api_client, op_type, topic_name)
                for op_type, topic_name in batch
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            operation_results.extend(batch_results)
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful_ops = []
        failed_ops = []
        
        for result in operation_results:
            if isinstance(result, Exception):
                failed_ops.append(str(result))
            else:
                op_type, topic_name, success, duration = result
                if success:
                    successful_ops.append((op_type, duration))
                else:
                    failed_ops.append(f"{op_type}:{topic_name}")
        
        # Calculate performance metrics
        success_count = len(successful_ops)
        failure_count = len(failed_ops)
        success_rate = success_count / len(operations)
        overall_throughput = success_count / total_time
        
        # Per-operation type analysis
        op_stats = {}
        for op_type, duration in successful_ops:
            if op_type not in op_stats:
                op_stats[op_type] = []
            op_stats[op_type].append(duration)
        
        print(f"\\n📊 Mixed Operations Results:")
        print(f"   Total operations: {len(operations)}")
        print(f"   Successful: {success_count}")
        print(f"   Failed: {failure_count}")
        print(f"   Success rate: {success_rate:.2%}")
        print(f"   Overall throughput: {overall_throughput:.2f} ops/sec")
        print(f"   Total time: {total_time:.2f}s")
        
        for op_type, durations in op_stats.items():
            avg_duration = statistics.mean(durations)
            p95_duration = statistics.quantiles(durations, n=20)[18] if len(durations) > 1 else durations[0]
            print(f"   {op_type:6s}: {len(durations):3d} ops, "
                  f"{avg_duration:.3f}s avg, {p95_duration:.3f}s p95")
        
        # Clean up
        all_created_topics = base_topics + [op[1] for op in operations if op[0] == 'create']
        cleanup_tasks = [
            api_client.delete_topic(topic_name)
            for topic_name in all_created_topics
        ]
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            print(f"🧹 Cleaned up {len(cleanup_tasks)} topics")
        
        # Verify performance requirements
        assert success_rate >= 0.8, f"Low success rate: {success_rate:.2%}"
        assert overall_throughput >= 1.0, f"Low throughput: {overall_throughput:.2f} ops/sec"
        
        print("✅ Mixed operations performance test completed!")
    
    async def _execute_operation_with_timing(
        self,
        api_client: APIClient,
        op_type: str,
        topic_name: str
    ) -> Tuple[str, str, bool, float]:
        """Execute an operation and measure timing.
        
        Returns:
            Tuple of (op_type, topic_name, success, duration)
        """
        start_time = time.time()
        
        try:
            if op_type == 'create':
                topic_config = {
                    "name": topic_name,
                    "partitions": 2,
                    "replication_factor": 1
                }
                response = await api_client.create_topic(topic_config)
                success = response.get('status') == 'success'
                
            elif op_type == 'read':
                response = await api_client.get_topic(topic_name)
                success = 'name' in response
                
            elif op_type == 'update':
                update_config = {
                    "config": {
                        "retention.ms": "7200000"
                    }
                }
                response = await api_client.update_topic(topic_name, update_config)
                success = response.get('status') == 'success'
                
            elif op_type == 'delete':
                response = await api_client.delete_topic(topic_name)
                success = response.get('status') == 'success'
                
            else:
                success = False
            
            duration = time.time() - start_time
            return op_type, topic_name, success, duration
            
        except Exception as e:
            duration = time.time() - start_time
            return op_type, topic_name, False, duration


class TestSystemLoadPerformance:
    """Test system performance under load."""
    
    @pytest.mark.asyncio
    async def test_sustained_load_performance(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        clean_test_data
    ):
        """Test system performance under sustained load."""
        print("\\n🔥 Testing sustained load performance...")
        
        # Test parameters
        duration_minutes = 2  # Reduced for integration tests
        operations_per_minute = 60
        concurrency = 5
        
        print(f"   Duration: {duration_minutes} minutes")
        print(f"   Target rate: {operations_per_minute} ops/min")
        print(f"   Concurrency: {concurrency}")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        operation_count = 0
        successful_operations = 0
        failed_operations = 0
        response_times = []
        
        # Track metrics over time
        metrics_snapshots = []
        
        while time.time() < end_time:
            batch_start = time.time()
            
            # Create a batch of operations
            batch_size = min(concurrency, operations_per_minute // 10)  # 10 batches per minute
            
            batch_tasks = []
            for i in range(batch_size):
                topic_name = f"load-test-{operation_count + i}"
                batch_tasks.append(
                    self._load_test_operation(api_client, topic_name)
                )
            
            # Execute batch
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results
            for result in batch_results:
                operation_count += 1
                
                if isinstance(result, Exception):
                    failed_operations += 1
                else:
                    success, duration = result
                    if success:
                        successful_operations += 1
                        response_times.append(duration)
                    else:
                        failed_operations += 1
            
            # Collect metrics snapshot
            try:
                metrics = await monitoring_client.get_metrics()
                metrics_snapshots.append({
                    'timestamp': time.time(),
                    'request_count': metrics['counters'].get('kafka_ops_requests_total', 0),
                    'error_count': metrics['counters'].get('kafka_ops_errors_total', 0),
                    'active_connections': metrics['gauges'].get('kafka_ops_active_connections', 0)
                })
            except Exception as e:
                print(f"   ⚠️  Failed to collect metrics: {e}")
            
            # Wait for next batch
            batch_duration = time.time() - batch_start
            target_batch_duration = 6.0  # 6 seconds per batch (10 batches per minute)
            
            if batch_duration < target_batch_duration:
                await asyncio.sleep(target_batch_duration - batch_duration)
            
            # Progress update
            elapsed_minutes = (time.time() - start_time) / 60
            if operation_count % 20 == 0:
                current_rate = operation_count / (elapsed_minutes * 60) * 60  # ops per minute
                print(f"   📊 {elapsed_minutes:.1f}min: {operation_count} ops, "
                      f"{current_rate:.1f} ops/min, "
                      f"{successful_operations}/{operation_count} success")
        
        total_duration = time.time() - start_time
        
        # Calculate final metrics
        success_rate = successful_operations / operation_count if operation_count > 0 else 0
        actual_rate = operation_count / (total_duration / 60)  # ops per minute
        
        if response_times:
            avg_response_time = statistics.mean(response_times)
            p95_response_time = statistics.quantiles(response_times, n=20)[18] if len(response_times) > 1 else response_times[0]
            p99_response_time = statistics.quantiles(response_times, n=100)[98] if len(response_times) > 1 else response_times[0]
        else:
            avg_response_time = 0
            p95_response_time = 0
            p99_response_time = 0
        
        print(f"\\n📊 Sustained Load Results:")
        print(f"   Duration: {total_duration:.1f}s")
        print(f"   Total operations: {operation_count}")
        print(f"   Successful: {successful_operations}")
        print(f"   Failed: {failed_operations}")
        print(f"   Success rate: {success_rate:.2%}")
        print(f"   Actual rate: {actual_rate:.1f} ops/min")
        print(f"   Avg response time: {avg_response_time:.3f}s")
        print(f"   P95 response time: {p95_response_time:.3f}s")
        print(f"   P99 response time: {p99_response_time:.3f}s")
        
        # Analyze metrics trends
        if len(metrics_snapshots) > 1:
            print(f"\\n📈 Metrics Trends:")
            first_snapshot = metrics_snapshots[0]
            last_snapshot = metrics_snapshots[-1]
            
            request_increase = last_snapshot['request_count'] - first_snapshot['request_count']
            error_increase = last_snapshot['error_count'] - first_snapshot['error_count']
            
            print(f"   Request count increase: {request_increase}")
            print(f"   Error count increase: {error_increase}")
            print(f"   Final active connections: {last_snapshot['active_connections']}")
        
        # Verify performance requirements
        assert success_rate >= 0.9, f"Low success rate under load: {success_rate:.2%}"
        assert avg_response_time < 5.0, f"High average response time: {avg_response_time:.3f}s"
        assert p95_response_time < 10.0, f"High P95 response time: {p95_response_time:.3f}s"
        
        print("✅ Sustained load performance test completed!")
    
    async def _load_test_operation(
        self,
        api_client: APIClient,
        topic_name: str
    ) -> Tuple[bool, float]:
        """Execute a load test operation.
        
        Returns:
            Tuple of (success, duration)
        """
        start_time = time.time()
        
        try:
            # Create topic
            topic_config = {
                "name": topic_name,
                "partitions": 1,
                "replication_factor": 1
            }
            
            create_response = await api_client.create_topic(topic_config)
            create_success = create_response.get('status') == 'success'
            
            if create_success:
                # Delete topic to clean up
                delete_response = await api_client.delete_topic(topic_name)
                delete_success = delete_response.get('status') == 'success'
                success = create_success and delete_success
            else:
                success = False
            
            duration = time.time() - start_time
            return success, duration
            
        except Exception as e:
            duration = time.time() - start_time
            return False, duration


if __name__ == '__main__':
    pytest.main([__file__, "-v", "-s"])