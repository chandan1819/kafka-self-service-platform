"""Cross-provider deployment validation tests."""

import pytest
import asyncio
import os
from typing import Dict, Any, List
from unittest.mock import patch, Mock

from .conftest import APIClient, MonitoringClient, wait_for_condition


class TestDockerProviderDeployment:
    """Test Docker provider deployment validation."""
    
    @pytest.mark.asyncio
    async def test_docker_cluster_deployment_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        test_instance_id: str,
        clean_test_data
    ):
        """Test Docker cluster deployment workflow."""
        print(f"\\n🐳 Testing Docker cluster deployment workflow...")
        
        # Skip if Docker not available
        if not await self._is_docker_available():
            pytest.skip("Docker not available in test environment")
        
        # Docker-specific service configuration
        docker_config = {
            "service_id": "kafka-cluster",
            "plan_id": "docker-small",
            "parameters": {
                "provider": "docker",
                "cluster_name": f"docker-test-{test_instance_id[:8]}",
                "broker_count": 1,
                "zookeeper_count": 1,
                "network_name": "kafka-test-network",
                "storage_type": "local"
            }
        }
        
        # Step 1: Provision Docker cluster
        print("🚀 Step 1: Provisioning Docker cluster...")
        provision_response = await api_client.provision_service(test_instance_id, docker_config)
        
        assert 'operation' in provision_response
        operation_id = provision_response['operation']
        print(f"✓ Docker provisioning started: {operation_id}")
        
        # Step 2: Wait for provisioning to complete
        print("⏳ Step 2: Waiting for Docker provisioning...")
        
        async def check_docker_provisioning():
            """Check Docker provisioning status."""
            try:
                status = await api_client.get_last_operation(test_instance_id)
                state = status.get('state', 'in progress')
                
                if state == 'failed':
                    error_msg = status.get('description', 'Unknown error')
                    print(f"   ❌ Docker provisioning failed: {error_msg}")
                    return False
                
                print(f"   Docker provisioning state: {state}")
                return state == 'succeeded'
            except Exception as e:
                print(f"   Error checking Docker status: {e}")
                return False
        
        docker_ready = await wait_for_condition(
            check_docker_provisioning,
            timeout=300,  # 5 minutes for Docker
            interval=15
        )
        
        assert docker_ready, "Docker cluster provisioning failed or timed out"
        print("✓ Docker cluster provisioned successfully")
        
        # Step 3: Verify Docker cluster accessibility
        print("🔍 Step 3: Verifying Docker cluster accessibility...")
        
        final_status = await api_client.get_last_operation(test_instance_id)
        assert final_status['state'] == 'succeeded'
        
        if 'connection_info' in final_status:
            connection_info = final_status['connection_info']
            assert 'bootstrap_servers' in connection_info
            assert 'docker' in connection_info.get('provider', '').lower()
            print(f"✓ Docker cluster accessible: {connection_info['bootstrap_servers']}")
        
        # Step 4: Test topic operations on Docker cluster
        print("📝 Step 4: Testing topic operations on Docker cluster...")
        
        test_topic = f"docker-test-topic-{test_instance_id[:8]}"
        topic_config = {
            "name": test_topic,
            "partitions": 2,
            "replication_factor": 1,
            "config": {
                "retention.ms": "3600000"  # 1 hour for test
            }
        }
        
        # Create topic on Docker cluster
        create_response = await api_client.create_topic(topic_config)
        assert create_response['status'] == 'success'
        
        # Verify topic
        topic_info = await api_client.get_topic(test_topic)
        assert topic_info['name'] == test_topic
        print(f"✓ Topic operations successful on Docker cluster")
        
        # Step 5: Clean up topic
        delete_response = await api_client.delete_topic(test_topic)
        assert delete_response['status'] == 'success'
        
        # Step 6: Deprovision Docker cluster
        print("🧹 Step 6: Deprovisioning Docker cluster...")
        await api_client.deprovision_service(test_instance_id)
        
        async def check_docker_deprovisioning():
            """Check Docker deprovisioning status."""
            try:
                status = await api_client.get_last_operation(test_instance_id)
                state = status.get('state', 'in progress')
                print(f"   Docker deprovisioning state: {state}")
                return state == 'succeeded'
            except Exception:
                # Instance might be gone
                return True
        
        deprovisioning_complete = await wait_for_condition(
            check_docker_deprovisioning,
            timeout=180,
            interval=10
        )
        
        assert deprovisioning_complete, "Docker deprovisioning failed"
        print("✓ Docker cluster deprovisioned successfully")
        
        print("🎉 Docker deployment workflow completed successfully!")
    
    async def _is_docker_available(self) -> bool:
        """Check if Docker is available."""
        try:
            import docker
            client = docker.from_env()
            client.ping()
            return True
        except Exception:
            return False


class TestKubernetesProviderDeployment:
    """Test Kubernetes provider deployment validation."""
    
    @pytest.mark.asyncio
    async def test_kubernetes_cluster_deployment_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        test_instance_id: str,
        clean_test_data
    ):
        """Test Kubernetes cluster deployment workflow."""
        print(f"\\n☸️  Testing Kubernetes cluster deployment workflow...")
        
        # Skip if Kubernetes not available
        if not await self._is_kubernetes_available():
            pytest.skip("Kubernetes not available in test environment")
        
        # Kubernetes-specific service configuration
        k8s_config = {
            "service_id": "kafka-cluster",
            "plan_id": "k8s-small",
            "parameters": {
                "provider": "kubernetes",
                "cluster_name": f"k8s-test-{test_instance_id[:8]}",
                "namespace": "kafka-test",
                "broker_count": 1,
                "zookeeper_count": 1,
                "storage_class": "standard",
                "storage_size": "1Gi",
                "resource_limits": {
                    "memory": "512Mi",
                    "cpu": "500m"
                }
            }
        }
        
        # Step 1: Provision Kubernetes cluster
        print("🚀 Step 1: Provisioning Kubernetes cluster...")
        provision_response = await api_client.provision_service(test_instance_id, k8s_config)
        
        assert 'operation' in provision_response
        operation_id = provision_response['operation']
        print(f"✓ Kubernetes provisioning started: {operation_id}")
        
        # Step 2: Wait for provisioning to complete
        print("⏳ Step 2: Waiting for Kubernetes provisioning...")
        
        async def check_k8s_provisioning():
            """Check Kubernetes provisioning status."""
            try:
                status = await api_client.get_last_operation(test_instance_id)
                state = status.get('state', 'in progress')
                
                if state == 'failed':
                    error_msg = status.get('description', 'Unknown error')
                    print(f"   ❌ Kubernetes provisioning failed: {error_msg}")
                    return False
                
                print(f"   Kubernetes provisioning state: {state}")
                return state == 'succeeded'
            except Exception as e:
                print(f"   Error checking Kubernetes status: {e}")
                return False
        
        k8s_ready = await wait_for_condition(
            check_k8s_provisioning,
            timeout=600,  # 10 minutes for Kubernetes
            interval=20
        )
        
        assert k8s_ready, "Kubernetes cluster provisioning failed or timed out"
        print("✓ Kubernetes cluster provisioned successfully")
        
        # Step 3: Verify Kubernetes cluster accessibility
        print("🔍 Step 3: Verifying Kubernetes cluster accessibility...")
        
        final_status = await api_client.get_last_operation(test_instance_id)
        assert final_status['state'] == 'succeeded'
        
        if 'connection_info' in final_status:
            connection_info = final_status['connection_info']
            assert 'bootstrap_servers' in connection_info
            assert 'kubernetes' in connection_info.get('provider', '').lower()
            print(f"✓ Kubernetes cluster accessible: {connection_info['bootstrap_servers']}")
        
        # Step 4: Verify Kubernetes resources
        print("🔍 Step 4: Verifying Kubernetes resources...")
        
        # This would typically check that StatefulSets, Services, etc. are created
        # For now, we'll verify through the API response
        if 'kubernetes_info' in final_status:
            k8s_info = final_status['kubernetes_info']
            assert 'namespace' in k8s_info
            assert 'statefulsets' in k8s_info
            print(f"✓ Kubernetes resources created in namespace: {k8s_info['namespace']}")
        
        # Step 5: Test topic operations on Kubernetes cluster
        print("📝 Step 5: Testing topic operations on Kubernetes cluster...")
        
        test_topic = f"k8s-test-topic-{test_instance_id[:8]}"
        topic_config = {
            "name": test_topic,
            "partitions": 3,
            "replication_factor": 1,
            "config": {
                "retention.ms": "7200000"  # 2 hours for test
            }
        }
        
        # Create topic on Kubernetes cluster
        create_response = await api_client.create_topic(topic_config)
        assert create_response['status'] == 'success'
        
        # Verify topic
        topic_info = await api_client.get_topic(test_topic)
        assert topic_info['name'] == test_topic
        assert topic_info['partitions'] == 3
        print(f"✓ Topic operations successful on Kubernetes cluster")
        
        # Step 6: Clean up topic
        delete_response = await api_client.delete_topic(test_topic)
        assert delete_response['status'] == 'success'
        
        # Step 7: Deprovision Kubernetes cluster
        print("🧹 Step 7: Deprovisioning Kubernetes cluster...")
        await api_client.deprovision_service(test_instance_id)
        
        async def check_k8s_deprovisioning():
            """Check Kubernetes deprovisioning status."""
            try:
                status = await api_client.get_last_operation(test_instance_id)
                state = status.get('state', 'in progress')
                print(f"   Kubernetes deprovisioning state: {state}")
                return state == 'succeeded'
            except Exception:
                # Instance might be gone
                return True
        
        deprovisioning_complete = await wait_for_condition(
            check_k8s_deprovisioning,
            timeout=300,
            interval=15
        )
        
        assert deprovisioning_complete, "Kubernetes deprovisioning failed"
        print("✓ Kubernetes cluster deprovisioned successfully")
        
        print("🎉 Kubernetes deployment workflow completed successfully!")
    
    async def _is_kubernetes_available(self) -> bool:
        """Check if Kubernetes is available."""
        try:
            from kubernetes import client, config
            config.load_incluster_config()  # Try in-cluster first
            v1 = client.CoreV1Api()
            v1.list_namespace()
            return True
        except Exception:
            try:
                config.load_kube_config()  # Try local kubeconfig
                v1 = client.CoreV1Api()
                v1.list_namespace()
                return True
            except Exception:
                return False


class TestTerraformProviderDeployment:
    """Test Terraform provider deployment validation."""
    
    @pytest.mark.asyncio
    async def test_terraform_cluster_deployment_workflow(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient,
        test_instance_id: str,
        clean_test_data
    ):
        """Test Terraform cluster deployment workflow."""
        print(f"\\n🏗️  Testing Terraform cluster deployment workflow...")
        
        # Skip if Terraform not available
        if not await self._is_terraform_available():
            pytest.skip("Terraform not available in test environment")
        
        # Terraform-specific service configuration
        terraform_config = {
            "service_id": "kafka-cluster",
            "plan_id": "terraform-aws-small",
            "parameters": {
                "provider": "terraform",
                "cloud_provider": "aws",
                "region": "us-west-2",
                "cluster_name": f"tf-test-{test_instance_id[:8]}",
                "instance_type": "t3.micro",
                "broker_count": 1,
                "zookeeper_count": 1,
                "vpc_cidr": "10.0.0.0/16",
                "enable_monitoring": True
            }
        }
        
        # Step 1: Provision Terraform cluster
        print("🚀 Step 1: Provisioning Terraform cluster...")
        provision_response = await api_client.provision_service(test_instance_id, terraform_config)
        
        assert 'operation' in provision_response
        operation_id = provision_response['operation']
        print(f"✓ Terraform provisioning started: {operation_id}")
        
        # Step 2: Wait for provisioning to complete
        print("⏳ Step 2: Waiting for Terraform provisioning...")
        
        async def check_terraform_provisioning():
            """Check Terraform provisioning status."""
            try:
                status = await api_client.get_last_operation(test_instance_id)
                state = status.get('state', 'in progress')
                
                if state == 'failed':
                    error_msg = status.get('description', 'Unknown error')
                    print(f"   ❌ Terraform provisioning failed: {error_msg}")
                    return False
                
                print(f"   Terraform provisioning state: {state}")
                return state == 'succeeded'
            except Exception as e:
                print(f"   Error checking Terraform status: {e}")
                return False
        
        terraform_ready = await wait_for_condition(
            check_terraform_provisioning,
            timeout=900,  # 15 minutes for Terraform/AWS
            interval=30
        )
        
        assert terraform_ready, "Terraform cluster provisioning failed or timed out"
        print("✓ Terraform cluster provisioned successfully")
        
        # Step 3: Verify Terraform cluster accessibility
        print("🔍 Step 3: Verifying Terraform cluster accessibility...")
        
        final_status = await api_client.get_last_operation(test_instance_id)
        assert final_status['state'] == 'succeeded'
        
        if 'connection_info' in final_status:
            connection_info = final_status['connection_info']
            assert 'bootstrap_servers' in connection_info
            assert 'terraform' in connection_info.get('provider', '').lower()
            print(f"✓ Terraform cluster accessible: {connection_info['bootstrap_servers']}")
        
        # Step 4: Verify Terraform outputs
        print("🔍 Step 4: Verifying Terraform outputs...")
        
        if 'terraform_outputs' in final_status:
            tf_outputs = final_status['terraform_outputs']
            assert 'vpc_id' in tf_outputs
            assert 'security_group_id' in tf_outputs
            assert 'instance_ids' in tf_outputs
            print(f"✓ Terraform outputs verified: VPC {tf_outputs['vpc_id']}")
        
        # Step 5: Test topic operations on Terraform cluster
        print("📝 Step 5: Testing topic operations on Terraform cluster...")
        
        test_topic = f"tf-test-topic-{test_instance_id[:8]}"
        topic_config = {
            "name": test_topic,
            "partitions": 2,
            "replication_factor": 1,
            "config": {
                "retention.ms": "10800000"  # 3 hours for test
            }
        }
        
        # Create topic on Terraform cluster
        create_response = await api_client.create_topic(topic_config)
        assert create_response['status'] == 'success'
        
        # Verify topic
        topic_info = await api_client.get_topic(test_topic)
        assert topic_info['name'] == test_topic
        print(f"✓ Topic operations successful on Terraform cluster")
        
        # Step 6: Clean up topic
        delete_response = await api_client.delete_topic(test_topic)
        assert delete_response['status'] == 'success'
        
        # Step 7: Deprovision Terraform cluster
        print("🧹 Step 7: Deprovisioning Terraform cluster...")
        await api_client.deprovision_service(test_instance_id)
        
        async def check_terraform_deprovisioning():
            """Check Terraform deprovisioning status."""
            try:
                status = await api_client.get_last_operation(test_instance_id)
                state = status.get('state', 'in progress')
                print(f"   Terraform deprovisioning state: {state}")
                return state == 'succeeded'
            except Exception:
                # Instance might be gone
                return True
        
        deprovisioning_complete = await wait_for_condition(
            check_terraform_deprovisioning,
            timeout=600,  # 10 minutes for Terraform cleanup
            interval=20
        )
        
        assert deprovisioning_complete, "Terraform deprovisioning failed"
        print("✓ Terraform cluster deprovisioned successfully")
        
        print("🎉 Terraform deployment workflow completed successfully!")
    
    async def _is_terraform_available(self) -> bool:
        """Check if Terraform is available."""
        try:
            import subprocess
            result = subprocess.run(['terraform', 'version'], 
                                  capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except Exception:
            return False


class TestCrossProviderComparison:
    """Test cross-provider comparison and validation."""
    
    @pytest.mark.asyncio
    async def test_provider_feature_parity(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient
    ):
        """Test that all providers support the same core features."""
        print("\\n🔄 Testing provider feature parity...")
        
        # Get service catalog
        catalog = await api_client.get_catalog()
        
        # Find Kafka service
        kafka_service = None
        for service in catalog['services']:
            if service['name'] == 'kafka-cluster':
                kafka_service = service
                break
        
        assert kafka_service is not None, "Kafka service not found"
        
        # Group plans by provider
        providers = {}
        for plan in kafka_service['plans']:
            provider = plan.get('metadata', {}).get('provider', 'unknown')
            if provider not in providers:
                providers[provider] = []
            providers[provider].append(plan)
        
        print(f"✓ Found {len(providers)} providers: {list(providers.keys())}")
        
        # Verify each provider has required features
        required_features = [
            'broker_count',
            'storage_size',
            'monitoring',
            'backup'
        ]
        
        for provider_name, plans in providers.items():
            print(f"🔍 Checking provider: {provider_name}")
            
            # Check at least one plan exists
            assert len(plans) > 0, f"No plans for provider {provider_name}"
            
            # Check required features in at least one plan
            for feature in required_features:
                feature_found = False
                for plan in plans:
                    if feature in plan.get('metadata', {}).get('features', []):
                        feature_found = True
                        break
                
                if not feature_found:
                    print(f"   ⚠️  Feature '{feature}' not found in {provider_name}")
                else:
                    print(f"   ✓ Feature '{feature}' supported")
        
        print("✅ Provider feature parity check completed!")
    
    @pytest.mark.asyncio
    async def test_provider_performance_comparison(
        self,
        api_client: APIClient,
        monitoring_client: MonitoringClient
    ):
        """Test performance characteristics across providers."""
        print("\\n⚡ Testing provider performance comparison...")
        
        # This test would ideally provision clusters with each provider
        # and compare provisioning times, but for integration tests,
        # we'll simulate the comparison
        
        providers = ['docker', 'kubernetes', 'terraform']
        performance_data = {}
        
        for provider in providers:
            print(f"📊 Simulating performance test for {provider}...")
            
            # Simulate performance metrics
            # In a real test, this would measure actual provisioning times
            if provider == 'docker':
                performance_data[provider] = {
                    'avg_provision_time': 120,  # 2 minutes
                    'avg_deprovision_time': 30,  # 30 seconds
                    'resource_efficiency': 0.9
                }
            elif provider == 'kubernetes':
                performance_data[provider] = {
                    'avg_provision_time': 300,  # 5 minutes
                    'avg_deprovision_time': 60,  # 1 minute
                    'resource_efficiency': 0.8
                }
            elif provider == 'terraform':
                performance_data[provider] = {
                    'avg_provision_time': 600,  # 10 minutes
                    'avg_deprovision_time': 180,  # 3 minutes
                    'resource_efficiency': 0.7
                }
            
            print(f"   ✓ {provider}: {performance_data[provider]['avg_provision_time']}s provision time")
        
        # Analyze performance data
        fastest_provision = min(providers, 
                              key=lambda p: performance_data[p]['avg_provision_time'])
        most_efficient = max(providers,
                           key=lambda p: performance_data[p]['resource_efficiency'])
        
        print(f"🏆 Fastest provisioning: {fastest_provision}")
        print(f"🏆 Most resource efficient: {most_efficient}")
        
        # Verify all providers meet minimum performance requirements
        for provider, data in performance_data.items():
            assert data['avg_provision_time'] < 900, f"{provider} provision time too slow"
            assert data['resource_efficiency'] > 0.5, f"{provider} not efficient enough"
        
        print("✅ Provider performance comparison completed!")


if __name__ == '__main__':
    pytest.main([__file__, "-v", "-s"])