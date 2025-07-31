#!/usr/bin/env python3
"""Test script for monitoring endpoints."""

import sys
import os
import asyncio
import aiohttp
import json
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class MonitoringTester:
    """Test monitoring endpoints."""
    
    def __init__(self, base_url: str = "http://localhost:8080"):
        """Initialize tester.
        
        Args:
            base_url: Base URL of monitoring server
        """
        self.base_url = base_url
        self.session = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def test_endpoint(self, endpoint: str, expected_status: int = 200) -> dict:
        """Test a single endpoint.
        
        Args:
            endpoint: Endpoint path
            expected_status: Expected HTTP status code
            
        Returns:
            Response data
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with self.session.get(url) as response:
                status = response.status
                
                # Try to parse JSON response
                try:
                    data = await response.json()
                except:
                    data = await response.text()
                
                # Check status
                status_ok = status == expected_status
                status_symbol = "✓" if status_ok else "✗"
                
                print(f"{status_symbol} {endpoint:<25} [{status}] {len(str(data)) if isinstance(data, str) else len(json.dumps(data))} bytes")
                
                if not status_ok:
                    print(f"   Expected {expected_status}, got {status}")
                    if isinstance(data, dict) and 'error' in data:
                        print(f"   Error: {data['error']}")
                
                return {
                    'endpoint': endpoint,
                    'status': status,
                    'expected_status': expected_status,
                    'success': status_ok,
                    'data': data
                }
                
        except aiohttp.ClientError as e:
            print(f"✗ {endpoint:<25} [ERROR] Connection failed: {e}")
            return {
                'endpoint': endpoint,
                'status': 0,
                'expected_status': expected_status,
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            print(f"✗ {endpoint:<25} [ERROR] Unexpected error: {e}")
            return {
                'endpoint': endpoint,
                'status': 0,
                'expected_status': expected_status,
                'success': False,
                'error': str(e)
            }
    
    async def test_all_endpoints(self):
        """Test all monitoring endpoints."""
        print("🧪 Testing Monitoring Endpoints")
        print("=" * 50)
        
        # Define endpoints to test
        endpoints = [
            # Health endpoints
            ("/health", 200),
            ("/health/ready", 200),
            ("/health/live", 200),
            ("/health/checks", 200),
            ("/health/history", 200),
            
            # Metrics endpoints
            ("/metrics", 200),
            ("/metrics/prometheus", 200),
            ("/metrics/history", 200),
            
            # Alert endpoints
            ("/alerts", 200),
            ("/alerts/history", 200),
            
            # System endpoints
            ("/info", 200),
            ("/status", 200),
            
            # Error endpoints
            ("/nonexistent", 404),
        ]
        
        results = []
        
        print("\n📋 Health Check Endpoints:")
        for endpoint, expected_status in endpoints[:5]:
            result = await self.test_endpoint(endpoint, expected_status)
            results.append(result)
        
        print("\n📊 Metrics Endpoints:")
        for endpoint, expected_status in endpoints[5:8]:
            result = await self.test_endpoint(endpoint, expected_status)
            results.append(result)
        
        print("\n🚨 Alert Endpoints:")
        for endpoint, expected_status in endpoints[8:10]:
            result = await self.test_endpoint(endpoint, expected_status)
            results.append(result)
        
        print("\n🔧 System Endpoints:")
        for endpoint, expected_status in endpoints[10:12]:
            result = await self.test_endpoint(endpoint, expected_status)
            results.append(result)
        
        print("\n❌ Error Handling:")
        for endpoint, expected_status in endpoints[12:]:
            result = await self.test_endpoint(endpoint, expected_status)
            results.append(result)
        
        return results
    
    async def test_specific_health_check(self, check_name: str):
        """Test a specific health check endpoint.
        
        Args:
            check_name: Name of health check to test
        """
        print(f"\n🔍 Testing specific health check: {check_name}")
        endpoint = f"/health/checks/{check_name}"
        result = await self.test_endpoint(endpoint, 200)
        
        if result['success'] and isinstance(result['data'], dict):
            data = result['data']
            print(f"   Status: {data.get('status', 'unknown')}")
            print(f"   Message: {data.get('message', 'N/A')}")
            print(f"   Duration: {data.get('duration_ms', 0):.1f}ms")
        
        return result
    
    async def analyze_responses(self, results: list):
        """Analyze test results.
        
        Args:
            results: List of test results
        """
        print("\n📈 Test Results Summary")
        print("=" * 30)
        
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r['success'])
        failed_tests = total_tests - successful_tests
        
        print(f"Total tests:      {total_tests}")
        print(f"Successful:       {successful_tests}")
        print(f"Failed:           {failed_tests}")
        print(f"Success rate:     {successful_tests/total_tests*100:.1f}%")
        
        if failed_tests > 0:
            print(f"\n❌ Failed tests:")
            for result in results:
                if not result['success']:
                    print(f"   {result['endpoint']} - {result.get('error', 'Status mismatch')}")
        
        # Analyze specific responses
        print(f"\n🔍 Response Analysis:")
        
        # Check health status
        health_results = [r for r in results if r['endpoint'] == '/health' and r['success']]
        if health_results:
            health_data = health_results[0]['data']
            if isinstance(health_data, dict):
                overall_status = health_data.get('overall_status', 'unknown')
                total_checks = health_data.get('summary', {}).get('total', 0)
                healthy_checks = health_data.get('summary', {}).get('healthy', 0)
                print(f"   Overall health: {overall_status}")
                print(f"   Health checks: {healthy_checks}/{total_checks} healthy")
        
        # Check metrics
        metrics_results = [r for r in results if r['endpoint'] == '/metrics' and r['success']]
        if metrics_results:
            metrics_data = metrics_results[0]['data']
            if isinstance(metrics_data, dict):
                counters = len(metrics_data.get('counters', {}))
                gauges = len(metrics_data.get('gauges', {}))
                histograms = len(metrics_data.get('histograms', {}))
                print(f"   Metrics: {counters} counters, {gauges} gauges, {histograms} histograms")
        
        # Check alerts
        alerts_results = [r for r in results if r['endpoint'] == '/alerts' and r['success']]
        if alerts_results:
            alerts_data = alerts_results[0]['data']
            if isinstance(alerts_data, dict):
                active_alerts = alerts_data.get('active_count', 0)
                total_rules = alerts_data.get('total_rules', 0)
                print(f"   Alerts: {active_alerts} active, {total_rules} rules")


async def main():
    """Main test function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test monitoring endpoints")
    parser.add_argument("--url", default="http://localhost:8080", 
                       help="Base URL of monitoring server")
    parser.add_argument("--check", help="Test specific health check")
    parser.add_argument("--wait", type=int, default=0,
                       help="Wait time before starting tests")
    
    args = parser.parse_args()
    
    if args.wait > 0:
        print(f"⏳ Waiting {args.wait} seconds for server to start...")
        await asyncio.sleep(args.wait)
    
    async with MonitoringTester(args.url) as tester:
        if args.check:
            # Test specific health check
            await tester.test_specific_health_check(args.check)
        else:
            # Test all endpoints
            results = await tester.test_all_endpoints()
            await tester.analyze_responses(results)
            
            # Test some specific health checks if they exist
            print(f"\n🔍 Testing Specific Health Checks:")
            for check_name in ["database", "kafka", "disk_space", "memory"]:
                await tester.test_specific_health_check(check_name)
    
    print(f"\n✅ Testing complete!")


if __name__ == '__main__':
    asyncio.run(main())