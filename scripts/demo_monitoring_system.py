#!/usr/bin/env python3
"""Demo script showcasing the monitoring system capabilities."""

import sys
import os
import asyncio
import time
import random
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.monitoring.health_checks import (
    HealthCheck, HealthCheckResult, HealthStatus, get_health_check_manager,
    DiskSpaceHealthCheck, MemoryHealthCheck
)
from kafka_ops_agent.monitoring.metrics import get_metrics_collector
from kafka_ops_agent.monitoring.alerts import (
    initialize_alert_manager, HealthStatusAlertRule, MetricThresholdAlertRule,
    ErrorRateAlertRule, LogAlertNotifier, AlertSeverity
)
from kafka_ops_agent.logging_config import setup_logging


class DemoHealthCheck(HealthCheck):
    """Demo health check that can simulate different states."""
    
    def __init__(self, name: str, initial_status: HealthStatus = HealthStatus.HEALTHY):
        """Initialize demo health check.
        
        Args:
            name: Name of the health check
            initial_status: Initial status to report
        """
        super().__init__(name, timeout_seconds=5.0)
        self.current_status = initial_status
        self.check_count = 0
    
    def set_status(self, status: HealthStatus):
        """Set the current status."""
        self.current_status = status
    
    async def check(self) -> HealthCheckResult:
        """Perform the demo health check."""
        self.check_count += 1
        
        # Simulate some processing time
        await asyncio.sleep(random.uniform(0.1, 0.5))
        
        messages = {
            HealthStatus.HEALTHY: f"Demo service {self.name} is running normally",
            HealthStatus.DEGRADED: f"Demo service {self.name} is experiencing issues",
            HealthStatus.UNHEALTHY: f"Demo service {self.name} is down",
            HealthStatus.UNKNOWN: f"Demo service {self.name} status is unknown"
        }
        
        return HealthCheckResult(
            name=self.name,
            status=self.current_status,
            message=messages[self.current_status],
            details={
                'check_count': self.check_count,
                'simulated': True,
                'last_check': time.time()
            }
        )


class MonitoringDemo:
    """Demonstration of the monitoring system."""
    
    def __init__(self):
        """Initialize the demo."""
        self.health_manager = get_health_check_manager()
        self.metrics_collector = get_metrics_collector()
        self.alert_manager = None
        self.demo_checks = {}
        self.running = False
    
    def setup_demo_health_checks(self):
        """Set up demo health checks."""
        print("🏥 Setting up demo health checks...")
        
        # Add demo service health checks
        services = ["api", "database", "cache", "queue"]
        for service in services:
            check = DemoHealthCheck(f"demo_{service}")
            self.demo_checks[service] = check
            self.health_manager.register_health_check(check)
        
        # Add system health checks
        disk_check = DiskSpaceHealthCheck(["/tmp", "."], warning_threshold=0.7, critical_threshold=0.9)
        self.health_manager.register_health_check(disk_check)
        
        memory_check = MemoryHealthCheck(warning_threshold=0.8, critical_threshold=0.9)
        self.health_manager.register_health_check(memory_check)
        
        print(f"✓ Registered {len(self.health_manager.get_health_check_names())} health checks")
    
    def setup_demo_metrics(self):
        """Set up demo metrics."""
        print("📊 Setting up demo metrics...")
        
        # Register custom metrics
        from kafka_ops_agent.monitoring.metrics import MetricType
        
        self.metrics_collector.register_metric(
            "demo_requests_total",
            MetricType.COUNTER,
            "Total demo requests processed"
        )
        
        self.metrics_collector.register_metric(
            "demo_active_users",
            MetricType.GAUGE,
            "Number of active demo users"
        )
        
        self.metrics_collector.register_metric(
            "demo_response_time_seconds",
            MetricType.HISTOGRAM,
            "Demo response time in seconds"
        )
        
        print("✓ Demo metrics registered")
    
    def setup_demo_alerts(self):
        """Set up demo alert rules."""
        print("🚨 Setting up demo alerts...")
        
        # Initialize alert manager
        self.alert_manager = initialize_alert_manager(self.health_manager, self.metrics_collector)
        
        # Add health status alerts
        self.alert_manager.add_alert_rule(
            HealthStatusAlertRule(
                "demo_system_unhealthy",
                AlertSeverity.CRITICAL,
                HealthStatus.UNHEALTHY
            )
        )
        
        self.alert_manager.add_alert_rule(
            HealthStatusAlertRule(
                "demo_system_degraded",
                AlertSeverity.WARNING,
                HealthStatus.DEGRADED
            )
        )
        
        # Add metric threshold alerts
        self.alert_manager.add_alert_rule(
            MetricThresholdAlertRule(
                "demo_high_request_count",
                AlertSeverity.INFO,
                "demo_requests_total",
                threshold=100,
                comparison="greater_than"
            )
        )
        
        self.alert_manager.add_alert_rule(
            MetricThresholdAlertRule(
                "demo_high_active_users",
                AlertSeverity.WARNING,
                "demo_active_users",
                threshold=50,
                comparison="greater_than"
            )
        )
        
        # Add error rate alert
        self.alert_manager.add_alert_rule(
            ErrorRateAlertRule(
                "demo_high_error_rate",
                AlertSeverity.CRITICAL,
                error_rate_threshold=0.1  # 10%
            )
        )
        
        # Add log notifier
        self.alert_manager.add_notifier(LogAlertNotifier("demo.alerts"))
        
        print(f"✓ Configured {len(self.alert_manager.alert_rules)} alert rules")
    
    async def simulate_metrics(self):
        """Simulate realistic metrics."""
        # Simulate request processing
        request_count = random.randint(1, 20)
        self.metrics_collector.increment_counter("demo_requests_total", request_count)
        self.metrics_collector.increment_counter("kafka_ops_requests_total", request_count)
        
        # Simulate active users
        active_users = random.randint(10, 80)
        self.metrics_collector.set_gauge("demo_active_users", active_users)
        self.metrics_collector.set_gauge("kafka_ops_active_connections", active_users // 2)
        
        # Simulate response times
        response_time = random.uniform(0.05, 2.0)
        self.metrics_collector.observe_histogram("demo_response_time_seconds", response_time)
        self.metrics_collector.observe_histogram("kafka_ops_request_duration_seconds", response_time)
        
        # Occasionally simulate errors
        if random.random() < 0.15:  # 15% chance
            error_count = random.randint(1, 5)
            self.metrics_collector.increment_counter("kafka_ops_errors_total", error_count)
        
        # Simulate topic and cluster counts
        self.metrics_collector.set_gauge("kafka_ops_topics_total", random.randint(20, 100))
        self.metrics_collector.set_gauge("kafka_ops_clusters_total", random.randint(2, 8))
    
    async def simulate_health_changes(self):
        """Simulate health status changes."""
        # Randomly change health status of demo services
        for service_name, check in self.demo_checks.items():
            if random.random() < 0.1:  # 10% chance of status change
                current_status = check.current_status
                
                # Define possible transitions
                transitions = {
                    HealthStatus.HEALTHY: [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY],
                    HealthStatus.DEGRADED: [HealthStatus.HEALTHY, HealthStatus.UNHEALTHY],
                    HealthStatus.UNHEALTHY: [HealthStatus.DEGRADED, HealthStatus.HEALTHY],
                    HealthStatus.UNKNOWN: [HealthStatus.HEALTHY]
                }
                
                if current_status in transitions:
                    new_status = random.choice(transitions[current_status])
                    check.set_status(new_status)
                    print(f"🔄 {service_name} status changed: {current_status.value} → {new_status.value}")
    
    async def print_status_summary(self):
        """Print current system status."""
        print("\\n" + "="*60)
        print(f"📊 MONITORING SYSTEM STATUS - {time.strftime('%H:%M:%S')}")
        print("="*60)
        
        # Health status
        health_summary = await self.health_manager.run_all_health_checks()
        print(f"\\n🏥 HEALTH STATUS: {health_summary.overall_status.value.upper()}")
        print(f"   Total checks: {health_summary.total_checks}")
        print(f"   Healthy: {health_summary.healthy_count}")
        print(f"   Degraded: {health_summary.degraded_count}")
        print(f"   Unhealthy: {health_summary.unhealthy_count}")
        print(f"   Unknown: {health_summary.unknown_count}")
        
        # Show individual check results
        for result in health_summary.check_results:
            status_emoji = {
                HealthStatus.HEALTHY: "✅",
                HealthStatus.DEGRADED: "⚠️",
                HealthStatus.UNHEALTHY: "❌",
                HealthStatus.UNKNOWN: "❓"
            }
            emoji = status_emoji.get(result.status, "❓")
            print(f"   {emoji} {result.name}: {result.message} ({result.duration_ms:.1f}ms)")
        
        # Metrics summary
        metrics = self.metrics_collector.get_metrics()
        print(f"\\n📈 METRICS SUMMARY:")
        print(f"   Counters: {len(metrics['counters'])}")
        print(f"   Gauges: {len(metrics['gauges'])}")
        print(f"   Histograms: {len(metrics['histograms'])}")
        
        # Show key metrics
        counters = metrics['counters']
        gauges = metrics['gauges']
        
        if 'demo_requests_total' in counters:
            print(f"   📊 Demo requests: {counters['demo_requests_total']}")
        if 'demo_active_users' in gauges:
            print(f"   👥 Active users: {gauges['demo_active_users']}")
        if 'kafka_ops_errors_total' in counters:
            print(f"   ❌ Total errors: {counters['kafka_ops_errors_total']}")
        
        # Alert status
        if self.alert_manager:
            alert_summary = self.alert_manager.get_alert_summary()
            print(f"\\n🚨 ALERT STATUS:")
            print(f"   Active alerts: {alert_summary['active_count']}")
            print(f"   Total rules: {alert_summary['total_rules']}")
            
            # Show active alerts
            for alert_data in alert_summary['active_alerts']:
                severity_emoji = {
                    "critical": "🔴",
                    "warning": "🟡",
                    "info": "🔵"
                }
                emoji = severity_emoji.get(alert_data['severity'], "⚪")
                print(f"   {emoji} {alert_data['name']}: {alert_data['message']}")
    
    async def run_demo(self, duration_minutes: int = 5):
        """Run the monitoring demo.
        
        Args:
            duration_minutes: How long to run the demo
        """
        print("🚀 Starting Monitoring System Demo")
        print(f"⏱️  Demo will run for {duration_minutes} minutes")
        print("="*60)
        
        # Set up components
        self.setup_demo_health_checks()
        self.setup_demo_metrics()
        self.setup_demo_alerts()
        
        # Start alert manager
        await self.alert_manager.start(check_interval=15)  # Check every 15 seconds
        print("✓ Alert manager started")
        
        self.running = True
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        try:
            iteration = 0
            while self.running and time.time() < end_time:
                iteration += 1
                
                # Simulate metrics and health changes
                await self.simulate_metrics()
                await self.simulate_health_changes()
                
                # Print status every 30 seconds or on first iteration
                if iteration == 1 or iteration % 6 == 0:  # Every 6 iterations (30 seconds)
                    await self.print_status_summary()
                
                # Wait 5 seconds between iterations
                await asyncio.sleep(5)
            
            # Final status
            print("\\n🏁 Demo completed!")
            await self.print_status_summary()
            
        except KeyboardInterrupt:
            print("\\n⏹️  Demo interrupted by user")
        
        finally:
            # Clean up
            if self.alert_manager:
                await self.alert_manager.stop()
                print("✓ Alert manager stopped")
            
            self.running = False
    
    async def interactive_demo(self):
        """Run interactive demo with user commands."""
        print("🎮 Interactive Monitoring Demo")
        print("Available commands:")
        print("  status  - Show current status")
        print("  health  - Run health checks")
        print("  metrics - Show metrics")
        print("  alerts  - Show alerts")
        print("  fail <service> - Make a service unhealthy")
        print("  fix <service>  - Make a service healthy")
        print("  load   - Simulate high load")
        print("  quit   - Exit demo")
        print("="*50)
        
        # Set up components
        self.setup_demo_health_checks()
        self.setup_demo_metrics()
        self.setup_demo_alerts()
        
        # Start alert manager
        await self.alert_manager.start(check_interval=10)
        
        try:
            while True:
                command = input("\\n> ").strip().lower()
                
                if command == "quit":
                    break
                elif command == "status":
                    await self.print_status_summary()
                elif command == "health":
                    summary = await self.health_manager.run_all_health_checks()
                    print(f"Health check completed: {summary.overall_status.value}")
                elif command == "metrics":
                    await self.simulate_metrics()
                    metrics = self.metrics_collector.get_metrics()
                    print(f"Metrics updated: {len(metrics['counters'])} counters, {len(metrics['gauges'])} gauges")
                elif command == "alerts":
                    summary = self.alert_manager.get_alert_summary()
                    print(f"Active alerts: {summary['active_count']}")
                elif command.startswith("fail "):
                    service = command.split(" ", 1)[1]
                    if service in self.demo_checks:
                        self.demo_checks[service].set_status(HealthStatus.UNHEALTHY)
                        print(f"❌ {service} marked as unhealthy")
                    else:
                        print(f"Service '{service}' not found. Available: {list(self.demo_checks.keys())}")
                elif command.startswith("fix "):
                    service = command.split(" ", 1)[1]
                    if service in self.demo_checks:
                        self.demo_checks[service].set_status(HealthStatus.HEALTHY)
                        print(f"✅ {service} marked as healthy")
                    else:
                        print(f"Service '{service}' not found. Available: {list(self.demo_checks.keys())}")
                elif command == "load":
                    # Simulate high load
                    for _ in range(10):
                        await self.simulate_metrics()
                    print("📈 High load simulated")
                else:
                    print("Unknown command. Type 'quit' to exit.")
        
        finally:
            await self.alert_manager.stop()


async def main():
    """Main demo function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitoring system demo")
    parser.add_argument("--duration", type=int, default=5,
                       help="Demo duration in minutes (default: 5)")
    parser.add_argument("--interactive", action="store_true",
                       help="Run interactive demo")
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging()
    
    demo = MonitoringDemo()
    
    if args.interactive:
        await demo.interactive_demo()
    else:
        await demo.run_demo(args.duration)


if __name__ == '__main__':
    asyncio.run(main())