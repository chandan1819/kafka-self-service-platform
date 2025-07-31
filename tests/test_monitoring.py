"""Tests for monitoring system."""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta

from kafka_ops_agent.monitoring.health_checks import (
    HealthCheck, HealthCheckResult, HealthStatus, HealthCheckManager,
    DatabaseHealthCheck, KafkaHealthCheck, ServiceHealthCheck,
    DiskSpaceHealthCheck, MemoryHealthCheck
)
from kafka_ops_agent.monitoring.metrics import (
    MetricsCollector, MetricType, Metric, HistogramData, TimerContext
)
from kafka_ops_agent.monitoring.alerts import (
    Alert, AlertSeverity, AlertState, AlertRule, AlertManager,
    HealthStatusAlertRule, MetricThresholdAlertRule, ErrorRateAlertRule,
    LogAlertNotifier
)


class TestHealthChecks:
    """Test health check system."""
    
    def test_health_check_result_creation(self):
        """Test health check result creation."""
        result = HealthCheckResult(
            name="test_check",
            status=HealthStatus.HEALTHY,
            message="All good",
            details={"key": "value"}
        )
        
        assert result.name == "test_check"
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "All good"
        assert result.details == {"key": "value"}
        assert isinstance(result.timestamp, datetime)
        
        # Test to_dict conversion
        result_dict = result.to_dict()
        assert result_dict["name"] == "test_check"
        assert result_dict["status"] == "healthy"
        assert result_dict["message"] == "All good"
        assert "timestamp" in result_dict
    
    @pytest.mark.asyncio
    async def test_health_check_manager(self):
        """Test health check manager."""
        manager = HealthCheckManager()
        
        # Create mock health check
        mock_check = Mock(spec=HealthCheck)
        mock_check.name = "test_check"
        mock_check.run_check = AsyncMock(return_value=HealthCheckResult(
            name="test_check",
            status=HealthStatus.HEALTHY,
            message="Test passed"
        ))
        
        # Register health check
        manager.register_health_check(mock_check)
        assert "test_check" in manager.get_health_check_names()
        
        # Run specific health check
        result = await manager.run_health_check("test_check")
        assert result is not None
        assert result.name == "test_check"
        assert result.status == HealthStatus.HEALTHY
        
        # Run all health checks
        summary = await manager.run_all_health_checks()
        assert summary.total_checks == 1
        assert summary.healthy_count == 1
        assert summary.overall_status == HealthStatus.HEALTHY
        
        # Test unregistering
        manager.unregister_health_check("test_check")
        assert "test_check" not in manager.get_health_check_names()
    
    @pytest.mark.asyncio
    async def test_database_health_check(self):
        """Test database health check."""
        # Mock successful connection
        mock_db = Mock()
        mock_db.execute = AsyncMock()
        
        async def get_db_connection():
            return mock_db
        
        check = DatabaseHealthCheck(get_db_connection)
        result = await check.check()
        
        assert result.status == HealthStatus.HEALTHY
        assert "Database connection successful" in result.message
        mock_db.execute.assert_called_once_with("SELECT 1")
        
        # Test connection failure
        async def failing_connection():
            raise Exception("Connection failed")
        
        check = DatabaseHealthCheck(failing_connection)
        result = await check.check()
        
        assert result.status == HealthStatus.UNHEALTHY
        assert "Database connection failed" in result.message
    
    @pytest.mark.asyncio
    async def test_kafka_health_check(self):
        """Test Kafka health check."""
        # Mock successful Kafka client
        mock_metadata = Mock()
        mock_metadata.brokers = [Mock(), Mock()]  # 2 brokers
        mock_metadata.topics = [Mock(), Mock(), Mock()]  # 3 topics
        mock_metadata.cluster_id = "test-cluster"
        
        mock_client = Mock()
        mock_client.get_cluster_metadata = AsyncMock(return_value=mock_metadata)
        
        async def create_kafka_client():
            return mock_client
        
        check = KafkaHealthCheck(create_kafka_client)
        result = await check.check()
        
        assert result.status == HealthStatus.HEALTHY
        assert "2 brokers" in result.message
        assert result.details["broker_count"] == 2
        assert result.details["topic_count"] == 3
        
        # Test no brokers available
        mock_metadata.brokers = []
        result = await check.check()
        
        assert result.status == HealthStatus.UNHEALTHY
        assert "No Kafka brokers available" in result.message
    
    @pytest.mark.asyncio
    async def test_service_health_check(self):
        """Test service health check."""
        # Mock healthy service
        async def healthy_service():
            return True, {"status": "running"}
        
        check = ServiceHealthCheck("test_service", healthy_service)
        result = await check.check()
        
        assert result.status == HealthStatus.HEALTHY
        assert "test_service is healthy" in result.message
        assert result.details["status"] == "running"
        
        # Mock unhealthy service
        async def unhealthy_service():
            return False, {"error": "service down"}
        
        check = ServiceHealthCheck("test_service", unhealthy_service)
        result = await check.check()
        
        assert result.status == HealthStatus.UNHEALTHY
        assert "test_service is unhealthy" in result.message
    
    @pytest.mark.asyncio
    async def test_disk_space_health_check(self):
        """Test disk space health check."""
        with patch('shutil.disk_usage') as mock_disk_usage:
            # Mock disk usage: 80% used (warning threshold)
            total = 1000 * 1024**3  # 1TB
            used = 800 * 1024**3   # 800GB
            free = 200 * 1024**3   # 200GB
            mock_disk_usage.return_value = (total, used, free)
            
            check = DiskSpaceHealthCheck(["/tmp"], warning_threshold=0.7, critical_threshold=0.9)
            result = await check.check()
            
            assert result.status == HealthStatus.DEGRADED
            assert "High disk usage" in result.message
            assert result.details["max_usage_percent"] == 80.0
    
    @pytest.mark.asyncio
    async def test_memory_health_check(self):
        """Test memory health check."""
        with patch('kafka_ops_agent.monitoring.health_checks.psutil') as mock_psutil:
            # Mock memory info
            mock_memory = Mock()
            mock_memory.percent = 75.0
            mock_memory.total = 16 * 1024**3  # 16GB
            mock_memory.available = 4 * 1024**3  # 4GB
            mock_memory.used = 12 * 1024**3  # 12GB
            mock_psutil.virtual_memory.return_value = mock_memory
            
            check = MemoryHealthCheck(warning_threshold=0.8, critical_threshold=0.9)
            result = await check.check()
            
            assert result.status == HealthStatus.HEALTHY
            assert "Memory usage OK" in result.message
            assert result.details["usage_percent"] == 75.0


class TestMetrics:
    """Test metrics system."""
    
    def test_histogram_data(self):
        """Test histogram data structure."""
        histogram = HistogramData()
        
        # Add some values
        histogram.add_value(1.0)
        histogram.add_value(2.0)
        histogram.add_value(3.0)
        
        assert histogram.count == 3
        assert histogram.sum == 6.0
        assert histogram.min == 1.0
        assert histogram.max == 3.0
        assert histogram.get_mean() == 2.0
        
        # Test percentiles
        assert histogram.get_percentile(50) == 2.0  # median
        
        # Test to_dict
        data_dict = histogram.to_dict()
        assert data_dict["count"] == 3
        assert data_dict["mean"] == 2.0
    
    def test_metrics_collector(self):
        """Test metrics collector."""
        collector = MetricsCollector()
        
        # Test counter
        collector.increment_counter("test_counter", 5.0)
        collector.increment_counter("test_counter", 3.0)
        
        metrics = collector.get_metrics()
        assert metrics["counters"]["test_counter"] == 8.0
        
        # Test gauge
        collector.set_gauge("test_gauge", 42.0)
        metrics = collector.get_metrics()
        assert metrics["gauges"]["test_gauge"] == 42.0
        
        # Test histogram
        collector.observe_histogram("test_histogram", 1.5)
        collector.observe_histogram("test_histogram", 2.5)
        
        metrics = collector.get_metrics()
        histogram_data = metrics["histograms"]["test_histogram"]
        assert histogram_data["count"] == 2
        assert histogram_data["mean"] == 2.0
        
        # Test with labels
        collector.increment_counter("labeled_counter", 1.0, {"service": "api"})
        collector.increment_counter("labeled_counter", 2.0, {"service": "worker"})
        
        metrics = collector.get_metrics()
        assert "labeled_counter{service=api}" in metrics["counters"]
        assert "labeled_counter{service=worker}" in metrics["counters"]
    
    def test_timer_context(self):
        """Test timer context manager."""
        collector = MetricsCollector()
        
        with collector.time_operation("test_timer"):
            time.sleep(0.01)  # Sleep for 10ms
        
        metrics = collector.get_metrics()
        timer_data = metrics["timers"]["test_timer"]
        assert timer_data["count"] == 1
        assert timer_data["mean"] > 0.005  # Should be at least 5ms
    
    def test_prometheus_format(self):
        """Test Prometheus format output."""
        collector = MetricsCollector()
        
        collector.increment_counter("http_requests_total", 100)
        collector.set_gauge("memory_usage_bytes", 1024)
        
        prometheus_output = collector.get_prometheus_format()
        
        assert "http_requests_total 100" in prometheus_output
        assert "memory_usage_bytes 1024" in prometheus_output
        assert "# TYPE http_requests_total counter" in prometheus_output
        assert "# TYPE memory_usage_bytes gauge" in prometheus_output


class TestAlerts:
    """Test alert system."""
    
    def test_alert_creation(self):
        """Test alert creation."""
        alert = Alert(
            name="test_alert",
            severity=AlertSeverity.WARNING,
            state=AlertState.FIRING,
            message="Test alert message",
            labels={"service": "api"},
            annotations={"runbook": "http://example.com"}
        )
        
        assert alert.name == "test_alert"
        assert alert.severity == AlertSeverity.WARNING
        assert alert.state == AlertState.FIRING
        
        # Test to_dict
        alert_dict = alert.to_dict()
        assert alert_dict["name"] == "test_alert"
        assert alert_dict["severity"] == "warning"
        assert alert_dict["state"] == "firing"
    
    @pytest.mark.asyncio
    async def test_health_status_alert_rule(self):
        """Test health status alert rule."""
        from kafka_ops_agent.monitoring.health_checks import SystemHealthSummary
        
        rule = HealthStatusAlertRule(
            "system_unhealthy",
            AlertSeverity.CRITICAL,
            HealthStatus.UNHEALTHY
        )
        
        # Create unhealthy system summary
        unhealthy_summary = SystemHealthSummary(
            overall_status=HealthStatus.UNHEALTHY,
            healthy_count=1,
            unhealthy_count=2,
            degraded_count=0,
            unknown_count=0,
            total_checks=3,
            check_results=[]
        )
        
        alert = await rule.evaluate(unhealthy_summary, {})
        assert alert is not None
        assert alert.severity == AlertSeverity.CRITICAL
        assert "unhealthy" in alert.message.lower()
        
        # Test healthy system
        healthy_summary = SystemHealthSummary(
            overall_status=HealthStatus.HEALTHY,
            healthy_count=3,
            unhealthy_count=0,
            degraded_count=0,
            unknown_count=0,
            total_checks=3,
            check_results=[]
        )
        
        alert = await rule.evaluate(healthy_summary, {})
        assert alert is None
    
    @pytest.mark.asyncio
    async def test_metric_threshold_alert_rule(self):
        """Test metric threshold alert rule."""
        rule = MetricThresholdAlertRule(
            "high_error_count",
            AlertSeverity.WARNING,
            "error_count",
            threshold=10.0,
            comparison="greater_than"
        )
        
        # Test triggering condition
        metrics = {
            "counters": {"error_count": 15.0}
        }
        
        alert = await rule.evaluate(None, metrics)
        assert alert is not None
        assert alert.severity == AlertSeverity.WARNING
        assert "15.0" in alert.message
        
        # Test non-triggering condition
        metrics = {
            "counters": {"error_count": 5.0}
        }
        
        alert = await rule.evaluate(None, metrics)
        assert alert is None
    
    @pytest.mark.asyncio
    async def test_error_rate_alert_rule(self):
        """Test error rate alert rule."""
        rule = ErrorRateAlertRule(
            "high_error_rate",
            AlertSeverity.WARNING,
            error_rate_threshold=0.05  # 5%
        )
        
        # Test high error rate (10%)
        metrics = {
            "counters": {
                "kafka_ops_errors_total": 10.0,
                "kafka_ops_requests_total": 100.0
            }
        }
        
        alert = await rule.evaluate(None, metrics)
        assert alert is not None
        assert "10.00%" in alert.message
        
        # Test low error rate (1%)
        metrics = {
            "counters": {
                "kafka_ops_errors_total": 1.0,
                "kafka_ops_requests_total": 100.0
            }
        }
        
        alert = await rule.evaluate(None, metrics)
        assert alert is None
    
    @pytest.mark.asyncio
    async def test_log_alert_notifier(self):
        """Test log alert notifier."""
        notifier = LogAlertNotifier()
        
        alert = Alert(
            name="test_alert",
            severity=AlertSeverity.CRITICAL,
            state=AlertState.FIRING,
            message="Test alert"
        )
        
        with patch.object(notifier.logger, 'log') as mock_log:
            success = await notifier.send_alert(alert)
            assert success is True
            mock_log.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_alert_manager(self):
        """Test alert manager."""
        # Mock dependencies
        mock_health_manager = Mock()
        mock_health_manager.run_all_health_checks = AsyncMock()
        
        mock_metrics_collector = Mock()
        mock_metrics_collector.get_metrics.return_value = {}
        
        manager = AlertManager(mock_health_manager, mock_metrics_collector)
        
        # Add a mock alert rule
        mock_rule = Mock(spec=AlertRule)
        mock_rule.name = "test_rule"
        mock_rule.should_check.return_value = True
        mock_rule.evaluate = AsyncMock(return_value=Alert(
            name="test_alert",
            severity=AlertSeverity.WARNING,
            state=AlertState.FIRING,
            message="Test alert"
        ))
        
        manager.add_alert_rule(mock_rule)
        
        # Add a mock notifier
        mock_notifier = Mock(spec=LogAlertNotifier)
        mock_notifier.send_alert = AsyncMock(return_value=True)
        manager.add_notifier(mock_notifier)
        
        # Evaluate rules
        alerts = await manager.evaluate_rules()
        assert len(alerts) == 1
        assert alerts[0].name == "test_alert"
        
        # Process alerts
        await manager.process_alerts(alerts)
        
        # Check active alerts
        active_alerts = manager.get_active_alerts()
        assert len(active_alerts) == 1
        
        # Check alert summary
        summary = manager.get_alert_summary()
        assert summary["active_count"] == 1
        assert summary["total_rules"] == 1


class TestMonitoringIntegration:
    """Test monitoring system integration."""
    
    @pytest.mark.asyncio
    async def test_full_monitoring_workflow(self):
        """Test complete monitoring workflow."""
        from kafka_ops_agent.monitoring.health_checks import get_health_check_manager
        from kafka_ops_agent.monitoring.metrics import get_metrics_collector
        from kafka_ops_agent.monitoring.alerts import initialize_alert_manager, HealthStatusAlertRule
        
        # Set up components
        health_manager = get_health_check_manager()
        metrics_collector = get_metrics_collector()
        alert_manager = initialize_alert_manager(health_manager, metrics_collector)
        
        # Add a simple health check
        class SimpleHealthCheck(HealthCheck):
            def __init__(self, name: str, status: HealthStatus):
                super().__init__(name)
                self.status = status
            
            async def check(self) -> HealthCheckResult:
                return HealthCheckResult(
                    name=self.name,
                    status=self.status,
                    message=f"Status is {self.status.value}"
                )
        
        health_manager.register_health_check(
            SimpleHealthCheck("test_check", HealthStatus.HEALTHY)
        )
        
        # Add some metrics
        metrics_collector.increment_counter("test_requests", 100)
        metrics_collector.set_gauge("test_connections", 5)
        
        # Add alert rule
        alert_manager.add_alert_rule(
            HealthStatusAlertRule(
                "test_unhealthy",
                AlertSeverity.CRITICAL,
                HealthStatus.UNHEALTHY
            )
        )
        
        # Run health checks
        health_summary = await health_manager.run_all_health_checks()
        assert health_summary.overall_status == HealthStatus.HEALTHY
        assert health_summary.total_checks == 1
        
        # Get metrics
        metrics = metrics_collector.get_metrics()
        assert metrics["counters"]["test_requests"] == 100
        assert metrics["gauges"]["test_connections"] == 5
        
        # Evaluate alerts (should be none since system is healthy)
        alerts = await alert_manager.evaluate_rules()
        assert len(alerts) == 0
        
        # Now make system unhealthy
        health_manager.unregister_health_check("test_check")
        health_manager.register_health_check(
            SimpleHealthCheck("test_check", HealthStatus.UNHEALTHY)
        )
        
        # Run again
        health_summary = await health_manager.run_all_health_checks()
        assert health_summary.overall_status == HealthStatus.UNHEALTHY
        
        # Should trigger alert
        alerts = await alert_manager.evaluate_rules()
        assert len(alerts) == 1
        assert alerts[0].severity == AlertSeverity.CRITICAL


if __name__ == '__main__':
    pytest.main([__file__, "-v"])