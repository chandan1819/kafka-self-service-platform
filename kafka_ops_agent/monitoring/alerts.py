"""Alert management system for monitoring."""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from .health_checks import HealthStatus, HealthCheckResult, SystemHealthSummary
from .metrics import MetricsCollector

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertState(str, Enum):
    """Alert states."""
    FIRING = "firing"
    RESOLVED = "resolved"
    PENDING = "pending"


@dataclass
class Alert:
    """Individual alert."""
    name: str
    severity: AlertSeverity
    state: AlertState
    message: str
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    starts_at: datetime = field(default_factory=datetime.utcnow)
    ends_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'severity': self.severity.value,
            'state': self.state.value,
            'message': self.message,
            'labels': self.labels,
            'annotations': self.annotations,
            'starts_at': self.starts_at.isoformat(),
            'ends_at': self.ends_at.isoformat() if self.ends_at else None
        }


class AlertRule(ABC):
    """Abstract base class for alert rules."""
    
    def __init__(self, name: str, severity: AlertSeverity, 
                 check_interval: int = 60):
        """Initialize alert rule.
        
        Args:
            name: Rule name
            severity: Alert severity
            check_interval: Check interval in seconds
        """
        self.name = name
        self.severity = severity
        self.check_interval = check_interval
        self.last_check = None
        self.last_alert_state = AlertState.RESOLVED
    
    @abstractmethod
    async def evaluate(self, health_summary: SystemHealthSummary,
                      metrics: Dict[str, Any]) -> Optional[Alert]:
        """Evaluate the alert rule.
        
        Args:
            health_summary: Current health summary
            metrics: Current metrics
            
        Returns:
            Alert if rule is triggered, None otherwise
        """
        pass
    
    def should_check(self) -> bool:
        """Check if rule should be evaluated."""
        if self.last_check is None:
            return True
        
        return (datetime.utcnow() - self.last_check).seconds >= self.check_interval


class HealthStatusAlertRule(AlertRule):
    """Alert rule based on health check status."""
    
    def __init__(self, name: str, severity: AlertSeverity,
                 target_status: HealthStatus, check_names: Optional[List[str]] = None):
        """Initialize health status alert rule.
        
        Args:
            name: Rule name
            severity: Alert severity
            target_status: Status to alert on
            check_names: Specific health checks to monitor (None for all)
        """
        super().__init__(name, severity)
        self.target_status = target_status
        self.check_names = check_names
    
    async def evaluate(self, health_summary: SystemHealthSummary,
                      metrics: Dict[str, Any]) -> Optional[Alert]:
        """Evaluate health status alert rule."""
        self.last_check = datetime.utcnow()
        
        # Check overall status
        if self.check_names is None:
            if health_summary.overall_status == self.target_status:
                return Alert(
                    name=self.name,
                    severity=self.severity,
                    state=AlertState.FIRING,
                    message=f"System health is {self.target_status.value}",
                    labels={'alert_type': 'health_status'},
                    annotations={
                        'overall_status': health_summary.overall_status.value,
                        'unhealthy_count': str(health_summary.unhealthy_count),
                        'total_checks': str(health_summary.total_checks)
                    }
                )
        else:
            # Check specific health checks
            failing_checks = []
            for result in health_summary.check_results:
                if (result.name in self.check_names and 
                    result.status == self.target_status):
                    failing_checks.append(result.name)
            
            if failing_checks:
                return Alert(
                    name=self.name,
                    severity=self.severity,
                    state=AlertState.FIRING,
                    message=f"Health checks failing: {', '.join(failing_checks)}",
                    labels={'alert_type': 'health_status'},
                    annotations={
                        'failing_checks': ','.join(failing_checks),
                        'target_status': self.target_status.value
                    }
                )
        
        return None


class MetricThresholdAlertRule(AlertRule):
    """Alert rule based on metric thresholds."""
    
    def __init__(self, name: str, severity: AlertSeverity,
                 metric_name: str, threshold: float, 
                 comparison: str = "greater_than"):
        """Initialize metric threshold alert rule.
        
        Args:
            name: Rule name
            severity: Alert severity
            metric_name: Name of metric to monitor
            threshold: Threshold value
            comparison: Comparison operator (greater_than, less_than, equals)
        """
        super().__init__(name, severity)
        self.metric_name = metric_name
        self.threshold = threshold
        self.comparison = comparison
    
    async def evaluate(self, health_summary: SystemHealthSummary,
                      metrics: Dict[str, Any]) -> Optional[Alert]:
        """Evaluate metric threshold alert rule."""
        self.last_check = datetime.utcnow()
        
        # Find metric value
        metric_value = None
        
        # Check counters
        if self.metric_name in metrics.get('counters', {}):
            metric_value = metrics['counters'][self.metric_name]
        
        # Check gauges
        elif self.metric_name in metrics.get('gauges', {}):
            metric_value = metrics['gauges'][self.metric_name]
        
        # Check histograms (use count)
        elif self.metric_name in metrics.get('histograms', {}):
            metric_value = metrics['histograms'][self.metric_name]['count']
        
        if metric_value is None:
            return None
        
        # Evaluate threshold
        triggered = False
        if self.comparison == "greater_than":
            triggered = metric_value > self.threshold
        elif self.comparison == "less_than":
            triggered = metric_value < self.threshold
        elif self.comparison == "equals":
            triggered = metric_value == self.threshold
        
        if triggered:
            return Alert(
                name=self.name,
                severity=self.severity,
                state=AlertState.FIRING,
                message=f"Metric {self.metric_name} is {metric_value} (threshold: {self.threshold})",
                labels={'alert_type': 'metric_threshold'},
                annotations={
                    'metric_name': self.metric_name,
                    'current_value': str(metric_value),
                    'threshold': str(self.threshold),
                    'comparison': self.comparison
                }
            )
        
        return None


class ErrorRateAlertRule(AlertRule):
    """Alert rule for error rates."""
    
    def __init__(self, name: str, severity: AlertSeverity,
                 error_rate_threshold: float = 0.05, window_minutes: int = 5):
        """Initialize error rate alert rule.
        
        Args:
            name: Rule name
            severity: Alert severity
            error_rate_threshold: Error rate threshold (0.0-1.0)
            window_minutes: Time window for calculation
        """
        super().__init__(name, severity)
        self.error_rate_threshold = error_rate_threshold
        self.window_minutes = window_minutes
    
    async def evaluate(self, health_summary: SystemHealthSummary,
                      metrics: Dict[str, Any]) -> Optional[Alert]:
        """Evaluate error rate alert rule."""
        self.last_check = datetime.utcnow()
        
        # Get error and request counts
        error_count = metrics.get('counters', {}).get('kafka_ops_errors_total', 0)
        request_count = metrics.get('counters', {}).get('kafka_ops_requests_total', 0)
        
        if request_count == 0:
            return None
        
        error_rate = error_count / request_count
        
        if error_rate > self.error_rate_threshold:
            return Alert(
                name=self.name,
                severity=self.severity,
                state=AlertState.FIRING,
                message=f"High error rate: {error_rate:.2%} (threshold: {self.error_rate_threshold:.2%})",
                labels={'alert_type': 'error_rate'},
                annotations={
                    'error_rate': f"{error_rate:.2%}",
                    'error_count': str(error_count),
                    'request_count': str(request_count),
                    'threshold': f"{self.error_rate_threshold:.2%}"
                }
            )
        
        return None


class AlertNotifier(ABC):
    """Abstract base class for alert notifiers."""
    
    @abstractmethod
    async def send_alert(self, alert: Alert) -> bool:
        """Send an alert notification.
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully, False otherwise
        """
        pass


class LogAlertNotifier(AlertNotifier):
    """Alert notifier that logs alerts."""
    
    def __init__(self, logger_name: str = "kafka_ops_agent.alerts"):
        """Initialize log alert notifier.
        
        Args:
            logger_name: Logger name to use
        """
        self.logger = logging.getLogger(logger_name)
    
    async def send_alert(self, alert: Alert) -> bool:
        """Send alert to logs."""
        try:
            log_level = logging.ERROR if alert.severity == AlertSeverity.CRITICAL else logging.WARNING
            self.logger.log(
                log_level,
                f"ALERT [{alert.severity.value.upper()}] {alert.name}: {alert.message}",
                extra={
                    'alert_name': alert.name,
                    'alert_severity': alert.severity.value,
                    'alert_state': alert.state.value,
                    'alert_labels': alert.labels,
                    'alert_annotations': alert.annotations
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send log alert: {e}")
            return False


class WebhookAlertNotifier(AlertNotifier):
    """Alert notifier that sends webhooks."""
    
    def __init__(self, webhook_url: str, timeout: int = 30):
        """Initialize webhook alert notifier.
        
        Args:
            webhook_url: Webhook URL to send alerts to
            timeout: Request timeout in seconds
        """
        self.webhook_url = webhook_url
        self.timeout = timeout
    
    async def send_alert(self, alert: Alert) -> bool:
        """Send alert via webhook."""
        try:
            import aiohttp
            
            payload = {
                'alert': alert.to_dict(),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    return response.status < 400
                    
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
            return False


class AlertManager:
    """Manages alert rules and notifications."""
    
    def __init__(self, health_check_manager, metrics_collector: MetricsCollector):
        """Initialize alert manager.
        
        Args:
            health_check_manager: Health check manager
            metrics_collector: Metrics collector
        """
        self.health_check_manager = health_check_manager
        self.metrics_collector = metrics_collector
        self.alert_rules: List[AlertRule] = []
        self.notifiers: List[AlertNotifier] = []
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.max_history = 1000
        self.running = False
        self.check_task = None
    
    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule.
        
        Args:
            rule: Alert rule to add
        """
        self.alert_rules.append(rule)
        logger.info(f"Added alert rule: {rule.name}")
    
    def remove_alert_rule(self, rule_name: str):
        """Remove an alert rule.
        
        Args:
            rule_name: Name of rule to remove
        """
        self.alert_rules = [r for r in self.alert_rules if r.name != rule_name]
        logger.info(f"Removed alert rule: {rule_name}")
    
    def add_notifier(self, notifier: AlertNotifier):
        """Add an alert notifier.
        
        Args:
            notifier: Alert notifier to add
        """
        self.notifiers.append(notifier)
        logger.info(f"Added alert notifier: {type(notifier).__name__}")
    
    async def evaluate_rules(self) -> List[Alert]:
        """Evaluate all alert rules.
        
        Returns:
            List of triggered alerts
        """
        # Get current health and metrics
        health_summary = await self.health_check_manager.run_all_health_checks()
        metrics = self.metrics_collector.get_metrics()
        
        triggered_alerts = []
        
        for rule in self.alert_rules:
            if not rule.should_check():
                continue
            
            try:
                alert = await rule.evaluate(health_summary, metrics)
                if alert:
                    triggered_alerts.append(alert)
            except Exception as e:
                logger.error(f"Error evaluating alert rule {rule.name}: {e}")
        
        return triggered_alerts
    
    async def process_alerts(self, alerts: List[Alert]):
        """Process triggered alerts.
        
        Args:
            alerts: List of alerts to process
        """
        for alert in alerts:
            # Check if this is a new alert or state change
            existing_alert = self.active_alerts.get(alert.name)
            
            if existing_alert is None or existing_alert.state != alert.state:
                # New alert or state change
                self.active_alerts[alert.name] = alert
                self.alert_history.append(alert)
                
                # Trim history
                if len(self.alert_history) > self.max_history:
                    self.alert_history.pop(0)
                
                # Send notifications
                await self.send_notifications(alert)
        
        # Check for resolved alerts
        current_alert_names = {alert.name for alert in alerts}
        resolved_alerts = []
        
        for name, active_alert in list(self.active_alerts.items()):
            if (name not in current_alert_names and 
                active_alert.state == AlertState.FIRING):
                # Alert resolved
                resolved_alert = Alert(
                    name=name,
                    severity=active_alert.severity,
                    state=AlertState.RESOLVED,
                    message=f"Alert {name} resolved",
                    labels=active_alert.labels,
                    annotations=active_alert.annotations,
                    starts_at=active_alert.starts_at,
                    ends_at=datetime.utcnow()
                )
                
                self.active_alerts[name] = resolved_alert
                self.alert_history.append(resolved_alert)
                resolved_alerts.append(resolved_alert)
        
        # Send resolved notifications
        for alert in resolved_alerts:
            await self.send_notifications(alert)
    
    async def send_notifications(self, alert: Alert):
        """Send alert notifications.
        
        Args:
            alert: Alert to send
        """
        for notifier in self.notifiers:
            try:
                success = await notifier.send_alert(alert)
                if not success:
                    logger.warning(f"Failed to send alert via {type(notifier).__name__}")
            except Exception as e:
                logger.error(f"Error sending alert via {type(notifier).__name__}: {e}")
    
    async def start(self, check_interval: int = 60):
        """Start the alert manager.
        
        Args:
            check_interval: Check interval in seconds
        """
        if self.running:
            return
        
        self.running = True
        self.check_task = asyncio.create_task(self._check_loop(check_interval))
        logger.info("Alert manager started")
    
    async def stop(self):
        """Stop the alert manager."""
        if not self.running:
            return
        
        self.running = False
        if self.check_task:
            self.check_task.cancel()
            try:
                await self.check_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Alert manager stopped")
    
    async def _check_loop(self, check_interval: int):
        """Main alert checking loop."""
        while self.running:
            try:
                alerts = await self.evaluate_rules()
                await self.process_alerts(alerts)
            except Exception as e:
                logger.error(f"Error in alert check loop: {e}")
            
            await asyncio.sleep(check_interval)
    
    def get_active_alerts(self) -> List[Alert]:
        """Get currently active alerts.
        
        Returns:
            List of active alerts
        """
        return [alert for alert in self.active_alerts.values() 
                if alert.state == AlertState.FIRING]
    
    def get_alert_history(self, limit: int = 100) -> List[Alert]:
        """Get alert history.
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of historical alerts
        """
        return self.alert_history[-limit:]
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get alert summary.
        
        Returns:
            Alert summary dictionary
        """
        active_alerts = self.get_active_alerts()
        
        severity_counts = {severity: 0 for severity in AlertSeverity}
        for alert in active_alerts:
            severity_counts[alert.severity] += 1
        
        return {
            'active_count': len(active_alerts),
            'total_rules': len(self.alert_rules),
            'severity_counts': {k.value: v for k, v in severity_counts.items()},
            'last_check': datetime.utcnow().isoformat(),
            'active_alerts': [alert.to_dict() for alert in active_alerts]
        }


# Global alert manager
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> Optional[AlertManager]:
    """Get global alert manager.
    
    Returns:
        Alert manager instance or None if not initialized
    """
    return _alert_manager


def initialize_alert_manager(health_check_manager, metrics_collector: MetricsCollector) -> AlertManager:
    """Initialize global alert manager.
    
    Args:
        health_check_manager: Health check manager
        metrics_collector: Metrics collector
        
    Returns:
        Alert manager instance
    """
    global _alert_manager
    
    _alert_manager = AlertManager(health_check_manager, metrics_collector)
    return _alert_manager