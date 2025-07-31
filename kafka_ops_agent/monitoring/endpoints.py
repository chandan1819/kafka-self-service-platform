"""HTTP endpoints for monitoring and health checks."""

import logging
import asyncio
from typing import Dict, Any, Optional
from flask import Flask, jsonify, request
from datetime import datetime

from .health_checks import get_health_check_manager, HealthStatus
from .metrics import get_metrics_collector
from .alerts import get_alert_manager

logger = logging.getLogger(__name__)


def create_monitoring_app(app_name: str = "kafka-ops-monitoring") -> Flask:
    """Create Flask app with monitoring endpoints.
    
    Args:
        app_name: Name of the Flask application
        
    Returns:
        Flask application with monitoring endpoints
    """
    app = Flask(app_name)
    
    # Health check endpoints
    @app.route('/health', methods=['GET'])
    async def health_check():
        """Basic health check endpoint."""
        try:
            health_manager = get_health_check_manager()
            summary = await health_manager.run_all_health_checks()
            
            status_code = 200
            if summary.overall_status == HealthStatus.UNHEALTHY:
                status_code = 503
            elif summary.overall_status == HealthStatus.DEGRADED:
                status_code = 200  # Still serving but degraded
            
            return jsonify(summary.to_dict()), status_code
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return jsonify({
                'overall_status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }), 503
    
    @app.route('/health/ready', methods=['GET'])
    async def readiness_check():
        """Kubernetes readiness probe endpoint."""
        try:
            health_manager = get_health_check_manager()
            summary = await health_manager.run_all_health_checks()
            
            # Ready if not unhealthy
            if summary.overall_status != HealthStatus.UNHEALTHY:
                return jsonify({
                    'status': 'ready',
                    'timestamp': datetime.utcnow().isoformat()
                }), 200
            else:
                return jsonify({
                    'status': 'not_ready',
                    'reason': f"{summary.unhealthy_count} unhealthy checks",
                    'timestamp': datetime.utcnow().isoformat()
                }), 503
                
        except Exception as e:
            logger.error(f"Readiness check failed: {e}")
            return jsonify({
                'status': 'not_ready',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }), 503
    
    @app.route('/health/live', methods=['GET'])
    def liveness_check():
        """Kubernetes liveness probe endpoint."""
        # Simple liveness check - just return OK if the service is running
        return jsonify({
            'status': 'alive',
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    
    @app.route('/health/checks', methods=['GET'])
    async def list_health_checks():
        """List all registered health checks."""
        try:
            health_manager = get_health_check_manager()
            check_names = health_manager.get_health_check_names()
            last_results = health_manager.get_last_results()
            
            checks = []
            for name in check_names:
                check_info = {'name': name}
                if name in last_results:
                    check_info.update(last_results[name].to_dict())
                checks.append(check_info)
            
            return jsonify({
                'checks': checks,
                'total_count': len(check_names),
                'timestamp': datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to list health checks: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/health/checks/<check_name>', methods=['GET'])
    async def run_specific_health_check(check_name: str):
        """Run a specific health check."""
        try:
            health_manager = get_health_check_manager()
            result = await health_manager.run_health_check(check_name)
            
            if result is None:
                return jsonify({'error': f'Health check {check_name} not found'}), 404
            
            status_code = 200
            if result.status == HealthStatus.UNHEALTHY:
                status_code = 503
            
            return jsonify(result.to_dict()), status_code
            
        except Exception as e:
            logger.error(f"Failed to run health check {check_name}: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/health/history', methods=['GET'])
    def health_history():
        """Get health check history."""
        try:
            limit = request.args.get('limit', 10, type=int)
            health_manager = get_health_check_manager()
            history = health_manager.get_health_history(limit)
            
            return jsonify({
                'history': [summary.to_dict() for summary in history],
                'count': len(history),
                'timestamp': datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to get health history: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Metrics endpoints
    @app.route('/metrics', methods=['GET'])
    def get_metrics():
        """Get current metrics."""
        try:
            metrics_collector = get_metrics_collector()
            metrics = metrics_collector.get_metrics()
            
            return jsonify(metrics), 200
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/metrics/prometheus', methods=['GET'])
    def get_prometheus_metrics():
        """Get metrics in Prometheus format."""
        try:
            metrics_collector = get_metrics_collector()
            prometheus_format = metrics_collector.get_prometheus_format()
            
            return prometheus_format, 200, {'Content-Type': 'text/plain; charset=utf-8'}
            
        except Exception as e:
            logger.error(f"Failed to get Prometheus metrics: {e}")
            return f"# Error getting metrics: {str(e)}", 500, {'Content-Type': 'text/plain'}
    
    @app.route('/metrics/history', methods=['GET'])
    def metrics_history():
        """Get metrics history."""
        try:
            limit = request.args.get('limit', 10, type=int)
            metrics_collector = get_metrics_collector()
            history = metrics_collector.get_metric_history(limit)
            
            return jsonify({
                'history': history,
                'count': len(history),
                'timestamp': datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to get metrics history: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Alert endpoints
    @app.route('/alerts', methods=['GET'])
    def get_alerts():
        """Get current alerts."""
        try:
            alert_manager = get_alert_manager()
            if alert_manager is None:
                return jsonify({
                    'message': 'Alert manager not initialized',
                    'active_alerts': [],
                    'summary': {}
                }), 200
            
            summary = alert_manager.get_alert_summary()
            return jsonify(summary), 200
            
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/alerts/history', methods=['GET'])
    def alerts_history():
        """Get alert history."""
        try:
            limit = request.args.get('limit', 100, type=int)
            alert_manager = get_alert_manager()
            
            if alert_manager is None:
                return jsonify({
                    'message': 'Alert manager not initialized',
                    'history': []
                }), 200
            
            history = alert_manager.get_alert_history(limit)
            
            return jsonify({
                'history': [alert.to_dict() for alert in history],
                'count': len(history),
                'timestamp': datetime.utcnow().isoformat()
            }), 200
            
        except Exception as e:
            logger.error(f"Failed to get alert history: {e}")
            return jsonify({'error': str(e)}), 500
    
    # System info endpoint
    @app.route('/info', methods=['GET'])
    def system_info():
        """Get system information."""
        try:
            import platform
            import sys
            import os
            
            health_manager = get_health_check_manager()
            metrics_collector = get_metrics_collector()
            alert_manager = get_alert_manager()
            
            info = {
                'service': 'kafka-ops-agent',
                'version': '1.0.0',  # TODO: Get from package
                'python_version': sys.version,
                'platform': platform.platform(),
                'hostname': platform.node(),
                'pid': os.getpid(),
                'uptime_seconds': 0,  # TODO: Track actual uptime
                'components': {
                    'health_checks': len(health_manager.get_health_check_names()),
                    'alert_manager_active': alert_manager is not None,
                    'metrics_collector_active': True
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return jsonify(info), 200
            
        except Exception as e:
            logger.error(f"Failed to get system info: {e}")
            return jsonify({'error': str(e)}), 500
    
    # Status endpoint combining health, metrics, and alerts
    @app.route('/status', methods=['GET'])
    async def overall_status():
        """Get overall system status."""
        try:
            # Get health status
            health_manager = get_health_check_manager()
            health_summary = await health_manager.run_all_health_checks()
            
            # Get metrics
            metrics_collector = get_metrics_collector()
            metrics = metrics_collector.get_metrics()
            
            # Get alerts
            alert_manager = get_alert_manager()
            alert_summary = {}
            if alert_manager:
                alert_summary = alert_manager.get_alert_summary()
            
            # Determine overall status
            overall_status = "healthy"
            if health_summary.overall_status == HealthStatus.UNHEALTHY:
                overall_status = "unhealthy"
            elif health_summary.overall_status == HealthStatus.DEGRADED:
                overall_status = "degraded"
            elif alert_summary.get('active_count', 0) > 0:
                # Check if there are critical alerts
                critical_alerts = sum(1 for alert in alert_summary.get('active_alerts', [])
                                    if alert.get('severity') == 'critical')
                if critical_alerts > 0:
                    overall_status = "unhealthy"
                else:
                    overall_status = "degraded"
            
            status_code = 200
            if overall_status == "unhealthy":
                status_code = 503
            
            return jsonify({
                'overall_status': overall_status,
                'health': health_summary.to_dict(),
                'metrics_summary': {
                    'counters_count': len(metrics.get('counters', {})),
                    'gauges_count': len(metrics.get('gauges', {})),
                    'histograms_count': len(metrics.get('histograms', {})),
                    'last_updated': metrics.get('timestamp')
                },
                'alerts': alert_summary,
                'timestamp': datetime.utcnow().isoformat()
            }), status_code
            
        except Exception as e:
            logger.error(f"Failed to get overall status: {e}")
            return jsonify({
                'overall_status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }), 503
    
    # Error handlers
    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 errors."""
        return jsonify({
            'error': 'Endpoint not found',
            'message': 'The requested monitoring endpoint does not exist',
            'timestamp': datetime.utcnow().isoformat()
        }), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 errors."""
        return jsonify({
            'error': 'Internal server error',
            'message': 'An unexpected error occurred',
            'timestamp': datetime.utcnow().isoformat()
        }), 500
    
    return app


def setup_default_health_checks():
    """Set up default health checks."""
    from .health_checks import (
        DiskSpaceHealthCheck, MemoryHealthCheck, 
        get_health_check_manager
    )
    
    health_manager = get_health_check_manager()
    
    # Add disk space check
    disk_check = DiskSpaceHealthCheck(['/tmp', '.'])
    health_manager.register_health_check(disk_check)
    
    # Add memory check
    memory_check = MemoryHealthCheck()
    health_manager.register_health_check(memory_check)
    
    logger.info("Default health checks registered")


def setup_default_alerts():
    """Set up default alert rules."""
    from .alerts import (
        HealthStatusAlertRule, MetricThresholdAlertRule, ErrorRateAlertRule,
        LogAlertNotifier, AlertSeverity, get_alert_manager,
        initialize_alert_manager
    )
    from .health_checks import HealthStatus, get_health_check_manager
    
    health_manager = get_health_check_manager()
    metrics_collector = get_metrics_collector()
    
    # Initialize alert manager
    alert_manager = initialize_alert_manager(health_manager, metrics_collector)
    
    # Add default alert rules
    alert_manager.add_alert_rule(
        HealthStatusAlertRule(
            "system_unhealthy",
            AlertSeverity.CRITICAL,
            HealthStatus.UNHEALTHY
        )
    )
    
    alert_manager.add_alert_rule(
        ErrorRateAlertRule(
            "high_error_rate",
            AlertSeverity.WARNING,
            error_rate_threshold=0.05
        )
    )
    
    alert_manager.add_alert_rule(
        MetricThresholdAlertRule(
            "high_request_count",
            AlertSeverity.INFO,
            "kafka_ops_requests_total",
            threshold=1000,
            comparison="greater_than"
        )
    )
    
    # Add log notifier
    alert_manager.add_notifier(LogAlertNotifier())
    
    logger.info("Default alert rules and notifiers configured")
    
    return alert_manager


if __name__ == '__main__':
    # For testing the monitoring app standalone
    import sys
    import os
    
    # Add parent directory to path for imports
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create and configure the app
    app = create_monitoring_app()
    setup_default_health_checks()
    alert_manager = setup_default_alerts()
    
    # Start alert manager
    async def start_alert_manager():
        await alert_manager.start()
    
    # Run the app
    print("Starting monitoring server on http://localhost:8080")
    print("Available endpoints:")
    print("  GET /health - Health check")
    print("  GET /health/ready - Readiness probe")
    print("  GET /health/live - Liveness probe")
    print("  GET /metrics - Current metrics")
    print("  GET /metrics/prometheus - Prometheus format metrics")
    print("  GET /alerts - Current alerts")
    print("  GET /status - Overall system status")
    print("  GET /info - System information")
    
    app.run(host='0.0.0.0', port=8080, debug=True)