"""Health check system for monitoring component health."""

import logging
import asyncio
import time
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health check status values."""
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    name: str
    status: HealthStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
            'duration_ms': self.duration_ms
        }


class HealthCheck(ABC):
    """Abstract base class for health checks."""
    
    def __init__(self, name: str, timeout_seconds: float = 30.0):
        """Initialize health check.
        
        Args:
            name: Name of the health check
            timeout_seconds: Timeout for the health check
        """
        self.name = name
        self.timeout_seconds = timeout_seconds
    
    @abstractmethod
    async def check(self) -> HealthCheckResult:
        """Perform the health check.
        
        Returns:
            Health check result
        """
        pass
    
    async def run_check(self) -> HealthCheckResult:
        """Run the health check with timeout and error handling.
        
        Returns:
            Health check result
        """
        start_time = time.time()
        
        try:
            # Run check with timeout
            result = await asyncio.wait_for(
                self.check(), 
                timeout=self.timeout_seconds
            )
            result.duration_ms = (time.time() - start_time) * 1000
            return result
            
        except asyncio.TimeoutError:
            duration_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self.timeout_seconds}s",
                duration_ms=duration_ms
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(f"Health check {self.name} failed: {e}")
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                details={'error': str(e), 'error_type': type(e).__name__},
                duration_ms=duration_ms
            )


class DatabaseHealthCheck(HealthCheck):
    """Health check for database connectivity."""
    
    def __init__(self, get_db_connection: Callable):
        """Initialize database health check.
        
        Args:
            get_db_connection: Function to get database connection
        """
        super().__init__("database", timeout_seconds=10.0)
        self.get_db_connection = get_db_connection
    
    async def check(self) -> HealthCheckResult:
        """Check database connectivity."""
        try:
            # Get database connection
            db = await self.get_db_connection()
            
            # Simple query to test connectivity
            if hasattr(db, 'execute'):
                await db.execute("SELECT 1")
            else:
                # For synchronous connections
                db.execute("SELECT 1")
            
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                message="Database connection successful",
                details={'connection_type': type(db).__name__}
            )
            
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                details={'error': str(e)}
            )


class KafkaHealthCheck(HealthCheck):
    """Health check for Kafka connectivity."""
    
    def __init__(self, kafka_client_factory: Callable):
        """Initialize Kafka health check.
        
        Args:
            kafka_client_factory: Function to create Kafka client
        """
        super().__init__("kafka", timeout_seconds=15.0)
        self.kafka_client_factory = kafka_client_factory
    
    async def check(self) -> HealthCheckResult:
        """Check Kafka connectivity."""
        try:
            # Create Kafka client
            client = await self.kafka_client_factory()
            
            # Get cluster metadata
            metadata = await client.get_cluster_metadata()
            
            broker_count = len(metadata.brokers)
            topic_count = len(metadata.topics)
            
            if broker_count == 0:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message="No Kafka brokers available",
                    details={'broker_count': broker_count, 'topic_count': topic_count}
                )
            
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                message=f"Kafka cluster accessible with {broker_count} brokers",
                details={
                    'broker_count': broker_count,
                    'topic_count': topic_count,
                    'cluster_id': metadata.cluster_id
                }
            )
            
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Kafka connection failed: {str(e)}",
                details={'error': str(e)}
            )


class ServiceHealthCheck(HealthCheck):
    """Health check for internal services."""
    
    def __init__(self, service_name: str, service_checker: Callable):
        """Initialize service health check.
        
        Args:
            service_name: Name of the service
            service_checker: Function to check service health
        """
        super().__init__(f"service_{service_name}", timeout_seconds=5.0)
        self.service_name = service_name
        self.service_checker = service_checker
    
    async def check(self) -> HealthCheckResult:
        """Check service health."""
        try:
            is_healthy, details = await self.service_checker()
            
            if is_healthy:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.HEALTHY,
                    message=f"Service {self.service_name} is healthy",
                    details=details or {}
                )
            else:
                return HealthCheckResult(
                    name=self.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Service {self.service_name} is unhealthy",
                    details=details or {}
                )
                
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Service {self.service_name} check failed: {str(e)}",
                details={'error': str(e)}
            )


class DiskSpaceHealthCheck(HealthCheck):
    """Health check for disk space."""
    
    def __init__(self, paths: List[str], warning_threshold: float = 0.8, 
                 critical_threshold: float = 0.9):
        """Initialize disk space health check.
        
        Args:
            paths: List of paths to check
            warning_threshold: Warning threshold (0.0-1.0)
            critical_threshold: Critical threshold (0.0-1.0)
        """
        super().__init__("disk_space", timeout_seconds=5.0)
        self.paths = paths
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
    
    async def check(self) -> HealthCheckResult:
        """Check disk space usage."""
        import shutil
        
        try:
            disk_info = {}
            max_usage = 0.0
            critical_paths = []
            warning_paths = []
            
            for path in self.paths:
                try:
                    total, used, free = shutil.disk_usage(path)
                    usage_ratio = used / total
                    
                    disk_info[path] = {
                        'total_gb': round(total / (1024**3), 2),
                        'used_gb': round(used / (1024**3), 2),
                        'free_gb': round(free / (1024**3), 2),
                        'usage_percent': round(usage_ratio * 100, 1)
                    }
                    
                    max_usage = max(max_usage, usage_ratio)
                    
                    if usage_ratio >= self.critical_threshold:
                        critical_paths.append(path)
                    elif usage_ratio >= self.warning_threshold:
                        warning_paths.append(path)
                        
                except OSError as e:
                    disk_info[path] = {'error': str(e)}
            
            # Determine overall status
            if critical_paths:
                status = HealthStatus.UNHEALTHY
                message = f"Critical disk usage on: {', '.join(critical_paths)}"
            elif warning_paths:
                status = HealthStatus.DEGRADED
                message = f"High disk usage on: {', '.join(warning_paths)}"
            else:
                status = HealthStatus.HEALTHY
                message = f"Disk usage OK (max: {max_usage*100:.1f}%)"
            
            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details={
                    'disk_info': disk_info,
                    'max_usage_percent': round(max_usage * 100, 1),
                    'warning_threshold': self.warning_threshold,
                    'critical_threshold': self.critical_threshold
                }
            )
            
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Disk space check failed: {str(e)}",
                details={'error': str(e)}
            )


class MemoryHealthCheck(HealthCheck):
    """Health check for memory usage."""
    
    def __init__(self, warning_threshold: float = 0.8, critical_threshold: float = 0.9):
        """Initialize memory health check.
        
        Args:
            warning_threshold: Warning threshold (0.0-1.0)
            critical_threshold: Critical threshold (0.0-1.0)
        """
        super().__init__("memory", timeout_seconds=5.0)
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
    
    async def check(self) -> HealthCheckResult:
        """Check memory usage."""
        try:
            import psutil
            
            memory = psutil.virtual_memory()
            usage_ratio = memory.percent / 100.0
            
            # Determine status
            if usage_ratio >= self.critical_threshold:
                status = HealthStatus.UNHEALTHY
                message = f"Critical memory usage: {memory.percent:.1f}%"
            elif usage_ratio >= self.warning_threshold:
                status = HealthStatus.DEGRADED
                message = f"High memory usage: {memory.percent:.1f}%"
            else:
                status = HealthStatus.HEALTHY
                message = f"Memory usage OK: {memory.percent:.1f}%"
            
            return HealthCheckResult(
                name=self.name,
                status=status,
                message=message,
                details={
                    'total_gb': round(memory.total / (1024**3), 2),
                    'available_gb': round(memory.available / (1024**3), 2),
                    'used_gb': round(memory.used / (1024**3), 2),
                    'usage_percent': memory.percent,
                    'warning_threshold': self.warning_threshold * 100,
                    'critical_threshold': self.critical_threshold * 100
                }
            )
            
        except ImportError:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNKNOWN,
                message="psutil not available for memory monitoring",
                details={'error': 'psutil package not installed'}
            )
        except Exception as e:
            return HealthCheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Memory check failed: {str(e)}",
                details={'error': str(e)}
            )


@dataclass
class SystemHealthSummary:
    """Summary of system health."""
    overall_status: HealthStatus
    healthy_count: int
    unhealthy_count: int
    degraded_count: int
    unknown_count: int
    total_checks: int
    check_results: List[HealthCheckResult]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'overall_status': self.overall_status.value,
            'summary': {
                'healthy': self.healthy_count,
                'unhealthy': self.unhealthy_count,
                'degraded': self.degraded_count,
                'unknown': self.unknown_count,
                'total': self.total_checks
            },
            'checks': [result.to_dict() for result in self.check_results],
            'timestamp': self.timestamp.isoformat()
        }


class HealthCheckManager:
    """Manages and executes health checks."""
    
    def __init__(self):
        """Initialize health check manager."""
        self.health_checks: Dict[str, HealthCheck] = {}
        self.last_results: Dict[str, HealthCheckResult] = {}
        self.check_history: List[SystemHealthSummary] = []
        self.max_history = 100
    
    def register_health_check(self, health_check: HealthCheck):
        """Register a health check.
        
        Args:
            health_check: Health check to register
        """
        self.health_checks[health_check.name] = health_check
        logger.info(f"Registered health check: {health_check.name}")
    
    def unregister_health_check(self, name: str):
        """Unregister a health check.
        
        Args:
            name: Name of health check to unregister
        """
        if name in self.health_checks:
            del self.health_checks[name]
            self.last_results.pop(name, None)
            logger.info(f"Unregistered health check: {name}")
    
    async def run_health_check(self, name: str) -> Optional[HealthCheckResult]:
        """Run a specific health check.
        
        Args:
            name: Name of health check to run
            
        Returns:
            Health check result or None if not found
        """
        health_check = self.health_checks.get(name)
        if not health_check:
            return None
        
        result = await health_check.run_check()
        self.last_results[name] = result
        return result
    
    async def run_all_health_checks(self) -> SystemHealthSummary:
        """Run all registered health checks.
        
        Returns:
            System health summary
        """
        if not self.health_checks:
            return SystemHealthSummary(
                overall_status=HealthStatus.UNKNOWN,
                healthy_count=0,
                unhealthy_count=0,
                degraded_count=0,
                unknown_count=0,
                total_checks=0,
                check_results=[]
            )
        
        # Run all health checks concurrently
        tasks = [
            health_check.run_check() 
            for health_check in self.health_checks.values()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        check_results = []
        status_counts = {status: 0 for status in HealthStatus}
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Handle exceptions from gather
                health_check_name = list(self.health_checks.keys())[i]
                result = HealthCheckResult(
                    name=health_check_name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Health check failed with exception: {str(result)}",
                    details={'error': str(result)}
                )
            
            check_results.append(result)
            status_counts[result.status] += 1
            self.last_results[result.name] = result
        
        # Determine overall status
        if status_counts[HealthStatus.UNHEALTHY] > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif status_counts[HealthStatus.DEGRADED] > 0:
            overall_status = HealthStatus.DEGRADED
        elif status_counts[HealthStatus.UNKNOWN] > 0:
            overall_status = HealthStatus.UNKNOWN
        else:
            overall_status = HealthStatus.HEALTHY
        
        summary = SystemHealthSummary(
            overall_status=overall_status,
            healthy_count=status_counts[HealthStatus.HEALTHY],
            unhealthy_count=status_counts[HealthStatus.UNHEALTHY],
            degraded_count=status_counts[HealthStatus.DEGRADED],
            unknown_count=status_counts[HealthStatus.UNKNOWN],
            total_checks=len(check_results),
            check_results=check_results
        )
        
        # Store in history
        self.check_history.append(summary)
        if len(self.check_history) > self.max_history:
            self.check_history.pop(0)
        
        return summary
    
    def get_last_results(self) -> Dict[str, HealthCheckResult]:
        """Get last health check results.
        
        Returns:
            Dictionary of last results by check name
        """
        return self.last_results.copy()
    
    def get_health_history(self, limit: int = 10) -> List[SystemHealthSummary]:
        """Get health check history.
        
        Args:
            limit: Maximum number of history entries to return
            
        Returns:
            List of health summaries
        """
        return self.check_history[-limit:]
    
    def get_health_check_names(self) -> List[str]:
        """Get names of registered health checks.
        
        Returns:
            List of health check names
        """
        return list(self.health_checks.keys())


# Global health check manager
_health_check_manager: Optional[HealthCheckManager] = None


def get_health_check_manager() -> HealthCheckManager:
    """Get global health check manager.
    
    Returns:
        Health check manager instance
    """
    global _health_check_manager
    
    if _health_check_manager is None:
        _health_check_manager = HealthCheckManager()
    
    return _health_check_manager