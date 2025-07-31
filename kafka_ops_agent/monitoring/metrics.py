"""Metrics collection and reporting system."""

import logging
import time
import threading
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from collections import defaultdict, deque
import statistics

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Metric:
    """Individual metric data point."""
    name: str
    type: MetricType
    value: Union[int, float]
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'type': self.type.value,
            'value': self.value,
            'labels': self.labels,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class HistogramData:
    """Histogram metric data."""
    count: int = 0
    sum: float = 0.0
    min: float = float('inf')
    max: float = float('-inf')
    buckets: Dict[float, int] = field(default_factory=dict)
    values: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def add_value(self, value: float):
        """Add a value to the histogram."""
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        self.values.append(value)
        
        # Update buckets
        for bucket_limit in self.buckets:
            if value <= bucket_limit:
                self.buckets[bucket_limit] += 1
    
    def get_percentile(self, percentile: float) -> float:
        """Get percentile value."""
        if not self.values:
            return 0.0
        
        sorted_values = sorted(self.values)
        index = int((percentile / 100.0) * len(sorted_values))
        return sorted_values[min(index, len(sorted_values) - 1)]
    
    def get_mean(self) -> float:
        """Get mean value."""
        return self.sum / self.count if self.count > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'count': self.count,
            'sum': self.sum,
            'min': self.min if self.min != float('inf') else 0.0,
            'max': self.max if self.max != float('-inf') else 0.0,
            'mean': self.get_mean(),
            'p50': self.get_percentile(50),
            'p95': self.get_percentile(95),
            'p99': self.get_percentile(99),
            'buckets': self.buckets
        }


class MetricsCollector:
    """Collects and manages application metrics."""
    
    def __init__(self, retention_hours: int = 24):
        """Initialize metrics collector.
        
        Args:
            retention_hours: How long to retain metrics data
        """
        self.retention_hours = retention_hours
        self.retention_delta = timedelta(hours=retention_hours)
        
        # Metric storage
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, HistogramData] = defaultdict(HistogramData)
        self.timers: Dict[str, HistogramData] = defaultdict(HistogramData)
        
        # Metric metadata
        self.metric_labels: Dict[str, Dict[str, str]] = {}
        self.metric_help: Dict[str, str] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Metric history for time series
        self.metric_history: List[Dict[str, Any]] = []
        self.max_history = 1000
        
        # Built-in metrics
        self._register_builtin_metrics()
    
    def _register_builtin_metrics(self):
        """Register built-in system metrics."""
        self.register_metric(
            "kafka_ops_requests_total",
            MetricType.COUNTER,
            "Total number of requests processed"
        )
        
        self.register_metric(
            "kafka_ops_request_duration_seconds",
            MetricType.HISTOGRAM,
            "Request duration in seconds"
        )
        
        self.register_metric(
            "kafka_ops_active_connections",
            MetricType.GAUGE,
            "Number of active connections"
        )
        
        self.register_metric(
            "kafka_ops_topics_total",
            MetricType.GAUGE,
            "Total number of topics managed"
        )
        
        self.register_metric(
            "kafka_ops_clusters_total",
            MetricType.GAUGE,
            "Total number of clusters managed"
        )
        
        self.register_metric(
            "kafka_ops_errors_total",
            MetricType.COUNTER,
            "Total number of errors encountered"
        )
    
    def register_metric(self, name: str, metric_type: MetricType, 
                       help_text: str, labels: Optional[Dict[str, str]] = None):
        """Register a metric.
        
        Args:
            name: Metric name
            metric_type: Type of metric
            help_text: Help text describing the metric
            labels: Default labels for the metric
        """
        with self.lock:
            self.metric_help[name] = help_text
            if labels:
                self.metric_labels[name] = labels
            
            # Initialize metric storage based on type
            if metric_type == MetricType.COUNTER:
                self.counters[name] = 0.0
            elif metric_type == MetricType.GAUGE:
                self.gauges[name] = 0.0
            elif metric_type == MetricType.HISTOGRAM:
                self.histograms[name] = HistogramData()
            elif metric_type == MetricType.TIMER:
                self.timers[name] = HistogramData()
        
        logger.debug(f"Registered metric: {name} ({metric_type.value})")
    
    def increment_counter(self, name: str, value: float = 1.0, 
                         labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric.
        
        Args:
            name: Counter name
            value: Value to increment by
            labels: Labels for this metric instance
        """
        with self.lock:
            metric_key = self._get_metric_key(name, labels)
            self.counters[metric_key] += value
    
    def set_gauge(self, name: str, value: float, 
                  labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric value.
        
        Args:
            name: Gauge name
            value: Value to set
            labels: Labels for this metric instance
        """
        with self.lock:
            metric_key = self._get_metric_key(name, labels)
            self.gauges[metric_key] = value
    
    def observe_histogram(self, name: str, value: float,
                         labels: Optional[Dict[str, str]] = None):
        """Observe a value in a histogram metric.
        
        Args:
            name: Histogram name
            value: Value to observe
            labels: Labels for this metric instance
        """
        with self.lock:
            metric_key = self._get_metric_key(name, labels)
            if metric_key not in self.histograms:
                self.histograms[metric_key] = HistogramData()
            self.histograms[metric_key].add_value(value)
    
    def time_operation(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations.
        
        Args:
            name: Timer name
            labels: Labels for this metric instance
            
        Returns:
            Context manager for timing
        """
        return TimerContext(self, name, labels)
    
    def _get_metric_key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        """Get metric key with labels."""
        if not labels:
            return name
        
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all current metrics.
        
        Returns:
            Dictionary of all metrics
        """
        with self.lock:
            metrics = {
                'counters': dict(self.counters),
                'gauges': dict(self.gauges),
                'histograms': {k: v.to_dict() for k, v in self.histograms.items()},
                'timers': {k: v.to_dict() for k, v in self.timers.items()},
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Add to history
            self.metric_history.append(metrics)
            if len(self.metric_history) > self.max_history:
                self.metric_history.pop(0)
            
            return metrics
    
    def get_metric_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get metric history.
        
        Args:
            limit: Maximum number of history entries
            
        Returns:
            List of metric snapshots
        """
        with self.lock:
            return self.metric_history[-limit:]
    
    def reset_metrics(self):
        """Reset all metrics."""
        with self.lock:
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()
            self.timers.clear()
            self.metric_history.clear()
    
    def get_prometheus_format(self) -> str:
        """Get metrics in Prometheus format.
        
        Returns:
            Metrics in Prometheus exposition format
        """
        with self.lock:
            lines = []
            
            # Add help text
            for name, help_text in self.metric_help.items():
                lines.append(f"# HELP {name} {help_text}")
            
            # Add counters
            for metric_key, value in self.counters.items():
                name, labels = self._parse_metric_key(metric_key)
                lines.append(f"# TYPE {name} counter")
                if labels:
                    lines.append(f"{name}{{{labels}}} {value}")
                else:
                    lines.append(f"{name} {value}")
            
            # Add gauges
            for metric_key, value in self.gauges.items():
                name, labels = self._parse_metric_key(metric_key)
                lines.append(f"# TYPE {name} gauge")
                if labels:
                    lines.append(f"{name}{{{labels}}} {value}")
                else:
                    lines.append(f"{name} {value}")
            
            # Add histograms
            for metric_key, histogram in self.histograms.items():
                name, labels = self._parse_metric_key(metric_key)
                lines.append(f"# TYPE {name} histogram")
                
                label_prefix = f"{{{labels}}}" if labels else ""
                lines.append(f"{name}_count{label_prefix} {histogram.count}")
                lines.append(f"{name}_sum{label_prefix} {histogram.sum}")
                
                for bucket_limit, count in histogram.buckets.items():
                    bucket_labels = f"le=\"{bucket_limit}\""
                    if labels:
                        bucket_labels = f"{labels},{bucket_labels}"
                    lines.append(f"{name}_bucket{{{bucket_labels}}} {count}")
            
            return "\n".join(lines)
    
    def _parse_metric_key(self, metric_key: str) -> tuple[str, str]:
        """Parse metric key into name and labels."""
        if '{' in metric_key:
            name, labels_part = metric_key.split('{', 1)
            labels = labels_part.rstrip('}')
            return name, labels
        return metric_key, ""


class TimerContext:
    """Context manager for timing operations."""
    
    def __init__(self, collector: MetricsCollector, name: str, 
                 labels: Optional[Dict[str, str]] = None):
        """Initialize timer context.
        
        Args:
            collector: Metrics collector
            name: Timer name
            labels: Labels for the metric
        """
        self.collector = collector
        self.name = name
        self.labels = labels
        self.start_time = None
    
    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and record duration."""
        if self.start_time is not None:
            duration = time.time() - self.start_time
            metric_key = self.collector._get_metric_key(self.name, self.labels)
            
            with self.collector.lock:
                if metric_key not in self.collector.timers:
                    self.collector.timers[metric_key] = HistogramData()
                self.collector.timers[metric_key].add_value(duration)


# Global metrics collector
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector.
    
    Returns:
        Metrics collector instance
    """
    global _metrics_collector
    
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    
    return _metrics_collector


def increment_counter(name: str, value: float = 1.0, 
                     labels: Optional[Dict[str, str]] = None):
    """Increment a counter metric using global collector."""
    get_metrics_collector().increment_counter(name, value, labels)


def set_gauge(name: str, value: float, 
              labels: Optional[Dict[str, str]] = None):
    """Set a gauge metric using global collector."""
    get_metrics_collector().set_gauge(name, value, labels)


def observe_histogram(name: str, value: float,
                     labels: Optional[Dict[str, str]] = None):
    """Observe a histogram metric using global collector."""
    get_metrics_collector().observe_histogram(name, value, labels)


def time_operation(name: str, labels: Optional[Dict[str, str]] = None):
    """Time an operation using global collector."""
    return get_metrics_collector().time_operation(name, labels)