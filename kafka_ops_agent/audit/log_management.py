"""Log management utilities for rotation, aggregation, and archival."""

import os
import gzip
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import json

from kafka_ops_agent.audit.log_analyzer import LogAnalyzer, LogQuery


@dataclass
class LogRotationConfig:
    """Configuration for log rotation."""
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    max_files: int = 10
    compress_rotated: bool = True
    rotation_time: str = "midnight"  # midnight, hourly, daily, weekly
    backup_count: int = 30


@dataclass
class LogAggregationConfig:
    """Configuration for log aggregation."""
    enabled: bool = True
    interval_minutes: int = 60  # Aggregate every hour
    retention_days: int = 90
    aggregate_fields: List[str] = None
    output_format: str = "json"  # json, csv


class LogRotationManager:
    """Manages log file rotation and archival."""
    
    def __init__(self, config: LogRotationConfig):
        """Initialize log rotation manager.
        
        Args:
            config: Rotation configuration
        """
        self.config = config
        self.logger = logging.getLogger('kafka_ops_agent.log_management')
    
    def should_rotate(self, log_file_path: str) -> bool:
        """Check if log file should be rotated.
        
        Args:
            log_file_path: Path to log file
            
        Returns:
            True if file should be rotated
        """
        log_path = Path(log_file_path)
        
        if not log_path.exists():
            return False
        
        # Check file size
        if log_path.stat().st_size >= self.config.max_file_size:
            return True
        
        # Check time-based rotation
        if self.config.rotation_time == "hourly":
            file_time = datetime.fromtimestamp(log_path.stat().st_mtime)
            return datetime.now() - file_time >= timedelta(hours=1)
        
        elif self.config.rotation_time == "daily":
            file_time = datetime.fromtimestamp(log_path.stat().st_mtime)
            return datetime.now().date() > file_time.date()
        
        elif self.config.rotation_time == "midnight":
            file_time = datetime.fromtimestamp(log_path.stat().st_mtime)
            now = datetime.now()
            return (now.hour == 0 and now.minute < 5 and 
                   file_time.date() < now.date())
        
        return False
    
    def rotate_log(self, log_file_path: str) -> bool:
        """Rotate a log file.
        
        Args:
            log_file_path: Path to log file to rotate
            
        Returns:
            True if rotation was successful
        """
        try:
            log_path = Path(log_file_path)
            
            if not log_path.exists():
                return False
            
            # Generate rotated filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            rotated_name = f"{log_path.stem}_{timestamp}{log_path.suffix}"
            rotated_path = log_path.parent / rotated_name
            
            # Move current log to rotated name
            shutil.move(str(log_path), str(rotated_path))
            
            # Compress if configured
            if self.config.compress_rotated:
                compressed_path = rotated_path.with_suffix(rotated_path.suffix + '.gz')
                with open(rotated_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                # Remove uncompressed file
                rotated_path.unlink()
                rotated_path = compressed_path
            
            self.logger.info(f"Rotated log file: {log_file_path} -> {rotated_path}")
            
            # Clean up old rotated files
            self._cleanup_old_files(log_path)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to rotate log file {log_file_path}: {e}")
            return False
    
    def _cleanup_old_files(self, log_path: Path):
        """Clean up old rotated log files.
        
        Args:
            log_path: Original log file path
        """
        try:
            # Find all rotated files for this log
            pattern = f"{log_path.stem}_*{log_path.suffix}*"
            rotated_files = list(log_path.parent.glob(pattern))
            
            # Sort by modification time (newest first)
            rotated_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            
            # Remove files beyond max_files limit
            for old_file in rotated_files[self.config.max_files:]:
                old_file.unlink()
                self.logger.info(f"Removed old rotated log: {old_file}")
                
        except Exception as e:
            self.logger.error(f"Failed to cleanup old log files: {e}")
    
    def get_rotated_files(self, log_file_path: str) -> List[Path]:
        """Get list of rotated files for a log.
        
        Args:
            log_file_path: Original log file path
            
        Returns:
            List of rotated file paths
        """
        log_path = Path(log_file_path)
        pattern = f"{log_path.stem}_*{log_path.suffix}*"
        
        rotated_files = list(log_path.parent.glob(pattern))
        rotated_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        
        return rotated_files


class LogAggregator:
    """Aggregates log data for analysis and reporting."""
    
    def __init__(self, config: LogAggregationConfig):
        """Initialize log aggregator.
        
        Args:
            config: Aggregation configuration
        """
        self.config = config
        self.logger = logging.getLogger('kafka_ops_agent.log_aggregation')
        self.analyzer = LogAnalyzer()
        
        if self.config.aggregate_fields is None:
            self.config.aggregate_fields = [
                'level', 'event_type', 'user_id', 'resource_type'
            ]
    
    def aggregate_logs(
        self,
        log_file_path: str,
        start_time: datetime,
        end_time: datetime,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Aggregate logs for a time period.
        
        Args:
            log_file_path: Path to log file
            start_time: Start of aggregation period
            end_time: End of aggregation period
            output_path: Optional path to save aggregated data
            
        Returns:
            Aggregated log data
        """
        try:
            # Create query for time period
            query = LogQuery(
                filters=[],
                start_time=start_time,
                end_time=end_time
            )
            
            # Set analyzer log file
            self.analyzer.log_file_path = log_file_path
            
            # Get statistics
            stats = self.analyzer.get_log_statistics(query)
            
            # Get aggregations for each field
            aggregations = {}
            for field in self.config.aggregate_fields:
                aggregations[field] = self.analyzer.aggregate_logs(query, field)
            
            # Create aggregated data structure
            aggregated_data = {
                'period': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat(),
                    'duration_hours': (end_time - start_time).total_seconds() / 3600
                },
                'statistics': stats,
                'aggregations': aggregations,
                'generated_at': datetime.utcnow().isoformat()
            }
            
            # Save to file if requested
            if output_path:
                self._save_aggregated_data(aggregated_data, output_path)
            
            self.logger.info(
                f"Aggregated logs for period {start_time} to {end_time}: "
                f"{stats['total_entries']} entries"
            )
            
            return aggregated_data
            
        except Exception as e:
            self.logger.error(f"Failed to aggregate logs: {e}")
            return {}
    
    def _save_aggregated_data(self, data: Dict[str, Any], output_path: str):
        """Save aggregated data to file.
        
        Args:
            data: Aggregated data
            output_path: Output file path
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        if self.config.output_format == "json":
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        
        elif self.config.output_format == "csv":
            # Convert to CSV format (simplified)
            import csv
            
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write statistics
                writer.writerow(['Metric', 'Value'])
                stats = data['statistics']
                writer.writerow(['Total Entries', stats['total_entries']])
                
                # Write aggregations
                writer.writerow([])
                for field, aggregation in data['aggregations'].items():
                    writer.writerow([f'{field.title()} Distribution'])
                    writer.writerow(['Value', 'Count'])
                    for value, count in aggregation.items():
                        writer.writerow([value, count])
                    writer.writerow([])
    
    def create_daily_aggregation(self, log_file_path: str, date: datetime) -> Dict[str, Any]:
        """Create daily aggregation for a specific date.
        
        Args:
            log_file_path: Path to log file
            date: Date to aggregate
            
        Returns:
            Daily aggregated data
        """
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)
        
        # Generate output path
        output_dir = Path("logs/aggregated")
        output_file = output_dir / f"daily_{date.strftime('%Y%m%d')}.json"
        
        return self.aggregate_logs(log_file_path, start_time, end_time, str(output_file))
    
    def create_hourly_aggregation(self, log_file_path: str, hour: datetime) -> Dict[str, Any]:
        """Create hourly aggregation.
        
        Args:
            log_file_path: Path to log file
            hour: Hour to aggregate
            
        Returns:
            Hourly aggregated data
        """
        start_time = hour.replace(minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(hours=1)
        
        # Generate output path
        output_dir = Path("logs/aggregated")
        output_file = output_dir / f"hourly_{hour.strftime('%Y%m%d_%H')}.json"
        
        return self.aggregate_logs(log_file_path, start_time, end_time, str(output_file))
    
    def cleanup_old_aggregations(self, aggregation_dir: str = "logs/aggregated"):
        """Clean up old aggregation files.
        
        Args:
            aggregation_dir: Directory containing aggregation files
        """
        try:
            agg_dir = Path(aggregation_dir)
            if not agg_dir.exists():
                return
            
            cutoff_date = datetime.now() - timedelta(days=self.config.retention_days)
            
            for file_path in agg_dir.glob("*.json"):
                if file_path.stat().st_mtime < cutoff_date.timestamp():
                    file_path.unlink()
                    self.logger.info(f"Removed old aggregation file: {file_path}")
                    
        except Exception as e:
            self.logger.error(f"Failed to cleanup old aggregations: {e}")


class LogManager:
    """Comprehensive log management system."""
    
    def __init__(
        self,
        rotation_config: Optional[LogRotationConfig] = None,
        aggregation_config: Optional[LogAggregationConfig] = None
    ):
        """Initialize log manager.
        
        Args:
            rotation_config: Log rotation configuration
            aggregation_config: Log aggregation configuration
        """
        self.rotation_config = rotation_config or LogRotationConfig()
        self.aggregation_config = aggregation_config or LogAggregationConfig()
        
        self.rotation_manager = LogRotationManager(self.rotation_config)
        self.aggregator = LogAggregator(self.aggregation_config)
        
        self.logger = logging.getLogger('kafka_ops_agent.log_manager')
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="log-manager")
    
    def manage_log_file(self, log_file_path: str):
        """Perform management tasks for a log file.
        
        Args:
            log_file_path: Path to log file to manage
        """
        try:
            # Check if rotation is needed
            if self.rotation_manager.should_rotate(log_file_path):
                self.rotation_manager.rotate_log(log_file_path)
            
            # Perform aggregation if enabled
            if self.aggregation_config.enabled:
                self._perform_aggregation(log_file_path)
                
        except Exception as e:
            self.logger.error(f"Failed to manage log file {log_file_path}: {e}")
    
    def _perform_aggregation(self, log_file_path: str):
        """Perform log aggregation.
        
        Args:
            log_file_path: Path to log file
        """
        try:
            now = datetime.now()
            
            # Check if it's time for hourly aggregation
            if now.minute < 5:  # Run in first 5 minutes of hour
                hour_ago = now - timedelta(hours=1)
                self.aggregator.create_hourly_aggregation(log_file_path, hour_ago)
            
            # Check if it's time for daily aggregation
            if now.hour == 1 and now.minute < 5:  # Run at 1 AM
                yesterday = now - timedelta(days=1)
                self.aggregator.create_daily_aggregation(log_file_path, yesterday)
            
            # Cleanup old aggregations weekly
            if now.weekday() == 0 and now.hour == 2 and now.minute < 5:  # Monday at 2 AM
                self.aggregator.cleanup_old_aggregations()
                
        except Exception as e:
            self.logger.error(f"Failed to perform aggregation: {e}")
    
    def get_log_health_status(self, log_file_paths: List[str]) -> Dict[str, Any]:
        """Get health status of log files.
        
        Args:
            log_file_paths: List of log file paths to check
            
        Returns:
            Health status information
        """
        status = {
            'timestamp': datetime.utcnow().isoformat(),
            'files': {},
            'overall_status': 'healthy'
        }
        
        for log_path in log_file_paths:
            file_status = self._get_file_status(log_path)
            status['files'][log_path] = file_status
            
            if file_status['status'] != 'healthy':
                status['overall_status'] = 'warning'
        
        return status
    
    def _get_file_status(self, log_file_path: str) -> Dict[str, Any]:
        """Get status of a single log file.
        
        Args:
            log_file_path: Path to log file
            
        Returns:
            File status information
        """
        try:
            log_path = Path(log_file_path)
            
            if not log_path.exists():
                return {
                    'status': 'missing',
                    'message': 'Log file does not exist',
                    'size_mb': 0,
                    'age_hours': 0
                }
            
            stat = log_path.stat()
            size_mb = stat.st_size / (1024 * 1024)
            age_hours = (datetime.now().timestamp() - stat.st_mtime) / 3600
            
            # Determine status
            status = 'healthy'
            messages = []
            
            if size_mb > self.rotation_config.max_file_size / (1024 * 1024):
                status = 'warning'
                messages.append('File size exceeds rotation threshold')
            
            if age_hours > 24 and stat.st_size == 0:
                status = 'warning'
                messages.append('Empty log file older than 24 hours')
            
            return {
                'status': status,
                'message': '; '.join(messages) if messages else 'File is healthy',
                'size_mb': round(size_mb, 2),
                'age_hours': round(age_hours, 2),
                'last_modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Failed to check file status: {e}',
                'size_mb': 0,
                'age_hours': 0
            }
    
    def close(self):
        """Close log manager and cleanup resources."""
        self.executor.shutdown(wait=True)


# Global log manager instance
_log_manager: Optional[LogManager] = None


def get_log_manager() -> LogManager:
    """Get global log manager instance.
    
    Returns:
        Log manager instance
    """
    global _log_manager
    if _log_manager is None:
        _log_manager = LogManager()
    return _log_manager