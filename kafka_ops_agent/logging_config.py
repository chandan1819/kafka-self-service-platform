"""Logging configuration and setup."""

import logging
import logging.handlers
import json
from typing import Dict, Any
from datetime import datetime
from kafka_ops_agent.config import config


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add extra fields if present
        if hasattr(record, 'instance_id'):
            log_entry['instance_id'] = record.instance_id
        if hasattr(record, 'operation'):
            log_entry['operation'] = record.operation
        if hasattr(record, 'user_id'):
            log_entry['user_id'] = record.user_id
            
        return json.dumps(log_entry)


class AuditLogger:
    """Specialized logger for audit trail."""
    
    def __init__(self):
        self.logger = logging.getLogger('kafka_ops_agent.audit')
        
    def log_provisioning(self, instance_id: str, operation: str, 
                        user_id: str = None, details: Dict[str, Any] = None):
        """Log provisioning operations."""
        extra = {
            'instance_id': instance_id,
            'operation': operation,
            'user_id': user_id or 'system'
        }
        
        message = f"Provisioning operation: {operation} for instance {instance_id}"
        if details:
            message += f" - Details: {json.dumps(details)}"
            
        self.logger.info(message, extra=extra)
    
    def log_topic_operation(self, cluster_id: str, topic_name: str, 
                           operation: str, user_id: str = None, 
                           details: Dict[str, Any] = None):
        """Log topic operations."""
        extra = {
            'instance_id': cluster_id,
            'operation': f"topic_{operation}",
            'user_id': user_id or 'system'
        }
        
        message = f"Topic operation: {operation} on topic {topic_name} in cluster {cluster_id}"
        if details:
            message += f" - Details: {json.dumps(details)}"
            
        self.logger.info(message, extra=extra)


def setup_logging():
    """Set up logging configuration."""
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.logging.level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JSONFormatter())
    root_logger.addHandler(console_handler)
    
    # File handler if configured
    if config.logging.file_path:
        file_handler = logging.handlers.RotatingFileHandler(
            config.logging.file_path,
            maxBytes=config.logging.max_file_size,
            backupCount=config.logging.backup_count
        )
        file_handler.setFormatter(JSONFormatter())
        root_logger.addHandler(file_handler)
    
    # Set specific logger levels
    logging.getLogger('kafka_ops_agent').setLevel(logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('docker').setLevel(logging.WARNING)


# Initialize audit logger
audit_logger = AuditLogger()