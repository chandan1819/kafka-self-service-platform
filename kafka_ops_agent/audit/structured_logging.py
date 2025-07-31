"""Structured logging configuration for JSON format output."""

import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

from kafka_ops_agent.audit.audit_logger import request_context


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def __init__(self, include_context: bool = True):
        """Initialize JSON formatter.
        
        Args:
            include_context: Whether to include request context in logs
        """
        super().__init__()
        self.include_context = include_context
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON formatted log string
        """
        # Base log data
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                          'filename', 'module', 'lineno', 'funcName', 'created',
                          'msecs', 'relativeCreated', 'thread', 'threadName',
                          'processName', 'process', 'getMessage', 'exc_info',
                          'exc_text', 'stack_info']:
                log_data[key] = value
        
        # Add request context if available and enabled
        if self.include_context:
            context = request_context.get()
            if context:
                log_data['context'] = context
        
        return json.dumps(log_data, default=str, separators=(',', ':'))


class StructuredLogger:
    """Structured logger with JSON output and context support."""
    
    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        log_file: Optional[str] = None,
        max_file_size: int = 100 * 1024 * 1024,  # 100MB
        backup_count: int = 5,
        console_output: bool = True
    ):
        """Initialize structured logger.
        
        Args:
            name: Logger name
            level: Logging level
            log_file: Optional log file path
            max_file_size: Maximum file size before rotation
            backup_count: Number of backup files to keep
            console_output: Whether to output to console
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # JSON formatter
        formatter = JSONFormatter(include_context=True)
        
        # Console handler
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # File handler with rotation
        if log_file:
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_file_size,
                backupCount=backup_count
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def info(self, message: str, **kwargs):
        """Log info message with extra fields."""
        self.logger.info(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with extra fields."""
        self.logger.warning(message, extra=kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with extra fields."""
        self.logger.error(message, extra=kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with extra fields."""
        self.logger.debug(message, extra=kwargs)
    
    def critical(self, message: str, **kwargs):
        """Log critical message with extra fields."""
        self.logger.critical(message, extra=kwargs)


def setup_structured_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_audit_logging: bool = True
) -> Dict[str, StructuredLogger]:
    """Setup structured logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        enable_console: Whether to enable console output
        enable_audit_logging: Whether to enable audit logging
        
    Returns:
        Dictionary of configured loggers
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    loggers = {}
    
    # Main application logger
    loggers['app'] = StructuredLogger(
        name='kafka_ops_agent',
        level=level,
        log_file=log_file,
        console_output=enable_console
    )
    
    # API logger
    loggers['api'] = StructuredLogger(
        name='kafka_ops_agent.api',
        level=level,
        log_file=log_file.replace('.log', '_api.log') if log_file else None,
        console_output=enable_console
    )
    
    # Service logger
    loggers['service'] = StructuredLogger(
        name='kafka_ops_agent.services',
        level=level,
        log_file=log_file.replace('.log', '_services.log') if log_file else None,
        console_output=enable_console
    )
    
    # Audit logger (separate file)
    if enable_audit_logging:
        audit_file = log_file.replace('.log', '_audit.log') if log_file else 'logs/audit.log'
        loggers['audit'] = StructuredLogger(
            name='kafka_ops_agent.audit',
            level=logging.INFO,  # Always INFO for audit
            log_file=audit_file,
            console_output=False  # Audit logs typically don't go to console
        )
    
    return loggers


def create_log_context(**kwargs) -> Dict[str, Any]:
    """Create log context dictionary.
    
    Args:
        **kwargs: Context key-value pairs
        
    Returns:
        Context dictionary
    """
    return {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        **kwargs
    }


def log_with_context(logger: StructuredLogger, level: str, message: str, **context):
    """Log message with context.
    
    Args:
        logger: Structured logger instance
        level: Log level (info, warning, error, debug, critical)
        message: Log message
        **context: Additional context fields
    """
    log_method = getattr(logger, level.lower())
    log_method(message, **context)


# Example usage and configuration
def configure_application_logging():
    """Configure logging for the entire application."""
    import os
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Setup structured logging
    loggers = setup_structured_logging(
        log_level=os.getenv('LOG_LEVEL', 'INFO'),
        log_file='logs/kafka_ops_agent.log',
        enable_console=True,
        enable_audit_logging=True
    )
    
    # Configure root logger to use structured format
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove default handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add structured handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
    
    return loggers


# Global logger instances
_loggers: Optional[Dict[str, StructuredLogger]] = None


def get_structured_logger(name: str = 'app') -> StructuredLogger:
    """Get structured logger instance.
    
    Args:
        name: Logger name (app, api, service, audit)
        
    Returns:
        Structured logger instance
    """
    global _loggers
    
    if _loggers is None:
        _loggers = configure_application_logging()
    
    return _loggers.get(name, _loggers['app'])