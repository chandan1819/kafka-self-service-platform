"""Comprehensive audit logging system for Kafka Ops Agent."""

import logging
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from enum import Enum
from dataclasses import dataclass, asdict
from contextvars import ContextVar

from kafka_ops_agent.storage.base import AuditStore
from kafka_ops_agent.storage.factory import get_audit_store

# Context variables for request tracking
request_context: ContextVar[Optional[Dict[str, Any]]] = ContextVar('request_context', default=None)


class AuditEventType(str, Enum):
    """Types of audit events."""
    
    # Authentication and Authorization
    LOGIN = "auth.login"
    LOGOUT = "auth.logout"
    AUTH_FAILURE = "auth.failure"
    PERMISSION_DENIED = "auth.permission_denied"
    
    # Cluster Operations
    CLUSTER_CREATE = "cluster.create"
    CLUSTER_DELETE = "cluster.delete"
    CLUSTER_UPDATE = "cluster.update"
    CLUSTER_START = "cluster.start"
    CLUSTER_STOP = "cluster.stop"
    CLUSTER_HEALTH_CHECK = "cluster.health_check"
    
    # Topic Operations
    TOPIC_CREATE = "topic.create"
    TOPIC_DELETE = "topic.delete"
    TOPIC_UPDATE = "topic.update"
    TOPIC_PURGE = "topic.purge"
    TOPIC_LIST = "topic.list"
    TOPIC_DESCRIBE = "topic.describe"
    
    # Configuration Changes
    CONFIG_UPDATE = "config.update"
    CONFIG_VIEW = "config.view"
    
    # Cleanup Operations
    CLEANUP_TOPIC = "cleanup.topic"
    CLEANUP_CLUSTER = "cleanup.cluster"
    CLEANUP_METADATA = "cleanup.metadata"
    
    # API Access
    API_REQUEST = "api.request"
    API_ERROR = "api.error"
    
    # System Events
    SYSTEM_START = "system.start"
    SYSTEM_STOP = "system.stop"
    SYSTEM_ERROR = "system.error"
    
    # Data Access
    DATA_READ = "data.read"
    DATA_WRITE = "data.write"
    DATA_DELETE = "data.delete"


class AuditLevel(str, Enum):
    """Audit event severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Represents an audit event."""
    
    event_id: str
    event_type: AuditEventType
    timestamp: datetime
    level: AuditLevel
    message: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    source_ip: Optional[str] = None
    user_agent: Optional[str] = None
    operation_result: Optional[str] = None  # success, failure, partial
    details: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert audit event to dictionary."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert audit event to JSON string."""
        return json.dumps(self.to_dict(), default=str)


class AuditLogger:
    """Comprehensive audit logging system."""
    
    def __init__(self, audit_store: Optional[AuditStore] = None):
        """Initialize audit logger.
        
        Args:
            audit_store: Optional audit store for persistence
        """
        self.logger = logging.getLogger('kafka_ops_agent.audit')
        self.audit_store = audit_store
        self._session_events: Dict[str, List[AuditEvent]] = {}
    
    async def log_event(
        self,
        event_type: AuditEventType,
        message: str,
        level: AuditLevel = AuditLevel.INFO,
        user_id: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        operation_result: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None
    ) -> AuditEvent:
        """Log an audit event.
        
        Args:
            event_type: Type of audit event
            message: Human-readable message
            level: Severity level
            user_id: User performing the operation
            resource_type: Type of resource being accessed
            resource_id: ID of the resource
            operation_result: Result of the operation
            details: Additional event details
            tags: Event tags for categorization
            
        Returns:
            Created audit event
        """
        # Get context information
        context = request_context.get() or {}
        
        # Create audit event
        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=datetime.now(timezone.utc),
            level=level,
            message=message,
            user_id=user_id or context.get('user_id'),
            session_id=context.get('session_id'),
            request_id=context.get('request_id'),
            resource_type=resource_type,
            resource_id=resource_id,
            source_ip=context.get('source_ip'),
            user_agent=context.get('user_agent'),
            operation_result=operation_result,
            details=details or {},
            tags=tags or []
        )
        
        # Log to standard logger
        self.logger.info(event.to_json(), extra={
            'audit_event': True,
            'event_type': event_type.value,
            'user_id': event.user_id,
            'resource_type': resource_type,
            'resource_id': resource_id
        })
        
        # Store in audit store if available
        if self.audit_store:
            try:
                await self.audit_store.log_operation(
                    cluster_id=resource_id if resource_type == 'cluster' else 'system',
                    operation=event_type.value,
                    user_id=event.user_id or 'system',
                    details=event.to_dict()
                )
            except Exception as e:
                self.logger.error(f"Failed to store audit event: {e}")
        
        # Store in session events for correlation
        session_id = event.session_id
        if session_id:
            if session_id not in self._session_events:
                self._session_events[session_id] = []
            self._session_events[session_id].append(event)
        
        return event
    
    async def log_authentication(
        self,
        user_id: str,
        success: bool,
        method: str = "api_key",
        source_ip: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log authentication event.
        
        Args:
            user_id: User attempting authentication
            success: Whether authentication succeeded
            method: Authentication method used
            source_ip: Source IP address
            details: Additional authentication details
        """
        event_type = AuditEventType.LOGIN if success else AuditEventType.AUTH_FAILURE
        level = AuditLevel.INFO if success else AuditLevel.WARNING
        result = "success" if success else "failure"
        
        message = f"Authentication {result} for user {user_id} using {method}"
        
        await self.log_event(
            event_type=event_type,
            message=message,
            level=level,
            user_id=user_id,
            operation_result=result,
            details={
                'method': method,
                'source_ip': source_ip,
                **(details or {})
            },
            tags=['authentication']
        )
    
    async def log_cluster_operation(
        self,
        operation: str,
        cluster_id: str,
        user_id: Optional[str] = None,
        success: bool = True,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log cluster operation.
        
        Args:
            operation: Operation performed (create, delete, update, etc.)
            cluster_id: Cluster identifier
            user_id: User performing operation
            success: Whether operation succeeded
            details: Additional operation details
        """
        event_type_map = {
            'create': AuditEventType.CLUSTER_CREATE,
            'delete': AuditEventType.CLUSTER_DELETE,
            'update': AuditEventType.CLUSTER_UPDATE,
            'start': AuditEventType.CLUSTER_START,
            'stop': AuditEventType.CLUSTER_STOP,
            'health_check': AuditEventType.CLUSTER_HEALTH_CHECK
        }
        
        event_type = event_type_map.get(operation, AuditEventType.CLUSTER_UPDATE)
        level = AuditLevel.INFO if success else AuditLevel.ERROR
        result = "success" if success else "failure"
        
        message = f"Cluster {operation} {result} for cluster {cluster_id}"
        
        await self.log_event(
            event_type=event_type,
            message=message,
            level=level,
            user_id=user_id,
            resource_type='cluster',
            resource_id=cluster_id,
            operation_result=result,
            details=details,
            tags=['cluster', operation]
        )
    
    async def log_topic_operation(
        self,
        operation: str,
        topic_name: str,
        cluster_id: str,
        user_id: Optional[str] = None,
        success: bool = True,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log topic operation.
        
        Args:
            operation: Operation performed
            topic_name: Topic name
            cluster_id: Cluster identifier
            user_id: User performing operation
            success: Whether operation succeeded
            details: Additional operation details
        """
        event_type_map = {
            'create': AuditEventType.TOPIC_CREATE,
            'delete': AuditEventType.TOPIC_DELETE,
            'update': AuditEventType.TOPIC_UPDATE,
            'purge': AuditEventType.TOPIC_PURGE,
            'list': AuditEventType.TOPIC_LIST,
            'describe': AuditEventType.TOPIC_DESCRIBE
        }
        
        event_type = event_type_map.get(operation, AuditEventType.TOPIC_UPDATE)
        level = AuditLevel.INFO if success else AuditLevel.ERROR
        result = "success" if success else "failure"
        
        message = f"Topic {operation} {result} for topic {topic_name} in cluster {cluster_id}"
        
        await self.log_event(
            event_type=event_type,
            message=message,
            level=level,
            user_id=user_id,
            resource_type='topic',
            resource_id=f"{cluster_id}/{topic_name}",
            operation_result=result,
            details={
                'topic_name': topic_name,
                'cluster_id': cluster_id,
                **(details or {})
            },
            tags=['topic', operation]
        )
    
    async def log_api_request(
        self,
        method: str,
        path: str,
        status_code: int,
        user_id: Optional[str] = None,
        duration_ms: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log API request.
        
        Args:
            method: HTTP method
            path: Request path
            status_code: HTTP status code
            user_id: User making request
            duration_ms: Request duration in milliseconds
            details: Additional request details
        """
        success = 200 <= status_code < 400
        level = AuditLevel.INFO if success else AuditLevel.WARNING
        result = "success" if success else "failure"
        
        message = f"API {method} {path} -> {status_code}"
        if duration_ms:
            message += f" ({duration_ms:.1f}ms)"
        
        await self.log_event(
            event_type=AuditEventType.API_REQUEST,
            message=message,
            level=level,
            user_id=user_id,
            operation_result=result,
            details={
                'method': method,
                'path': path,
                'status_code': status_code,
                'duration_ms': duration_ms,
                **(details or {})
            },
            tags=['api', method.lower()]
        )
    
    async def log_cleanup_operation(
        self,
        cleanup_type: str,
        target: str,
        user_id: Optional[str] = None,
        success: bool = True,
        items_cleaned: int = 0,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log cleanup operation.
        
        Args:
            cleanup_type: Type of cleanup (topic, cluster, metadata)
            target: Cleanup target (cluster ID, etc.)
            user_id: User performing cleanup
            success: Whether cleanup succeeded
            items_cleaned: Number of items cleaned
            details: Additional cleanup details
        """
        event_type_map = {
            'topic': AuditEventType.CLEANUP_TOPIC,
            'cluster': AuditEventType.CLEANUP_CLUSTER,
            'metadata': AuditEventType.CLEANUP_METADATA
        }
        
        event_type = event_type_map.get(cleanup_type, AuditEventType.CLEANUP_TOPIC)
        level = AuditLevel.INFO if success else AuditLevel.ERROR
        result = "success" if success else "failure"
        
        message = f"Cleanup {cleanup_type} {result} for {target} - {items_cleaned} items cleaned"
        
        await self.log_event(
            event_type=event_type,
            message=message,
            level=level,
            user_id=user_id,
            resource_type=cleanup_type,
            resource_id=target,
            operation_result=result,
            details={
                'cleanup_type': cleanup_type,
                'items_cleaned': items_cleaned,
                **(details or {})
            },
            tags=['cleanup', cleanup_type]
        )
    
    async def log_system_event(
        self,
        event: str,
        message: str,
        level: AuditLevel = AuditLevel.INFO,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log system event.
        
        Args:
            event: System event type
            message: Event message
            level: Event severity level
            details: Additional event details
        """
        event_type_map = {
            'start': AuditEventType.SYSTEM_START,
            'stop': AuditEventType.SYSTEM_STOP,
            'error': AuditEventType.SYSTEM_ERROR
        }
        
        event_type = event_type_map.get(event, AuditEventType.SYSTEM_ERROR)
        
        await self.log_event(
            event_type=event_type,
            message=message,
            level=level,
            details=details,
            tags=['system', event]
        )
    
    def get_session_events(self, session_id: str) -> List[AuditEvent]:
        """Get all events for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            List of audit events for the session
        """
        return self._session_events.get(session_id, [])
    
    def clear_session_events(self, session_id: str):
        """Clear events for a session.
        
        Args:
            session_id: Session identifier
        """
        if session_id in self._session_events:
            del self._session_events[session_id]


def set_audit_context(
    request_id: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    source_ip: Optional[str] = None,
    user_agent: Optional[str] = None
):
    """Set audit context for current request.
    
    Args:
        request_id: Request identifier
        user_id: User identifier
        session_id: Session identifier
        source_ip: Source IP address
        user_agent: User agent string
    """
    context = {
        'request_id': request_id,
        'user_id': user_id,
        'session_id': session_id,
        'source_ip': source_ip,
        'user_agent': user_agent
    }
    request_context.set(context)


def clear_audit_context():
    """Clear audit context."""
    request_context.set(None)


# Global audit logger instance
_audit_logger: Optional[AuditLogger] = None


async def get_audit_logger() -> AuditLogger:
    """Get global audit logger instance.
    
    Returns:
        Audit logger instance
    """
    global _audit_logger
    if _audit_logger is None:
        audit_store = await get_audit_store()
        _audit_logger = AuditLogger(audit_store)
    return _audit_logger