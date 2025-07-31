"""Custom exception classes for Kafka Ops Agent."""

from typing import Optional, Dict, Any
from enum import Enum


class ErrorCode(str, Enum):
    """Error codes for different types of failures."""
    
    # General errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    
    # Authentication/Authorization errors
    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"
    AUTHORIZATION_FAILED = "AUTHORIZATION_FAILED"
    INVALID_API_KEY = "INVALID_API_KEY"
    
    # Kafka client errors
    KAFKA_CONNECTION_ERROR = "KAFKA_CONNECTION_ERROR"
    KAFKA_TIMEOUT_ERROR = "KAFKA_TIMEOUT_ERROR"
    KAFKA_AUTHENTICATION_ERROR = "KAFKA_AUTHENTICATION_ERROR"
    KAFKA_AUTHORIZATION_ERROR = "KAFKA_AUTHORIZATION_ERROR"
    
    # Topic management errors
    TOPIC_NOT_FOUND = "TOPIC_NOT_FOUND"
    TOPIC_ALREADY_EXISTS = "TOPIC_ALREADY_EXISTS"
    TOPIC_CREATION_FAILED = "TOPIC_CREATION_FAILED"
    TOPIC_DELETION_FAILED = "TOPIC_DELETION_FAILED"
    TOPIC_CONFIG_UPDATE_FAILED = "TOPIC_CONFIG_UPDATE_FAILED"
    INVALID_TOPIC_CONFIG = "INVALID_TOPIC_CONFIG"
    
    # Cluster management errors
    CLUSTER_NOT_FOUND = "CLUSTER_NOT_FOUND"
    CLUSTER_PROVISIONING_FAILED = "CLUSTER_PROVISIONING_FAILED"
    CLUSTER_DEPROVISIONING_FAILED = "CLUSTER_DEPROVISIONING_FAILED"
    CLUSTER_HEALTH_CHECK_FAILED = "CLUSTER_HEALTH_CHECK_FAILED"
    INSUFFICIENT_RESOURCES = "INSUFFICIENT_RESOURCES"
    
    # Storage errors
    STORAGE_CONNECTION_ERROR = "STORAGE_CONNECTION_ERROR"
    STORAGE_OPERATION_FAILED = "STORAGE_OPERATION_FAILED"
    MIGRATION_FAILED = "MIGRATION_FAILED"
    
    # Provider errors
    PROVIDER_NOT_FOUND = "PROVIDER_NOT_FOUND"
    PROVIDER_INITIALIZATION_FAILED = "PROVIDER_INITIALIZATION_FAILED"
    PROVIDER_OPERATION_FAILED = "PROVIDER_OPERATION_FAILED"
    
    # Service Broker errors
    SERVICE_NOT_FOUND = "SERVICE_NOT_FOUND"
    PLAN_NOT_FOUND = "PLAN_NOT_FOUND"
    INSTANCE_NOT_FOUND = "INSTANCE_NOT_FOUND"
    INSTANCE_ALREADY_EXISTS = "INSTANCE_ALREADY_EXISTS"
    BINDING_NOT_FOUND = "BINDING_NOT_FOUND"
    BINDING_ALREADY_EXISTS = "BINDING_ALREADY_EXISTS"
    OPERATION_IN_PROGRESS = "OPERATION_IN_PROGRESS"
    
    # Rate limiting and throttling
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    REQUEST_THROTTLED = "REQUEST_THROTTLED"
    
    # Cleanup and maintenance errors
    CLEANUP_CONFLICT = "CLEANUP_CONFLICT"
    CLEANUP_FAILED = "CLEANUP_FAILED"
    SCHEDULER_ERROR = "SCHEDULER_ERROR"


class KafkaOpsError(Exception):
    """Base exception class for Kafka Ops Agent."""
    
    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None
    ):
        """Initialize the exception.
        
        Args:
            message: Human-readable error message
            error_code: Specific error code for the failure
            details: Additional context about the error
            cause: The underlying exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.cause = cause
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses."""
        result = {
            'error': self.error_code.value,
            'message': self.message,
            'details': self.details
        }
        
        if self.cause:
            result['cause'] = str(self.cause)
        
        return result
    
    def __str__(self) -> str:
        """String representation of the exception."""
        base_str = f"{self.error_code.value}: {self.message}"
        
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            base_str += f" ({details_str})"
        
        if self.cause:
            base_str += f" [caused by: {self.cause}]"
        
        return base_str


class ValidationError(KafkaOpsError):
    """Exception for validation failures."""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None):
        details = {}
        if field:
            details['field'] = field
        if value is not None:
            details['value'] = str(value)
        
        super().__init__(
            message=message,
            error_code=ErrorCode.VALIDATION_ERROR,
            details=details
        )


class ConfigurationError(KafkaOpsError):
    """Exception for configuration-related errors."""
    
    def __init__(self, message: str, config_key: Optional[str] = None):
        details = {}
        if config_key:
            details['config_key'] = config_key
        
        super().__init__(
            message=message,
            error_code=ErrorCode.CONFIGURATION_ERROR,
            details=details
        )


class AuthenticationError(KafkaOpsError):
    """Exception for authentication failures."""
    
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(
            message=message,
            error_code=ErrorCode.AUTHENTICATION_FAILED
        )


class AuthorizationError(KafkaOpsError):
    """Exception for authorization failures."""
    
    def __init__(self, message: str = "Authorization failed", resource: Optional[str] = None):
        details = {}
        if resource:
            details['resource'] = resource
        
        super().__init__(
            message=message,
            error_code=ErrorCode.AUTHORIZATION_FAILED,
            details=details
        )


class KafkaConnectionError(KafkaOpsError):
    """Exception for Kafka connection failures."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, bootstrap_servers: Optional[str] = None):
        details = {}
        if cluster_id:
            details['cluster_id'] = cluster_id
        if bootstrap_servers:
            details['bootstrap_servers'] = bootstrap_servers
        
        super().__init__(
            message=message,
            error_code=ErrorCode.KAFKA_CONNECTION_ERROR,
            details=details
        )


class KafkaTimeoutError(KafkaOpsError):
    """Exception for Kafka operation timeouts."""
    
    def __init__(self, message: str, operation: Optional[str] = None, timeout_seconds: Optional[int] = None):
        details = {}
        if operation:
            details['operation'] = operation
        if timeout_seconds:
            details['timeout_seconds'] = timeout_seconds
        
        super().__init__(
            message=message,
            error_code=ErrorCode.KAFKA_TIMEOUT_ERROR,
            details=details
        )


class TopicError(KafkaOpsError):
    """Base exception for topic-related errors."""
    
    def __init__(self, message: str, error_code: ErrorCode, topic_name: Optional[str] = None):
        details = {}
        if topic_name:
            details['topic_name'] = topic_name
        
        super().__init__(
            message=message,
            error_code=error_code,
            details=details
        )


class TopicNotFoundError(TopicError):
    """Exception for when a topic is not found."""
    
    def __init__(self, topic_name: str):
        super().__init__(
            message=f"Topic '{topic_name}' not found",
            error_code=ErrorCode.TOPIC_NOT_FOUND,
            topic_name=topic_name
        )


class TopicAlreadyExistsError(TopicError):
    """Exception for when a topic already exists."""
    
    def __init__(self, topic_name: str):
        super().__init__(
            message=f"Topic '{topic_name}' already exists",
            error_code=ErrorCode.TOPIC_ALREADY_EXISTS,
            topic_name=topic_name
        )


class ClusterError(KafkaOpsError):
    """Base exception for cluster-related errors."""
    
    def __init__(self, message: str, error_code: ErrorCode, cluster_id: Optional[str] = None):
        details = {}
        if cluster_id:
            details['cluster_id'] = cluster_id
        
        super().__init__(
            message=message,
            error_code=error_code,
            details=details
        )


class ClusterNotFoundError(ClusterError):
    """Exception for when a cluster is not found."""
    
    def __init__(self, cluster_id: str):
        super().__init__(
            message=f"Cluster '{cluster_id}' not found",
            error_code=ErrorCode.CLUSTER_NOT_FOUND,
            cluster_id=cluster_id
        )


class ClusterProvisioningError(ClusterError):
    """Exception for cluster provisioning failures."""
    
    def __init__(self, message: str, cluster_id: Optional[str] = None, provider: Optional[str] = None):
        details = {}
        if provider:
            details['provider'] = provider
        
        super().__init__(
            message=message,
            error_code=ErrorCode.CLUSTER_PROVISIONING_FAILED,
            cluster_id=cluster_id
        )
        
        if provider:
            self.details.update(details)


class StorageError(KafkaOpsError):
    """Exception for storage-related errors."""
    
    def __init__(self, message: str, operation: Optional[str] = None):
        details = {}
        if operation:
            details['operation'] = operation
        
        super().__init__(
            message=message,
            error_code=ErrorCode.STORAGE_OPERATION_FAILED,
            details=details
        )


class ProviderError(KafkaOpsError):
    """Exception for provider-related errors."""
    
    def __init__(self, message: str, provider_name: Optional[str] = None, operation: Optional[str] = None):
        details = {}
        if provider_name:
            details['provider'] = provider_name
        if operation:
            details['operation'] = operation
        
        super().__init__(
            message=message,
            error_code=ErrorCode.PROVIDER_OPERATION_FAILED,
            details=details
        )


class ServiceBrokerError(KafkaOpsError):
    """Base exception for Service Broker API errors."""
    
    def __init__(self, message: str, error_code: ErrorCode, instance_id: Optional[str] = None):
        details = {}
        if instance_id:
            details['instance_id'] = instance_id
        
        super().__init__(
            message=message,
            error_code=error_code,
            details=details
        )


class InstanceNotFoundError(ServiceBrokerError):
    """Exception for when a service instance is not found."""
    
    def __init__(self, instance_id: str):
        super().__init__(
            message=f"Service instance '{instance_id}' not found",
            error_code=ErrorCode.INSTANCE_NOT_FOUND,
            instance_id=instance_id
        )


class InstanceAlreadyExistsError(ServiceBrokerError):
    """Exception for when a service instance already exists."""
    
    def __init__(self, instance_id: str):
        super().__init__(
            message=f"Service instance '{instance_id}' already exists",
            error_code=ErrorCode.INSTANCE_ALREADY_EXISTS,
            instance_id=instance_id
        )


class RateLimitError(KafkaOpsError):
    """Exception for rate limiting."""
    
    def __init__(self, message: str = "Rate limit exceeded", retry_after: Optional[int] = None):
        details = {}
        if retry_after:
            details['retry_after_seconds'] = retry_after
        
        super().__init__(
            message=message,
            error_code=ErrorCode.RATE_LIMIT_EXCEEDED,
            details=details
        )


class CleanupConflictError(KafkaOpsError):
    """Exception for cleanup operation conflicts."""
    
    def __init__(self, message: str, cleanup_type: Optional[str] = None, cluster_id: Optional[str] = None):
        details = {}
        if cleanup_type:
            details['cleanup_type'] = cleanup_type
        if cluster_id:
            details['cluster_id'] = cluster_id
        
        super().__init__(
            message=message,
            error_code=ErrorCode.CLEANUP_CONFLICT,
            details=details
        )


# Utility functions for error handling

def wrap_kafka_error(func):
    """Decorator to wrap Kafka client errors into our custom exceptions."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # Map common Kafka errors to our custom exceptions
            error_message = str(e).lower()
            
            if 'timeout' in error_message:
                raise KafkaTimeoutError(f"Kafka operation timed out: {e}") from e
            elif 'connection' in error_message or 'network' in error_message:
                raise KafkaConnectionError(f"Kafka connection failed: {e}") from e
            elif 'authentication' in error_message or 'sasl' in error_message:
                raise KafkaOpsError(
                    f"Kafka authentication failed: {e}",
                    ErrorCode.KAFKA_AUTHENTICATION_ERROR
                ) from e
            elif 'authorization' in error_message or 'acl' in error_message:
                raise KafkaOpsError(
                    f"Kafka authorization failed: {e}",
                    ErrorCode.KAFKA_AUTHORIZATION_ERROR
                ) from e
            else:
                # Generic Kafka error
                raise KafkaOpsError(
                    f"Kafka operation failed: {e}",
                    ErrorCode.INTERNAL_ERROR
                ) from e
    
    return wrapper


def format_error_response(error: Exception, include_traceback: bool = False) -> Dict[str, Any]:
    """Format an exception into a standardized error response."""
    if isinstance(error, KafkaOpsError):
        response = error.to_dict()
    else:
        # Handle non-custom exceptions
        response = {
            'error': ErrorCode.INTERNAL_ERROR.value,
            'message': str(error),
            'details': {}
        }
    
    if include_traceback:
        import traceback
        response['traceback'] = traceback.format_exc()
    
    return response