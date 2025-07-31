"""Error handling utilities for API responses."""

import logging
import traceback
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from flask import Flask, request, jsonify

from kafka_ops_agent.exceptions import (
    KafkaOpsError, ErrorCode, ValidationError, AuthenticationError,
    AuthorizationError, RateLimitError, format_error_response
)

logger = logging.getLogger(__name__)


class ErrorResponseFormatter:
    """Formats error responses for different API contexts."""
    
    @staticmethod
    def format_api_error(
        error: Exception,
        request_id: Optional[str] = None,
        include_details: bool = True,
        include_traceback: bool = False
    ) -> Dict[str, Any]:
        """Format error for API response.
        
        Args:
            error: The exception to format
            request_id: Optional request ID for tracking
            include_details: Whether to include error details
            include_traceback: Whether to include stack trace (debug only)
        
        Returns:
            Formatted error response dictionary
        """
        response = {
            'success': False,
            'timestamp': datetime.utcnow().isoformat(),
        }
        
        if request_id:
            response['request_id'] = request_id
        
        if isinstance(error, KafkaOpsError):
            response.update({
                'error_code': error.error_code.value,
                'message': error.message,
            })
            
            if include_details and error.details:
                response['details'] = error.details
            
            # Map error codes to HTTP status codes
            response['http_status'] = ErrorResponseFormatter._get_http_status(error.error_code)
            
        else:
            # Handle non-custom exceptions
            response.update({
                'error_code': ErrorCode.INTERNAL_ERROR.value,
                'message': str(error),
                'http_status': 500
            })
        
        if include_traceback:
            response['traceback'] = traceback.format_exc()
        
        return response
    
    @staticmethod
    def format_osb_error(error: Exception) -> Tuple[Dict[str, Any], int]:
        """Format error for Open Service Broker API compliance.
        
        Returns:
            Tuple of (response_dict, http_status_code)
        """
        if isinstance(error, KafkaOpsError):
            # Map to OSB error format
            osb_response = {
                'error': error.error_code.value,
                'description': error.message
            }
            
            http_status = ErrorResponseFormatter._get_http_status(error.error_code)
            
            # OSB-specific error mappings
            if error.error_code == ErrorCode.INSTANCE_NOT_FOUND:
                http_status = 410  # Gone
            elif error.error_code == ErrorCode.INSTANCE_ALREADY_EXISTS:
                http_status = 409  # Conflict
            elif error.error_code == ErrorCode.OPERATION_IN_PROGRESS:
                http_status = 422  # Unprocessable Entity
            
            return osb_response, http_status
        
        else:
            return {
                'error': 'InternalError',
                'description': str(error)
            }, 500
    
    @staticmethod
    def format_validation_errors(errors: list) -> Dict[str, Any]:
        """Format validation errors for API response."""
        return {
            'success': False,
            'error_code': ErrorCode.VALIDATION_ERROR.value,
            'message': 'Validation failed',
            'validation_errors': errors,
            'timestamp': datetime.utcnow().isoformat(),
            'http_status': 400
        }
    
    @staticmethod
    def _get_http_status(error_code: ErrorCode) -> int:
        """Map error codes to HTTP status codes."""
        status_map = {
            # 4xx Client Errors
            ErrorCode.VALIDATION_ERROR: 400,
            ErrorCode.AUTHENTICATION_FAILED: 401,
            ErrorCode.INVALID_API_KEY: 401,
            ErrorCode.AUTHORIZATION_FAILED: 403,
            ErrorCode.TOPIC_NOT_FOUND: 404,
            ErrorCode.CLUSTER_NOT_FOUND: 404,
            ErrorCode.INSTANCE_NOT_FOUND: 404,
            ErrorCode.SERVICE_NOT_FOUND: 404,
            ErrorCode.PLAN_NOT_FOUND: 404,
            ErrorCode.TOPIC_ALREADY_EXISTS: 409,
            ErrorCode.INSTANCE_ALREADY_EXISTS: 409,
            ErrorCode.CLEANUP_CONFLICT: 409,
            ErrorCode.OPERATION_IN_PROGRESS: 409,
            ErrorCode.INVALID_TOPIC_CONFIG: 422,
            ErrorCode.RATE_LIMIT_EXCEEDED: 429,
            ErrorCode.REQUEST_THROTTLED: 429,
            
            # 5xx Server Errors
            ErrorCode.INTERNAL_ERROR: 500,
            ErrorCode.KAFKA_CONNECTION_ERROR: 502,
            ErrorCode.KAFKA_TIMEOUT_ERROR: 504,
            ErrorCode.CLUSTER_PROVISIONING_FAILED: 500,
            ErrorCode.STORAGE_OPERATION_FAILED: 500,
            ErrorCode.PROVIDER_OPERATION_FAILED: 500,
        }
        
        return status_map.get(error_code, 500)


def register_error_handlers(app: Flask, debug: bool = False):
    """Register global error handlers for Flask app.
    
    Args:
        app: Flask application instance
        debug: Whether to include debug information in responses
    """
    
    @app.errorhandler(ValidationError)
    def handle_validation_error(error: ValidationError):
        """Handle validation errors."""
        logger.warning(f"Validation error: {error}")
        
        response = ErrorResponseFormatter.format_api_error(
            error,
            request_id=getattr(request, 'id', None),
            include_traceback=debug
        )
        
        return jsonify(response), response['http_status']
    
    @app.errorhandler(AuthenticationError)
    def handle_authentication_error(error: AuthenticationError):
        """Handle authentication errors."""
        logger.warning(f"Authentication error: {error}")
        
        response = ErrorResponseFormatter.format_api_error(
            error,
            request_id=getattr(request, 'id', None)
        )
        
        return jsonify(response), response['http_status']
    
    @app.errorhandler(AuthorizationError)
    def handle_authorization_error(error: AuthorizationError):
        """Handle authorization errors."""
        logger.warning(f"Authorization error: {error}")
        
        response = ErrorResponseFormatter.format_api_error(
            error,
            request_id=getattr(request, 'id', None)
        )
        
        return jsonify(response), response['http_status']
    
    @app.errorhandler(RateLimitError)
    def handle_rate_limit_error(error: RateLimitError):
        """Handle rate limiting errors."""
        logger.warning(f"Rate limit error: {error}")
        
        response = ErrorResponseFormatter.format_api_error(
            error,
            request_id=getattr(request, 'id', None)
        )
        
        # Add Retry-After header if available
        flask_response = jsonify(response)
        if 'retry_after_seconds' in error.details:
            flask_response.headers['Retry-After'] = str(error.details['retry_after_seconds'])
        
        return flask_response, response['http_status']
    
    @app.errorhandler(KafkaOpsError)
    def handle_kafka_ops_error(error: KafkaOpsError):
        """Handle custom Kafka Ops errors."""
        logger.error(f"Kafka Ops error: {error}")
        
        response = ErrorResponseFormatter.format_api_error(
            error,
            request_id=getattr(request, 'id', None),
            include_traceback=debug
        )
        
        return jsonify(response), response['http_status']
    
    @app.errorhandler(Exception)
    def handle_generic_error(error: Exception):
        """Handle unexpected errors."""
        logger.error(f"Unexpected error: {error}", exc_info=True)
        
        response = ErrorResponseFormatter.format_api_error(
            error,
            request_id=getattr(request, 'id', None),
            include_traceback=debug
        )
        
        return jsonify(response), response['http_status']
    
    @app.errorhandler(404)
    def handle_not_found(error):
        """Handle 404 errors."""
        response = {
            'success': False,
            'error_code': 'NOT_FOUND',
            'message': 'The requested resource was not found',
            'timestamp': datetime.utcnow().isoformat(),
            'http_status': 404
        }
        
        return jsonify(response), 404
    
    @app.errorhandler(405)
    def handle_method_not_allowed(error):
        """Handle 405 errors."""
        response = {
            'success': False,
            'error_code': 'METHOD_NOT_ALLOWED',
            'message': 'The requested method is not allowed for this resource',
            'timestamp': datetime.utcnow().isoformat(),
            'http_status': 405
        }
        
        return jsonify(response), 405


def create_error_response(
    message: str,
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR,
    details: Optional[Dict[str, Any]] = None,
    http_status: Optional[int] = None
) -> Tuple[Dict[str, Any], int]:
    """Create a standardized error response.
    
    Args:
        message: Error message
        error_code: Error code
        details: Additional error details
        http_status: HTTP status code (auto-determined if None)
    
    Returns:
        Tuple of (response_dict, http_status_code)
    """
    if http_status is None:
        http_status = ErrorResponseFormatter._get_http_status(error_code)
    
    response = {
        'success': False,
        'error_code': error_code.value,
        'message': message,
        'timestamp': datetime.utcnow().isoformat(),
        'http_status': http_status
    }
    
    if details:
        response['details'] = details
    
    return response, http_status


def log_error_context(error: Exception, context: Dict[str, Any]):
    """Log error with additional context information.
    
    Args:
        error: The exception that occurred
        context: Additional context information
    """
    context_str = ", ".join(f"{k}={v}" for k, v in context.items())
    
    if isinstance(error, KafkaOpsError):
        logger.error(
            f"Error {error.error_code.value}: {error.message} "
            f"[Context: {context_str}]",
            extra={'error_details': error.details}
        )
    else:
        logger.error(
            f"Unexpected error: {error} [Context: {context_str}]",
            exc_info=True
        )


class RequestContextLogger:
    """Middleware to add request context to error logs."""
    
    def __init__(self, app: Flask):
        """Initialize the middleware.
        
        Args:
            app: Flask application instance
        """
        self.app = app
        self.init_app(app)
    
    def init_app(self, app: Flask):
        """Initialize the middleware with the app."""
        
        @app.before_request
        def before_request():
            """Add request ID and context."""
            import uuid
            request.id = str(uuid.uuid4())
            request.start_time = datetime.utcnow()
        
        @app.after_request
        def after_request(response):
            """Log request completion."""
            if hasattr(request, 'start_time'):
                duration = (datetime.utcnow() - request.start_time).total_seconds()
                
                logger.info(
                    f"{request.method} {request.path} -> {response.status_code} "
                    f"({duration:.3f}s)",
                    extra={
                        'request_id': getattr(request, 'id', None),
                        'method': request.method,
                        'path': request.path,
                        'status_code': response.status_code,
                        'duration_seconds': duration
                    }
                )
            
            return response


def safe_execute(func, *args, **kwargs):
    """Safely execute a function and return result or error.
    
    Args:
        func: Function to execute
        *args: Function arguments
        **kwargs: Function keyword arguments
    
    Returns:
        Tuple of (success: bool, result_or_error: Any)
    """
    try:
        result = func(*args, **kwargs)
        return True, result
    except Exception as e:
        logger.error(f"Safe execution failed for {func.__name__}: {e}")
        return False, e


async def safe_execute_async(func, *args, **kwargs):
    """Safely execute an async function and return result or error.
    
    Args:
        func: Async function to execute
        *args: Function arguments
        **kwargs: Function keyword arguments
    
    Returns:
        Tuple of (success: bool, result_or_error: Any)
    """
    try:
        result = await func(*args, **kwargs)
        return True, result
    except Exception as e:
        logger.error(f"Safe async execution failed for {func.__name__}: {e}")
        return False, e