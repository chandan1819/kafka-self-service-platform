"""Authentication and authorization decorators."""

import logging
from functools import wraps
from typing import List, Optional, Callable, Any
from flask import request, jsonify

from .middleware import get_current_user, get_auth_context
from .models import User, Role, Permission
from kafka_ops_agent.exceptions import AuthenticationError, AuthorizationError

logger = logging.getLogger(__name__)


def authenticated(f: Callable) -> Callable:
    """Decorator to require authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_context = get_auth_context()
        if not auth_context or not auth_context.authenticated:
            raise AuthenticationError("Authentication required")
        return f(*args, **kwargs)
    return decorated_function


def authorized(permission: Permission = None, 
               role: Role = None,
               permissions: List[Permission] = None,
               roles: List[Role] = None,
               require_all: bool = False):
    """Decorator for authorization checks.
    
    Args:
        permission: Single permission required
        role: Single role required
        permissions: List of permissions (any or all based on require_all)
        roles: List of roles (any or all based on require_all)
        require_all: If True, require all permissions/roles; if False, require any
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                raise AuthenticationError("Authentication required")
            
            # Check single permission
            if permission and not user.has_permission(permission):
                raise AuthorizationError(f"Permission '{permission.value}' required")
            
            # Check single role
            if role and not user.has_role(role):
                raise AuthorizationError(f"Role '{role.value}' required")
            
            # Check multiple permissions
            if permissions:
                if require_all:
                    if not user.has_all_permissions(permissions):
                        perm_names = [p.value for p in permissions]
                        raise AuthorizationError(f"All permissions required: {perm_names}")
                else:
                    if not user.has_any_permission(permissions):
                        perm_names = [p.value for p in permissions]
                        raise AuthorizationError(f"One of these permissions required: {perm_names}")
            
            # Check multiple roles
            if roles:
                if require_all:
                    if not all(user.has_role(r) for r in roles):
                        role_names = [r.value for r in roles]
                        raise AuthorizationError(f"All roles required: {role_names}")
                else:
                    if not any(user.has_role(r) for r in roles):
                        role_names = [r.value for r in roles]
                        raise AuthorizationError(f"One of these roles required: {role_names}")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def rate_limited(requests_per_minute: int = 60, 
                 per_user: bool = True,
                 key_func: Optional[Callable] = None):
    """Decorator for rate limiting.
    
    Args:
        requests_per_minute: Number of requests allowed per minute
        per_user: If True, limit per user; if False, limit per IP
        key_func: Custom function to generate rate limit key
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # This is a placeholder - actual rate limiting is handled by middleware
            # This decorator can be used for endpoint-specific rate limits
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def admin_required(f: Callable) -> Callable:
    """Decorator to require admin role."""
    return authorized(role=Role.ADMIN)(f)


def operator_required(f: Callable) -> Callable:
    """Decorator to require operator role or higher."""
    return authorized(roles=[Role.ADMIN, Role.OPERATOR])(f)


def service_broker_required(f: Callable) -> Callable:
    """Decorator to require service broker role."""
    return authorized(role=Role.SERVICE_BROKER)(f)


def cluster_read_required(f: Callable) -> Callable:
    """Decorator to require cluster read permission."""
    return authorized(permission=Permission.CLUSTER_READ)(f)


def cluster_write_required(f: Callable) -> Callable:
    """Decorator to require cluster write permissions."""
    return authorized(permissions=[
        Permission.CLUSTER_CREATE,
        Permission.CLUSTER_UPDATE,
        Permission.CLUSTER_DELETE
    ])(f)


def topic_read_required(f: Callable) -> Callable:
    """Decorator to require topic read permission."""
    return authorized(permission=Permission.TOPIC_READ)(f)


def topic_write_required(f: Callable) -> Callable:
    """Decorator to require topic write permissions."""
    return authorized(permissions=[
        Permission.TOPIC_CREATE,
        Permission.TOPIC_UPDATE,
        Permission.TOPIC_DELETE
    ])(f)


def cleanup_required(f: Callable) -> Callable:
    """Decorator to require cleanup permissions."""
    return authorized(permission=Permission.CLEANUP_EXECUTE)(f)


def audit_context(f: Callable) -> Callable:
    """Decorator to add audit context to function calls."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = get_current_user()
        auth_context = get_auth_context()
        
        # Add audit information to kwargs if not present
        if 'user_id' not in kwargs and user:
            kwargs['user_id'] = user.user_id
        
        if 'request_id' not in kwargs and auth_context:
            kwargs['request_id'] = auth_context.request_id
        
        # Log the operation
        if user:
            logger.info(f"Operation: {f.__name__}, User: {user.username}, "
                       f"IP: {auth_context.client_ip if auth_context else 'unknown'}")
        
        return f(*args, **kwargs)
    return decorated_function


def validate_request_data(required_fields: List[str] = None,
                         optional_fields: List[str] = None):
    """Decorator to validate request data.
    
    Args:
        required_fields: List of required fields in request JSON
        optional_fields: List of optional fields (for documentation)
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not request.is_json:
                raise ValidationError("Request must be JSON")
            
            data = request.get_json()
            if not data:
                raise ValidationError("Request body cannot be empty")
            
            # Check required fields
            if required_fields:
                missing_fields = [field for field in required_fields if field not in data]
                if missing_fields:
                    raise ValidationError(f"Missing required fields: {missing_fields}")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def log_operation(operation_name: str = None):
    """Decorator to log operations with user context.
    
    Args:
        operation_name: Name of the operation (defaults to function name)
    """
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            auth_context = get_auth_context()
            op_name = operation_name or f.__name__
            
            # Log operation start
            logger.info(f"Starting operation: {op_name}, "
                       f"User: {user.username if user else 'anonymous'}, "
                       f"IP: {auth_context.client_ip if auth_context else 'unknown'}")
            
            try:
                result = f(*args, **kwargs)
                
                # Log operation success
                logger.info(f"Operation completed: {op_name}, "
                           f"User: {user.username if user else 'anonymous'}")
                
                return result
                
            except Exception as e:
                # Log operation failure
                logger.error(f"Operation failed: {op_name}, "
                            f"User: {user.username if user else 'anonymous'}, "
                            f"Error: {str(e)}")
                raise
        
        return decorated_function
    return decorator