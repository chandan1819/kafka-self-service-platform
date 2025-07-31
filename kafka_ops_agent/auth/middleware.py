"""Authentication and authorization middleware."""

import logging
import time
from typing import Dict, Any, List, Optional, Callable
from functools import wraps
from flask import request, jsonify, g
from collections import defaultdict
from datetime import datetime, timedelta

from .providers import AuthProvider, CompositeAuthProvider, APIKeyProvider, BasicAuthProvider
from .models import User, Role, Permission, AuthContext, RateLimitInfo
from kafka_ops_agent.exceptions import AuthenticationError, AuthorizationError

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple in-memory rate limiter."""
    
    def __init__(self):
        """Initialize rate limiter."""
        self.requests = defaultdict(list)
        self.limits = {
            'default': {'requests': 100, 'window': 60},  # 100 requests per minute
            'admin': {'requests': 1000, 'window': 60},   # 1000 requests per minute for admins
            'service_broker': {'requests': 500, 'window': 60}  # 500 requests per minute for service broker
        }
    
    def is_allowed(self, identifier: str, user: Optional[User] = None) -> tuple[bool, RateLimitInfo]:
        """Check if request is allowed under rate limits."""
        now = datetime.utcnow()
        
        # Determine rate limit based on user role
        limit_key = 'default'
        if user:
            if user.has_role(Role.ADMIN):
                limit_key = 'admin'
            elif user.has_role(Role.SERVICE_BROKER):
                limit_key = 'service_broker'
        
        limit_config = self.limits[limit_key]
        window_seconds = limit_config['window']
        max_requests = limit_config['requests']
        
        # Clean old requests
        cutoff_time = now - timedelta(seconds=window_seconds)
        self.requests[identifier] = [
            req_time for req_time in self.requests[identifier]
            if req_time > cutoff_time
        ]
        
        current_requests = len(self.requests[identifier])
        
        # Check if limit exceeded
        if current_requests >= max_requests:
            reset_time = min(self.requests[identifier]) + timedelta(seconds=window_seconds)
            rate_limit_info = RateLimitInfo(
                limit=max_requests,
                remaining=0,
                reset_time=reset_time,
                window_seconds=window_seconds
            )
            return False, rate_limit_info
        
        # Add current request
        self.requests[identifier].append(now)
        
        rate_limit_info = RateLimitInfo(
            limit=max_requests,
            remaining=max_requests - current_requests - 1,
            reset_time=now + timedelta(seconds=window_seconds),
            window_seconds=window_seconds
        )
        
        return True, rate_limit_info


class AuthMiddleware:
    """Authentication and authorization middleware."""
    
    def __init__(self, auth_provider: AuthProvider = None, 
                 enable_rate_limiting: bool = True,
                 require_auth_by_default: bool = True):
        """Initialize auth middleware.
        
        Args:
            auth_provider: Authentication provider
            enable_rate_limiting: Enable rate limiting
            require_auth_by_default: Require authentication by default
        """
        self.auth_provider = auth_provider or self._create_default_provider()
        self.rate_limiter = RateLimiter() if enable_rate_limiting else None
        self.require_auth_by_default = require_auth_by_default
        
        # Endpoints that don't require authentication
        self.public_endpoints = {
            '/health',
            '/metrics',
            '/v2/catalog'  # OSB catalog endpoint is typically public
        }
    
    def _create_default_provider(self) -> AuthProvider:
        """Create default composite auth provider."""
        return CompositeAuthProvider([
            APIKeyProvider(),
            BasicAuthProvider()
        ])
    
    def init_app(self, app):
        """Initialize middleware with Flask app."""
        app.before_request(self.before_request)
        app.after_request(self.after_request)
        
        # Register error handlers
        app.errorhandler(AuthenticationError)(self.handle_auth_error)
        app.errorhandler(AuthorizationError)(self.handle_authz_error)
    
    def before_request(self):
        """Process request before routing."""
        # Skip authentication for public endpoints
        if request.endpoint in self.public_endpoints or request.path in self.public_endpoints:
            g.auth_context = AuthContext()
            return
        
        # Extract request data
        request_data = {
            'headers': dict(request.headers),
            'client_ip': request.remote_addr,
            'user_agent': request.headers.get('User-Agent'),
            'request_id': request.headers.get('X-Request-ID')
        }
        
        # Authenticate request
        auth_context = self.auth_provider.authenticate(request_data)
        
        if not auth_context or not auth_context.authenticated:
            if self.require_auth_by_default:
                raise AuthenticationError("Authentication required")
            else:
                auth_context = AuthContext()
        
        # Rate limiting
        if self.rate_limiter and auth_context.authenticated:
            identifier = auth_context.user.user_id if auth_context.user else request.remote_addr
            allowed, rate_limit_info = self.rate_limiter.is_allowed(identifier, auth_context.user)
            
            if not allowed:
                response = jsonify({
                    'error': 'Rate limit exceeded',
                    'message': f'Too many requests. Limit: {rate_limit_info.limit} per {rate_limit_info.window_seconds}s',
                    'retry_after': int((rate_limit_info.reset_time - datetime.utcnow()).total_seconds())
                })
                response.status_code = 429
                response.headers['X-RateLimit-Limit'] = str(rate_limit_info.limit)
                response.headers['X-RateLimit-Remaining'] = str(rate_limit_info.remaining)
                response.headers['X-RateLimit-Reset'] = str(int(rate_limit_info.reset_time.timestamp()))
                return response
            
            # Add rate limit headers
            g.rate_limit_info = rate_limit_info
        
        # Store auth context in Flask g
        g.auth_context = auth_context
        
        # Log authentication
        if auth_context.authenticated:
            logger.info(f"Authenticated request: user={auth_context.user.username}, "
                       f"method={request.method}, path={request.path}")
    
    def after_request(self, response):
        """Process response after request."""
        # Add rate limit headers if available
        if hasattr(g, 'rate_limit_info'):
            rate_limit_info = g.rate_limit_info
            response.headers['X-RateLimit-Limit'] = str(rate_limit_info.limit)
            response.headers['X-RateLimit-Remaining'] = str(rate_limit_info.remaining)
            response.headers['X-RateLimit-Reset'] = str(int(rate_limit_info.reset_time.timestamp()))
        
        # Add security headers
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['X-XSS-Protection'] = '1; mode=block'
        
        return response
    
    def handle_auth_error(self, error):
        """Handle authentication errors."""
        logger.warning(f"Authentication error: {error}")
        return jsonify({
            'error': 'Authentication failed',
            'message': str(error),
            'code': 'AUTHENTICATION_FAILED'
        }), 401
    
    def handle_authz_error(self, error):
        """Handle authorization errors."""
        logger.warning(f"Authorization error: {error}")
        return jsonify({
            'error': 'Authorization failed',
            'message': str(error),
            'code': 'AUTHORIZATION_FAILED'
        }), 403


def get_current_user() -> Optional[User]:
    """Get current authenticated user."""
    auth_context = getattr(g, 'auth_context', None)
    return auth_context.user if auth_context else None


def get_auth_context() -> Optional[AuthContext]:
    """Get current authentication context."""
    return getattr(g, 'auth_context', None)


def require_auth(f):
    """Decorator to require authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_context = get_auth_context()
        if not auth_context or not auth_context.authenticated:
            raise AuthenticationError("Authentication required")
        return f(*args, **kwargs)
    return decorated_function


def require_role(role: Role):
    """Decorator to require specific role."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                raise AuthenticationError("Authentication required")
            
            if not user.has_role(role):
                raise AuthorizationError(f"Role '{role.value}' required")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_permission(permission: Permission):
    """Decorator to require specific permission."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                raise AuthenticationError("Authentication required")
            
            if not user.has_permission(permission):
                raise AuthorizationError(f"Permission '{permission.value}' required")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_any_permission(permissions: List[Permission]):
    """Decorator to require any of the specified permissions."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                raise AuthenticationError("Authentication required")
            
            if not user.has_any_permission(permissions):
                perm_names = [p.value for p in permissions]
                raise AuthorizationError(f"One of these permissions required: {perm_names}")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_all_permissions(permissions: List[Permission]):
    """Decorator to require all of the specified permissions."""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = get_current_user()
            if not user:
                raise AuthenticationError("Authentication required")
            
            if not user.has_all_permissions(permissions):
                perm_names = [p.value for p in permissions]
                raise AuthorizationError(f"All of these permissions required: {perm_names}")
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator