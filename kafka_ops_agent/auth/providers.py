"""Authentication providers."""

import logging
import hashlib
import hmac
import base64
import jwt
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from flask import request

from .models import User, Role, AuthContext

logger = logging.getLogger(__name__)


class AuthProvider(ABC):
    """Abstract base class for authentication providers."""
    
    @abstractmethod
    def authenticate(self, request_data: Dict[str, Any]) -> Optional[AuthContext]:
        """Authenticate a request and return auth context."""
        pass
    
    @abstractmethod
    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        pass


class APIKeyProvider(AuthProvider):
    """API Key authentication provider."""
    
    def __init__(self, api_keys: Dict[str, Dict[str, Any]] = None):
        """Initialize with API key configuration.
        
        Args:
            api_keys: Dictionary mapping API keys to user info
                     Format: {api_key: {user_id, username, roles, permissions}}
        """
        self.api_keys = api_keys or {}
        
        # Default API keys for development/testing
        if not self.api_keys:
            self._setup_default_keys()
    
    def _setup_default_keys(self):
        """Setup default API keys for development."""
        self.api_keys = {
            'admin-key-12345': {
                'user_id': 'admin',
                'username': 'admin',
                'email': 'admin@kafka-ops.local',
                'roles': [Role.ADMIN],
                'permissions': []
            },
            'operator-key-67890': {
                'user_id': 'operator',
                'username': 'operator',
                'email': 'operator@kafka-ops.local',
                'roles': [Role.OPERATOR],
                'permissions': []
            },
            'developer-key-abcde': {
                'user_id': 'developer',
                'username': 'developer',
                'email': 'developer@kafka-ops.local',
                'roles': [Role.DEVELOPER],
                'permissions': []
            },
            'service-broker-key-xyz': {
                'user_id': 'service-broker',
                'username': 'service-broker',
                'email': 'broker@kafka-ops.local',
                'roles': [Role.SERVICE_BROKER],
                'permissions': []
            }
        }
    
    def authenticate(self, request_data: Dict[str, Any]) -> Optional[AuthContext]:
        """Authenticate using API key."""
        # Try different header formats
        api_key = (
            request_data.get('headers', {}).get('X-API-Key') or
            request_data.get('headers', {}).get('Authorization', '').replace('Bearer ', '') or
            request_data.get('headers', {}).get('Authorization', '').replace('ApiKey ', '')
        )
        
        if not api_key:
            return None
        
        user_info = self.api_keys.get(api_key)
        if not user_info:
            logger.warning(f"Invalid API key attempted: {api_key[:8]}...")
            return None
        
        user = User(
            user_id=user_info['user_id'],
            username=user_info['username'],
            email=user_info.get('email'),
            roles=user_info.get('roles', []),
            permissions=user_info.get('permissions', []),
            last_login=datetime.utcnow()
        )
        
        return AuthContext(
            user=user,
            token=api_key,
            auth_method='api_key',
            client_ip=request_data.get('client_ip'),
            user_agent=request_data.get('user_agent'),
            request_id=request_data.get('request_id'),
            authenticated=True
        )
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        for api_key, user_info in self.api_keys.items():
            if user_info['user_id'] == user_id:
                return User(
                    user_id=user_info['user_id'],
                    username=user_info['username'],
                    email=user_info.get('email'),
                    roles=user_info.get('roles', []),
                    permissions=user_info.get('permissions', [])
                )
        return None
    
    def add_api_key(self, api_key: str, user_info: Dict[str, Any]):
        """Add a new API key."""
        self.api_keys[api_key] = user_info
    
    def revoke_api_key(self, api_key: str):
        """Revoke an API key."""
        self.api_keys.pop(api_key, None)


class BasicAuthProvider(AuthProvider):
    """Basic HTTP authentication provider."""
    
    def __init__(self, users: Dict[str, Dict[str, Any]] = None):
        """Initialize with user credentials.
        
        Args:
            users: Dictionary mapping usernames to user info
                  Format: {username: {password_hash, user_id, roles, permissions}}
        """
        self.users = users or {}
        
        # Default users for development/testing
        if not self.users:
            self._setup_default_users()
    
    def _setup_default_users(self):
        """Setup default users for development."""
        self.users = {
            'admin': {
                'password_hash': self._hash_password('admin123'),
                'user_id': 'admin',
                'email': 'admin@kafka-ops.local',
                'roles': [Role.ADMIN],
                'permissions': []
            },
            'operator': {
                'password_hash': self._hash_password('operator123'),
                'user_id': 'operator',
                'email': 'operator@kafka-ops.local',
                'roles': [Role.OPERATOR],
                'permissions': []
            }
        }
    
    def _hash_password(self, password: str) -> str:
        """Hash password using SHA-256."""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        return hmac.compare_digest(self._hash_password(password), password_hash)
    
    def authenticate(self, request_data: Dict[str, Any]) -> Optional[AuthContext]:
        """Authenticate using Basic Auth."""
        auth_header = request_data.get('headers', {}).get('Authorization', '')
        
        if not auth_header.startswith('Basic '):
            return None
        
        try:
            # Decode base64 credentials
            encoded_credentials = auth_header[6:]  # Remove 'Basic '
            decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
            username, password = decoded_credentials.split(':', 1)
        except (ValueError, UnicodeDecodeError):
            logger.warning("Invalid Basic Auth format")
            return None
        
        user_info = self.users.get(username)
        if not user_info:
            logger.warning(f"Unknown username: {username}")
            return None
        
        if not self._verify_password(password, user_info['password_hash']):
            logger.warning(f"Invalid password for user: {username}")
            return None
        
        user = User(
            user_id=user_info['user_id'],
            username=username,
            email=user_info.get('email'),
            roles=user_info.get('roles', []),
            permissions=user_info.get('permissions', []),
            last_login=datetime.utcnow()
        )
        
        return AuthContext(
            user=user,
            token=auth_header,
            auth_method='basic_auth',
            client_ip=request_data.get('client_ip'),
            user_agent=request_data.get('user_agent'),
            request_id=request_data.get('request_id'),
            authenticated=True
        )
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        for username, user_info in self.users.items():
            if user_info['user_id'] == user_id:
                return User(
                    user_id=user_info['user_id'],
                    username=username,
                    email=user_info.get('email'),
                    roles=user_info.get('roles', []),
                    permissions=user_info.get('permissions', [])
                )
        return None


class JWTProvider(AuthProvider):
    """JWT authentication provider."""
    
    def __init__(self, secret_key: str, algorithm: str = 'HS256', 
                 token_expiry_hours: int = 24):
        """Initialize JWT provider.
        
        Args:
            secret_key: Secret key for JWT signing
            algorithm: JWT algorithm (default: HS256)
            token_expiry_hours: Token expiry time in hours
        """
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.token_expiry_hours = token_expiry_hours
    
    def authenticate(self, request_data: Dict[str, Any]) -> Optional[AuthContext]:
        """Authenticate using JWT token."""
        auth_header = request_data.get('headers', {}).get('Authorization', '')
        
        if not auth_header.startswith('Bearer '):
            return None
        
        token = auth_header[7:]  # Remove 'Bearer '
        
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check token expiry
            if 'exp' in payload and datetime.utcfromtimestamp(payload['exp']) < datetime.utcnow():
                logger.warning("JWT token expired")
                return None
            
            user = User(
                user_id=payload['user_id'],
                username=payload['username'],
                email=payload.get('email'),
                roles=[Role(role) for role in payload.get('roles', [])],
                permissions=[Permission(perm) for perm in payload.get('permissions', [])],
                last_login=datetime.utcnow()
            )
            
            return AuthContext(
                user=user,
                token=token,
                auth_method='jwt',
                client_ip=request_data.get('client_ip'),
                user_agent=request_data.get('user_agent'),
                request_id=request_data.get('request_id'),
                authenticated=True
            )
            
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid JWT token: {e}")
            return None
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID (JWT provider doesn't store users)."""
        # JWT provider is stateless, cannot retrieve users
        return None
    
    def create_token(self, user: User) -> str:
        """Create JWT token for user."""
        payload = {
            'user_id': user.user_id,
            'username': user.username,
            'email': user.email,
            'roles': [role.value for role in user.roles],
            'permissions': [perm.value for perm in user.permissions],
            'iat': datetime.utcnow(),
            'exp': datetime.utcnow() + timedelta(hours=self.token_expiry_hours)
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)


class CompositeAuthProvider(AuthProvider):
    """Composite authentication provider that tries multiple providers."""
    
    def __init__(self, providers: list[AuthProvider]):
        """Initialize with list of providers."""
        self.providers = providers
    
    def authenticate(self, request_data: Dict[str, Any]) -> Optional[AuthContext]:
        """Try authentication with each provider."""
        for provider in self.providers:
            auth_context = provider.authenticate(request_data)
            if auth_context and auth_context.authenticated:
                return auth_context
        return None
    
    def get_user(self, user_id: str) -> Optional[User]:
        """Get user from any provider."""
        for provider in self.providers:
            user = provider.get_user(user_id)
            if user:
                return user
        return None