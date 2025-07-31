"""Authentication and authorization models."""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


class Permission(str, Enum):
    """System permissions."""
    # Cluster permissions
    CLUSTER_CREATE = "cluster:create"
    CLUSTER_READ = "cluster:read"
    CLUSTER_UPDATE = "cluster:update"
    CLUSTER_DELETE = "cluster:delete"
    
    # Topic permissions
    TOPIC_CREATE = "topic:create"
    TOPIC_READ = "topic:read"
    TOPIC_UPDATE = "topic:update"
    TOPIC_DELETE = "topic:delete"
    TOPIC_PURGE = "topic:purge"
    
    # Admin permissions
    ADMIN_READ = "admin:read"
    ADMIN_WRITE = "admin:write"
    
    # Service broker permissions
    BROKER_PROVISION = "broker:provision"
    BROKER_BIND = "broker:bind"
    BROKER_UNBIND = "broker:unbind"
    BROKER_DEPROVISION = "broker:deprovision"
    
    # Cleanup permissions
    CLEANUP_EXECUTE = "cleanup:execute"
    CLEANUP_READ = "cleanup:read"


class Role(str, Enum):
    """System roles with predefined permissions."""
    ADMIN = "admin"
    OPERATOR = "operator"
    DEVELOPER = "developer"
    VIEWER = "viewer"
    SERVICE_BROKER = "service_broker"


# Role to permissions mapping
ROLE_PERMISSIONS = {
    Role.ADMIN: [
        Permission.CLUSTER_CREATE, Permission.CLUSTER_READ, Permission.CLUSTER_UPDATE, Permission.CLUSTER_DELETE,
        Permission.TOPIC_CREATE, Permission.TOPIC_READ, Permission.TOPIC_UPDATE, Permission.TOPIC_DELETE, Permission.TOPIC_PURGE,
        Permission.ADMIN_READ, Permission.ADMIN_WRITE,
        Permission.BROKER_PROVISION, Permission.BROKER_BIND, Permission.BROKER_UNBIND, Permission.BROKER_DEPROVISION,
        Permission.CLEANUP_EXECUTE, Permission.CLEANUP_READ
    ],
    Role.OPERATOR: [
        Permission.CLUSTER_READ, Permission.CLUSTER_UPDATE,
        Permission.TOPIC_CREATE, Permission.TOPIC_READ, Permission.TOPIC_UPDATE, Permission.TOPIC_DELETE,
        Permission.CLEANUP_EXECUTE, Permission.CLEANUP_READ
    ],
    Role.DEVELOPER: [
        Permission.CLUSTER_READ,
        Permission.TOPIC_CREATE, Permission.TOPIC_READ, Permission.TOPIC_UPDATE, Permission.TOPIC_DELETE,
        Permission.CLEANUP_READ
    ],
    Role.VIEWER: [
        Permission.CLUSTER_READ,
        Permission.TOPIC_READ,
        Permission.CLEANUP_READ
    ],
    Role.SERVICE_BROKER: [
        Permission.BROKER_PROVISION, Permission.BROKER_BIND, Permission.BROKER_UNBIND, Permission.BROKER_DEPROVISION,
        Permission.CLUSTER_CREATE, Permission.CLUSTER_READ, Permission.CLUSTER_DELETE
    ]
}


@dataclass
class User:
    """User model for authentication and authorization."""
    user_id: str
    username: str
    email: Optional[str] = None
    roles: List[Role] = None
    permissions: List[Permission] = None
    metadata: Dict[str, Any] = None
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    is_active: bool = True
    
    def __post_init__(self):
        """Initialize default values."""
        if self.roles is None:
            self.roles = []
        if self.permissions is None:
            self.permissions = []
        if self.metadata is None:
            self.metadata = {}
        if self.created_at is None:
            self.created_at = datetime.utcnow()
    
    def has_role(self, role: Role) -> bool:
        """Check if user has a specific role."""
        return role in self.roles
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has a specific permission."""
        # Check direct permissions
        if permission in self.permissions:
            return True
        
        # Check role-based permissions
        for role in self.roles:
            if permission in ROLE_PERMISSIONS.get(role, []):
                return True
        
        return False
    
    def has_any_permission(self, permissions: List[Permission]) -> bool:
        """Check if user has any of the specified permissions."""
        return any(self.has_permission(perm) for perm in permissions)
    
    def has_all_permissions(self, permissions: List[Permission]) -> bool:
        """Check if user has all of the specified permissions."""
        return all(self.has_permission(perm) for perm in permissions)
    
    def get_all_permissions(self) -> List[Permission]:
        """Get all permissions for the user (direct + role-based)."""
        all_permissions = set(self.permissions)
        
        for role in self.roles:
            all_permissions.update(ROLE_PERMISSIONS.get(role, []))
        
        return list(all_permissions)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert user to dictionary."""
        return {
            'user_id': self.user_id,
            'username': self.username,
            'email': self.email,
            'roles': [role.value for role in self.roles],
            'permissions': [perm.value for perm in self.permissions],
            'metadata': self.metadata,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'is_active': self.is_active
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'User':
        """Create user from dictionary."""
        return cls(
            user_id=data['user_id'],
            username=data['username'],
            email=data.get('email'),
            roles=[Role(role) for role in data.get('roles', [])],
            permissions=[Permission(perm) for perm in data.get('permissions', [])],
            metadata=data.get('metadata', {}),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            last_login=datetime.fromisoformat(data['last_login']) if data.get('last_login') else None,
            is_active=data.get('is_active', True)
        )


@dataclass
class AuthContext:
    """Authentication context for requests."""
    user: Optional[User] = None
    token: Optional[str] = None
    auth_method: Optional[str] = None
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    request_id: Optional[str] = None
    authenticated: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert auth context to dictionary."""
        return {
            'user': self.user.to_dict() if self.user else None,
            'token': self.token,
            'auth_method': self.auth_method,
            'client_ip': self.client_ip,
            'user_agent': self.user_agent,
            'request_id': self.request_id,
            'authenticated': self.authenticated
        }


@dataclass
class RateLimitInfo:
    """Rate limiting information."""
    limit: int
    remaining: int
    reset_time: datetime
    window_seconds: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'limit': self.limit,
            'remaining': self.remaining,
            'reset_time': self.reset_time.isoformat(),
            'window_seconds': self.window_seconds
        }