"""SQLite implementation of metadata storage."""

import sqlite3
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path

from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.models.cluster import ServiceInstance, ClusterStatus, RuntimeProvider
from kafka_ops_agent.config import config

logger = logging.getLogger(__name__)


class SQLiteMetadataStore(MetadataStore):
    """SQLite implementation of metadata storage."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize SQLite store."""
        self.db_path = db_path or config.database.sqlite_path
        self.connection: Optional[sqlite3.Connection] = None
        
    async def initialize(self) -> None:
        """Initialize the SQLite database."""
        try:
            # Ensure directory exists
            db_path = Path(self.db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Connect to database
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            
            # Create tables
            await self._create_tables()
            
            logger.info(f"SQLite metadata store initialized at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQLite store: {e}")
            raise
    
    async def _create_tables(self) -> None:
        """Create database tables."""
        cursor = self.connection.cursor()
        
        # Service instances table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_instances (
                instance_id TEXT PRIMARY KEY,
                service_id TEXT NOT NULL,
                plan_id TEXT NOT NULL,
                organization_guid TEXT NOT NULL,
                space_guid TEXT NOT NULL,
                parameters TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                connection_info TEXT,
                runtime_provider TEXT NOT NULL,
                runtime_config TEXT NOT NULL,
                error_message TEXT
            )
        ''')
        
        # Audit logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id TEXT,
                operation TEXT NOT NULL,
                user_id TEXT,
                details TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (instance_id) REFERENCES service_instances (instance_id)
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_instances_status ON service_instances (status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_instances_created ON service_instances (created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_instance ON audit_logs (instance_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs (timestamp)')
        
        self.connection.commit()
        logger.info("Database tables created successfully")
    
    async def create_instance(self, instance: ServiceInstance) -> bool:
        """Create a new service instance record."""
        try:
            cursor = self.connection.cursor()
            
            cursor.execute('''
                INSERT INTO service_instances (
                    instance_id, service_id, plan_id, organization_guid, space_guid,
                    parameters, status, created_at, updated_at, connection_info,
                    runtime_provider, runtime_config, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                instance.instance_id,
                instance.service_id,
                instance.plan_id,
                instance.organization_guid,
                instance.space_guid,
                json.dumps(instance.parameters),
                instance.status.value,
                instance.created_at.isoformat(),
                instance.updated_at.isoformat(),
                json.dumps(instance.connection_info.dict()) if instance.connection_info else None,
                instance.runtime_provider.value,
                json.dumps(instance.runtime_config),
                instance.error_message
            ))
            
            self.connection.commit()
            logger.info(f"Created service instance {instance.instance_id}")
            return True
            
        except sqlite3.IntegrityError as e:
            logger.error(f"Instance {instance.instance_id} already exists: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to create instance {instance.instance_id}: {e}")
            return False
    
    async def get_instance(self, instance_id: str) -> Optional[ServiceInstance]:
        """Retrieve service instance by ID."""
        try:
            cursor = self.connection.cursor()
            cursor.execute('SELECT * FROM service_instances WHERE instance_id = ?', (instance_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return self._row_to_instance(row)
            
        except Exception as e:
            logger.error(f"Failed to get instance {instance_id}: {e}")
            return None
    
    async def update_instance(self, instance: ServiceInstance) -> bool:
        """Update service instance data."""
        try:
            cursor = self.connection.cursor()
            
            cursor.execute('''
                UPDATE service_instances SET
                    service_id = ?, plan_id = ?, organization_guid = ?, space_guid = ?,
                    parameters = ?, status = ?, updated_at = ?, connection_info = ?,
                    runtime_provider = ?, runtime_config = ?, error_message = ?
                WHERE instance_id = ?
            ''', (
                instance.service_id,
                instance.plan_id,
                instance.organization_guid,
                instance.space_guid,
                json.dumps(instance.parameters),
                instance.status.value,
                datetime.utcnow().isoformat(),
                json.dumps(instance.connection_info.dict()) if instance.connection_info else None,
                instance.runtime_provider.value,
                json.dumps(instance.runtime_config),
                instance.error_message,
                instance.instance_id
            ))
            
            if cursor.rowcount == 0:
                logger.warning(f"No instance found with ID {instance.instance_id}")
                return False
            
            self.connection.commit()
            logger.info(f"Updated service instance {instance.instance_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update instance {instance.instance_id}: {e}")
            return False
    
    async def delete_instance(self, instance_id: str) -> bool:
        """Delete service instance record."""
        try:
            cursor = self.connection.cursor()
            
            # Delete audit logs first (foreign key constraint)
            cursor.execute('DELETE FROM audit_logs WHERE instance_id = ?', (instance_id,))
            
            # Delete instance
            cursor.execute('DELETE FROM service_instances WHERE instance_id = ?', (instance_id,))
            
            if cursor.rowcount == 0:
                logger.warning(f"No instance found with ID {instance_id}")
                return False
            
            self.connection.commit()
            logger.info(f"Deleted service instance {instance_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete instance {instance_id}: {e}")
            return False
    
    async def list_instances(self, filters: Optional[Dict[str, Any]] = None) -> List[ServiceInstance]:
        """List all service instances with optional filters."""
        try:
            cursor = self.connection.cursor()
            
            query = 'SELECT * FROM service_instances'
            params = []
            
            if filters:
                conditions = []
                for key, value in filters.items():
                    if key == 'status':
                        conditions.append('status = ?')
                        params.append(value)
                    elif key == 'runtime_provider':
                        conditions.append('runtime_provider = ?')
                        params.append(value)
                    elif key == 'service_id':
                        conditions.append('service_id = ?')
                        params.append(value)
                
                if conditions:
                    query += ' WHERE ' + ' AND '.join(conditions)
            
            query += ' ORDER BY created_at DESC'
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [self._row_to_instance(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to list instances: {e}")
            return []
    
    async def instance_exists(self, instance_id: str) -> bool:
        """Check if instance exists."""
        try:
            cursor = self.connection.cursor()
            cursor.execute('SELECT 1 FROM service_instances WHERE instance_id = ?', (instance_id,))
            return cursor.fetchone() is not None
            
        except Exception as e:
            logger.error(f"Failed to check instance existence {instance_id}: {e}")
            return False
    
    async def get_instances_by_status(self, status: str) -> List[ServiceInstance]:
        """Get instances by status."""
        return await self.list_instances({'status': status})
    
    async def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("SQLite connection closed")
    
    def _row_to_instance(self, row: sqlite3.Row) -> ServiceInstance:
        """Convert database row to ServiceInstance."""
        from kafka_ops_agent.models.cluster import ConnectionInfo
        
        connection_info = None
        if row['connection_info']:
            connection_info_data = json.loads(row['connection_info'])
            connection_info = ConnectionInfo(**connection_info_data)
        
        return ServiceInstance(
            instance_id=row['instance_id'],
            service_id=row['service_id'],
            plan_id=row['plan_id'],
            organization_guid=row['organization_guid'],
            space_guid=row['space_guid'],
            parameters=json.loads(row['parameters']),
            status=ClusterStatus(row['status']),
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at']),
            connection_info=connection_info,
            runtime_provider=RuntimeProvider(row['runtime_provider']),
            runtime_config=json.loads(row['runtime_config']),
            error_message=row['error_message']
        )


class SQLiteAuditStore(AuditStore):
    """SQLite implementation of audit logging."""
    
    def __init__(self, metadata_store: SQLiteMetadataStore):
        """Initialize audit store with shared connection."""
        self.metadata_store = metadata_store
    
    @property
    def connection(self) -> sqlite3.Connection:
        """Get database connection."""
        return self.metadata_store.connection
    
    async def log_operation(
        self, 
        instance_id: str, 
        operation: str, 
        user_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an operation for audit purposes."""
        try:
            cursor = self.connection.cursor()
            
            cursor.execute('''
                INSERT INTO audit_logs (instance_id, operation, user_id, details, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                instance_id,
                operation,
                user_id,
                json.dumps(details) if details else None,
                datetime.utcnow().isoformat()
            ))
            
            self.connection.commit()
            logger.debug(f"Logged audit operation: {operation} for instance {instance_id}")
            
        except Exception as e:
            logger.error(f"Failed to log audit operation: {e}")
    
    async def get_audit_logs(
        self, 
        instance_id: Optional[str] = None,
        operation: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Retrieve audit logs with optional filters."""
        try:
            cursor = self.connection.cursor()
            
            query = 'SELECT * FROM audit_logs'
            params = []
            conditions = []
            
            if instance_id:
                conditions.append('instance_id = ?')
                params.append(instance_id)
            
            if operation:
                conditions.append('operation = ?')
                params.append(operation)
            
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
            
            query += ' ORDER BY timestamp DESC LIMIT ?'
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            logs = []
            for row in rows:
                log_entry = {
                    'id': row['id'],
                    'instance_id': row['instance_id'],
                    'operation': row['operation'],
                    'user_id': row['user_id'],
                    'timestamp': row['timestamp'],
                    'details': json.loads(row['details']) if row['details'] else None
                }
                logs.append(log_entry)
            
            return logs
            
        except Exception as e:
            logger.error(f"Failed to get audit logs: {e}")
            return []