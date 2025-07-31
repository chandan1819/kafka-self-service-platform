"""PostgreSQL implementation of metadata storage."""

import asyncpg
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.models.cluster import ServiceInstance, ClusterStatus, RuntimeProvider
from kafka_ops_agent.config import config

logger = logging.getLogger(__name__)


class PostgreSQLMetadataStore(MetadataStore):
    """PostgreSQL implementation of metadata storage."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """Initialize PostgreSQL store."""
        if connection_string:
            self.connection_string = connection_string
        else:
            self.connection_string = (
                f"postgresql://{config.database.username}:{config.database.password}"
                f"@{config.database.host}:{config.database.port}/{config.database.database}"
            )
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self) -> None:
        """Initialize the PostgreSQL connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            
            # Create tables
            await self._create_tables()
            
            logger.info("PostgreSQL metadata store initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL store: {e}")
            raise
    
    async def _create_tables(self) -> None:
        """Create database tables."""
        async with self.pool.acquire() as connection:
            # Service instances table
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS service_instances (
                    instance_id TEXT PRIMARY KEY,
                    service_id TEXT NOT NULL,
                    plan_id TEXT NOT NULL,
                    organization_guid TEXT NOT NULL,
                    space_guid TEXT NOT NULL,
                    parameters JSONB NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    connection_info JSONB,
                    runtime_provider TEXT NOT NULL,
                    runtime_config JSONB NOT NULL,
                    error_message TEXT
                )
            ''')
            
            # Audit logs table
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id SERIAL PRIMARY KEY,
                    instance_id TEXT,
                    operation TEXT NOT NULL,
                    user_id TEXT,
                    details JSONB,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    FOREIGN KEY (instance_id) REFERENCES service_instances (instance_id) ON DELETE CASCADE
                )
            ''')
            
            # Create indexes
            await connection.execute('CREATE INDEX IF NOT EXISTS idx_instances_status ON service_instances (status)')
            await connection.execute('CREATE INDEX IF NOT EXISTS idx_instances_created ON service_instances (created_at)')
            await connection.execute('CREATE INDEX IF NOT EXISTS idx_audit_instance ON audit_logs (instance_id)')
            await connection.execute('CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs (timestamp)')
            
            logger.info("PostgreSQL tables created successfully")
    
    async def create_instance(self, instance: ServiceInstance) -> bool:
        """Create a new service instance record."""
        try:
            async with self.pool.acquire() as connection:
                await connection.execute('''
                    INSERT INTO service_instances (
                        instance_id, service_id, plan_id, organization_guid, space_guid,
                        parameters, status, created_at, updated_at, connection_info,
                        runtime_provider, runtime_config, error_message
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ''', 
                    instance.instance_id,
                    instance.service_id,
                    instance.plan_id,
                    instance.organization_guid,
                    instance.space_guid,
                    json.dumps(instance.parameters),
                    instance.status.value,
                    instance.created_at,
                    instance.updated_at,
                    json.dumps(instance.connection_info.dict()) if instance.connection_info else None,
                    instance.runtime_provider.value,
                    json.dumps(instance.runtime_config),
                    instance.error_message
                )
                
                logger.info(f"Created service instance {instance.instance_id}")
                return True
                
        except asyncpg.UniqueViolationError:
            logger.error(f"Instance {instance.instance_id} already exists")
            return False
        except Exception as e:
            logger.error(f"Failed to create instance {instance.instance_id}: {e}")
            return False
    
    async def get_instance(self, instance_id: str) -> Optional[ServiceInstance]:
        """Retrieve service instance by ID."""
        try:
            async with self.pool.acquire() as connection:
                row = await connection.fetchrow(
                    'SELECT * FROM service_instances WHERE instance_id = $1',
                    instance_id
                )
                
                if not row:
                    return None
                
                return self._row_to_instance(row)
                
        except Exception as e:
            logger.error(f"Failed to get instance {instance_id}: {e}")
            return None
    
    async def update_instance(self, instance: ServiceInstance) -> bool:
        """Update service instance data."""
        try:
            async with self.pool.acquire() as connection:
                result = await connection.execute('''
                    UPDATE service_instances SET
                        service_id = $2, plan_id = $3, organization_guid = $4, space_guid = $5,
                        parameters = $6, status = $7, updated_at = $8, connection_info = $9,
                        runtime_provider = $10, runtime_config = $11, error_message = $12
                    WHERE instance_id = $1
                ''',
                    instance.instance_id,
                    instance.service_id,
                    instance.plan_id,
                    instance.organization_guid,
                    instance.space_guid,
                    json.dumps(instance.parameters),
                    instance.status.value,
                    datetime.utcnow(),
                    json.dumps(instance.connection_info.dict()) if instance.connection_info else None,
                    instance.runtime_provider.value,
                    json.dumps(instance.runtime_config),
                    instance.error_message
                )
                
                if result == 'UPDATE 0':
                    logger.warning(f"No instance found with ID {instance.instance_id}")
                    return False
                
                logger.info(f"Updated service instance {instance.instance_id}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update instance {instance.instance_id}: {e}")
            return False
    
    async def delete_instance(self, instance_id: str) -> bool:
        """Delete service instance record."""
        try:
            async with self.pool.acquire() as connection:
                result = await connection.execute(
                    'DELETE FROM service_instances WHERE instance_id = $1',
                    instance_id
                )
                
                if result == 'DELETE 0':
                    logger.warning(f"No instance found with ID {instance_id}")
                    return False
                
                logger.info(f"Deleted service instance {instance_id}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to delete instance {instance_id}: {e}")
            return False
    
    async def list_instances(self, filters: Optional[Dict[str, Any]] = None) -> List[ServiceInstance]:
        """List all service instances with optional filters."""
        try:
            async with self.pool.acquire() as connection:
                query = 'SELECT * FROM service_instances'
                params = []
                
                if filters:
                    conditions = []
                    param_count = 1
                    
                    for key, value in filters.items():
                        if key in ['status', 'runtime_provider', 'service_id']:
                            conditions.append(f'{key} = ${param_count}')
                            params.append(value)
                            param_count += 1
                    
                    if conditions:
                        query += ' WHERE ' + ' AND '.join(conditions)
                
                query += ' ORDER BY created_at DESC'
                
                rows = await connection.fetch(query, *params)
                return [self._row_to_instance(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Failed to list instances: {e}")
            return []
    
    async def instance_exists(self, instance_id: str) -> bool:
        """Check if instance exists."""
        try:
            async with self.pool.acquire() as connection:
                result = await connection.fetchval(
                    'SELECT 1 FROM service_instances WHERE instance_id = $1',
                    instance_id
                )
                return result is not None
                
        except Exception as e:
            logger.error(f"Failed to check instance existence {instance_id}: {e}")
            return False
    
    async def get_instances_by_status(self, status: str) -> List[ServiceInstance]:
        """Get instances by status."""
        return await self.list_instances({'status': status})
    
    async def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("PostgreSQL connection pool closed")
    
    def _row_to_instance(self, row) -> ServiceInstance:
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
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            connection_info=connection_info,
            runtime_provider=RuntimeProvider(row['runtime_provider']),
            runtime_config=json.loads(row['runtime_config']),
            error_message=row['error_message']
        )


class PostgreSQLAuditStore(AuditStore):
    """PostgreSQL implementation of audit logging."""
    
    def __init__(self, metadata_store: PostgreSQLMetadataStore):
        """Initialize audit store with shared pool."""
        self.metadata_store = metadata_store
    
    @property
    def pool(self) -> asyncpg.Pool:
        """Get connection pool."""
        return self.metadata_store.pool
    
    async def log_operation(
        self, 
        instance_id: str, 
        operation: str, 
        user_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an operation for audit purposes."""
        try:
            async with self.pool.acquire() as connection:
                await connection.execute('''
                    INSERT INTO audit_logs (instance_id, operation, user_id, details, timestamp)
                    VALUES ($1, $2, $3, $4, $5)
                ''',
                    instance_id,
                    operation,
                    user_id,
                    json.dumps(details) if details else None,
                    datetime.utcnow()
                )
                
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
            async with self.pool.acquire() as connection:
                query = 'SELECT * FROM audit_logs'
                params = []
                conditions = []
                param_count = 1
                
                if instance_id:
                    conditions.append(f'instance_id = ${param_count}')
                    params.append(instance_id)
                    param_count += 1
                
                if operation:
                    conditions.append(f'operation = ${param_count}')
                    params.append(operation)
                    param_count += 1
                
                if conditions:
                    query += ' WHERE ' + ' AND '.join(conditions)
                
                query += f' ORDER BY timestamp DESC LIMIT ${param_count}'
                params.append(limit)
                
                rows = await connection.fetch(query, *params)
                
                logs = []
                for row in rows:
                    log_entry = {
                        'id': row['id'],
                        'instance_id': row['instance_id'],
                        'operation': row['operation'],
                        'user_id': row['user_id'],
                        'timestamp': row['timestamp'].isoformat(),
                        'details': json.loads(row['details']) if row['details'] else None
                    }
                    logs.append(log_entry)
                
                return logs
                
        except Exception as e:
            logger.error(f"Failed to get audit logs: {e}")
            return []