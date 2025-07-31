"""Database migration system."""

import logging
from typing import List, Dict, Any
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class Migration(ABC):
    """Abstract base class for database migrations."""
    
    def __init__(self, version: str, description: str):
        self.version = version
        self.description = description
    
    @abstractmethod
    async def up(self, connection) -> None:
        """Apply the migration."""
        pass
    
    @abstractmethod
    async def down(self, connection) -> None:
        """Rollback the migration."""
        pass


class Migration001_InitialSchema(Migration):
    """Initial database schema migration."""
    
    def __init__(self):
        super().__init__("001", "Initial database schema")
    
    async def up(self, connection) -> None:
        """Create initial tables."""
        # This is handled by the store initialization
        # but could be moved here for better migration control
        pass
    
    async def down(self, connection) -> None:
        """Drop initial tables."""
        if hasattr(connection, 'execute'):  # SQLite
            await connection.execute('DROP TABLE IF EXISTS audit_logs')
            await connection.execute('DROP TABLE IF EXISTS service_instances')
        else:  # PostgreSQL
            await connection.execute('DROP TABLE IF EXISTS audit_logs CASCADE')
            await connection.execute('DROP TABLE IF EXISTS service_instances CASCADE')


class Migration002_AddIndexes(Migration):
    """Add performance indexes."""
    
    def __init__(self):
        super().__init__("002", "Add performance indexes")
    
    async def up(self, connection) -> None:
        """Add indexes."""
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_instances_provider ON service_instances (runtime_provider)',
            'CREATE INDEX IF NOT EXISTS idx_instances_org ON service_instances (organization_guid)',
            'CREATE INDEX IF NOT EXISTS idx_audit_operation ON audit_logs (operation)',
            'CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_logs (user_id)'
        ]
        
        for index_sql in indexes:
            if hasattr(connection, 'execute'):  # SQLite
                connection.execute(index_sql)
            else:  # PostgreSQL
                await connection.execute(index_sql)
    
    async def down(self, connection) -> None:
        """Drop indexes."""
        indexes = [
            'DROP INDEX IF EXISTS idx_instances_provider',
            'DROP INDEX IF EXISTS idx_instances_org',
            'DROP INDEX IF EXISTS idx_audit_operation',
            'DROP INDEX IF EXISTS idx_audit_user'
        ]
        
        for index_sql in indexes:
            if hasattr(connection, 'execute'):  # SQLite
                connection.execute(index_sql)
            else:  # PostgreSQL
                await connection.execute(index_sql)


class Migration003_AddMetricsTable(Migration):
    """Add metrics storage table."""
    
    def __init__(self):
        super().__init__("003", "Add metrics storage table")
    
    async def up(self, connection) -> None:
        """Create metrics table."""
        if hasattr(connection, 'execute'):  # SQLite
            connection.execute('''
                CREATE TABLE IF NOT EXISTS cluster_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    instance_id TEXT NOT NULL,
                    broker_count INTEGER NOT NULL,
                    topic_count INTEGER NOT NULL,
                    partition_count INTEGER NOT NULL,
                    total_messages INTEGER,
                    total_size_bytes INTEGER,
                    cpu_usage_percent REAL,
                    memory_usage_percent REAL,
                    disk_usage_percent REAL,
                    collected_at TEXT NOT NULL,
                    FOREIGN KEY (instance_id) REFERENCES service_instances (instance_id)
                )
            ''')
            connection.execute('CREATE INDEX IF NOT EXISTS idx_metrics_instance ON cluster_metrics (instance_id)')
            connection.execute('CREATE INDEX IF NOT EXISTS idx_metrics_collected ON cluster_metrics (collected_at)')
        else:  # PostgreSQL
            await connection.execute('''
                CREATE TABLE IF NOT EXISTS cluster_metrics (
                    id SERIAL PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    broker_count INTEGER NOT NULL,
                    topic_count INTEGER NOT NULL,
                    partition_count INTEGER NOT NULL,
                    total_messages BIGINT,
                    total_size_bytes BIGINT,
                    cpu_usage_percent REAL,
                    memory_usage_percent REAL,
                    disk_usage_percent REAL,
                    collected_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    FOREIGN KEY (instance_id) REFERENCES service_instances (instance_id) ON DELETE CASCADE
                )
            ''')
            await connection.execute('CREATE INDEX IF NOT EXISTS idx_metrics_instance ON cluster_metrics (instance_id)')
            await connection.execute('CREATE INDEX IF NOT EXISTS idx_metrics_collected ON cluster_metrics (collected_at)')
    
    async def down(self, connection) -> None:
        """Drop metrics table."""
        if hasattr(connection, 'execute'):  # SQLite
            connection.execute('DROP TABLE IF EXISTS cluster_metrics')
        else:  # PostgreSQL
            await connection.execute('DROP TABLE IF EXISTS cluster_metrics CASCADE')


class MigrationManager:
    """Manages database migrations."""
    
    def __init__(self, metadata_store):
        self.metadata_store = metadata_store
        self.migrations: List[Migration] = [
            Migration001_InitialSchema(),
            Migration002_AddIndexes(),
            Migration003_AddMetricsTable()
        ]
    
    async def get_current_version(self) -> str:
        """Get current database version."""
        try:
            if hasattr(self.metadata_store, 'connection'):  # SQLite
                cursor = self.metadata_store.connection.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at TEXT NOT NULL
                    )
                ''')
                cursor.execute('SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1')
                result = cursor.fetchone()
                return result[0] if result else "000"
            else:  # PostgreSQL
                async with self.metadata_store.pool.acquire() as connection:
                    await connection.execute('''
                        CREATE TABLE IF NOT EXISTS schema_migrations (
                            version TEXT PRIMARY KEY,
                            applied_at TIMESTAMP WITH TIME ZONE NOT NULL
                        )
                    ''')
                    result = await connection.fetchval(
                        'SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1'
                    )
                    return result or "000"
        except Exception as e:
            logger.error(f"Failed to get current version: {e}")
            return "000"
    
    async def apply_migrations(self) -> None:
        """Apply pending migrations."""
        current_version = await self.get_current_version()
        logger.info(f"Current database version: {current_version}")
        
        pending_migrations = [
            m for m in self.migrations 
            if m.version > current_version
        ]
        
        if not pending_migrations:
            logger.info("No pending migrations")
            return
        
        logger.info(f"Applying {len(pending_migrations)} migrations")
        
        for migration in pending_migrations:
            try:
                logger.info(f"Applying migration {migration.version}: {migration.description}")
                
                if hasattr(self.metadata_store, 'connection'):  # SQLite
                    await migration.up(self.metadata_store.connection)
                    cursor = self.metadata_store.connection.cursor()
                    cursor.execute(
                        'INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)',
                        (migration.version, datetime.utcnow().isoformat())
                    )
                    self.metadata_store.connection.commit()
                else:  # PostgreSQL
                    async with self.metadata_store.pool.acquire() as connection:
                        await migration.up(connection)
                        await connection.execute(
                            'INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)',
                            migration.version, datetime.utcnow()
                        )
                
                logger.info(f"Migration {migration.version} applied successfully")
                
            except Exception as e:
                logger.error(f"Failed to apply migration {migration.version}: {e}")
                raise
    
    async def rollback_migration(self, target_version: str) -> None:
        """Rollback to a specific version."""
        current_version = await self.get_current_version()
        
        if target_version >= current_version:
            logger.info("No rollback needed")
            return
        
        migrations_to_rollback = [
            m for m in reversed(self.migrations)
            if target_version < m.version <= current_version
        ]
        
        logger.info(f"Rolling back {len(migrations_to_rollback)} migrations")
        
        for migration in migrations_to_rollback:
            try:
                logger.info(f"Rolling back migration {migration.version}: {migration.description}")
                
                if hasattr(self.metadata_store, 'connection'):  # SQLite
                    await migration.down(self.metadata_store.connection)
                    cursor = self.metadata_store.connection.cursor()
                    cursor.execute('DELETE FROM schema_migrations WHERE version = ?', (migration.version,))
                    self.metadata_store.connection.commit()
                else:  # PostgreSQL
                    async with self.metadata_store.pool.acquire() as connection:
                        await migration.down(connection)
                        await connection.execute(
                            'DELETE FROM schema_migrations WHERE version = $1',
                            migration.version
                        )
                
                logger.info(f"Migration {migration.version} rolled back successfully")
                
            except Exception as e:
                logger.error(f"Failed to rollback migration {migration.version}: {e}")
                raise