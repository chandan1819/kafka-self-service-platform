"""Factory for creating storage instances."""

import logging
from typing import Tuple

from kafka_ops_agent.storage.base import MetadataStore, AuditStore
from kafka_ops_agent.storage.sqlite_store import SQLiteMetadataStore, SQLiteAuditStore
from kafka_ops_agent.storage.postgres_store import PostgreSQLMetadataStore, PostgreSQLAuditStore
from kafka_ops_agent.config import config

logger = logging.getLogger(__name__)


class StorageFactory:
    """Factory for creating storage instances."""
    
    @staticmethod
    async def create_stores() -> Tuple[MetadataStore, AuditStore]:
        """Create metadata and audit stores based on configuration."""
        
        if config.database.type.lower() == 'sqlite':
            logger.info("Creating SQLite storage backend")
            metadata_store = SQLiteMetadataStore(config.database.sqlite_path)
            await metadata_store.initialize()
            audit_store = SQLiteAuditStore(metadata_store)
            
        elif config.database.type.lower() == 'postgresql':
            logger.info("Creating PostgreSQL storage backend")
            metadata_store = PostgreSQLMetadataStore()
            await metadata_store.initialize()
            audit_store = PostgreSQLAuditStore(metadata_store)
            
        else:
            raise ValueError(f"Unsupported database type: {config.database.type}")
        
        return metadata_store, audit_store
    
    @staticmethod
    async def create_metadata_store() -> MetadataStore:
        """Create only metadata store."""
        metadata_store, _ = await StorageFactory.create_stores()
        return metadata_store
    
    @staticmethod
    async def create_audit_store() -> AuditStore:
        """Create only audit store (requires metadata store)."""
        _, audit_store = await StorageFactory.create_stores()
        return audit_store


# Global storage instances (initialized on first use)
_metadata_store: MetadataStore = None
_audit_store: AuditStore = None


async def get_metadata_store() -> MetadataStore:
    """Get global metadata store instance."""
    global _metadata_store
    if _metadata_store is None:
        _metadata_store = await StorageFactory.create_metadata_store()
    return _metadata_store


async def get_audit_store() -> AuditStore:
    """Get global audit store instance."""
    global _audit_store
    if _audit_store is None:
        _, _audit_store = await StorageFactory.create_stores()
    return _audit_store


async def close_stores():
    """Close all storage connections."""
    global _metadata_store, _audit_store
    
    if _metadata_store:
        await _metadata_store.close()
        _metadata_store = None
    
    _audit_store = None  # Audit store shares connection with metadata store
    
    logger.info("All storage connections closed")