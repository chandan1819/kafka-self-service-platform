#!/usr/bin/env python3
"""Database migration runner for Kafka Ops Agent."""

import sys
import os
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any
import asyncpg
import argparse

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from kafka_ops_agent.config import get_config


class MigrationRunner:
    """Database migration runner."""
    
    def __init__(self, database_url: str):
        """Initialize migration runner.
        
        Args:
            database_url: Database connection URL
        """
        self.database_url = database_url
        self.migrations_dir = project_root / "migrations"
        self.logger = logging.getLogger(__name__)
    
    async def run_migrations(self, target_version: str = None, dry_run: bool = False):
        """Run database migrations.
        
        Args:
            target_version: Target migration version (None for latest)
            dry_run: If True, show what would be done without executing
        """
        self.logger.info("Starting database migrations...")
        
        # Connect to database
        conn = await asyncpg.connect(self.database_url)
        
        try:
            # Create migrations table if it doesn't exist
            await self._create_migrations_table(conn)
            
            # Get applied migrations
            applied_migrations = await self._get_applied_migrations(conn)
            self.logger.info(f"Found {len(applied_migrations)} applied migrations")
            
            # Get available migrations
            available_migrations = self._get_available_migrations()
            self.logger.info(f"Found {len(available_migrations)} available migrations")
            
            # Determine migrations to run
            migrations_to_run = self._get_migrations_to_run(
                applied_migrations, available_migrations, target_version
            )
            
            if not migrations_to_run:
                self.logger.info("No migrations to run")
                return
            
            self.logger.info(f"Will run {len(migrations_to_run)} migrations:")
            for migration in migrations_to_run:
                self.logger.info(f"  - {migration['version']}: {migration['description']}")
            
            if dry_run:
                self.logger.info("Dry run mode - not executing migrations")
                return
            
            # Run migrations
            for migration in migrations_to_run:
                await self._run_migration(conn, migration)
            
            self.logger.info("All migrations completed successfully")
            
        finally:
            await conn.close()
    
    async def rollback_migration(self, target_version: str, dry_run: bool = False):
        """Rollback to a specific migration version.
        
        Args:
            target_version: Target version to rollback to
            dry_run: If True, show what would be done without executing
        """
        self.logger.info(f"Rolling back to version {target_version}...")
        
        conn = await asyncpg.connect(self.database_url)
        
        try:
            # Get applied migrations
            applied_migrations = await self._get_applied_migrations(conn)
            
            # Find migrations to rollback
            migrations_to_rollback = []
            for migration in reversed(applied_migrations):
                if migration['version'] > target_version:
                    migrations_to_rollback.append(migration)
                else:
                    break
            
            if not migrations_to_rollback:
                self.logger.info("No migrations to rollback")
                return
            
            self.logger.info(f"Will rollback {len(migrations_to_rollback)} migrations:")
            for migration in migrations_to_rollback:
                self.logger.info(f"  - {migration['version']}: {migration['description']}")
            
            if dry_run:
                self.logger.info("Dry run mode - not executing rollbacks")
                return
            
            # Rollback migrations
            for migration in migrations_to_rollback:
                await self._rollback_migration(conn, migration)
            
            self.logger.info("Rollback completed successfully")
            
        finally:
            await conn.close()
    
    async def show_status(self):
        """Show migration status."""
        conn = await asyncpg.connect(self.database_url)
        
        try:
            # Create migrations table if it doesn't exist
            await self._create_migrations_table(conn)
            
            # Get applied migrations
            applied_migrations = await self._get_applied_migrations(conn)
            
            # Get available migrations
            available_migrations = self._get_available_migrations()
            
            print("\\nMigration Status:")
            print("=" * 50)
            
            applied_versions = {m['version'] for m in applied_migrations}
            
            for migration in available_migrations:
                version = migration['version']
                status = "✅ Applied" if version in applied_versions else "⏳ Pending"
                print(f"{status} {version}: {migration['description']}")
            
            print(f"\\nTotal migrations: {len(available_migrations)}")
            print(f"Applied migrations: {len(applied_migrations)}")
            print(f"Pending migrations: {len(available_migrations) - len(applied_migrations)}")
            
        finally:
            await conn.close()
    
    async def _create_migrations_table(self, conn):
        """Create migrations tracking table."""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(255) PRIMARY KEY,
                description TEXT,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                checksum VARCHAR(64)
            )
        """)
    
    async def _get_applied_migrations(self, conn) -> List[Dict[str, Any]]:
        """Get list of applied migrations."""
        rows = await conn.fetch("""
            SELECT version, description, applied_at, checksum
            FROM schema_migrations
            ORDER BY version
        """)
        
        return [dict(row) for row in rows]
    
    def _get_available_migrations(self) -> List[Dict[str, Any]]:
        """Get list of available migration files."""
        migrations = []
        
        if not self.migrations_dir.exists():
            return migrations
        
        for file_path in sorted(self.migrations_dir.glob("*.sql")):
            version = file_path.stem
            
            # Read migration file to extract description
            content = file_path.read_text()
            description = self._extract_description(content)
            
            # Calculate checksum
            import hashlib
            checksum = hashlib.sha256(content.encode()).hexdigest()[:16]
            
            migrations.append({
                'version': version,
                'description': description,
                'file_path': file_path,
                'checksum': checksum
            })
        
        return migrations
    
    def _extract_description(self, content: str) -> str:
        """Extract description from migration file."""
        lines = content.split('\\n')
        for line in lines:
            if line.strip().startswith('-- Description:'):
                return line.split('-- Description:', 1)[1].strip()
        return "No description"
    
    def _get_migrations_to_run(
        self, 
        applied_migrations: List[Dict[str, Any]], 
        available_migrations: List[Dict[str, Any]], 
        target_version: str = None
    ) -> List[Dict[str, Any]]:
        """Determine which migrations need to be run."""
        applied_versions = {m['version'] for m in applied_migrations}
        
        migrations_to_run = []
        for migration in available_migrations:
            version = migration['version']
            
            # Skip if already applied
            if version in applied_versions:
                continue
            
            # Skip if beyond target version
            if target_version and version > target_version:
                continue
            
            migrations_to_run.append(migration)
        
        return migrations_to_run
    
    async def _run_migration(self, conn, migration: Dict[str, Any]):
        """Run a single migration."""
        version = migration['version']
        description = migration['description']
        file_path = migration['file_path']
        checksum = migration['checksum']
        
        self.logger.info(f"Running migration {version}: {description}")
        
        # Read migration SQL
        sql_content = file_path.read_text()
        
        # Start transaction
        async with conn.transaction():
            try:
                # Execute migration SQL
                await conn.execute(sql_content)
                
                # Record migration as applied
                await conn.execute("""
                    INSERT INTO schema_migrations (version, description, checksum)
                    VALUES ($1, $2, $3)
                """, version, description, checksum)
                
                self.logger.info(f"✅ Migration {version} completed successfully")
                
            except Exception as e:
                self.logger.error(f"❌ Migration {version} failed: {e}")
                raise
    
    async def _rollback_migration(self, conn, migration: Dict[str, Any]):
        """Rollback a single migration."""
        version = migration['version']
        description = migration['description']
        
        self.logger.info(f"Rolling back migration {version}: {description}")
        
        # Check if rollback script exists
        rollback_file = self.migrations_dir / f"{version}_rollback.sql"
        
        if not rollback_file.exists():
            self.logger.warning(f"No rollback script found for {version}, skipping")
            return
        
        # Read rollback SQL
        sql_content = rollback_file.read_text()
        
        # Start transaction
        async with conn.transaction():
            try:
                # Execute rollback SQL
                await conn.execute(sql_content)
                
                # Remove migration record
                await conn.execute("""
                    DELETE FROM schema_migrations WHERE version = $1
                """, version)
                
                self.logger.info(f"✅ Migration {version} rolled back successfully")
                
            except Exception as e:
                self.logger.error(f"❌ Rollback of {version} failed: {e}")
                raise


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Database migration runner")
    parser.add_argument("command", choices=["migrate", "rollback", "status"], 
                       help="Migration command")
    parser.add_argument("--target", help="Target migration version")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be done without executing")
    parser.add_argument("--database-url", help="Database URL (overrides config)")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Verbose logging")
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Get database URL
    if args.database_url:
        database_url = args.database_url
    else:
        config = get_config()
        database_url = config.get('database', {}).get('url')
        
        if not database_url:
            print("Error: No database URL provided. Use --database-url or configure in config file.")
            sys.exit(1)
    
    # Create migration runner
    runner = MigrationRunner(database_url)
    
    try:
        if args.command == "migrate":
            await runner.run_migrations(args.target, args.dry_run)
        elif args.command == "rollback":
            if not args.target:
                print("Error: --target is required for rollback command")
                sys.exit(1)
            await runner.rollback_migration(args.target, args.dry_run)
        elif args.command == "status":
            await runner.show_status()
            
    except Exception as e:
        logging.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())