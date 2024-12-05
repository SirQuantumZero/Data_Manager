# src/database/migrations.py
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from .core import Core

class Migrations:
    def __init__(self, core: Core):
        self.core = core
        self.logger = logging.getLogger(__name__)
        self._ensure_migrations_table()

    def _ensure_migrations_table(self) -> None:
        """Create migrations tracking table if not exists"""
        self.core.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                version VARCHAR(50) NOT NULL UNIQUE,
                applied_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                description TEXT
            )
        """)

    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions"""
        result = self.core.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        )
        return [row['version'] for row in result]

    def apply_migration(self, version: str, sql: str, description: Optional[str] = None) -> bool:
        """Apply single migration"""
        try:
            with self.core.transaction() as conn:
                cursor = conn.cursor()
                # Split SQL into statements
                statements = [s.strip() for s in sql.split(';') if s.strip()]
                for statement in statements:
                    cursor.execute(statement)
                
                cursor.execute(
                    "INSERT INTO schema_migrations (version, description) VALUES (%s, %s)",
                    (version, description)
                )
            self.logger.info(f"Applied migration {version}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to apply migration {version}: {e}")
            return False

    def rollback_migration(self, version: str, down_sql: str) -> bool:
        """Rollback single migration"""
        try:
            with self.core.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(down_sql)
                cursor.execute("DELETE FROM schema_migrations WHERE version = %s", (version,))
            self.logger.info(f"Rolled back migration {version}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to rollback migration {version}: {e}")
            return False

    def apply_migrations(self, migrations_dir: Path) -> bool:
        """Apply all pending migrations from directory"""
        applied = set(self.get_applied_migrations())
        success = True
        
        for migration_file in sorted(migrations_dir.glob('*.sql')):
            version = migration_file.stem
            if version in applied:
                continue
                
            with open(migration_file) as f:
                sql = f.read()
            
            if not self.apply_migration(version, sql):
                success = False
                break
                
        return success