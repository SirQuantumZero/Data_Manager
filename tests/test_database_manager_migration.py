# tests/test_database_manager_migration.py

import pytest
import mysql.connector
from pathlib import Path
from datetime import datetime
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def migration_db_manager():
    """Setup test database for migration testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_migration"
    manager.format_database(force=True)
    return manager

def test_schema_version_tracking(migration_db_manager):
    """Test schema version tracking functionality"""
    with migration_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            # Check initial version
            version = migration_db_manager.get_current_schema_version(cursor)
            assert version == migration_db_manager.SCHEMA_VERSION
            
            # Apply new version
            cursor.execute("""
                INSERT INTO schema_versions (version, applied_at)
                VALUES (%s, %s)
            """, ("1.0.1", datetime.now()))
            
            # Verify update
            new_version = migration_db_manager.get_current_schema_version(cursor)
            assert new_version == "1.0.1"

def test_schema_migration_process(migration_db_manager):
    """Test schema migration process"""
    # Create test migration
    migration_path = Path("db/migrations/V1_0_1__add_test_table.sql")
    migration_path.parent.mkdir(parents=True, exist_ok=True)
    
    migration_sql = """
    CREATE TABLE test_table (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(50),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    migration_path.write_text(migration_sql)
    
    try:
        # Apply migration
        with migration_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                migration_db_manager.apply_migration(cursor, "V1_0_1__add_test_table.sql")
                
                # Verify table exists
                cursor.execute("SHOW TABLES LIKE 'test_table'")
                assert cursor.fetchone() is not None
                
                # Verify version updated
                version = migration_db_manager.get_current_schema_version(cursor)
                assert version == "1.0.1"
    finally:
        migration_path.unlink()

def test_migration_rollback(migration_db_manager):
    """Test migration rollback functionality"""
    with migration_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            # Apply migration that will fail
            with pytest.raises(mysql.connector.Error):
                cursor.execute("CREATE TABLE invalid_sql")
            
            # Verify database state unchanged
            cursor.execute("SHOW TABLES")
            tables_before = set(t[0] for t in cursor.fetchall())
            
            # Attempt migration
            try:
                migration_db_manager.apply_migration(cursor, "invalid_migration.sql")
            except:
                pass
            
            # Verify no changes persisted
            cursor.execute("SHOW TABLES")
            tables_after = set(t[0] for t in cursor.fetchall())
            assert tables_before == tables_after

if __name__ == "__main__":
    pytest.main([__file__])