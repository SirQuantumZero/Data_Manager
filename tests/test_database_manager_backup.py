# tests/test_database_manager_backup.py

import pytest
import os
from pathlib import Path
from datetime import datetime
import mysql.connector
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def backup_db_manager():
    """Setup test database for backup/restore testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_backup"
    manager.format_database(force=True)
    manager.backup_dir = Path("db/test_backups")
    manager.backup_dir.mkdir(parents=True, exist_ok=True)
    return manager

def test_backup_creation(backup_db_manager):
    """Test database backup creation"""
    # Insert test data
    with backup_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            backup_db_manager.populate_test_data(cursor)
    
    # Create backup
    backup_file = backup_db_manager.backup_database()
    assert os.path.exists(backup_file)
    assert os.path.getsize(backup_file) > 0

def test_backup_rotation(backup_db_manager):
    """Test backup file rotation"""
    max_backups = 5
    
    # Create multiple backups
    backup_files = []
    for _ in range(max_backups + 2):
        backup_file = backup_db_manager.backup_database()
        backup_files.append(backup_file)
    
    # Check rotation
    existing_backups = list(backup_db_manager.backup_dir.glob("backup_*.sql"))
    assert len(existing_backups) <= max_backups
    assert not os.path.exists(backup_files[0])  # Oldest should be removed

def test_backup_restore(backup_db_manager):
    """Test database restore from backup"""
    # Create initial data and backup
    with backup_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            backup_db_manager.populate_test_data(cursor)
            cursor.execute("SELECT COUNT(*) FROM MarketData")
            initial_count = cursor.fetchone()[0]
    
    backup_file = backup_db_manager.backup_database()
    
    # Corrupt database
    with backup_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE MarketData")
    
    # Restore from backup
    backup_db_manager.restore_from_backup(backup_file)
    
    # Verify restoration
    with backup_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM MarketData")
            restored_count = cursor.fetchone()[0]
            assert restored_count == initial_count

if __name__ == "__main__":
    pytest.main([__file__])