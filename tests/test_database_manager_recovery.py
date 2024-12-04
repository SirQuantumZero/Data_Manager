# tests/test_database_manager_recovery.py

import pytest
import os
from pathlib import Path
import shutil
from datetime import datetime
import mysql.connector
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def recovery_db_manager():
    """Setup test database for recovery testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_recovery"
    manager.format_database(force=True)
    manager.backup_dir = Path("db/test_recovery_backups")
    manager.backup_dir.mkdir(parents=True, exist_ok=True)
    return manager

def test_partial_restore(recovery_db_manager):
    """Test recovery from partially corrupted backup"""
    # Create initial backup
    with recovery_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            recovery_db_manager.populate_test_data(cursor)
    
    backup_file = recovery_db_manager.backup_database()
    
    # Corrupt backup file
    with open(backup_file, 'r+b') as f:
        f.seek(-100, os.SEEK_END)  # Go near end of file
        f.truncate()  # Truncate file
    
    # Attempt restore
    with pytest.raises(Exception):
        recovery_db_manager.restore_from_backup(backup_file)
    
    # Verify database is still in consistent state
    with recovery_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            assert cursor.fetchall()

def test_incremental_backup(recovery_db_manager):
    """Test incremental backup and restore"""
    backup_files = []
    
    # Create series of backups with incremental data
    for i in range(3):
        with recovery_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO MarketData (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (f"TEST{i}", datetime.now(), 100, 101, 99, 100, 1000))
        
        backup_files.append(recovery_db_manager.backup_database())
    
    # Restore from each backup incrementally
    for backup_file in backup_files:
        recovery_db_manager.restore_from_backup(backup_file)
        
        with recovery_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM MarketData")
                count = cursor.fetchone()[0]
                assert count > 0

def test_backup_verification(recovery_db_manager):
    """Test backup verification process"""
    backup_file = recovery_db_manager.backup_database()
    
    # Verify backup integrity
    assert recovery_db_manager.verify_backup(backup_file)
    
    # Corrupt backup and verify detection
    with open(backup_file, 'a') as f:
        f.write("corrupted data")
    
    assert not recovery_db_manager.verify_backup(backup_file)

if __name__ == "__main__":
    pytest.main([__file__])