# tests/test_database_manager_failover.py

import pytest
import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def failover_db_manager():
    """Setup test database for failover testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_failover"
    manager.format_database(force=True)
    return manager

def test_connection_retry(failover_db_manager):
    """Test connection retry mechanism"""
    retry_count = 0
    max_retries = 3
    
    def mock_connect(*args, **kwargs):
        nonlocal retry_count
        retry_count += 1
        if retry_count < max_retries:
            raise Error("Connection failed")
        return mysql.connector.connect(*args, **kwargs)
    
    with pytest.Monkeys.patch('mysql.connector.connect', side_effect=mock_connect):
        failover_db_manager.execute_query("SELECT 1", [])
        assert retry_count == max_retries

def test_data_recovery(failover_db_manager):
    """Test data recovery after failure"""
    # Insert test data
    test_data = [
        ('AAPL', datetime.now(), 150.0, 151.0, 149.0, 150.5, 1000000),
        ('GOOGL', datetime.now(), 2800.0, 2810.0, 2790.0, 2805.0, 500000)
    ]
    
    try:
        # Simulate failure during insert
        with failover_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                for record in test_data:
                    cursor.execute("""
                        INSERT INTO MarketData (
                            symbol, timestamp, open, high, low, close, volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, record)
                raise Error("Simulated failure")
    except Error:
        pass
    
    # Verify data integrity
    results = failover_db_manager.get_historical_data(
        "MarketData", 
        {"symbol": "AAPL"}
    )
    assert len(results) == 0  # Transaction should have rolled back

def test_backup_recovery(failover_db_manager):
    """Test recovery from backup"""
    # Create backup
    backup_file = failover_db_manager.backup_database()
    
    # Corrupt database
    with failover_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE MarketData")
    
    # Restore from backup
    failover_db_manager.restore_from_backup(backup_file)
    
    # Verify restoration
    with failover_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            assert ("MarketData",) in tables

if __name__ == "__main__":
    pytest.main([__file__])