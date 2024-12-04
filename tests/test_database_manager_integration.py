# tests/test_database_manager_integration.py

import pytest
import mysql.connector
from datetime import datetime, timedelta
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def test_db_manager():
    """Create DatabaseManager with test database"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data"
    
    # Setup test database
    with mysql.connector.connect(
        host=manager.db_config.host,
        user=manager.db_config.user,
        password=manager.db_config.password
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {manager.db_config.database}")
    
    # Initialize schema
    manager.format_database(force=True)
    return manager

@pytest.fixture(autouse=True)
def cleanup_db(test_db_manager):
    """Clean up after each test"""
    yield
    with mysql.connector.connect(**test_db_manager.db_config.__dict__) as conn:
        with conn.cursor() as cursor:
            test_db_manager.drop_all_tables(cursor)
            test_db_manager.apply_schema(cursor)

def test_full_database_lifecycle(test_db_manager):
    """Test complete database lifecycle"""
    # 1. Initialize with test data
    with mysql.connector.connect(**test_db_manager.db_config.__dict__) as conn:
        with conn.cursor() as cursor:
            test_db_manager.populate_test_data(cursor)
            
            # 2. Verify data
            cursor.execute("SELECT COUNT(*) FROM Users")
            assert cursor.fetchone()[0] > 0
            
            cursor.execute("SELECT COUNT(*) FROM MarketData")
            assert cursor.fetchone()[0] > 0
            
            # 3. Test backup
            assert test_db_manager.backup_database()
            
            # 4. Query historical data
            data = test_db_manager.get_historical_data(
                "MarketData", 
                {"symbol": test_db_manager.symbols[0]}
            )
            assert len(data) > 0

def test_schema_migration(test_db_manager):
    """Test schema version control"""
    with mysql.connector.connect(**test_db_manager.db_config.__dict__) as conn:
        with conn.cursor() as cursor:
            version = test_db_manager.get_current_schema_version(cursor)
            assert version == test_db_manager.SCHEMA_VERSION

def test_data_consistency(test_db_manager):
    """Test referential integrity and data consistency"""
    with mysql.connector.connect(**test_db_manager.db_config.__dict__) as conn:
        with conn.cursor() as cursor:
            test_db_manager.populate_test_data(cursor)
            
            # Check foreign key relationships
            cursor.execute("""
                SELECT t.symbol, s.symbol 
                FROM Trades t 
                LEFT JOIN MarketData s ON t.symbol = s.symbol
                WHERE s.symbol IS NULL
            """)
            assert len(cursor.fetchall()) == 0

def test_concurrent_operations(test_db_manager):
    """Test concurrent database operations"""
    import threading
    
    def worker():
        with mysql.connector.connect(**test_db_manager.db_config.__dict__) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM Users")
    
    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

if __name__ == "__main__":
    pytest.main([__file__])