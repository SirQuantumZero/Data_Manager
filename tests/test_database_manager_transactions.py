# tests/test_database_manager_transactions.py

import pytest
import threading
import queue
from datetime import datetime
import time
import mysql.connector
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def transaction_db_manager():
    """Setup test database for transaction testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_transactions"
    manager.format_database(force=True)
    return manager

def test_atomic_transactions(transaction_db_manager):
    """Test atomic transaction handling"""
    with transaction_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            try:
                # Insert valid data
                cursor.execute("""
                    INSERT INTO MarketData (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, ('AAPL', datetime.now(), 100, 101, 99, 100, 1000))
                
                # Insert invalid data to trigger rollback
                cursor.execute("""
                    INSERT INTO MarketData (symbol, timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, ('INVALID', datetime.now(), -1, -1, -1, -1, -1))
                
                conn.commit()
            except:
                conn.rollback()
                
            # Verify transaction rolled back
            cursor.execute("SELECT COUNT(*) FROM MarketData WHERE symbol = 'AAPL'")
            assert cursor.fetchone()[0] == 0

def test_deadlock_handling(transaction_db_manager):
    """Test deadlock detection and resolution"""
    error_queue = queue.Queue()
    
    def transaction1():
        try:
            with transaction_db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("START TRANSACTION")
                    cursor.execute("UPDATE MarketData SET volume = 1000 WHERE symbol = 'AAPL'")
                    time.sleep(1)  # Induce potential deadlock
                    cursor.execute("UPDATE MarketData SET volume = 2000 WHERE symbol = 'GOOGL'")
                    conn.commit()
        except Exception as e:
            error_queue.put(e)
    
    def transaction2():
        try:
            with transaction_db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("START TRANSACTION")
                    cursor.execute("UPDATE MarketData SET volume = 3000 WHERE symbol = 'GOOGL'")
                    time.sleep(1)  # Induce potential deadlock
                    cursor.execute("UPDATE MarketData SET volume = 4000 WHERE symbol = 'AAPL'")
                    conn.commit()
        except Exception as e:
            error_queue.put(e)
    
    # Run concurrent transactions
    t1 = threading.Thread(target=transaction1)
    t2 = threading.Thread(target=transaction2)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Check for deadlock errors
    while not error_queue.empty():
        error = error_queue.get()
        assert isinstance(error, mysql.connector.errors.DatabaseError)

if __name__ == "__main__":
    pytest.main([__file__])