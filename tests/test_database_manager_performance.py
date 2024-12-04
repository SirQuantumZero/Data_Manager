# tests/test_database_manager_performance.py

import pytest
import time
import statistics
from datetime import datetime, timedelta
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def perf_db_manager():
    """Setup test database for performance testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_perf"
    manager.format_database(force=True)
    return manager

def measure_execution_time(func):
    """Decorator to measure function execution time"""
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        return result, end - start
    return wrapper

def test_bulk_insert_performance(perf_db_manager):
    """Test bulk insert performance"""
    batch_sizes = [100, 1000, 5000]
    results = {}
    
    @measure_execution_time
    def insert_batch(size):
        with perf_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                for _ in range(size):
                    cursor.execute("""
                        INSERT INTO MarketData (
                            symbol, timestamp, open, high, low, close, volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, ('AAPL', datetime.now(), 100, 101, 99, 100.5, 1000000))
            conn.commit()
    
    for size in batch_sizes:
        _, duration = insert_batch(size)
        results[size] = duration / size  # Time per record
        
    # Assert performance expectations
    assert results[1000] < results[100]  # Bulk inserts should be more efficient

def test_query_performance(perf_db_manager):
    """Test query performance with different conditions"""
    queries = [
        "SELECT * FROM MarketData WHERE symbol = 'AAPL' LIMIT 1000",
        "SELECT symbol, AVG(close) FROM MarketData GROUP BY symbol",
        "SELECT * FROM MarketData m JOIN Trades t ON m.symbol = t.symbol LIMIT 1000"
    ]
    
    timings = []
    for query in queries:
        start = time.perf_counter()
        perf_db_manager.execute_query(query, [])
        duration = time.perf_counter() - start
        timings.append(duration)
        
    avg_time = statistics.mean(timings)
    assert avg_time < 1.0  # Average query should complete within 1 second

def test_backup_restore_performance(perf_db_manager):
    """Test backup and restore performance"""
    # Measure backup time
    start = time.perf_counter()
    perf_db_manager.backup_database()
    backup_time = time.perf_counter() - start
    
    # Assert backup performance
    assert backup_time < 30.0  # Backup should complete within 30 seconds

def test_concurrent_query_performance(perf_db_manager):
    """Test performance under concurrent load"""
    import threading
    
    query_times = []
    lock = threading.Lock()
    
    def worker():
        start = time.perf_counter()
        perf_db_manager.get_historical_data("MarketData", {"symbol": "AAPL"})
        duration = time.perf_counter() - start
        with lock:
            query_times.append(duration)
    
    # Run 10 concurrent queries
    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
        
    avg_time = statistics.mean(query_times)
    assert avg_time < 0.5  # Average concurrent query should complete within 500ms

if __name__ == "__main__":
    pytest.main([__file__])