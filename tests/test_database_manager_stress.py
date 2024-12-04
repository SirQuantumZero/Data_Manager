# tests/test_database_manager_stress.py

import pytest
import concurrent.futures
import random
from datetime import datetime, timedelta
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def stress_db_manager():
    """Setup test database for stress testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_stress"
    manager.format_database(force=True)
    return manager

def test_concurrent_inserts(stress_db_manager):
    """Test concurrent inserts under heavy load"""
    def worker(symbol):
        with stress_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                for _ in range(100):
                    cursor.execute("""
                        INSERT INTO MarketData (
                            symbol, timestamp, open, high, low, close, volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        symbol,
                        datetime.now(),
                        random.uniform(100, 200),
                        random.uniform(100, 200),
                        random.uniform(100, 200),
                        random.uniform(100, 200),
                        random.randint(10000, 1000000)
                    ))
            conn.commit()

    symbols = [f"SYM{i}" for i in range(50)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(worker, symbol) for symbol in symbols]
        concurrent.futures.wait(futures)

    # Verify data integrity
    with stress_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM MarketData")
            assert cursor.fetchone()[0] == 50

def test_query_under_load(stress_db_manager):
    """Test query performance under concurrent read/write load"""
    read_count = 0
    write_count = 0
    error_count = 0

    def read_worker():
        nonlocal read_count
        try:
            stress_db_manager.get_historical_data(
                "MarketData",
                {"symbol": random.choice(stress_db_manager.symbols)}
            )
            read_count += 1
        except Exception:
            nonlocal error_count
            error_count += 1

    def write_worker():
        nonlocal write_count
        try:
            with stress_db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO MarketData (symbol, timestamp, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        random.choice(stress_db_manager.symbols),
                        datetime.now(),
                        100,
                        101,
                        99,
                        100,
                        10000
                    ))
                conn.commit()
            write_count += 1
        except Exception:
            nonlocal error_count
            error_count += 1

    # Run concurrent read/write operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = []
        for _ in range(100):
            futures.append(executor.submit(read_worker))
            futures.append(executor.submit(write_worker))
        concurrent.futures.wait(futures)

    # Verify results
    assert error_count == 0
    assert read_count > 0
    assert write_count > 0

if __name__ == "__main__":
    pytest.main([__file__])