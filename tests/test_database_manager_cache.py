# tests/test_database_manager_cache.py

import pytest
import time
from datetime import datetime, timedelta
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def cache_db_manager():
    """Setup test database for cache testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_cache"
    manager.format_database(force=True)
    return manager

def test_cache_hit(cache_db_manager):
    """Test cache hit functionality"""
    # First request - should hit database
    start_time = time.perf_counter()
    data1 = cache_db_manager.get_market_data(
        symbol="AAPL",
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )
    first_request_time = time.perf_counter() - start_time

    # Second request - should hit cache
    start_time = time.perf_counter()
    data2 = cache_db_manager.get_market_data(
        symbol="AAPL",
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )
    second_request_time = time.perf_counter() - start_time

    # Cache hit should be faster
    assert second_request_time < first_request_time
    assert data1 == data2

def test_cache_invalidation(cache_db_manager):
    """Test cache invalidation"""
    # Cache some data
    cache_db_manager.get_market_data(
        symbol="GOOGL",
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )

    # Invalidate cache
    cache_db_manager.invalidate_cache("GOOGL")

    # Verify cache miss
    start_time = time.perf_counter()
    cache_db_manager.get_market_data(
        symbol="GOOGL",
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )
    request_time = time.perf_counter() - start_time

    # Should take longer as it hits database
    assert request_time > 0.001  # Arbitrary threshold

def test_cache_expiration(cache_db_manager):
    """Test cache expiration"""
    # Set short TTL for test
    cache_db_manager.cache_ttl = 1  # 1 second

    # Cache data
    cache_db_manager.get_market_data(
        symbol="MSFT",
        start_date=datetime.now() - timedelta(days=1),
        end_date=datetime.now()
    )

    # Wait for expiration
    time.sleep(2)

    # Verify expired data is refreshed
    assert not cache_db_manager.is_cached("MSFT")

if __name__ == "__main__":
    pytest.main([__file__])