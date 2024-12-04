# tests/test_database_manager_realtime.py

import pytest
import asyncio
import websockets
import json
from datetime import datetime
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def realtime_db_manager():
    """Setup test database for real-time data testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_realtime"
    manager.format_database(force=True)
    return manager

@pytest.mark.asyncio
async def test_realtime_data_insertion(realtime_db_manager):
    """Test real-time market data insertion"""
    async def mock_market_feed():
        """Simulate real-time market data feed"""
        data = {
            'symbol': 'AAPL',
            'timestamp': datetime.now().isoformat(),
            'price': 150.0,
            'volume': 1000
        }
        return json.dumps(data)

    async def process_market_data(data_str):
        """Process incoming market data"""
        data = json.loads(data_str)
        with realtime_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO MarketData (symbol, timestamp, price, volume)
                    VALUES (%s, %s, %s, %s)
                """, (
                    data['symbol'],
                    datetime.fromisoformat(data['timestamp']),
                    data['price'],
                    data['volume']
                ))
                conn.commit()

    # Simulate receiving 10 real-time updates
    for _ in range(10):
        data = await mock_market_feed()
        await process_market_data(data)

    # Verify data was stored
    with realtime_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM MarketData")
            assert cursor.fetchone()[0] == 10

@pytest.mark.asyncio
async def test_concurrent_realtime_updates(realtime_db_manager):
    """Test concurrent real-time data processing"""
    async def update_worker(symbol):
        with realtime_db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO MarketData (symbol, timestamp, price, volume)
                    VALUES (%s, %s, %s, %s)
                """, (symbol, datetime.now(), 100.0, 1000))
                conn.commit()

    # Run concurrent updates
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META']
    tasks = [update_worker(symbol) for symbol in symbols]
    await asyncio.gather(*tasks)

    # Verify all updates processed
    with realtime_db_manager.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM MarketData")
            assert cursor.fetchone()[0] == len(symbols)

if __name__ == "__main__":
    pytest.main([__file__])