# tests/test_database_manager_api.py

import pytest
import aiohttp
import asyncio
from datetime import datetime, timedelta
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def api_db_manager():
    """Setup test database for API integration testing"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_api"
    manager.format_database(force=True)
    return manager

@pytest.mark.asyncio
async def test_polygon_api_integration(api_db_manager):
    """Test Polygon.io API integration"""
    async with aiohttp.ClientSession() as session:
        # Test market data fetch
        symbol = "AAPL"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=5)
        
        data = await api_db_manager.fetch_polygon_data(
            session,
            symbol,
            start_date,
            end_date
        )
        
        assert len(data) > 0
        assert all(key in data[0] for key in ['open', 'high', 'low', 'close'])

@pytest.mark.asyncio
async def test_alpaca_api_integration(api_db_manager):
    """Test Alpaca API integration"""
    symbols = ["AAPL", "GOOGL"]
    
    # Test batch data fetch
    data = await api_db_manager.fetch_alpaca_data(symbols)
    assert len(data) == len(symbols)
    
    # Test streaming data
    async def process_stream():
        messages = []
        async for msg in api_db_manager.stream_alpaca_data(symbols):
            messages.append(msg)
            if len(messages) >= 5:
                break
        return messages
    
    messages = await asyncio.wait_for(process_stream(), timeout=30)
    assert len(messages) == 5

@pytest.mark.asyncio
async def test_api_error_handling(api_db_manager):
    """Test API error handling"""
    # Test invalid symbol
    with pytest.raises(ValueError):
        await api_db_manager.fetch_polygon_data(
            None,
            "INVALID_SYMBOL",
            datetime.now() - timedelta(days=1),
            datetime.now()
        )
    
    # Test rate limiting
    async def rapid_requests():
        for _ in range(100):
            await api_db_manager.fetch_polygon_data(
                None,
                "AAPL",
                datetime.now() - timedelta(days=1),
                datetime.now()
            )
    
    await api_db_manager.handle_rate_limit(rapid_requests())

if __name__ == "__main__":
    pytest.main([__file__])