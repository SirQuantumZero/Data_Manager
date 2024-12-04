# tests/test_data_manager.py

import pytest
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import Mock, patch

# Update relative imports from src directory
from src.data_manager import DataManager
from src.models import MarketData, MarketDataRequest
from src.fetch_modules.mock.mock_api import MockAPIClient
from src.exceptions import DataValidationError

@pytest.fixture
def data_manager():
    """Fixture for testing with mock data source"""
    mock_source = MockAPIClient({})
    return DataManager(data_source=mock_source)

@pytest.mark.asyncio
async def test_get_market_data():
    """Test market data retrieval"""
    manager = data_manager()  # Fixed fixture usage
    
    request = MarketDataRequest(
        symbol="AAPL",
        start_date=datetime.now() - timedelta(days=7),
        end_date=datetime.now(),
        interval="1d"
    )
    
    result = await manager.process_request({
        "type": "market_data",
        "payload": request
    })
    
    assert isinstance(result, dict)
    assert "data" in result
    assert isinstance(result["data"], pd.DataFrame)