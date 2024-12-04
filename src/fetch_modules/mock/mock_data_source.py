# FILE: src/core/services/mock_api.py

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from ..base.data_source_base import DataSource


class MockAPI:
    """
    Simulates API responses for development purposes.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Base values for mock data
        self.base_prices = {
            'AAPL': 150.0,
            'GOOGL': 2800.0,
            'MSFT': 300.0,
            'DEFAULT': 100.0
        }

    def get_mock_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get mock stock data for a given symbol.

        Args:
            symbol (str): The stock symbol to get mock data for.

        Returns:
            Dict[str, Any]: The mock stock data.
        """
        self.logger.info(f"Getting mock stock data for symbol: {symbol}")
        mock_data = {
            "symbol": symbol,
            "price": 150.00,
            "volume": 1000,
            "timestamp": "2024-11-30T17:00:00Z",
        }
        return mock_data

    def get_mock_crypto_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get mock cryptocurrency data for a given symbol.

        Args:
            symbol (str): The cryptocurrency symbol to get mock data for.

        Returns:
            Dict[str, Any]: The mock cryptocurrency data.
        """
        self.logger.info(f"Getting mock cryptocurrency data for symbol: {symbol}")
        mock_data = {
            "symbol": symbol,
            "price": 50000.00,
            "volume": 500,
            "timestamp": "2024-11-30T17:00:00Z",
        }
        return mock_data

    def get_mock_market_data(
        self, 
        symbol: str, 
        start_date: datetime,
        end_date: datetime,
        timeframe: str = '1d'
    ) -> pd.DataFrame:
        """Generate mock market data with realistic price movements"""
        try:
            mock_data = []
            current_date = start_date
            base_price = self.base_prices.get(symbol, self.base_prices['DEFAULT'])
            
            while current_date <= end_date:
                # Generate realistic price movement
                daily_volatility = np.random.normal(0, 0.02)  # 2% std dev
                high = base_price * (1 + abs(daily_volatility))
                low = base_price * (1 - abs(daily_volatility))
                close = base_price * (1 + daily_volatility)
                
                mock_data.append({
                    'timestamp': current_date,
                    'open': base_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': int(np.random.normal(1000000, 200000)),
                    'vwap': (high + low + close) / 3,
                    'transactions': int(np.random.normal(2000, 500)),
                    'source': 'MOCK'
                })
                
                # Update base price for next iteration
                base_price = close
                current_date += pd.Timedelta(days=1)
            
            df = pd.DataFrame(mock_data)
            df.set_index('timestamp', inplace=True)
            
            self.logger.info(f"Generated {len(df)} mock records for {symbol}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to generate mock data: {e}")
            return pd.DataFrame()


class MockSource(DataSource):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.mock_api = MockAPI()
        
    def fetch_data(self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str) -> pd.DataFrame:
        """Fetch mock market data"""
        return self.mock_api.get_mock_market_data(symbol, start_date, end_date, timeframe)


class MockDataSource(DataSource):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def fetch_data(self, symbol, start_date, end_date, timeframe):
        try:
            mock_data = []
            current_date = start_date
            while current_date <= end_date:
                mock_data.append({
                    'timestamp': current_date,
                    'symbol': symbol,
                    'price': 100.0  # Mock price
                })
                current_date += timedelta(days=1)
            return pd.DataFrame(mock_data)
        except Exception as e:
            self.logger.error(f"Mock fetch failed: {e}")
            return pd.DataFrame()
