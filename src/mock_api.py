# FILE: src/core/services/mock_api.py

import logging
from typing import Dict, Any


class MockAPI:
    """
    Simulates API responses for development purposes.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

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
