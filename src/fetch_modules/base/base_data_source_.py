# src/fetch_modules/base/data_source_base.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

class DataSourceBase(ABC):
    """Abstract base class for all data sources"""

    @abstractmethod
    async def fetch_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch market data for a symbol
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with OHLCV data
        """
        pass

    @abstractmethod
    async def fetch_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """Fetch fundamental data for a symbol"""
        pass

    @abstractmethod
    async def fetch_news(self, symbol: str, limit: int = 10) -> pd.DataFrame:
        """Fetch news data for a symbol"""
        pass