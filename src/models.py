# src/core/data/models.py
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Dict, Any
from decimal import Decimal
import json

@dataclass
class MarketData:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None
    transactions: Optional[int] = None
    source: str = 'UNKNOWN'

    def __post_init__(self):
        if not self.symbol:
            raise ValueError("Symbol cannot be empty")
        if self.volume < 0:
            raise ValueError("Volume cannot be negative")
        if not (self.low <= self.open <= self.high and 
                self.low <= self.close <= self.high):
            raise ValueError("Price values are inconsistent")
        if self.transactions is not None and self.transactions < 0:
            raise ValueError("Transactions cannot be negative")
        if self.vwap is not None and self.vwap <= 0:
            raise ValueError("VWAP must be positive")

    @property
    def price_range(self) -> float:
        """Calculate the price range (high - low)"""
        return self.high - self.low

    @property
    def price_movement(self) -> float:
        """Calculate price movement percentage"""
        return ((self.close - self.open) / self.open) * 100

    @property
    def is_positive_day(self) -> bool:
        """Check if closing price is higher than opening price"""
        return self.close > self.open

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with ISO format timestamp"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MarketData':
        """Create MarketData instance from dictionary"""
        if isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class DataSourceConfig:
    api_key: Optional[str] = None
    cache_size: int = 1000
    batch_size: int = 100
    max_retries: int = 3
    timeout: float = 30.0
    retry_delay: float = 1.0

    def __post_init__(self):
        if self.cache_size <= 0:
            raise ValueError("Cache size must be positive")
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.timeout <= 0:
            raise ValueError("Timeout must be positive")
        if self.retry_delay <= 0:
            raise ValueError("Retry delay must be positive")

# src/core/data/exceptions.py
class DataError(Exception):
    """Base class for data-related errors"""
    pass

class DataValidationError(DataError):
    """Raised when data validation fails"""
    pass

class DataFetchError(DataError):
    """Raised when data fetching fails"""
    pass

# src/core/data/data_manager.py
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
import asyncio
from functools import lru_cache

from .models import MarketData, DataSourceConfig
from .exceptions import DataValidationError, DataFetchError
from .database_client import DatabaseClient
from .fetch_modules.polygon_client import PolygonClient

class DataSource(ABC):
    """Base interface for data sources"""
    @abstractmethod
    async def fetch_data(self, symbol: str, start_date: datetime, 
                        end_date: datetime, timeframe: str) -> pd.DataFrame:
        pass
    
    @abstractmethod
    async def fetch_batch(self, symbols: List[str], start_date: datetime,
                         end_date: datetime, timeframe: str) -> Dict[str, pd.DataFrame]:
        pass

class PolygonSource(DataSource):
    def __init__(self, config: DataSourceConfig):
        self.logger = logging.getLogger(__name__)
        self.client = PolygonClient(config.api_key)
        
    async def fetch_data(self, symbol: str, start_date: datetime, 
                        end_date: datetime, timeframe: str) -> pd.DataFrame:
        try:
            data = await self.client.get_market_data(symbol, start_date, end_date, timeframe)
            return self._process_data(data)
        except Exception as e:
            raise DataFetchError(f"Error fetching {symbol}: {e}")

    async def fetch_batch(self, symbols: List[str], start_date: datetime,
                         end_date: datetime, timeframe: str) -> Dict[str, pd.DataFrame]:
        tasks = [self.fetch_data(symbol, start_date, end_date, timeframe) 
                for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            symbol: result for symbol, result in zip(symbols, results) 
            if not isinstance(result, Exception)
        }

    def _process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process and validate raw data"""
        if not self._validate_data(data):
            raise DataValidationError("Invalid data format")
        return data

class DataManager:
    """Manages market data operations and caching"""

    def __init__(self, source: DataSource, config: DataSourceConfig):
        self.logger = logging.getLogger(__name__)
        self.source = source
        self.config = config
        self.cache = lru_cache(maxsize=config.cache_size)(self._fetch_from_source)
        
    async def get_market_data(self, symbol: str, start_date: datetime, 
                            end_date: datetime, timeframe: str = "1d") -> MarketData:
        """Get market data with caching and validation"""
        try:
            df = await self._get_cached_data(symbol, start_date, end_date, timeframe)
            return self._convert_to_market_data(df, symbol)
        except Exception as e:
            self.logger.error(f"Error getting market data for {symbol}: {e}")
            raise

    async def get_batch_market_data(self, symbols: List[str], start_date: datetime,
                                  end_date: datetime, timeframe: str = "1d") -> Dict[str, MarketData]:
        """Get market data for multiple symbols in batches"""
        results = {}
        for i in range(0, len(symbols), self.config.batch_size):
            batch = symbols[i:i + self.config.batch_size]
            batch_results = await self.source.fetch_batch(batch, start_date, end_date, timeframe)
            results.update({
                symbol: self._convert_to_market_data(data, symbol)
                for symbol, data in batch_results.items()
            })
        return results

    async def _get_cached_data(self, symbol: str, start_date: datetime,
                             end_date: datetime, timeframe: str) -> pd.DataFrame:
        """Get data from cache or source"""
        try:
            return self.cache(symbol, start_date, end_date, timeframe)
        except Exception:
            self.cache.cache_clear()  # Clear cache on error
            raise

    def _convert_to_market_data(self, df: pd.DataFrame, symbol: str) -> MarketData:
        """Convert DataFrame to MarketData object"""
        latest = df.iloc[-1]
        return MarketData(
            symbol=symbol,
            timestamp=latest.name,
            open=latest['open'],
            high=latest['high'],
            low=latest['low'],
            close=latest['close'],
            volume=latest['volume'],
            vwap=latest.get('vwap'),
            transactions=latest.get('transactions'),
            source=latest.get('source', 'UNKNOWN')
        )

    def clear_cache(self) -> None:
        """Clear the data cache"""
        self.cache.cache_clear()