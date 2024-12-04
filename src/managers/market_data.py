# src/core/data/managers/market_data.py
from .base import BaseManager
from ..cache.memory_cache import DataCache
from ..validation.market_data import MarketDataValidator
from datetime import datetime, timedelta
import asyncio
import pandas as pd
from typing import Dict, List, Optional, Any

class MarketDataManager(BaseManager[MarketData]):
    def __init__(self, source: DataSource, cache_size: int = 1000):
        super().__init__()
        self.source = source
        self.cache = DataCache(cache_size)
        self.validator = MarketDataValidator()
        self.batch_size = 100
        self.max_retries = 3
        self.base_delay = 1.0  # Base delay for exponential backoff

    async def get_market_data(self, symbol: str, start_date: datetime, 
                            end_date: datetime, timeframe: str = "1d") -> pd.DataFrame:
        """Get validated market data with caching"""
        cache_key = f"{symbol}:{start_date}:{end_date}:{timeframe}"
        
        try:
            # Validate inputs
            if not symbol:
                raise ValueError("Symbol cannot be empty")
            if end_date < start_date:
                raise ValueError("End date must be after start date")
            await self.validate_timeframe(timeframe)
            
            # Check cache
            if cached := await self.cache.get(cache_key):
                self.logger.debug(f"Cache hit for {symbol}")
                return cached

            # Start performance tracking
            start_time = datetime.now()
            
            # Fetch with retry
            data = await self._fetch_with_retry(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                timeframe=timeframe
            )
            
            # Validate fetched data
            is_valid, validation_errors = self.validator.validate(data)
            if not is_valid:
                raise ValueError(f"Invalid market data for {symbol}: {validation_errors}")

            # Update cache with valid data
            await self.cache.set(cache_key, data)
            
            # Log performance metrics
            elapsed = (datetime.now() - start_time).total_seconds()
            self.logger.info(
                f"Fetched {len(data)} records for {symbol} "
                f"({elapsed:.2f}s)"
            )
            
            return data

        except ValueError as e:
            self.logger.error(f"Validation error for {symbol}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(
                f"Failed to fetch market data for {symbol}: {str(e)}", 
                exc_info=True
            )
            raise RuntimeError(f"Market data fetch failed: {str(e)}") from e

    async def get_batch_market_data(self, symbols: List[str], start_date: datetime,
                                  end_date: datetime, timeframe: str = "1d") -> Dict[str, pd.DataFrame]:
        """Fetch market data for multiple symbols in batches"""
        results = {}
        errors = {}

        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i:i + self.batch_size]
            batch_tasks = [
                self.get_market_data(symbol, start_date, end_date, timeframe)
                for symbol in batch
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for symbol, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    errors[symbol] = str(result)
                    self.logger.error(f"Failed to fetch {symbol}: {result}")
                else:
                    results[symbol] = result

        if errors:
            self.logger.warning(f"Batch fetch completed with {len(errors)} errors")
            
        return results

    async def _fetch_with_retry(self, symbol: str, start_date: datetime,
                              end_date: datetime, timeframe: str, 
                              attempt: int = 0) -> pd.DataFrame:
        """Fetch data with exponential backoff retry"""
        try:
            return await self.source.fetch_data(symbol, start_date, end_date, timeframe)
        except Exception as e:
            if attempt >= self.max_retries:
                raise RuntimeError(
                    f"Failed to fetch {symbol} after {attempt} attempts"
                ) from e
            
            delay = self.base_delay * (2 ** attempt)
            self.logger.warning(
                f"Retry {attempt + 1}/{self.max_retries} for {symbol} "
                f"after {delay}s delay"
            )
            await asyncio.sleep(delay)
            return await self._fetch_with_retry(
                symbol, start_date, end_date, timeframe, attempt + 1
            )

    async def refresh_symbol(self, symbol: str) -> None:
        """Force refresh data for a symbol"""
        pattern = f"{symbol}:*"
        await self.cache.remove_pattern(pattern)
        self.logger.info(f"Cleared cache for {symbol}")

    async def get_cached_symbols(self) -> List[str]:
        """Get list of currently cached symbols"""
        keys = await self.cache.get_all_keys()
        return list(set(key.split(':')[0] for key in keys))

    async def monitor_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        stats = {
            'size': await self.cache.size(),
            'hits': await self.cache.get_hits(),
            'misses': await self.cache.get_misses(),
            'symbols': len(await self.get_cached_symbols()),
            'memory_usage': await self.cache.get_memory_usage(),
            'last_cleanup': await self.cache.get_last_cleanup_time()
        }
        self.logger.debug(f"Cache stats: {stats}")
        return stats

    async def cleanup_old_data(self, max_age: timedelta) -> int:
        """Remove data older than max_age from cache"""
        try:
            count = await self.cache.cleanup(max_age)
            self.logger.info(f"Cleaned up {count} old cache entries")
            return count
        except Exception as e:
            self.logger.error(f"Cache cleanup failed: {e}")
            raise

    async def validate_timeframe(self, timeframe: str) -> bool:
        """Validate timeframe format"""
        valid_timeframes = {'1m', '5m', '15m', '30m', '1h', '1d', '1w'}
        if timeframe not in valid_timeframes:
            raise ValueError(f"Invalid timeframe: {timeframe}. Must be one of {valid_timeframes}")
        return True

    async def health_check(self) -> Dict[str, Any]:
        """Check health of data manager components"""
        status = {
            'cache': await self.cache.is_healthy(),
            'source': await self.source.is_healthy(),
            'last_error': None,
            'uptime': datetime.now() - self._start_time,
            'total_requests': self._request_count
        }
        return status

    async def __aenter__(self):
        """Setup resources"""
        self._start_time = datetime.now()
        self._request_count = 0
        await self.cache.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources"""
        await self.cache.disconnect()
        if exc_val:
            self.logger.error(f"Error during cleanup: {exc_val}")