# src/core/data/cache/memory_cache.py
from typing import Optional, Any, Dict
from datetime import datetime, timedelta
import asyncio
from collections import OrderedDict

class DataCache:
    """Thread-safe LRU cache with TTL support"""
    def __init__(self, maxsize: int = 1000, ttl: int = 300):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: OrderedDict[str, tuple[Any, datetime]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        async with self._lock:
            if key not in self._cache:
                return None
            
            value, timestamp = self._cache[key]
            if datetime.now() - timestamp > timedelta(seconds=self.ttl):
                del self._cache[key]
                return None
                
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            return value

    async def set(self, key: str, value: Any) -> None:
        """Set value in cache with timestamp"""
        async with self._lock:
            if len(self._cache) >= self.maxsize:
                # Remove oldest item
                self._cache.popitem(last=False)
            self._cache[key] = (value, datetime.now())

    async def clear(self) -> None:
        """Clear all cached data"""
        async with self._lock:
            self._cache.clear()

    async def remove(self, key: str) -> None:
        """Remove specific key from cache"""
        async with self._lock:
            self._cache.pop(key, None)

# src/core/data/validation/market_data.py
from dataclasses import dataclass
from typing import List, Set
import pandas as pd

@dataclass
class ValidationRule:
    name: str
    columns: Set[str]
    check: callable

class MarketDataValidator:
    """Validates market data structure and content"""
    
    def __init__(self):
        self.rules = [
            ValidationRule(
                name="required_columns",
                columns={'open', 'high', 'low', 'close', 'volume'},
                check=self._check_required_columns
            ),
            ValidationRule(
                name="price_consistency",
                columns={'open', 'high', 'low', 'close'},
                check=self._check_price_consistency
            ),
            ValidationRule(
                name="positive_values",
                columns={'volume', 'vwap'},
                check=self._check_positive_values
            )
        ]

    def validate(self, df: pd.DataFrame) -> tuple[bool, List[str]]:
        """Validate DataFrame against all rules"""
        if df.empty:
            return False, ["DataFrame is empty"]

        errors = []
        for rule in self.rules:
            if not rule.check(df):
                errors.append(f"Failed {rule.name} validation")
        
        return len(errors) == 0, errors

    def _check_required_columns(self, df: pd.DataFrame) -> bool:
        """Check if all required columns are present"""
        return all(col in df.columns for col in self.rules[0].columns)

    def _check_price_consistency(self, df: pd.DataFrame) -> bool:
        """Verify price relationships (low <= open/close <= high)"""
        return (
            (df['low'] <= df['open']) & 
            (df['low'] <= df['close']) & 
            (df['high'] >= df['open']) & 
            (df['high'] >= df['close'])
        ).all()

    def _check_positive_values(self, df: pd.DataFrame) -> bool:
        """Check if volume and VWAP are positive"""
        checks = []
        if 'volume' in df.columns:
            checks.append((df['volume'] >= 0).all())
        if 'vwap' in df.columns:
            checks.append((df['vwap'] > 0).all())
        return all(checks)