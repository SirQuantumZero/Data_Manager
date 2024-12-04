import logging
from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd

# Update relative imports
from ..base.base_data_source_ import DataSourceBase
from ...models import MarketData
from .polygon_client import PolygonClient

class PolygonDataSource(DataSourceBase):
    """High-level interface for Polygon.io data"""

    def __init__(self, api_key: Optional[str] = None):
        self.client = PolygonClient(api_key)
        self.logger = logging.getLogger(__name__)

    async def fetch_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch market data for a symbol"""
        try:
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            aggs = self.client.get_aggs(symbol, 1, "day", start_str, end_str)
            
            if not aggs:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
                
            data = [{
                'timestamp': pd.Timestamp(agg.timestamp),
                'symbol': symbol,
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume,
                'vwap': agg.vwap
            } for agg in aggs]
            
            df = pd.DataFrame(data)
            if not df.empty:
                df.set_index('timestamp', inplace=True)
            return df
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    async def fetch_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """Fetch fundamental data for a symbol"""
        try:
            details = self.client.get_ticker_details(symbol)
            return {
                "market_cap": getattr(details, 'market_cap', None),
                "description": getattr(details, 'description', ''),
                "sector": getattr(details, 'sector', ''),
                "industry": getattr(details, 'industry', '')
            }
        except Exception as e:
            self.logger.error(f"Error fetching fundamentals for {symbol}: {e}")
            return {}

    async def fetch_news(self, symbol: str, limit: int = 10) -> pd.DataFrame:
        """Fetch news data for a symbol"""
        try:
            news_items = self.client.get_ticker_news(symbol, limit=limit)
            if not news_items:
                return pd.DataFrame()
                
            data = [{
                'timestamp': pd.Timestamp(item.published_utc),
                'title': getattr(item, 'title', ''),
                'url': getattr(item, 'article_url', ''),
                'source': getattr(item.publisher, 'name', '') if hasattr(item, 'publisher') else ''
            } for item in news_items]
            
            df = pd.DataFrame(data)
            if not df.empty:
                df.set_index('timestamp', inplace=True)
            return df
        except Exception as e:
            self.logger.error(f"Error fetching news for {symbol}: {e}")
            return pd.DataFrame()