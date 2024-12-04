# src/core/data/polygon_client.py
import pandas as pd
from polygon import RESTClient


class PolygonClient:
    def __init__(self, api_key: str):
        self.client = RESTClient(api_key)
        self.endpoints = {
            "crypto": {
                "aggregates": "/v2/aggs/ticker/{}/range/{}/{}/{}/{}",
                "daily": "/v1/open-close/crypto/{}/{}/{}",
                "last_trade": "/v1/last/crypto/{}/{}",
            },
            "forex": {
                "aggregates": "/v2/aggs/ticker/{}/range/{}/{}/{}/{}",
                "quotes": "/v3/quotes/{}",
                "conversion": "/v1/conversion/{}/{}",
            },
            "stocks": {
                "aggregates": "/v2/aggs/ticker/{}/range/{}/{}/{}/{}",
                "quotes": "/v3/quotes/{}",
                "trades": "/v3/trades/{}",
            },
            "reference": {
                "financials": "/vX/reference/financials",
                "news": "/v2/reference/news",
                "dividends": "/v3/reference/dividends",
            },
        }

    def get_market_data(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch market data from Polygon API with error handling
        """
        try:
            if not self.client:
                raise Exception("Polygon API client not initialized - missing API key")

            # Fetch data from Polygon API
            aggs = self.client.get_aggs(symbol, 1, "day", start_date, end_date)

            # Convert to DataFrame
            data = pd.DataFrame(
                [
                    {
                        "timestamp": agg.timestamp,
                        "open": agg.open,
                        "high": agg.high,
                        "low": agg.low,
                        "close": agg.close,
                        "volume": agg.volume,
                    }
                    for agg in aggs
                ]
            )

            return data

        except Exception as e:
            raise Exception(f"Failed to fetch market data: {e}")

    async def fetch_all_data(self, symbol: str, start_date: str, end_date: str):
        """Fetch comprehensive market data"""
        data = {
            "market_data": await self._fetch_market_data(symbol, start_date, end_date),
            "sentiment": await self._fetch_news_sentiment(symbol),
            "fundamentals": await self._fetch_fundamentals(symbol),
            "technical": await self._fetch_technical_indicators(symbol),
        }
        return data
