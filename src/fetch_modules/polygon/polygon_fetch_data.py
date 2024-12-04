# src/core/data/fetch_modules/polygon/fetch_data.py
import os
import argparse
import asyncio
import logging
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
from typing import Optional

from .polygon_data_source import PolygonDataSource
from .polygon_client import PolygonClient
from ..base.base_data_source_ import DataSourceBase
from ...models import MarketData

logger = logging.getLogger(__name__)

class PolygonFetcher:
    """Handles data fetching from Polygon.io"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.data_source = PolygonDataSource(api_key)
        self.logger = logging.getLogger(__name__)

    async def fetch_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch historical market data"""
        return await self.data_source.fetch_data(symbol, start_date, end_date)

async def main():
    """CLI tool for fetching Polygon data"""
    parser = argparse.ArgumentParser(description='Fetch data from Polygon API')
    parser.add_argument('symbol', help='Stock symbol to fetch')
    parser.add_argument('--days', type=int, default=30, help='Number of days of historical data')
    args = parser.parse_args()

    data_source = PolygonDataSource()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)

    try:
        data = await data_source.fetch_data(args.symbol, start_date, end_date)
        print(data)
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())