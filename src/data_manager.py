# src/data_manager.py

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
import pandas as pd
import asyncio
from functools import lru_cache
import argparse
import schedule
import time
import json
from enum import Enum
import os
from dotenv import load_dotenv
import numpy as np

# Current imports
from .database_client import DatabaseClient
from .database_config import DatabaseConfig
from .fetch_modules.polygon.polygon_client import PolygonClient
from .models import MarketDataValidator

# Based on project structure, these are correct since:
# - All imported files are at same level or in subdirectories
# - Relative imports (.) are correct since files are siblings
# - fetch_modules path follows the correct directory structure

load_dotenv()

class DataSource(Enum):
    POLYGON = "polygon"
    DATABASE = "database"
    MOCK = "mock"
    
class RequestType(Enum):
    MARKET_DATA = "market_data"
    BACKTEST = "backtest"
    DATABASE_OP = "database_op"
    SCHEDULED = "scheduled"

class DataManager:
    """Centralized data management service"""

    def __init__(self, config_path: str = "config/data_config.json"):
        self.logger = logging.getLogger(__name__)
        self._load_config(config_path)
        self.db_client = DatabaseClient(self.config['database'])
        self.polygon_api_key = os.getenv('POLYGON_API_KEY')
        self.polygon_client = PolygonClient(self.polygon_api_key)
        self.validator = MarketDataValidator()
        self.cache = lru_cache(maxsize=1000)(self._fetch_from_source)
        self.scheduled_tasks = {}

    def _load_config(self, config_path: str) -> None:
        """Load configuration from JSON file"""
        try:
            with open(config_path) as f:
                self.config = json.load(f)
        except FileNotFoundError as e:
            self.logger.error(f"Configuration file not found: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON format: {e}")
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            raise

    async def process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Main request processing entry point"""
        try:
            request_type = RequestType(request.get('type'))
            
            handlers = {
                RequestType.MARKET_DATA: self._handle_market_data_request,
                RequestType.BACKTEST: self._handle_backtest_request,
                RequestType.DATABASE_OP: self._handle_database_request,
                RequestType.SCHEDULED: self._handle_scheduled_request
            }
            
            handler = handlers.get(request_type)
            if not handler:
                raise ValueError(f"Unknown request type: {request_type}")
                
            return await handler(request)
            
        except Exception as e:
            self.logger.error(f"Request processing failed: {e}")
            return {"status": "error", "message": str(e)}

    async def _handle_market_data_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle market data requests"""
        symbol = request['symbol']
        start_date = datetime.fromisoformat(request['start_date'])
        end_date = datetime.fromisoformat(request['end_date'])
        source = DataSource(request.get('source', 'polygon'))
        
        if source == DataSource.POLYGON:
            data = await self._fetch_polygon_data(symbol, start_date, end_date)
        elif source == DataSource.DATABASE:
            data = await self._fetch_database_data(symbol, start_date, end_date)
        else:
            data = await self._fetch_mock_data(symbol, start_date, end_date)
            
        # Store in database if requested
        if request.get('store', False):
            await self._store_market_data(symbol, data)
            
        return {
            "status": "success",
            "data": data.to_dict('records')
        }

    async def _handle_backtest_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle backtest data requests"""
        symbols = request['symbols']
        start_date = datetime.fromisoformat(request['start_date'])
        end_date = datetime.fromisoformat(request['end_date'])
        strategy_id = request.get('strategy_id')
        
        # Fetch required data
        data = {}
        for symbol in symbols:
            # Try database first, then Polygon if needed
            db_data = await self._fetch_database_data(symbol, start_date, end_date)
            if db_data.empty:
                polygon_data = await self._fetch_polygon_data(symbol, start_date, end_date)
                await self._store_market_data(symbol, polygon_data)
                data[symbol] = polygon_data
            else:
                data[symbol] = db_data
                
        return {
            "status": "success",
            "data": {symbol: df.to_dict('records') for symbol, df in data.items()}
        }

    async def _handle_database_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle database management requests"""
        operation = request['operation']
        
        operations = {
            'backup': self.db_client.backup_database,
            'format': self.db_client.format_database,
            'stats': self.db_client.get_statistics,
        }
        
        if operation not in operations:
            raise ValueError(f"Unknown database operation: {operation}")
            
        result = await operations[operation]()
        return {
            "status": "success",
            "result": result
        }

    async def _handle_scheduled_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle scheduled task requests"""
        action = request['action']
        
        if action == 'add':
            return await self._add_scheduled_task(request)
        elif action == 'remove':
            return await self._remove_scheduled_task(request['task_id'])
        elif action == 'list':
            return {"status": "success", "tasks": self.scheduled_tasks}
        
        raise ValueError(f"Unknown scheduled action: {action}")

    async def _fetch_polygon_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        data = await self.polygon_client.get_market_data(symbol, start_date, end_date)
        # Rest of the code...

    async def _fetch_database_data(self, symbol: str, start_date: datetime,
                                 end_date: datetime) -> pd.DataFrame:
        """Fetch data from database"""
        try:
            query = """
                SELECT * FROM market_data 
                WHERE symbol = %s AND timestamp BETWEEN %s AND %s
                ORDER BY timestamp
            """
            return await self.db_client.execute_query(query, (symbol, start_date, end_date))
        except Exception as e:
            self.logger.error(f"Database fetch failed for {symbol}: {e}")
            raise

    async def _store_market_data(self, symbol: str, data: pd.DataFrame) -> None:
        """Store market data in database"""
        try:
            await self.db_client.store_market_data(symbol, data)
        except Exception as e:
            self.logger.error(f"Failed to store data for {symbol}: {e}")
            raise

    async def _add_scheduled_task(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new scheduled task"""
        task_id = request['task_id']
        schedule_type = request['schedule_type']
        
        if schedule_type == 'daily':
            schedule.every().day.at(request['time']).do(
                self._run_scheduled_task, request['task_config']
            )
        elif schedule_type == 'interval':
            schedule.every(request['interval']).minutes.do(
                self._run_scheduled_task, request['task_config']
            )
            
        self.scheduled_tasks[task_id] = request
        return {"status": "success", "task_id": task_id}

    async def _fetch_from_source(self, symbol: str, start_date: datetime, 
                               end_date: datetime, source: DataSource) -> pd.DataFrame:
        """Fetch data from specified source with caching"""
        if source == DataSource.POLYGON:
            return await self._fetch_polygon_data(symbol, start_date, end_date)
        elif source == DataSource.DATABASE:
            return await self._fetch_database_data(symbol, start_date, end_date)
        elif source == DataSource.MOCK:
            return await self._fetch_mock_data(symbol, start_date, end_date)
        raise ValueError(f"Unknown data source: {source}")

    async def _run_scheduled_task(self, task_config: Dict[str, Any]) -> None:
        """Execute a scheduled task"""
        try:
            task_type = task_config.get('type')
            self.logger.info(f"Running scheduled task: {task_type}")
            
            if task_type == 'market_data':
                await self._handle_market_data_request(task_config)
            elif task_type == 'backtest':
                await self._handle_backtest_request(task_config)
            elif task_type == 'database':
                await self._handle_database_request(task_config)
            else:
                raise ValueError(f"Unknown task type: {task_type}")
                
        except Exception as e:
            self.logger.error(f"Scheduled task failed: {e}")
            
    async def _remove_scheduled_task(self, task_id: str) -> Dict[str, Any]:
        """Remove a scheduled task"""
        if task_id not in self.scheduled_tasks:
            raise ValueError(f"Task not found: {task_id}")
            
        # Remove from schedule
        schedule.clear(task_id)
        # Remove from our tracking
        del self.scheduled_tasks[task_id]
        
        return {
            "status": "success",
            "message": f"Removed task: {task_id}"
        }

    async def _fetch_mock_data(self, symbol: str, start_date: datetime,
                              end_date: datetime) -> pd.DataFrame:
        """Generate mock market data for testing"""
        try:
            dates = pd.date_range(start=start_date, end=end_date)
            base_price = 100.0  # Starting price
            
            data = []
            for date in dates:
                # Generate realistic-looking price movement
                daily_volatility = np.random.normal(0, 0.02)  # 2% standard deviation
                high = base_price * (1 + abs(daily_volatility))
                low = base_price * (1 - abs(daily_volatility))
                close = base_price * (1 + daily_volatility)
                
                data.append({
                    'timestamp': date,
                    'symbol': symbol,
                    'open': base_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': int(np.random.normal(1000000, 200000)),
                    'vwap': (high + low + close) / 3,
                    'source': 'MOCK'
                })
                
                # Update base price for next iteration
                base_price = close
                
            df = pd.DataFrame(data)
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Mock data generation failed: {e}")
            raise

    def run_cli(self):
        """Run the CLI interface"""
        parser = argparse.ArgumentParser(description='Data Manager CLI')
        parser.add_argument('command', choices=['fetch', 'backtest', 'database', 'schedule'])
        parser.add_argument('--symbol', help='Stock symbol')
        parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
        parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
        parser.add_argument('--source', choices=['polygon', 'database', 'mock'])
        parser.add_argument('--store', action='store_true', help='Store in database')
        
        args = parser.parse_args()
        
        # Convert CLI args to request format
        request = {
            "type": args.command,
            "symbol": args.symbol,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "source": args.source,
            "store": args.store
        }
        
        # Process request
        asyncio.run(self.process_request(request))

def main():
    """CLI entry point"""
    manager = DataManager()
    manager.run_cli()

if __name__ == "__main__":
    main()
