# src/core/data/api/endpoints.py

from fastapi import FastAPI, HTTPException, Depends
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime
import logging

# Update relative imports
from ..data_manager import DataManager, RequestType, DataSource
from ..models import MarketDataRequest, MarketData
from ..database_config import DatabaseConfig

app = FastAPI(title="Market Data API")
logger = logging.getLogger(__name__)

class MarketDataEndpoints:
    def __init__(self):
        self.data_manager = DataManager()
        
    async def get_market_data(
        self, 
        request: MarketDataRequest
    ) -> Dict[str, Any]:
        """Fetch market data endpoint"""
        try:
            return await self.data_manager.process_request({
                "type": "market_data",
                "payload": request
            })
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")
            raise HTTPException(status_code=500, detail=str(e))

class BacktestRequest(BaseModel):
    symbols: List[str]
    start_date: str
    end_date: str
    strategy_id: Optional[str] = None

class ScheduledTaskRequest(BaseModel):
    task_id: str
    schedule_type: str
    time: Optional[str] = None
    interval: Optional[int] = None
    task_config: Dict[str, Any]

market_data_endpoints = MarketDataEndpoints()

@app.post("/market-data/")
async def get_market_data(request: MarketDataRequest):
    """Fetch market data for a symbol"""
    try:
        return await market_data_endpoints.data_manager.process_request({
            "type": RequestType.MARKET_DATA.value,
            **request.dict()
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/backtest/")
async def run_backtest(request: BacktestRequest):
    """Run backtest data request"""
    try:
        return await market_data_endpoints.data_manager.process_request({
            "type": RequestType.BACKTEST.value,
            **request.dict()
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/schedule/")
async def schedule_task(request: ScheduledTaskRequest):
    """Schedule a new task"""
    try:
        return await market_data_endpoints.data_manager.process_request({
            "type": RequestType.SCHEDULED.value,
            "action": "add",
            **request.dict()
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/schedule/{task_id}")
async def remove_task(task_id: str):
    """Remove a scheduled task"""
    try:
        return await market_data_endpoints.data_manager.process_request({
            "type": RequestType.SCHEDULED.value,
            "action": "remove",
            "task_id": task_id
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/schedule/")
async def list_tasks():
    """List all scheduled tasks"""
    try:
        return await market_data_endpoints.data_manager.process_request({
            "type": RequestType.SCHEDULED.value,
            "action": "list"
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/database/{operation}")
async def database_operation(operation: str):
    """Execute database operations"""
    try:
        return await market_data_endpoints.data_manager.process_request({
            "type": RequestType.DATABASE_OP.value,
            "operation": operation
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))