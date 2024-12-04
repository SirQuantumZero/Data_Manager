import aiohttp
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from polygon import RESTClient
from dotenv import load_dotenv
import os

# Update relative imports to match fetch_modules structure
from ..base.base_data_source_ import DataSourceBase
from ...models import MarketData

class PolygonClient(DataSourceBase):
    """Low-level client for Polygon.io API"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.polygon.io/v2"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = None
        self.logger = logging.getLogger(__name__)
        
        if not api_key:
            load_dotenv()
            api_key = os.getenv("POLYGON_API_KEY")
            
        if not api_key:
            raise ValueError("Polygon API key not found")
            
        self.client = RESTClient(api_key)

    def get_client(self) -> RESTClient:
        """Get the underlying RESTClient instance"""
        return self.client