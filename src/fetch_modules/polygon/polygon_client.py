import logging
from typing import Optional
from polygon import RESTClient
from dotenv import load_dotenv
import os

class PolygonClient:
    """Low-level client for Polygon.io API"""
    
    def __init__(self, api_key: Optional[str] = None):
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