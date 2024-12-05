# src/database/config.py
import os
from dataclasses import dataclass
from typing import Dict, Any
from dotenv import load_dotenv

@dataclass
class Config:
    host: str = 'localhost'
    port: int = 3306
    user: str = None
    password: str = None
    database: str = None
    pool_size: int = 5
    pool_name: str = 'database_pool'
    
    def __init__(self):
        load_dotenv()
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.database = os.getenv('DB_DATABASE')
        self.host = os.getenv('DB_HOST', self.host)
        self.port = int(os.getenv('DB_PORT', self.port))
        self.validate()
    
    def validate(self) -> None:
        """Validate required configuration"""
        required = ['user', 'password', 'database']
        missing = [f for f in required if not getattr(self, f)]
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Get parameters for connection pool"""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database
        }