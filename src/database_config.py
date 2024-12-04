# src/core/data/database_config.py
from dataclasses import dataclass
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv
import mysql.connector
import logging

@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    ssl_ca: Optional[str] = None
    
    @classmethod
    def from_env(cls):
        load_dotenv()
        return cls(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '3306')),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'quantumzero'),
        )

class DatabaseClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None

    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(**self.config)
            return True
        except mysql.connector.Error as e:
            logging.error(f"Error connecting to database: {e}")
            return False

    # Other methods...

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'quantumzero',
}