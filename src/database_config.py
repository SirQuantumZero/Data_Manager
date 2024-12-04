# src/database_config.py

from dataclasses import dataclass
from typing import Dict, Any, Optional
import os
from dotenv import load_dotenv

@dataclass
class DBCredentials:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306

class DatabaseConfig:
    def __init__(self):
        load_dotenv()
        self.credentials = DBCredentials(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'trading_user'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_DATABASE', 'trading_data'),  # Changed from DB_NAME to DB_DATABASE
            port=int(os.getenv('DB_PORT', 3306))
        )