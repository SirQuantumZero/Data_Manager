# src/database_client.py
import mysql.connector
from typing import Dict, Any
import logging
from datetime import datetime
import pandas as pd

# If importing from database_config.py (same directory level)
from .database_config import DatabaseConfig


class DatabaseClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.connection_params)
            self.cursor = self.connection.cursor()
        except mysql.connector.Error as e:
            raise Exception(f"Database connection failed: {e}")

    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from database with error handling"""
        try:
            self.connect()
            self.cursor.execute(
                "SELECT * FROM market_data"
            )  # Replace with your table name
            columns = [desc[0] for desc in self.cursor.description]
            data = self.cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            raise Exception(f"Error fetching data: {e}")
        finally:
            self.disconnect()

    def execute_query(self, query: str, params: tuple = None) -> None:
        """Execute database query with error handling"""
        try:
            self.connect()
            self.cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise Exception(f"Query execution failed: {e}")
        finally:
            self.disconnect()
