# src/database/core.py
import logging
from typing import Optional, Any, Generator
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection
from contextlib import contextmanager
from .config import Config  # Fix import path

class Core:
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.pool = self._create_connection_pool()

    def _create_connection_pool(self) -> MySQLConnectionPool:
        try:
            return MySQLConnectionPool(
                pool_name="database_pool",
                pool_size=5,
                **self.config.get_connection_params()
            )
        except mysql.connector.Error as e:
            self.logger.error(f"Failed to create connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self) -> Generator[PooledMySQLConnection, None, None]:
        conn = None
        try:
            conn = self.pool.get_connection()
            yield conn
        except mysql.connector.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except mysql.connector.Error:
                    self.logger.warning("Failed to close connection")

    def execute(self, query: str, params: Optional[tuple] = None) -> list[dict]:
        """Execute single query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(query, params or ())
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_many(self, query: str, params_list: list[tuple]) -> None:
        """Execute batch query with multiple parameter sets"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(query, params_list)
                conn.commit()
            finally:
                cursor.close()

    @contextmanager
    def transaction(self) -> Generator[PooledMySQLConnection, None, None]:
        """Transaction context manager"""
        with self.get_connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Transaction failed: {e}")
                raise

    def ping(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                conn.ping(reconnect=True)
                return True
        except mysql.connector.Error as e:
            self.logger.error(f"Ping failed: {e}")
            return False

    def close(self) -> None:
        """Close connection pool"""
        if hasattr(self, 'pool'):
            try:
                self.pool.close()
            except mysql.connector.Error as e:
                self.logger.error(f"Error closing pool: {e}")