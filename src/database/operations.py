# src/database/operations.py
from typing import Any, List, Dict, Optional
from .core import Core

class Operations:
    def __init__(self, core: Core):
        self.core = core
        
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """Insert single record and return inserted id"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self.core.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(data.values()))
            return cursor.lastrowid
            
    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple records"""
        if not data:
            return
            
        columns = ', '.join(data[0].keys())
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        values = [tuple(row.values()) for row in data]
        self.core.execute_many(query, values)
        
    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> int:
        """Update records matching criteria"""
        set_clause = ', '.join(f"{k} = %s" for k in data.keys())
        where_clause = ' AND '.join(f"{k} = %s" for k in where.keys())
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        
        params = (*data.values(), *where.values())
        with self.core.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.rowcount
            
    def delete(self, table: str, where: Dict[str, Any]) -> int:
        """Delete records matching criteria"""
        where_clause = ' AND '.join(f"{k} = %s" for k in where.keys())
        query = f"DELETE FROM {table} WHERE {where_clause}"
        
        with self.core.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(where.values()))
            return cursor.rowcount

    def select_one(self, table: str, where: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Get single record matching criteria"""
        results = self.select(table, where, limit=1)
        return results[0] if results else None

    def select(self, 
               table: str, 
               where: Optional[Dict[str, Any]] = None,
               order_by: Optional[str] = None,
               limit: Optional[int] = None,
               offset: Optional[int] = None) -> List[Dict]:
        """Get records matching criteria with sorting and pagination"""
        query = f"SELECT * FROM {table}"
        params = []

        if where:
            conditions = ' AND '.join(f"{k} = %s" for k in where.keys())
            query += f" WHERE {conditions}"
            params.extend(where.values())

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"

        return self.core.execute(query, tuple(params))

    def count(self, table: str, where: Optional[Dict[str, Any]] = None) -> int:
        """Count records matching criteria"""
        query = f"SELECT COUNT(*) as count FROM {table}"
        params = []

        if where:
            conditions = ' AND '.join(f"{k} = %s" for k in where.keys())
            query += f" WHERE {conditions}"
            params.extend(where.values())

        result = self.core.execute(query, tuple(params))
        return result[0]['count']

    def exists(self, table: str, where: Dict[str, Any]) -> bool:
        """Check if records exist matching criteria"""
        return self.count(table, where) > 0