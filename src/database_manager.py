# src/database_manager.py
import mysql.connector

class DatabaseManager:
    def __init__(self, host='localhost', user='username', password='password'):
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        self.cursor = self.connection.cursor()

    def create_database(self, database_name):
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        self.connection.database = database_name

    def drop_database(self, database_name):
        self.cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")

    def create_table(self, table_name, schema):
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")

    def drop_table(self, table_name):
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    def close(self):
        self.cursor.close()
        self.connection.close()