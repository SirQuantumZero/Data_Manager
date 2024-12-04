# tests/test_schema_runner.py

import pytest
import mysql.connector
import logging
from pathlib import Path
from datetime import datetime
import subprocess
import json
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def schema_runner():
    """Create test runner with test database config"""
    runner = SchemaRunner()
    runner.config['database'] = 'test_trading_data'
    return runner

class SchemaRunner:
    def __init__(self, config_path: str = ".env"):
        self.load_config(config_path)
        self.setup_logging()
        
    def load_config(self, config_path: str):
        """Load database configuration from environment"""
        load_dotenv(config_path)
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'trading_user'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_DATABASE', 'trading_data')
        }
        
        # Root credentials for database creation
        self.root_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': 'root',  # MySQL root user
            'password': os.getenv('MYSQL_ROOT_PASSWORD', ''),  # Set this in .env
        }

    def create_test_database(self) -> bool:
        """Create test database and grant permissions using root credentials"""
        try:
            with mysql.connector.connect(**self.root_config) as connection:
                cursor = connection.cursor()
                
                # Create database if not exists
                cursor.execute(f"DROP DATABASE IF EXISTS {self.config['database']}")
                cursor.execute(f"CREATE DATABASE {self.config['database']}")
                
                # Grant permissions to trading_user
                grant_stmt = f"""GRANT ALL PRIVILEGES ON {self.config['database']}.* 
                               TO '{self.config['user']}'@'localhost'"""
                cursor.execute(grant_stmt)
                cursor.execute("FLUSH PRIVILEGES")
                
                return True
        except Exception as e:
            self.logger.error(f"Database setup failed: {str(e)}")
            return False

    def setup_logging(self):
        """Configure logging for the schema runner"""
        self.logger = logging.getLogger(__name__)
        
    def execute_schema_file(self, schema_path: str) -> bool:
        """Execute SQL schema file against the database"""
        try:
            if not self.create_test_database():
                return False
                
            with mysql.connector.connect(**self.config) as connection:
                cursor = connection.cursor()
                
                with open(schema_path, 'r') as f:
                    content = f.read()
                    # Split on semicolon but preserve them in statements
                    statements = [s.strip() + ';' for s in content.split(';') if s.strip()]
                    
                for statement in statements:
                    try:
                        # Skip empty statements
                        if not statement.strip():
                            continue
                        
                        # Execute each statement individually
                        cursor.execute(statement)
                        connection.commit()
                        
                    except mysql.connector.Error as sql_err:
                        self.logger.error(f"SQL Error executing statement: {sql_err}")
                        self.logger.debug(f"Failed statement: {statement}")
                        return False
                        
                return True
                
        except Exception as e:
            self.logger.error(f"Schema execution failed: {str(e)}")
            return False
            
    def verify_test_results(self) -> bool:
        """Verify the database schema changes"""
        try:
            with mysql.connector.connect(**self.config) as connection:
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                return len(tables) > 0
                
        except Exception as e:
            self.logger.error(f"Verification failed: {str(e)}")
            return False

def test_schema_execution(schema_runner):
    """Test schema file execution"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
def test_results_verification(schema_runner): 
    """Test results verification"""
    assert schema_runner.verify_test_results()

if __name__ == "__main__":
    pytest.main([__file__])