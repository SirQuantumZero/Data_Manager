# tests/test_database_manager_logging.py

import pytest
import logging
from pathlib import Path
import json
from datetime import datetime
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def log_db_manager():
    """Setup test database with logging configuration"""
    # Setup test log directory
    log_dir = Path("logs/test")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "test_db.log"
    
    # Configure logging
    logging.basicConfig(
        filename=str(log_file),
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_logs"
    manager.format_database(force=True)
    return manager, log_file

def test_operation_logging(log_db_manager):
    """Test database operation logging"""
    manager, log_file = log_db_manager
    
    # Perform operations
    with manager.get_connection() as conn:
        with conn.cursor() as cursor:
            manager.populate_test_data(cursor)
    
    # Check logs
    with open(log_file) as f:
        logs = f.readlines()
        assert any("test data population complete" in log.lower() for log in logs)
        assert any("market data" in log.lower() for log in logs)

def test_error_logging(log_db_manager):
    """Test error logging"""
    manager, log_file = log_db_manager
    
    # Trigger an error
    try:
        manager.execute_query("SELECT * FROM nonexistent_table", [])
    except Exception:
        pass
    
    # Verify error was logged
    with open(log_file) as f:
        logs = f.readlines()
        assert any("error" in log.lower() for log in logs)
        assert any("nonexistent_table" in log for log in logs)

def test_performance_logging(log_db_manager):
    """Test performance metric logging"""
    manager, log_file = log_db_manager
    
    # Perform operation with timing
    start = datetime.now()
    manager.get_historical_data("MarketData", {"symbol": "AAPL"})
    duration = (datetime.now() - start).total_seconds()
    
    # Check performance logs
    with open(log_file) as f:
        logs = f.readlines()
        assert any("query execution time" in log.lower() for log in logs)
        assert any(str(round(duration, 2)) in log for log in logs)

if __name__ == "__main__":
    pytest.main([__file__])