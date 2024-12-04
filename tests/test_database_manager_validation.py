# tests/test_database_manager_validation.py

import pytest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from src.database_manager import DatabaseManager

@pytest.fixture(scope="session")
def validation_db_manager():
    """Setup test database for data validation"""
    manager = DatabaseManager()
    manager.db_config.database = "test_trading_data_validation"
    manager.format_database(force=True)
    return manager

def test_market_data_validation(validation_db_manager):
    """Test market data validation rules"""
    valid_data = {
        'symbol': 'AAPL',
        'timestamp': datetime.now(),
        'open': 150.0,
        'high': 155.0,
        'low': 149.0,
        'close': 152.0,
        'volume': 1000000
    }
    
    invalid_cases = [
        # Price validation
        {**valid_data, 'high': 148.0},  # High < Low
        {**valid_data, 'close': -1.0},  # Negative price
        {**valid_data, 'open': 1000000.0},  # Unrealistic price
        
        # Volume validation
        {**valid_data, 'volume': -100},  # Negative volume
        {**valid_data, 'volume': 0},  # Zero volume
        
        # Symbol validation
        {**valid_data, 'symbol': ''},  # Empty symbol
        {**valid_data, 'symbol': 'A' * 21},  # Too long
        
        # Timestamp validation
        {**valid_data, 'timestamp': datetime.now() + timedelta(days=1)}  # Future date
    ]
    
    # Test valid data
    assert validation_db_manager.validate_market_data(valid_data)
    
    # Test invalid cases
    for invalid_data in invalid_cases:
        with pytest.raises(ValueError):
            validation_db_manager.validate_market_data(invalid_data)

def test_data_type_validation(validation_db_manager):
    """Test data type validation"""
    test_cases = [
        ('symbol', 'AAPL', str),
        ('timestamp', datetime.now(), datetime),
        ('open', 150.0, (int, float)),
        ('volume', 1000000, int)
    ]
    
    for field, value, expected_type in test_cases:
        assert validation_db_manager.validate_field_type(field, value, expected_type)
        with pytest.raises(TypeError):
            validation_db_manager.validate_field_type(field, "invalid", expected_type)

def test_data_consistency(validation_db_manager):
    """Test data consistency rules"""
    # Test OHLC consistency
    with pytest.raises(ValueError):
        validation_db_manager.validate_ohlc(
            open_price=100,
            high_price=95,  # High should be >= Open
            low_price=90,
            close_price=92
        )
    
    # Test time series consistency
    data = pd.DataFrame({
        'timestamp': pd.date_range(start='2024-01-01', periods=5),
        'close': [100, 101, np.nan, 103, 104]  # Contains gap
    })
    
    gaps = validation_db_manager.find_data_gaps(data)
    assert len(gaps) == 1

def test_schema_validation(validation_db_manager):
    """Test database schema validation"""
    required_tables = ['MarketData', 'Trades', 'SystemLogs']
    missing = validation_db_manager.validate_schema(required_tables)
    assert not missing, f"Missing required tables: {missing}"

if __name__ == "__main__":
    pytest.main([__file__])