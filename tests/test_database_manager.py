# tests/test_database_manager.py

import pytest
import mysql.connector
from unittest.mock import Mock, patch, MagicMock
import os
from datetime import datetime
from src.database_manager import DatabaseManager, DBConfig, TradeData

@pytest.fixture
def db_manager():
    """Create DatabaseManager instance with test config"""
    with patch.dict(os.environ, {
        'DB_HOST': 'localhost',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_pass',
        'DB_DATABASE': 'test_db'
    }):
        return DatabaseManager()

@pytest.fixture
def mock_cursor():
    """Create mock cursor"""
    cursor = MagicMock()
    cursor.fetchone.return_value = ["1.0.0"]
    cursor.fetchall.return_value = [("table1",), ("table2",)]
    return cursor

@pytest.fixture
def mock_connection(mock_cursor):
    """Create mock connection"""
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = mock_cursor
    return conn

def test_init(db_manager):
    """Test initialization"""
    assert db_manager.SCHEMA_VERSION == "1.0.0"
    assert isinstance(db_manager.db_config, DBConfig)
    assert db_manager.backup_dir == 'db/backups'

@patch('mysql.connector.connect')
def test_backup_database(mock_connect, db_manager, tmp_path):
    """Test database backup"""
    db_manager.backup_dir = str(tmp_path)
    assert db_manager.backup_database()
    backup_files = list(tmp_path.glob('backup_*.sql'))
    assert len(backup_files) == 1

@patch('mysql.connector.connect')
def test_format_database(mock_connect, db_manager, mock_connection, mock_cursor):
    """Test database formatting"""
    mock_connect.return_value = mock_connection
    
    with patch.object(db_manager, 'confirm_format', return_value=True):
        assert db_manager.format_database(populate=True)
        mock_cursor.execute.assert_called()

@patch('mysql.connector.connect')
def test_apply_schema(mock_connect, db_manager, mock_connection, mock_cursor):
    """Test schema application"""
    mock_connect.return_value = mock_connection
    assert db_manager.apply_schema(mock_cursor)

def test_generate_test_data(db_manager, mock_cursor):
    """Test test data generation"""
    db_manager.populate_test_data(mock_cursor)
    assert len(db_manager.user_ids) > 0

@patch('mysql.connector.connect')
def test_execute_query(mock_connect, db_manager, mock_connection):
    """Test query execution"""
    mock_connect.return_value = mock_connection
    result = db_manager.execute_query("SELECT * FROM test", [])
    assert isinstance(result, list)

def test_print_statistics(capsys, db_manager, mock_cursor):
    """Test statistics display"""
    db_manager.print_statistics(mock_cursor)
    captured = capsys.readouterr()
    assert "Database Statistics:" in captured.out

def test_error_handling(db_manager):
    """Test error handling"""
    with pytest.raises(Exception):
        db_manager.execute_query("INVALID SQL", [])

@pytest.mark.parametrize("table,conditions", [
    ("market_data", {"symbol": "AAPL"}),
    ("news_data", {"source": "Reuters"}),
])
def test_historical_data(db_manager, table, conditions):
    """Test historical data retrieval"""
    with patch.object(db_manager, 'execute_query') as mock_execute:
        mock_execute.return_value = [{"data": "test"}]
        result = db_manager.get_historical_data(table, conditions)
        assert result == [{"data": "test"}]

def test_schema_version(db_manager, mock_cursor):
    """Test schema version checking"""
    version = db_manager.get_current_schema_version(mock_cursor)
    assert version == "1.0.0"