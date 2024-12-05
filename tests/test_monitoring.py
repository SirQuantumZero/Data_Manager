# tests/test_monitoring.py

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from src.database.monitoring import Monitoring

@pytest.fixture
def mock_core():
    return Mock()

@pytest.fixture
def monitoring(mock_core):
    return Monitoring(mock_core)

def test_record_query(monitoring):
    """Test recording query metrics"""
    monitoring.record_query("SELECT * FROM test", 1.5, 100)
    
    monitoring.core.execute.assert_called_once_with(
        """
            INSERT INTO query_metrics 
                (query_hash, query_text, execution_time, rows_affected)
            VALUES (%s, %s, %s, %s)
            """,
        (hash("SELECT * FROM test"), "SELECT * FROM test", 1.5, 100)
    )

def test_get_slow_queries(monitoring):
    """Test retrieving slow queries"""
    expected = [{'query_text': 'test', 'avg_time': 2.0}]
    monitoring.core.execute.return_value = expected
    
    result = monitoring.get_slow_queries(threshold=1.0, limit=10)
    
    assert result == expected
    monitoring.core.execute.assert_called_once()

def test_check_system_health(monitoring):
    """Test system health check"""
    monitoring.core.ping.return_value = True
    monitoring.core.execute.side_effect = [
        [{'count': 5}],  # slow queries
        [{}]  # table status
    ]
    
    result = monitoring.check_system_health()
    
    assert result['connection'] is True
    assert result['slow_queries'] is True
    assert result['disk_space'] is True

def test_alert_threshold(monitoring):
    """Test setting and checking alert thresholds"""
    monitoring.set_alert_threshold('cpu_usage', 90, '>')
    
    monitoring.core.execute.assert_called()
    assert 'metric_name' in monitoring.core.execute.call_args[0][1]

def test_process_notifications(monitoring):
    """Test notification processing"""
    alert = {
        'id': 1,
        'metric_name': 'cpu_usage',
        'notification_sent': False
    }
    setting = {
        'notification_type': 'EMAIL',
        'config': {'to': 'test@example.com'}
    }
    
    monitoring.core.execute.side_effect = [
        [alert],  # get_active_alerts
        [setting],  # get settings
        None,  # insert history
        None   # update alert
    ]
    
    with patch.object(monitoring, '_send_notification'):
        monitoring.process_notifications()
        monitoring._send_notification.assert_called_once()

def test_cleanup_old_metrics(monitoring):
    """Test metrics cleanup"""
    monitoring.core.execute.side_effect = [
        {'rowcount': 10},  # query metrics
        {'rowcount': 5}   # system metrics
    ]
    
    result = monitoring.cleanup_old_metrics(days=30)
    
    assert result == 15
    assert monitoring.core.execute.call_count == 2

def test_monitoring_stats(monitoring):
    """Test getting monitoring statistics"""
    monitoring.core.execute.side_effect = [
        [{'count': 100}],  # metrics count
        [{'count': 10}],   # alerts count
        [{'count': 2}],    # active alerts
        [{'success_rate': 95.5}]  # notification success
    ]
    
    stats = monitoring.get_monitoring_stats()
    
    assert stats['metrics_count'] == 100
    assert stats['alerts_count'] == 10
    assert stats['active_alerts'] == 2
    assert stats['notification_success_rate'] == 95.5