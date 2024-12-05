# tests/test_monitoring.py

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from src.database.monitoring import Monitoring

@pytest.fixture
def mock_core():
    """Create mock database core"""
    return Mock()

@pytest.fixture
def monitoring(mock_core):
    """Create monitoring instance with mock core"""
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
    expected = [
        {
            'query_text': 'SELECT * FROM users',
            'avg_time': 2.5,
            'executions': 10,
            'last_seen': datetime.now()
        }
    ]
    monitoring.core.execute.return_value = expected

    result = monitoring.get_slow_queries(threshold=1.0, limit=5)

    assert result == expected
    monitoring.core.execute.assert_called_once_with(
        """
            SELECT query_text, AVG(execution_time) as avg_time,
                   COUNT(*) as executions,
                   MAX(timestamp) as last_seen
            FROM query_metrics
            WHERE execution_time > %s
            GROUP BY query_hash, query_text
            ORDER BY avg_time DESC
            LIMIT %s
            """,
        (1.0, 5)
    )

def test_get_slow_queries_empty(monitoring):
    """Test retrieving slow queries when none exist"""
    monitoring.core.execute.return_value = []
    
    result = monitoring.get_slow_queries()
    
    assert result == []

def test_get_slow_queries_error(monitoring):
    """Test error handling for slow queries"""
    monitoring.core.execute.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        monitoring.get_slow_queries()

def test_get_query_stats(monitoring):
    """Test query statistics"""
    expected = {
        'total_queries': 100,
        'avg_time': 0.5,
        'max_time': 2.0,
        'min_time': 0.1,
        'total_rows': 1000
    }
    monitoring.core.execute.return_value = [expected]
    
    result = monitoring.get_query_stats(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2)
    )
    
    assert result == expected

def test_get_metric_history(monitoring):
    """Test metric history retrieval"""
    expected = [{'metric_value': 90.0, 'timestamp': datetime.now()}]
    monitoring.core.execute.return_value = expected
    
    result = monitoring.get_metric_history('cpu_usage', hours=24)
    assert result == expected

@pytest.mark.parametrize('comparison,value,threshold,expected', [
    ('>', 10, 5, True),
    ('>', 5, 10, False),
    ('<', 5, 10, True),
    ('<=', 10, 10, True),
    ('>=', 10, 10, True)
])
def test_evaluate_threshold(monitoring, comparison, value, threshold, expected):
    """Test threshold evaluation"""
    result = monitoring._evaluate_threshold(value, threshold, comparison)
    assert result == expected

def test_record_alert(monitoring):
    """Test alert recording"""
    alert = {
        'metric_name': 'cpu_usage',
        'threshold': 90,
        'current_value': 95,
        'timestamp': datetime.now()
    }
    
    monitoring.record_alert(alert)
    
    monitoring.core.execute.assert_called_once_with(
        """
            INSERT INTO alert_history 
                (metric_name, threshold, value, status)
            VALUES (%s, %s, %s, 'TRIGGERED')
        """,
        ('cpu_usage', 90, 95)
    )

def test_get_active_alerts(monitoring):
    """Test retrieving active alerts"""
    expected = [{
        'id': 1,
        'metric_name': 'cpu_usage',
        'threshold': 90,
        'value': 95,
        'status': 'TRIGGERED',
        'created_at': datetime.now()
    }]
    monitoring.core.execute.return_value = expected
    
    result = monitoring.get_active_alerts()
    
    assert result == expected
    assert "WHERE status = 'TRIGGERED'" in str(monitoring.core.execute.call_args)

def test_record_alert_error(monitoring):
    """Test error handling in alert recording"""
    monitoring.core.execute.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        monitoring.record_alert({
            'metric_name': 'cpu_usage',
            'threshold': 90,
            'current_value': 95
        })

def test_resolve_alert(monitoring):
    """Test alert resolution"""
    alert_id = 1
    monitoring.resolve_alert(alert_id)
    
    monitoring.core.execute.assert_called_once_with(
        """
            UPDATE alert_history 
            SET status = 'RESOLVED',
                resolved_at = NOW()
            WHERE id = %s
        """,
        (alert_id,)
    )

def test_cleanup_notifications(monitoring):
    """Test notification cleanup"""
    with patch('time.time', return_value=1000):
        monitoring.cleanup()
        assert monitoring.core.execute.call_count >= 4  # All retention periods

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

def test_record_metric(monitoring):
    """Test recording system metrics"""
    monitoring.record_metric("cpu_usage", 85.5, {"host": "server1"})
    
    monitoring.core.execute.assert_called_once_with(
        """
            INSERT INTO system_metrics (metric_name, metric_value, metadata)
            VALUES (%s, %s, %s)
        """,
        ("cpu_usage", 85.5, {"host": "server1"})
    )

@pytest.mark.parametrize("interval,expected_format", [
    ("1 HOUR", "%Y-%m-%d %H:00:00"),
    ("1 DAY", "%Y-%m-%d"),
    ("1 MONTH", "%Y-%m-01")
])
def test_aggregate_metrics(monitoring, interval, expected_format):
    """Test metric aggregation with different intervals"""
    monitoring.aggregate_metrics("cpu_usage", interval=interval)
    
    call_args = monitoring.core.execute.call_args[0]
    assert interval in call_args[0]
    assert "GROUP BY period" in call_args[0]

def test_check_system_health_failure(monitoring):
    """Test system health check with failures"""
    monitoring.core.ping.return_value = False
    monitoring.core.execute.side_effect = Exception("Connection failed")
    
    result = monitoring.check_system_health()
    assert result["connection"] is False
    assert result["slow_queries"] is False
    assert result["disk_space"] is False

@pytest.mark.parametrize("current,threshold,should_alert", [
    (95, 90, True),   # Above threshold
    (85, 90, False),  # Below threshold
    (90, 90, False)   # At threshold
])
def test_alert_threshold_check(monitoring, current, threshold, should_alert):
    """Test alert threshold checking"""
    monitoring.core.execute.return_value = [{"metric_value": current}]
    
    monitoring.set_alert_threshold("cpu_usage", threshold, ">")
    alerts = monitoring.check_alerts()
    
    assert bool(alerts) == should_alert

def test_notification_processing_error(monitoring):
    """Test notification processing with errors"""
    alert = {
        "id": 1,
        "metric_name": "cpu_usage",
        "notification_sent": False
    }
    monitoring.core.execute.return_value = [alert]
    
    with patch.object(monitoring, "_send_notification", side_effect=Exception("Send failed")):
        monitoring.process_notifications()
        assert "FAILED" in str(monitoring.core.execute.call_args)

def test_cleanup_with_retention(monitoring):
    """Test cleanup with retention periods"""
    monitoring.cleanup()
    
    calls = monitoring.core.execute.call_args_list
    assert len(calls) >= 4  # One call per table
    for call in calls:
        assert "DELETE FROM" in call[0][0]
        assert "DATE_SUB(NOW(), INTERVAL" in call[0][0]

if __name__ == '__main__':
    pytest.main([__file__, '-v'])