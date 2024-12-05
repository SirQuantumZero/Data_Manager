# src/database/migrations.py

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
from .core import Core

def rollback_migration(self, version: str, down_sql: str) -> bool:
        """Rollback single migration"""
        try:
            with self.core.transaction() as conn:
                cursor = conn.cursor()
                statements = [s.strip() for s in down_sql.split(';') if s.strip()]
                for statement in statements:
                    cursor.execute(statement)
                cursor.execute("DELETE FROM schema_migrations WHERE version = %s", (version,))
            self.logger.info(f"Rolled back migration {version}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to rollback migration {version}: {e}")
            return False

def rollback_all(self, migrations_dir: Path) -> bool:
        """Rollback all migrations in reverse order"""
        applied = self.get_applied_migrations()
        success = True
        
        for version in reversed(applied):
            down_file = migrations_dir / f"{version}.down.sql"
            if not down_file.exists():
                self.logger.error(f"Missing rollback file for {version}")
                success = False
                break
                
            with open(down_file) as f:
                down_sql = f.read()
                
            if not self.rollback_migration(version, down_sql):
                success = False
                break
                
        return success

def get_pending_migrations(self, migrations_dir: Path) -> List[str]:
        """Get list of migrations that haven't been applied"""
        applied = set(self.get_applied_migrations())
        all_versions = {f.stem for f in migrations_dir.glob('*.sql') 
                       if not f.stem.endswith('.down')}
        return sorted(list(all_versions - applied))



class Monitoring:
    def __init__(self, core: Core):
        self.core = core
        self.logger = logging.getLogger(__name__)
        self._ensure_monitoring_tables()
        self.metrics = {}

    def _ensure_monitoring_tables(self) -> None:
        """Create monitoring tables if they don't exist"""
        self.core.execute("""
            CREATE TABLE IF NOT EXISTS query_metrics (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                query_hash VARCHAR(64) NOT NULL,
                query_text TEXT NOT NULL,
                execution_time FLOAT NOT NULL,
                rows_affected INT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_query_hash (query_hash),
                INDEX idx_timestamp (timestamp)
            )
        """)

        self.core.execute("""
            CREATE TABLE IF NOT EXISTS system_metrics (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                metric_name VARCHAR(50) NOT NULL,
                metric_value FLOAT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                metadata JSON,
                INDEX idx_metric_name (metric_name),
                INDEX idx_timestamp (timestamp)
            )
        """)

    def record_query(self, query: str, execution_time: float, rows_affected: int) -> None:
        """Record query execution metrics"""
        query_hash = hash(query)
        self.core.execute(
            """
            INSERT INTO query_metrics 
                (query_hash, query_text, execution_time, rows_affected)
            VALUES (%s, %s, %s, %s)
            """,
            (query_hash, query, execution_time, rows_affected)
        )

    def record_metric(self, name: str, value: float, metadata: Optional[Dict] = None) -> None:
        """Record system metric with optional metadata"""
        self.core.execute(
            """
            INSERT INTO system_metrics (metric_name, metric_value, metadata)
            VALUES (%s, %s, %s)
            """,
            (name, value, metadata)
        )

    def get_slow_queries(self, 
                        threshold: float = 1.0, 
                        limit: int = 10) -> List[Dict]:
        """Get slow queries exceeding threshold (seconds)"""
        return self.core.execute(
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
            (threshold, limit)
        )

    def get_query_stats(self, 
                       start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None) -> Dict:
        """Get aggregated query statistics for time period"""
        where_clause = ""
        params = []
        
        if start_time:
            where_clause += " AND timestamp >= %s"
            params.append(start_time)
        if end_time:
            where_clause += " AND timestamp <= %s"
            params.append(end_time)

        return self.core.execute(
            f"""
            SELECT 
                COUNT(*) as total_queries,
                AVG(execution_time) as avg_time,
                MAX(execution_time) as max_time,
                MIN(execution_time) as min_time,
                SUM(rows_affected) as total_rows
            FROM query_metrics
            WHERE 1=1 {where_clause}
            """
        )[0]

    def get_metric_history(self,
                          metric_name: str,
                          hours: int = 24) -> List[Dict]:
        """Get historical values for a metric"""
        return self.core.execute(
            """
            SELECT metric_value, metadata, timestamp
            FROM system_metrics
            WHERE metric_name = %s
              AND timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            ORDER BY timestamp DESC
            """,
            (metric_name, hours)
        )

    def cleanup_old_metrics(self, days: int = 30) -> int:
        """Delete metrics older than specified days"""
        with self.core.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM query_metrics
                WHERE timestamp < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (days,)
            )
            query_count = cursor.rowcount
            
            cursor.execute(
                """
                DELETE FROM system_metrics
                WHERE timestamp < DATE_SUB(NOW(), INTERVAL %s DAY)
                """,
                (days,)
            )
            system_count = cursor.rowcount
            
        return query_count + system_count

    def check_system_health(self) -> Dict[str, bool]:
        """Check overall system health metrics"""
        checks = {
            'connection': False,
            'slow_queries': False,
            'disk_space': False
        }
        
        try:
            # Check database connection
            checks['connection'] = self.core.ping()
            
            # Check for excessive slow queries
            slow_count = self.core.execute("""
                SELECT COUNT(*) as count 
                FROM query_metrics 
                WHERE execution_time > 1.0
                AND timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            """)[0]['count']
            checks['slow_queries'] = slow_count < 10
            
            # Check available disk space
            space_info = self.core.execute("SHOW TABLE STATUS")[0]
            total_size = sum(
                row.get('Data_length', 0) + row.get('Index_length', 0) 
                for row in space_info
            )
            checks['disk_space'] = total_size < (1024 * 1024 * 1024 * 100)  # 100GB
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            
        return checks

    def get_performance_summary(self) -> Dict:
        """Get overall performance metrics summary"""
        return {
            'system_health': self.check_system_health(),
            'query_stats': self.get_query_stats(
                start_time=datetime.now() - timedelta(hours=24)
            ),
            'slow_queries': self.get_slow_queries(limit=5)
        }

    def set_alert_threshold(self, metric_name: str, threshold: float, 
                          comparison: str = '>') -> None:
        """Configure alert threshold for a metric"""
        self.core.execute("""
            CREATE TABLE IF NOT EXISTS alert_thresholds (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                metric_name VARCHAR(50) NOT NULL,
                threshold FLOAT NOT NULL,
                comparison VARCHAR(2) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_metric (metric_name)
            )
        """)
        
        self.core.execute("""
            INSERT INTO alert_thresholds (metric_name, threshold, comparison)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                threshold = VALUES(threshold),
                comparison = VALUES(comparison)
        """, (metric_name, threshold, comparison))

    def check_alerts(self) -> List[Dict]:
        """Check all metrics against their thresholds"""
        alerts = []
        thresholds = self.core.execute("SELECT * FROM alert_thresholds")
        
        for threshold in thresholds:
            latest = self.core.execute("""
                SELECT metric_value, timestamp 
                FROM system_metrics
                WHERE metric_name = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (threshold['metric_name'],))
            
            if not latest:
                continue
                
            value = latest[0]['metric_value']
            if self._evaluate_threshold(value, threshold['threshold'], 
                                     threshold['comparison']):
                alerts.append({
                    'metric_name': threshold['metric_name'],
                    'threshold': threshold['threshold'],
                    'current_value': value,
                    'timestamp': latest[0]['timestamp']
                })
                
        return alerts

    def _evaluate_threshold(self, value: float, threshold: float, 
                          comparison: str) -> bool:
        """Evaluate if value triggers threshold"""
        if comparison == '>':
            return value > threshold
        elif comparison == '<':
            return value < threshold
        elif comparison == '>=':
            return value >= threshold
        elif comparison == '<=':
            return value <= threshold
        return False

    def _ensure_alert_tables(self) -> None:
        """Create alert history and notification tables"""
        self.core.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                metric_name VARCHAR(50) NOT NULL,
                threshold FLOAT NOT NULL,
                value FLOAT NOT NULL,
                status ENUM('TRIGGERED', 'RESOLVED') NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                resolved_at DATETIME,
                notification_sent BOOLEAN DEFAULT FALSE,
                INDEX idx_metric (metric_name),
                INDEX idx_status (status)
            )
        """)

    def record_alert(self, alert: Dict) -> None:
        """Record new alert in history"""
        self.core.execute("""
            INSERT INTO alert_history 
                (metric_name, threshold, value, status)
            VALUES (%s, %s, %s, 'TRIGGERED')
        """, (
            alert['metric_name'],
            alert['threshold'],
            alert['current_value']
        ))

    def resolve_alert(self, alert_id: int) -> None:
        """Mark alert as resolved"""
        self.core.execute("""
            UPDATE alert_history 
            SET status = 'RESOLVED',
                resolved_at = NOW()
            WHERE id = %s
        """, (alert_id,))

    def get_active_alerts(self) -> List[Dict]:
        """Get all unresolved alerts"""
        return self.core.execute("""
            SELECT * FROM alert_history
            WHERE status = 'TRIGGERED'
            ORDER BY created_at DESC
        """)

    def _ensure_notification_tables(self) -> None:
        """Create notification configuration tables"""
        self.core.execute("""
            CREATE TABLE IF NOT EXISTS notification_settings (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                metric_name VARCHAR(50) NOT NULL,
                notification_type ENUM('EMAIL', 'SLACK', 'WEBHOOK') NOT NULL,
                config JSON NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_metric_type (metric_name, notification_type)
            )
        """)

        self.core.execute("""
            CREATE TABLE IF NOT EXISTS notification_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                alert_id BIGINT NOT NULL,
                notification_type VARCHAR(20) NOT NULL,
                status ENUM('SENT', 'FAILED') NOT NULL,
                error_message TEXT,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (alert_id) REFERENCES alert_history(id)
            )
        """)

    def set_notification(self, metric_name: str, notification_type: str, 
                        config: Dict) -> None:
        """Configure notification settings for metric"""
        self.core.execute("""
            INSERT INTO notification_settings 
                (metric_name, notification_type, config)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                config = VALUES(config),
                is_active = TRUE
        """, (metric_name, notification_type, config))

    def process_notifications(self) -> None:
        """Process pending notifications for active alerts"""
        alerts = self.get_active_alerts()
        for alert in alerts:
            if not alert['notification_sent']:
                settings = self.core.execute("""
                    SELECT * FROM notification_settings
                    WHERE metric_name = %s AND is_active = TRUE
                """, (alert['metric_name'],))
                
                for setting in settings:
                    try:
                        self._send_notification(alert, setting)
                        status = 'SENT'
                        error = None
                    except Exception as e:
                        status = 'FAILED'
                        error = str(e)
                    
                    self.core.execute("""
                        INSERT INTO notification_history 
                            (alert_id, notification_type, status, error_message)
                        VALUES (%s, %s, %s, %s)
                    """, (alert['id'], setting['notification_type'], 
                         status, error))
                
                self.core.execute("""
                    UPDATE alert_history
                    SET notification_sent = TRUE
                    WHERE id = %s
                """, (alert['id'],))

    def _send_notification(self, alert: Dict, setting: Dict) -> None:
        """Send notification based on type and config"""
        notification_type = setting['notification_type']
        config = setting['config']
        
        message = self._format_alert_message(alert)
        
        if notification_type == 'EMAIL':
            self._send_email_notification(message, config)
        elif notification_type == 'SLACK':
            self._send_slack_notification(message, config)
        elif notification_type == 'WEBHOOK':
            self._send_webhook_notification(message, config)

    def _format_alert_message(self, alert: Dict) -> str:
        """Format alert message for notifications"""
        return f"""
Alert: {alert['metric_name']}
Value: {alert['current_value']}
Threshold: {alert['threshold']}
Triggered at: {alert['timestamp']}
"""

    def _send_email_notification(self, message: str, config: Dict) -> None:
        """Send email notification"""
        # Implement email sending logic here
        pass

    def _send_slack_notification(self, message: str, config: Dict) -> None:
        """Send Slack notification"""
        # Implement Slack notification logic here
        pass

    def _send_webhook_notification(self, message: str, config: Dict) -> None:
        """Send webhook notification"""
        # Implement webhook notification logic here
        pass

    def aggregate_metrics(self, 
                        metric_name: str,
                        interval: str = '1 HOUR',
                        func: str = 'AVG') -> List[Dict]:
        """Aggregate metrics by time interval"""
        return self.core.execute(f"""
            SELECT 
                DATE_FORMAT(timestamp, 
                    CASE 
                        WHEN %s = '1 HOUR' THEN '%%Y-%%m-%%d %%H:00:00'
                        WHEN %s = '1 DAY' THEN '%%Y-%%m-%%d'
                        WHEN %s = '1 MONTH' THEN '%%Y-%%m-01'
                        ELSE '%%Y-%%m-%%d %%H:%%i:00'
                    END
                ) as period,
                {func}(metric_value) as value,
                COUNT(*) as count
            FROM system_metrics
            WHERE metric_name = %s
            GROUP BY period
            ORDER BY period DESC
        """, (interval, interval, interval, metric_name))

    def cleanup(self) -> None:
        """Cleanup old data from all monitoring tables"""
        retention_days = {
            'query_metrics': 30,
            'system_metrics': 90,
            'alert_history': 180,
            'notification_history': 180
        }
        
        for table, days in retention_days.items():
            self.core.execute(f"""
                DELETE FROM {table}
                WHERE timestamp < DATE_SUB(NOW(), INTERVAL %s DAY)
            """, (days,))

    def get_monitoring_stats(self) -> Dict:
        """Get overall monitoring system statistics"""
        return {
            'metrics_count': self.core.execute(
                "SELECT COUNT(*) as count FROM system_metrics"
            )[0]['count'],
            'alerts_count': self.core.execute(
                "SELECT COUNT(*) as count FROM alert_history"
            )[0]['count'],
            'active_alerts': self.core.execute(
                "SELECT COUNT(*) as count FROM alert_history WHERE status = 'TRIGGERED'"
            )[0]['count'],
            'notification_success_rate': self.core.execute("""
                SELECT 
                    ROUND(
                        SUM(CASE WHEN status = 'SENT' THEN 1 ELSE 0 END) * 100.0 / 
                        COUNT(*), 2
                    ) as success_rate
                FROM notification_history
            """)[0]['success_rate']
        }