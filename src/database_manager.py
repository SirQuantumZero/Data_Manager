# FILE: utils/database_manager.py
"""Database Schema Management Tool

Manages the database schema for QuantumZero trading platform.

Usage:
    # To apply schema preserving existing data:
    python utils/database_manager.py

    # To format database (will prompt for confirmation):
    python utils/database_manager.py --format

    # To format database without confirmation:
    python utils/database_manager.py --format --force

    # To backup database before formatting:
    python utils/database_manager.py --format --backup

    # To format and populate with test data:
    python utils/database_manager.py --format --populate

    # Full reset with backup and test data:
    python utils/database_manager.py --format --backup --populate

Arguments:
    --format     Drop all tables and recreate schema
    --force      Skip confirmation prompt when formatting
    --backup     Create backup before formatting
    --populate   Add test data after formatting

Environment Variables Required:
    DB_HOST      Database host
    DB_USER      Database username
    DB_PASSWORD  Database password
    DB_DATABASE  Database name
    DB_PORT      Database port (default: 3306)

Schema Version: 1.0.0
Last Updated: 2024-03-12

Recent Changes:
    - Added schema version tracking
    - Added backup rotation
    - Added test data population
    - Added performance metrics
    - Added logging improvements

Example Usage:
    from database_manager import DatabaseManager
    
    # Initialize manager
    db = DatabaseManager()
    
    # Backup and format
    db.backup_database()
    db.format_database(populate=True)
    
    # Get statistics
    db.print_statistics()
"""

import os
import sys
import logging
import mysql.connector
from dotenv import load_dotenv
from colorama import init, Fore, Style
from tqdm import tqdm
import random
import hashlib
import json
from datetime import datetime, timedelta
import subprocess
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable
from pathlib import Path
import argparse
import time
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Default to INFO level; can be configured externally
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class DBConfig:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306


@dataclass
class TradeData:
    user_id: int
    trade_date: datetime
    symbol: str
    quantity: int
    price: float
    trade_type: str
    asset_type: str
    order_type: str


class DatabaseError(Exception):
    """Base exception for database errors"""
    pass

class ConnectionError(DatabaseError):
    """Connection-related errors"""
    pass

class QueryError(DatabaseError):
    """Query execution errors"""
    pass

def retry_operation(max_attempts: int = 3, delay: float = 1.0):
    """Decorator for retrying database operations"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_error = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except mysql.connector.Error as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        time.sleep(delay * (attempt + 1))  # Exponential backoff
                        logger.warning(f"Retry attempt {attempt + 1} for {func.__name__}")
                    continue
            raise ConnectionError(f"Operation failed after {max_attempts} attempts: {last_error}")
        return wrapper
    return decorator

class DatabaseManager:
    SCHEMA_VERSION = "1.0.0"

    def __init__(self):
        """Initialize DatabaseManager with config and logging"""
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.db_config = self._load_config()
        self.schema_config = SchemaConfig()
        self.connection = None
        self.cursor = None

    def _load_config(self) -> Dict[str, Any]:
        """Load database configuration from environment"""
        config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'trading_user'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_DATABASE', 'trading_data'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        self.logger.info(f"Loaded config for database: {config['database']}")
        return config

    def get_connection(self):
        """Get a new database connection"""
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as err:
            self.logger.error(f"Failed to get connection: {err}")
            raise

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            self.cursor = self.connection.cursor(dictionary=True)
            self.logger.info("Database connection established")
        except mysql.connector.Error as err:
            self.logger.error(f"Connection failed: {err}")
            raise

    def disconnect(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

    def __enter__(self):
        """Context manager enter"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        if exc_type:
            self.logger.error(f"Error during database operation: {exc_val}")
            return False
        return True

    def reconnect(self, max_retries: int = 3, delay: float = 1.0) -> None:
        """Attempt to reconnect with retries"""
        for attempt in range(max_retries):
            try:
                self.disconnect()
                self.connect()
                return
            except mysql.connector.Error as err:
                self.logger.warning(f"Reconnect attempt {attempt + 1} failed: {err}")
                time.sleep(delay)
        raise mysql.connector.Error("Max reconnection attempts reached")

    def backup_database(self) -> bool:
        """Create a database backup with progress indication."""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = Path("db/backups")
            backup_dir.mkdir(parents=True, exist_ok=True)
            backup_file = backup_dir / f"backup_{timestamp}.sql"

            logger.info("Creating database backup...")
            with tqdm(total=100, desc="Backup Progress") as pbar:
                with open(backup_file, 'w') as outfile:
                    subprocess.run([
                        'mysqldump',
                        f"--host={self.db_config['host']}",
                        f"--user={self.db_config['user']}",
                        f"--password={self.db_config['password']}",
                        self.db_config['database']
                    ], stdout=outfile, stderr=subprocess.PIPE, check=True)
                pbar.update(100)

            # Verify backup
            if not self.verify_backup(backup_file):
                raise Exception("Backup verification failed")

            # Rotate old backups
            self._rotate_backups(backup_dir)

            size_mb = backup_file.stat().st_size / (1024 * 1024)
            logger.info(f"Backup created: {backup_file} ({size_mb:.2f}MB)")
            return True

        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False

    def _rotate_backups(self, backup_dir: Path, keep: int = 5) -> None:
        """Remove old backups keeping only the most recent ones"""
        backups = sorted(backup_dir.glob("backup_*.sql"))
        if len(backups) > keep:
            for backup in backups[:-keep]:
                backup.unlink()
                logger.info(f"Removed old backup: {backup}")

    def verify_backup(self, backup_file: Path) -> bool:
        """Verify backup file integrity"""
        try:
            with open(backup_file) as f:
                first_line = f.readline()
                if not first_line.startswith("-- MySQL dump"):
                    return False
            return True
        except Exception:
            return False

    def format_database(self, force: bool = False, populate: bool = False) -> bool:
        """Format the database by dropping all tables and applying the schema."""
        if not force and not self.confirm_format():
            return False

        try:
            with mysql.connector.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    self.drop_all_tables(cursor)
                    success = self.apply_schema(cursor)
                    if success and populate:
                        self.populate_test_data(cursor)
                    conn.commit()
                    return success
        except mysql.connector.Error as e:
            logger.error(f"Database error: {e}")
            return False

    def apply_schema(self, cursor) -> bool:
        """Apply the database schema from the schema file."""
        try:
            with open(self.schema_file, 'r') as f:
                schema_sql = f.read()
            for statement in schema_sql.split(';'):
                if statement.strip():
                    try:
                        cursor.execute(statement)
                        logger.debug(f"Executed: {statement.strip()[:100]}...")
                    except mysql.connector.Error as e:
                        if e.errno == 1050:  # Table already exists
                            logger.warning(f"Table already exists: {e}")
                        else:
                            raise
            logger.info("Schema applied successfully")
            return True
        except Exception as e:
            logger.error(f"Error applying schema: {e}")
            return False

    def get_current_schema_version(self, cursor) -> str:
        """Get current schema version from database"""
        try:
            cursor.execute("SELECT version FROM schema_versions ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else "0.0.0"
        except:
            return "0.0.0"

    def confirm_format(self) -> bool:
        """Ask the user to confirm formatting the database."""
        confirm = input("This will delete all data. Continue? (y/N): ")
        if confirm.lower() != 'y':
            logger.info("Operation cancelled")
            return False
        return True

    def drop_all_tables(self, cursor) -> None:
        """Drop all tables in the database."""
        cursor.execute("SHOW TABLES")
        for table in cursor.fetchall():
            cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
            logger.info(f"Dropped table {table[0]}")

    def populate_test_data(self, cursor) -> None:
        """Populate the database with test data."""
        self.user_ids = []
        self.generate_users(cursor)
        self.generate_market_data(cursor)
        self.generate_strategies(cursor)
        self.generate_trades(cursor)
        self.generate_technical_analysis(cursor)
        self.generate_news_data(cursor)
        self.generate_system_logs(cursor)
        logger.info("Test data population complete")

    def generate_users(self, cursor) -> None:
        """Generate test users."""
        logger.info("Generating test users...")
        for i in range(10):
            cursor.execute("""
                INSERT INTO Users (username, email, password_hash)
                VALUES (%s, %s, %s)
            """, (
                f'test_user_{i}',
                f'test_user_{i}@quantumzero.com',
                hashlib.sha256(f'password{i}'.encode()).hexdigest()
            ))
            self.user_ids.append(cursor.lastrowid)
        logger.info(f"Created {len(self.user_ids)} test users")

    def generate_market_data(self, cursor) -> None:
        """Generate market data for symbols."""
        logger.info("Generating market data...")
        for symbol in self.symbols:
            base_price = random.uniform(10, 1000)
            for day in range(30):
                timestamp = datetime.now() - timedelta(days=day)
                price = base_price * (1 + random.uniform(-0.02, 0.02))
                cursor.execute("""
                    INSERT INTO MarketData (
                        symbol, timestamp, data_type, source,
                        open, high, low, close, volume, vwap, transactions
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    symbol,
                    int(timestamp.timestamp()),
                    'STOCK',
                    'POLYGON',
                    price,
                    price * 1.01,
                    price * 0.99,
                    price * (1 + random.uniform(-0.01, 0.01)),
                    random.randint(100000, 1000000),
                    price,
                    random.randint(1000, 5000)
                ))
        logger.info(f"Created market data for {len(self.symbols)} symbols")

    def generate_strategies(self, cursor) -> None:
        """Generate trading strategies for users."""
        logger.info("Generating strategies...")
        for user_id in self.user_ids:
            cursor.execute("""
                INSERT INTO Strategies (
                    user_id, strategy_name, parameters, is_active
                ) VALUES (%s, %s, %s, %s)
            """, (
                user_id,
                f'Strategy_{random.randint(1,100)}',
                json.dumps({
                    'type': random.choice(['MA', 'RSI', 'MACD']),
                    'params': {'period': random.choice([14, 20, 50])}
                }),
                True
            ))
        logger.info("Created test strategies")

    def generate_trades(self, cursor) -> None:
        """Generate sample trade data."""
        logger.info("Generating trades...")
        trades = [
            TradeData(
                user_id=random.choice(self.user_ids),
                trade_date=datetime.now() - timedelta(days=random.randint(1, 30)),
                symbol=random.choice(self.symbols),
                quantity=random.randint(10, 1000),
                price=round(random.uniform(100, 500), 2),
                trade_type=random.choice(['BUY', 'SELL']),
                asset_type='STOCK',
                order_type='MARKET',
            )
            for _ in range(100)
        ]
        for trade in trades:
            cursor.execute("""
                INSERT INTO Trades (
                    user_id, trade_date, symbol, quantity, price,
                    trade_type, asset_type, order_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                trade.user_id, trade.trade_date, trade.symbol, trade.quantity,
                trade.price, trade.trade_type, trade.asset_type, trade.order_type
            ))
        logger.info(f"Created {len(trades)} test trades")

    def generate_technical_analysis(self, cursor) -> None:
        """Generate technical analysis data."""
        logger.info("Generating technical analysis data...")
        for _ in range(100):
            cursor.execute("""
                INSERT INTO TechnicalAnalysis (
                    symbol, timestamp, analysis_type, value, parameters
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                random.choice(self.symbols),
                int((datetime.now() - timedelta(days=random.randint(1, 30))).timestamp()),
                random.choice(['INDICATOR', 'CORRELATION']),
                round(random.uniform(-1, 1), 4),
                json.dumps({
                    'indicator': random.choice(['RSI', 'MACD']),
                    'period': random.choice([14, 20])
                })
            ))
        logger.info("Created technical analysis records")

    def generate_news_data(self, cursor) -> None:
        """Generate news data."""
        logger.info("Generating news data...")
        for _ in range(50):
            cursor.execute("""
                INSERT INTO NewsData (
                    symbol, timestamp, headline, source, sentiment, url
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                random.choice(self.symbols),
                int((datetime.now() - timedelta(days=random.randint(1, 30))).timestamp()),
                f"Market Update {random.randint(1,100)}",
                random.choice(['Reuters', 'Bloomberg', 'CNBC']),
                random.uniform(-1, 1),
                f"https://example.com/news/{random.randint(1000,9999)}"
            ))
        logger.info("Created news records")

    def generate_system_logs(self, cursor) -> None:
        """Generate system log entries."""
        logger.info("Generating system logs...")
        for _ in range(100):
            cursor.execute("""
                INSERT INTO SystemLogs (
                    timestamp, level, component, message, details
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                int((datetime.now() - timedelta(minutes=random.randint(1, 1440))).timestamp()),
                random.choice(['INFO', 'WARNING', 'ERROR']),
                random.choice(['TRADE_ENGINE', 'DATA_COLLECTOR', 'RISK_MANAGER']),
                f"System event {random.randint(1000,9999)}",
                json.dumps({'event_id': random.randint(1, 1000)})
            ))
        logger.info("Created system log entries")

    def print_statistics(self, cursor) -> None:
        """Print database statistics."""
        init()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        table_groups = {
            'Core': ['Users', 'Trades', 'Strategies', 'SystemLogs'],
            'Market Data': ['MarketData', 'FundamentalData'],
            'Analysis': ['TechnicalAnalysis', 'NewsData'],
            'AI/ML': ['AI_Models']
        }

        colors = {
            'Core': Fore.GREEN,
            'Market Data': Fore.BLUE,
            'Analysis': Fore.YELLOW,
            'AI/ML': Fore.MAGENTA
        }

        total_size = 0
        total_rows = 0

        print(f"\n{Style.BRIGHT}Database Statistics:{Style.RESET_ALL}")
        print("=" * 80)
        print(f"{'Table Name':<30} {'Rows':>10} {'Size (MB)':>15} {'Group':>20}")
        print("=" * 80)

        for group, group_tables in table_groups.items():
            group_size = 0
            group_rows = 0

            for table_name in group_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]

                    cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
                    status = cursor.fetchone()

                    if status:
                        size_mb = (status[6] + status[8]) / (1024 * 1024)
                        print(
                            f"{colors[group]}{table_name:<30} {row_count:>10} "
                            f"{size_mb:>14.2f} {group:>20}{Style.RESET_ALL}"
                        )

                        total_size += size_mb
                        total_rows += row_count
                        group_size += size_mb
                        group_rows += row_count

                except Exception as e:
                    logger.warning(f"Could not get stats for table {table_name}: {e}")
                    continue

            if group_size > 0:
                print("-" * 80)
                print(f"{colors[group]}Group Total:{' ':22} {group_rows:>10} {group_size:>14.2f}{Style.RESET_ALL}")
                print("-" * 80)

        print("=" * 80)
        print(f"{Style.BRIGHT}Summary:{Style.RESET_ALL}")
        print(f"Total Tables: {len(tables)}")
        print(f"Total Rows: {total_rows:,}")
        print(f"Total Size: {total_size:.2f} MB")
        print("=" * 80)

    def get_historical_data(self, table: str, conditions: Dict[str, Any]) -> Optional[Any]:
        """Get historical data from the database."""
        conditions_sql = ' AND '.join([f'{k}=%s' for k in conditions.keys()])
        sql = f"""
            SELECT * FROM {table}
            WHERE {conditions_sql}
            ORDER BY timestamp DESC
        """
        return self.execute_query(sql, list(conditions.values()))

    @retry_operation()
    def execute_with_retry(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute query with retry mechanism"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(query, params or ())
                    if query.lower().startswith('select'):
                        return cursor.fetchall()
                    conn.commit()
                    return []
        except mysql.connector.Error as err:
            self.logger.error(f"Query failed: {err}")
            raise QueryError(str(err))

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute a query with parameters"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(query, params or ())
                    if query.lower().startswith('select'):
                        return cursor.fetchall()
                    conn.commit()
                    return []
        except mysql.connector.Error as err:
            self.logger.error(f"Query failed: {err}")
            raise

    def execute_many(self, query: str, params: List[tuple]) -> None:
        """Execute same query with multiple parameter sets"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, params)
                    conn.commit()
        except mysql.connector.Error as err:
            self.logger.error(f"Batch query failed: {err}")
            raise

    def get_schema_version(self) -> str:
        """Get current schema version"""
        try:
            result = self.execute_query(
                "SELECT version FROM schema_versions ORDER BY id DESC LIMIT 1"
            )
            return result[0]['version'] if result else "0.0.0"
        except mysql.connector.Error:
            return "0.0.0"

    def process_query_results(self, results: List[Dict[str, Any]]) -> Any:
        """Process query results into a desired format."""
        # Process the results as needed
        return results

    def verify_schema(self) -> bool:
        """Verify database schema matches expected structure"""
        try:
            with mysql.connector.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    # Check schema version
                    version = self.get_current_schema_version(cursor)
                    if version != self.SCHEMA_VERSION:
                        logger.error(f"Schema version mismatch. Expected {self.SCHEMA_VERSION}, found {version}")
                        return False
                        
                    # Verify required tables
                    cursor.execute("SHOW TABLES")
                    tables = {t[0] for t in cursor.fetchall()}
                    required_tables = {'market_data', 'system_logs', 'schema_versions'}
                    
                    missing = required_tables - tables
                    if missing:
                        logger.error(f"Missing required tables: {missing}")
                        return False
                        
                    return True
                    
        except mysql.connector.Error as e:
            logger.error(f"Schema verification failed: {e}")
            return False


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Database Schema Management Tool",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        '--format',
        action='store_true',
        help='Drop all tables and recreate schema'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Skip confirmation prompt when formatting'
    )
    
    parser.add_argument(
        '--backup',
        action='store_true',
        help='Create backup before formatting'
    )
    
    parser.add_argument(
        '--populate',
        action='store_true',
        help='Add test data after formatting'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version=f'Schema Version: {DatabaseManager.SCHEMA_VERSION}'
    )
    
    return parser.parse_args()

def main():
    """Main entry point for CLI"""
    args = parse_args()
    manager = DatabaseManager()
    success = True
    
    try:
        # Create backup if requested
        if args.backup:
            if not manager.backup_database():
                return 1
        
        # Format database if requested
        if args.format:
            if not manager.format_database(
                force=args.force,
                populate=args.populate
            ):
                return 1
        
        # Apply any pending migrations
        if not args.format:
            if not manager.apply_migrations():
                return 1
                
        return 0
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())

import mysql.connector
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import json
import sys
from dotenv import load_dotenv

@dataclass
class SchemaConfig:
    version: str = "1.0.0"
    migrations_dir: str = "db/migrations"
    backup_dir: str = "db/backups"

class DatabaseManager:
    def __init__(self):
        """Initialize DatabaseManager with config and logging"""
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.db_config = self._load_config()
        self.schema_config = SchemaConfig()
        self.connection = None
        self.cursor = None

    def _load_config(self) -> Dict[str, Any]:
        """Load database configuration from environment"""
        config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'trading_user'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_DATABASE', 'trading_data'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        self.logger.info(f"Loaded config for database: {config['database']}")
        return config

    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            self.cursor = self.connection.cursor(dictionary=True)
            self.logger.info("Database connection established")
        except mysql.connector.Error as err:
            self.logger.error(f"Connection failed: {err}")
            raise

    def disconnect(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

    def __enter__(self):
        """Context manager enter"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        if exc_type:
            self.logger.error(f"Error during database operation: {exc_val}")
            return False
        return True

    def reconnect(self, max_retries: int = 3, delay: float = 1.0) -> None:
        """Attempt to reconnect with retries"""
        for attempt in range(max_retries):
            try:
                self.disconnect()
                self.connect()
                return
            except mysql.connector.Error as err:
                self.logger.warning(f"Reconnect attempt {attempt + 1} failed: {err}")
                time.sleep(delay)
        raise mysql.connector.Error("Max reconnection attempts reached")

    def initialize_schema(self) -> bool:
        """Initialize database schema"""
        try:
            with mysql.connector.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    # Create version tracking table
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS schema_versions (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            version VARCHAR(20),
                            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Apply base schema
                    schema_file = Path(self.schema_config.migrations_dir) / "schema.sql"
                    if schema_file.exists():
                        with open(schema_file) as f:
                            schema_sql = f.read()
                            for statement in schema_sql.split(';'):
                                if statement.strip():
                                    cursor.execute(statement)
                    
                    # Record schema version
                    cursor.execute("""
                        INSERT INTO schema_versions (version) 
                        VALUES (%s)
                    """, (self.schema_config.version,))
                    
                    conn.commit()
                    self.logger.info(f"Schema initialized to version {self.schema_config.version}")
                    return True
                    
        except mysql.connector.Error as err:
            self.logger.error(f"Schema initialization failed: {err}")
            return False
            
    def get_current_version(self) -> str:
        """Get current schema version"""
        try:
            with mysql.connector.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT version FROM schema_versions 
                        ORDER BY id DESC LIMIT 1
                    """)
                    result = cursor.fetchone()
                    return result[0] if result else "0.0.0"
        except mysql.connector.Error:
            return "0.0.0"
            
    def apply_migrations(self) -> bool:
        """Apply pending schema migrations"""
        current = self.get_current_version()
        migrations_dir = Path(self.schema_config.migrations_dir)
        
        try:
            with mysql.connector.connect(**self.db_config) as conn:
                with conn.cursor() as cursor:
                    for migration in sorted(migrations_dir.glob("V*__*.sql")):
                        version = migration.name.split('__')[0][1:].replace('_', '.')
                        if version > current:
                            self.logger.info(f"Applying migration {migration.name}")
                            with open(migration) as f:
                                cursor.execute(f.read())
                                cursor.execute(
                                    "INSERT INTO schema_versions (version) VALUES (%s)",
                                    (version,)
                                )
                    conn.commit()
                    return True
        except mysql.connector.Error as err:
            self.logger.error(f"Migration failed: {err}")
            return False

def main():
    manager = DatabaseManager()
    success = True

    if "--backup" in sys.argv:
        success = manager.backup_database()

    if success and "--format" in sys.argv:
        success = manager.initialize_schema()
    elif success:
        success = manager.apply_migrations()

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()