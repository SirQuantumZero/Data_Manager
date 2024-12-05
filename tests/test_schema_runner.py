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
from datetime import datetime, timedelta
import re
import contextlib
from typing import Optional, Generator
import time

# Load environment variables
load_dotenv()

# Root database configuration
root_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': 'root',
    'password': os.getenv('MYSQL_ROOT_PASSWORD'),
}

# Test categories
pytestmark = pytest.mark.schema

@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test database after each test"""
    yield
    try:
        with mysql.connector.connect(**root_config) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP DATABASE IF EXISTS test_trading_data")
    except mysql.connector.Error as e:
        logging.error(f"Cleanup failed: {e}")

@pytest.fixture(autouse=True)
def cleanup_sql_files():
    """Clean up temporary SQL files after tests"""
    yield
    temp_files = ['test.sql', 'invalid.sql']
    for file in temp_files:
        try:
            Path(file).unlink(missing_ok=True)
        except Exception as e:
            logging.warning(f"Failed to cleanup {file}: {e}")

@pytest.fixture
def schema_runner():
    """Create test runner with test database config"""
    runner = SchemaRunner()
    runner.config['database'] = 'test_trading_data'
    return runner

@pytest.fixture(scope="session")
def benchmark_logger():
    """Logger for performance benchmarks"""
    logger = logging.getLogger("benchmark")
    handler = logging.FileHandler('benchmark.log')
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levellevel)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

@pytest.fixture
def sample_data() -> Generator[dict, None, None]:
    """Generate sample data for testing"""
    data = {
        'users': [
            {'username': 'test_user', 'email': 'test@example.com'},
            {'username': 'test_user2', 'email': 'test2@example.com'}
        ],
        'market_data': [
            {'symbol': 'AAPL', 'price': 150.0, 'volume': 1000},
            {'symbol': 'GOOGL', 'price': 2500.0, 'volume': 500}
        ]
    }
    yield data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLDebugContext:
    def __init__(self, logger, operation: str):
        self.logger = logger
        self.operation = operation
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.debug(f"=== Starting {self.operation} ===")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.now() - self.start_time
        if exc_type:
            self.logger.error(f"Error in {self.operation}: {exc_val}")
            self.logger.debug(f"Exception type: {exc_type}")
            self.logger.debug(f"Traceback: {exc_tb}")
        self.logger.debug(f"=== Completed {self.operation} (took {duration.total_seconds():.3f}s) ===")
        return False

class SchemaRunner:
    def __init__(self, config_path: str = ".env"):
        self.load_config(config_path)
        self.setup_logging()
        self.debug_enabled = True
        self.debug_partitions = True  # New flag

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

    def setup_logging(self):
        """Configure logging for the schema runner"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # File handler for normal operation
        file_handler = logging.FileHandler('schema_runner.log')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Add test handler for capturing logs in tests
        self.test_handler = TestHandler()
        self.logger.addHandler(self.test_handler)

    def get_test_records(self):
        """Get test log records for verification"""
        return self.test_handler.records

    @contextlib.contextmanager
    def debug_operation(self, operation: str):
        """Context manager for logging the duration of an operation"""
        start_time = datetime.now()
        self.logger.debug(f"=== Starting {operation} ===")
        try:
            yield
        finally:
            duration = datetime.now() - start_time
            self.logger.debug(f"=== Completed {operation} (took {duration.total_seconds():.3f}s) ===")

    def create_test_database(self) -> bool:
        """Create test database and grant permissions using root credentials"""
        try:
            with mysql.connector.connect(**self.root_config) as connection:
                cursor = connection.cursor()
                
                # Set global variables for stored procedures
                cursor.execute("SET GLOBAL log_bin_trust_function_creators = 1")
                
                # Create database if not exists
                cursor.execute(f"DROP DATABASE IF EXISTS {self.config['database']}")
                cursor.execute(f"CREATE DATABASE {self.config['database']}")
                
                # Grant privileges separately
                cursor.execute(f"""
                    GRANT ALL PRIVILEGES 
                    ON {self.config['database']}.* 
                    TO '{self.config['user']}'@'localhost'
                """)
                
                # Grant SUPER separately
                cursor.execute(f"""
                    GRANT SUPER 
                    ON *.* 
                    TO '{self.config['user']}'@'localhost'
                """)
                
                cursor.execute("FLUSH PRIVILEGES")
                
                return True
        except Exception as e:
            self.logger.error(f"Database setup failed: {str(e)}")
            return False

    def execute_schema_file(self, schema_path: str, skip_system_config: bool = True) -> bool:
        """Execute SQL schema file with proper delimiter handling and debug logging"""
        with self.debug_operation("Schema Execution"):
            try:
                # Check if file exists first
                if not Path(schema_path).exists():
                    self.logger.error(f"Schema file not found: {schema_path}")
                    return False

                if not self.create_test_database():
                    return False

                with mysql.connector.connect(**self.config) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"USE {self.config['database']}")
                        self.logger.debug(f"Connected to database: {self.config['database']}")

                        # Read and parse SQL file
                        with open(schema_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        self.logger.debug(f"Read schema file: {len(content)} bytes")

                        statements = self._parse_sql_statements(content)
                        
                        # Execute statements
                        for i, stmt in enumerate(statements, 1):
                            try:
                                if skip_system_config and any(keyword in stmt.upper() for keyword in [
                                    'SET GLOBAL',
                                    'SET @@GLOBAL',
                                    'INNODB_',
                                    'MAX_CONNECTIONS',
                                    'PERFORMANCE_SCHEMA',
                                    'CREATE EVENT'  # Add this line
                                ]):
                                    self.logger.debug(f"Skipping system config statement: {stmt[:100]}...")
                                    continue

                                self.logger.debug(f"Executing statement {i}/{len(statements)}")
                                cursor.execute(stmt)
                                connection.commit()
                                self.logger.debug(f"Statement {i} executed successfully")

                            except mysql.connector.Error as e:
                                self.logger.error(f"MySQL Error in statement {i}: {e}")
                                self.logger.error(f"Failed statement:\n{stmt}")
                                connection.rollback()
                                return False

                        return True

            except FileNotFoundError:
                self.logger.error(f"Schema file not found: {schema_path}")
                return False
            except mysql.connector.Error as e:
                self.logger.error(f"Database error: {str(e)}")
                return False
            except Exception as e:
                self.logger.error(f"Schema execution failed: {str(e)}")
                self.logger.debug("Exception details:", exc_info=True)
                return False

    def _fetch_all_results(self, cursor):
        """Fetch all results to ensure the cursor is ready for the next command"""
        try:
            while True:
                # Only try to fetch if there are results to fetch
                if cursor.with_rows:
                    cursor.fetchall()
                if not cursor.nextset():
                    break
        except mysql.connector.Error:
            # Some statements don't return results, so errors can be ignored
            pass

    def verify_test_results(self) -> bool:
        """Verify the database schema changes"""
        try:
            with mysql.connector.connect(**self.config) as connection:
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                # Verify expected tables exist
                expected_tables = {
                    'schema_versions',
                    'system_logs',
                    'users',
                    'market_data',
                    'strategies'
                }
                
                actual_tables = {table[0].lower() for table in tables}
                missing_tables = expected_tables - actual_tables
                
                if missing_tables:
                    self.logger.error(f"Missing tables: {missing_tables}")
                    self.logger.debug(f"Found tables: {actual_tables}")
                    return False
                    
                return True
                
        except Exception as e:
            self.logger.error(f"Verification failed: {str(e)}")
            return False

    def _parse_sql_statements(self, content: str) -> list[str]:
        """Parse SQL content into individual statements, handling DELIMITER changes."""
        statements = []
        current_statement = []
        current_delimiter = ';'
        in_begin_block = False
        begin_count = 0
        
        self.logger.debug(f"Parsing SQL content ({len(content)} chars)")
        
        # First replace CRLF with LF for consistent line endings
        content = content.replace('\r\n', '\n')
        lines = content.split('\n')
        
        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            stripped_upper = stripped.upper()
            
            # Skip empty lines and comments
            if not stripped or stripped.startswith('--'):
                continue
            
            # Handle DELIMITER changes
            if stripped_upper.startswith('DELIMITER'):
                if current_statement:
                    statements.append('\n'.join(current_statement))
                    current_statement = []
                current_delimiter = stripped.split()[1]
                self.logger.debug(f"Line {line_num}: Changed delimiter to {current_delimiter}")
                continue
            
            # Track BEGIN/END blocks
            if 'BEGIN' in stripped_upper and not stripped.startswith('--'):
                in_begin_block = True
                begin_count += 1
            if 'END' in stripped_upper and not stripped.startswith('--'):
                begin_count -= 1
                if begin_count == 0:
                    in_begin_block = False
            
            # Add line to current statement
            current_statement.append(line)
            
            # Check if statement is complete
            is_complete = False
            if not in_begin_block and stripped.endswith(current_delimiter):
                is_complete = True
            elif not in_begin_block and begin_count == 0 and current_delimiter in stripped:
                is_complete = True
                
            if is_complete:
                full_stmt = '\n'.join(current_statement)
                if current_delimiter != ';':
                    full_stmt = full_stmt.replace(current_delimiter, ';')
                
                # Clean up the statement
                full_stmt = full_stmt.strip()
                if full_stmt:
                    # Fix BOOLEAN type before adding
                    full_stmt = re.sub(r'\bBOOLE(?:AN)?\b', 'BOOLEAN', full_stmt, flags=re.IGNORECASE)
                    statements.append(full_stmt)
                    self.logger.debug(f"Line {line_num}: Completed statement ({len(full_stmt)} chars)")
                current_statement = []
        
        # Add any remaining statement
        if current_statement:
            full_stmt = '\n'.join(current_statement).strip()
            if current_delimiter != ';':
                full_stmt = full_stmt.replace(current_delimiter, ';')
            if full_stmt:
                # Fix BOOLEAN type before adding
                full_stmt = re.sub(r'\bBOOLE(?:AN)?\b', 'BOOLEAN', full_stmt, flags=re.IGNORECASE)
                statements.append(full_stmt)
        
        # Additional validation
        validated_statements = []
        for stmt in statements:
            if not stmt.strip() or all(line.strip().startswith('--') for line in stmt.split('\n')):
                continue
            if not stmt.strip().endswith(';'):
                stmt += ';'
            validated_statements.append(stmt)
        
        self.logger.debug(f"Found {len(validated_statements)} valid statements")
        for i, stmt in enumerate(validated_statements, 1):
            self.logger.debug(f"Statement {i}:\n{'-'*40}\n{stmt}\n{'-'*40}")
        
        return validated_statements

class TestHandler(logging.Handler):
    """Custom handler for testing that stores records in memory"""
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def clear(self):
        self.records = []

def test_schema_execution(schema_runner):
    """Test schema file execution"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path, skip_system_config=True)
    
def test_results_verification(schema_runner, tmp_path):
    """Test database schema verification after execution"""
    # Setup schema path
    schema_path = Path('db/migrations/schema.sql')
    
    # First execute schema
    assert schema_runner.execute_schema_file(schema_path), "Schema execution failed"
    
    # Then verify results
    assert schema_runner.verify_test_results(), "Schema verification failed"

def test_invalid_schema_file():
    """Test behavior with nonexistent schema file"""
    runner = SchemaRunner()
    
    # Add test handler
    test_handler = TestHandler()
    runner.logger.addHandler(test_handler)
    
    # Test execution
    result = runner.execute_schema_file('nonexistent.sql')
    
    # Verify results
    assert not result
    assert any("Schema file not found" in record.msg 
              for record in test_handler.records)

def test_invalid_credentials():
    """Test behavior with invalid database credentials"""
    runner = SchemaRunner()
    runner.config['password'] = 'wrong_password'
    result = runner.execute_schema_file('db/migrations/schema.sql')
    
    assert not result
    assert any("Access denied" in record.msg 
              for record in runner.get_test_records())

def test_permission_denied():
    """Test behavior when user lacks required permissions"""
    runner = SchemaRunner()
    runner.root_config['password'] = 'wrong_password'
    result = runner.execute_schema_file('db/migrations/schema.sql')
    
    assert not result
    assert any("Access denied" in record.msg 
              for record in runner.get_test_records())

def test_syntax_error_handling():
    """Test handling of various SQL syntax errors"""
    runner = SchemaRunner()
    test_cases = [
        ('Invalid SQL', 'INVALID SQL;'),
        ('Unclosed quote', "CREATE TABLE test ('unclosed;"),
        ('Missing semicolon', 'CREATE TABLE test (id INT)'),  # No semicolon
        ('Invalid column type', 'CREATE TABLE test (id INVALID_TYPE);')
    ]
    
    for case_name, sql in test_cases:
        try:
            with open('test.sql', 'w') as f:
                f.write(sql)
            try:
                with mysql.connector.connect(**runner.config) as conn:
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    # If we get here, the SQL was valid
                    assert False, f"Should fail on {case_name}"
            except mysql.connector.Error:
                # This is expected - the SQL should be invalid
                pass
        finally:
            Path('test.sql').unlink(missing_ok=True)

def test_transaction_rollback(schema_runner):
    """Test transaction rollback on failure"""
    schema_path = Path('db/migrations/schema.sql')
    
    # Clean start
    with mysql.connector.connect(**schema_runner.root_config) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {schema_runner.config['database']}")
    
    # Execute valid schema first
    assert schema_runner.execute_schema_file(schema_path)
    
    # Get initial state
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        initial_tables = {t[0].lower() for t in cursor.fetchall()}
    
    # Try invalid SQL with explicit transaction
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # Set autocommit to False to ensure transaction control
        conn.autocommit = False
        
        try:
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            # Create test table and attempt insert in same transaction
            cursor.execute("""
                CREATE TEMPORARY TABLE test_rollback (
                    id INT NOT NULL,
                    value VARCHAR(50) NOT NULL
                )
            """)
            
            # Try invalid insert that should trigger rollback
            cursor.execute("""
                INSERT INTO test_rollback (id, value) 
                VALUES (1, 'valid'), (2, NULL)
            """)
            
            cursor.execute("COMMIT")
            assert False, "Should fail on NULL value constraint"
            
        except mysql.connector.Error:
            cursor.execute("ROLLBACK")
        finally:
            # Reset autocommit
            conn.autocommit = True
    
    # Verify state after rollback
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        final_tables = {t[0].lower() for t in cursor.fetchall()}
        
        assert final_tables == initial_tables, \
            f"Database state changed. Extra tables: {final_tables - initial_tables}"

def test_database_setup_diagnostic(schema_runner):
    """Diagnostic test for database setup and schema execution"""
    schema_path = Path("db/migrations/schema.sql")
    
    # 1. Test database creation
    assert schema_runner.create_test_database(), "Database creation failed"
    
    # 2. Verify database exists and is accessible
    try:
        with mysql.connector.connect(**schema_runner.config) as conn:
            cursor = conn.cursor()
            
            # Basic connectivity test
            cursor.execute(f"USE {schema_runner.config['database']}")
            schema_runner.logger.info(f"Successfully connected to {schema_runner.config['database']}")
            
            # Permission verification
            cursor.execute("SHOW GRANTS")
            grants = cursor.fetchall()
            schema_runner.logger.info("User permissions:")
            for grant in grants:
                schema_runner.logger.info(grant[0])
            
            # Basic table creation test
            cursor.execute("""
                CREATE TABLE test_permissions (
                    id INT PRIMARY KEY
                )
            """)
            cursor.execute("DROP TABLE test_permissions")
            schema_runner.logger.info("Successfully created and dropped test table")
            
            # Schema execution
            assert schema_runner.execute_schema_file(schema_path), "Schema execution failed"
            
            # Table verification
            cursor.execute("SHOW TABLES")
            tables = [t[0] for t in cursor.fetchall()]
            schema_runner.logger.info(f"Created tables: {tables}")
            
            # Verify table structures
            for table in tables:
                cursor.execute(f"SHOW CREATE TABLE {table}")
                create_stmt = cursor.fetchone()[1]
                schema_runner.logger.info(f"\nTable structure for {table}:\n{create_stmt}")
                
                cursor.execute(f"SHOW INDEX FROM {table}")
                indexes = cursor.fetchall()
                schema_runner.logger.info(f"\nIndexes for {table}:")
                for idx in indexes:
                    schema_runner.logger.info(f"- {idx[2]} ({idx[4]})")
            
            # Verify at least the essential tables exist
            essential_tables = {'users', 'market_data', 'trades', 'strategies'}
            assert all(table.lower() in [t.lower() for t in tables] for table in essential_tables), \
                "Missing essential tables"
            
    except mysql.connector.Error as e:
        schema_runner.logger.error(f"Database diagnostic failed: {str(e)}")
        raise

def test_partition_validation(schema_runner):
    """Test partition creation and validation"""
    try:
        # First execute schema to ensure database and tables exist
        schema_path = Path("db/migrations/schema.sql")
        assert schema_runner.execute_schema_file(schema_path), "Schema execution failed"
        
        with mysql.connector.connect(**schema_runner.config) as conn:
            cursor = conn.cursor()
            
            # Check market_data table partitions
            cursor.execute("""
                SELECT PARTITION_NAME, TABLE_ROWS, PARTITION_METHOD, PARTITION_EXPRESSION
                FROM information_schema.PARTITIONS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'market_data'
            """, (schema_runner.config['database'],))
            
            partitions = cursor.fetchall()
            schema_runner.logger.info("\nPartition information:")
            for partition in partitions:
                schema_runner.logger.info(f"Partition: {partition[0]}, Rows: {partition[1]}, "
                                        f"Method: {partition[2]}, Expression: {partition[3]}")
                
            assert len(partitions) > 0, "No partitions found for market_data table"
            
    except mysql.connector.Error as e:
        schema_runner.logger.error(f"Partition validation failed: {str(e)}")
        raise

@pytest.mark.benchmark
def test_schema_performance(schema_runner, benchmark_logger):
    """Test schema creation performance"""
    start_time = time.time()
    schema_path = Path("db/migrations/schema.sql")
    
    assert schema_runner.execute_schema_file(schema_path)
    
    duration = time.time() - start_time
    benchmark_logger.info(f"Schema creation took {duration:.2f} seconds")
    assert duration < 5.0, "Schema creation took too long"

@pytest.mark.benchmark
def test_bulk_insert_performance(schema_runner, benchmark_logger, sample_data):
    """Test bulk data insertion performance"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
    start_time = time.time()
    
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # Generate bulk test data
        users = [
            {'username': f'user_{i}', 'email': f'user_{i}@test.com'}
            for i in range(1000)
        ]
        
        # Bulk insert
        cursor.executemany(
            "INSERT INTO users (username, email) VALUES (%s, %s)",
            [(user['username'], user['email']) for user in users]
        )
        conn.commit()
        
    duration = time.time() - start_time
    benchmark_logger.info(f"Bulk insert took {duration:.2f} seconds")
    assert duration < 2.0, "Bulk insert took too long"

def test_data_insertion(schema_runner, sample_data):
    """Test data insertion after schema creation"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # Insert test users
        for user in sample_data['users']:
            cursor.execute(
                "INSERT INTO users (username, email) VALUES (%s, %s)",
                (user['username'], user['email'])
            )
        
        # Insert market data
        for data in sample_data['market_data']:
            cursor.execute(
                "INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume) "
                "VALUES (%s, NOW(), 'STOCK', %s, %s, %s, %s, %s)",
                (data['symbol'], data['price'], data['price'], data['price'], 
                 data['price'], data['volume'])
            )
        
        conn.commit()
        
        # Verify data
        cursor.execute("SELECT COUNT(*) FROM users")
        assert cursor.fetchone()[0] == len(sample_data['users'])
        
        cursor.execute("SELECT COUNT(*) FROM market_data")
        assert cursor.fetchone()[0] == len(sample_data['market_data'])

@pytest.mark.benchmark
@pytest.mark.parametrize("table_count", [10, 100, 1000])
def test_bulk_insert_scaling(schema_runner, benchmark_logger, table_count):
    """Test bulk insert performance scaling"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
    # Generate test data
    test_data = [
        {
            'symbol': f'TEST_{i}',
            'price': 100.0 + i,
            'volume': 1000 * i
        }
        for i in range(table_count)
    ]
    
    start_time = time.time()
    
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # Bulk insert using executemany
        cursor.executemany(
            """
            INSERT INTO market_data 
                (symbol, timestamp, data_type, open, high, low, close, volume)
            VALUES 
                (%s, NOW(), 'STOCK', %s, %s, %s, %s, %s)
            """,
            [
                (data['symbol'], data['price'], data['price'], 
                 data['price'], data['price'], data['volume'])
                for data in test_data
            ]
        )
        conn.commit()
    
    duration = time.time() - start_time
    benchmark_logger.info(
        f"Bulk insert of {table_count} records took {duration:.2f} seconds"
    )
    
    # Scale timing expectations with data size
    max_duration = 0.5 + (table_count / 1000)
    assert duration < max_duration, f"Bulk insert of {table_count} records too slow"

@pytest.mark.benchmark
def test_query_performance(schema_runner, benchmark_logger):
    """Test query performance with indexes"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
    # Insert test data
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # Insert sample data
        for i in range(1000):
            cursor.execute(
                """
                INSERT INTO market_data 
                    (symbol, timestamp, data_type, open, high, low, close, volume)
                VALUES 
                    (%s, DATE_SUB(NOW(), INTERVAL %s HOUR), 'STOCK', 100, 100, 100, 100, 1000)
                """,
                (f'TEST_{i}', i)
            )
        conn.commit()
        
        # Test indexed query performance
        start_time = time.time()
        cursor.execute(
            """
            SELECT * FROM market_data 
            WHERE symbol = 'TEST_1' 
            AND timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """
        )
        results = cursor.fetchall()
        duration = time.time() - start_time
        
        benchmark_logger.info(f"Indexed query took {duration:.3f} seconds")
        assert duration < 0.1, "Indexed query too slow"

@pytest.mark.benchmark
def test_partition_performance(schema_runner, benchmark_logger):
    """Test partition table performance"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # Insert data across partitions
        start_time = time.time()
        for year in range(2024, 2027):
            cursor.executemany(
                """
                INSERT INTO market_data 
                    (symbol, timestamp, data_type, open, high, low, close, volume)
                VALUES 
                    (%s, %s, 'STOCK', 100, 100, 100, 100, 1000)
                """,
                [
                    (f'TEST_{i}', f'{year}-{month:02d}-01')
                    for i in range(100)
                    for month in range(1, 13)
                ]
            )
        conn.commit()
        
        insert_duration = time.time() - start_time
        benchmark_logger.info(
            f"Partition insert took {insert_duration:.2f} seconds"
        )
        
        # Test partition pruning
        start_time = time.time()
        cursor.execute(
            """
            SELECT COUNT(*) FROM market_data 
            WHERE timestamp BETWEEN '2025-01-01' AND '2025-12-31'
            """
        )
        count = cursor.fetchone()[0]
        query_duration = time.time() - start_time
        
        benchmark_logger.info(
            f"Partition query took {query_duration:.3f} seconds"
        )
        
        assert insert_duration < 5.0, "Partition insert too slow"
        assert query_duration < 0.1, "Partition query too slow"

def test_comprehensive_schema_validation(schema_runner, benchmark_logger):
    """Test all core schema features"""
    schema_path = Path("db/migrations/schema.sql")
    assert schema_runner.execute_schema_file(schema_path)
    
    with mysql.connector.connect(**schema_runner.config) as conn:
        cursor = conn.cursor()
        
        # 1. Test table partitioning
        cursor.execute("""
            SELECT PARTITION_NAME, TABLE_ROWS 
            FROM information_schema.PARTITIONS 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_NAME = 'market_data'
        """, (schema_runner.config['database'],))
        partitions = cursor.fetchall()
        assert len(partitions) > 0, "No partitions found"
        
        # 2. Test constraints
        try:
            # Should fail: high < low
            cursor.execute("""
                INSERT INTO market_data 
                    (symbol, timestamp, data_type, open, high, low, close, volume)
                VALUES 
                    ('TEST', NOW(), 'STOCK', 100, 90, 95, 98, 1000)
            """)
            assert False, "Price constraint validation failed"
        except mysql.connector.Error as e:
            assert "chk_prices" in str(e)
            
        # 3. Test indexes
        cursor.execute("EXPLAIN SELECT * FROM market_data WHERE symbol = 'TEST'")
        plan = cursor.fetchone()
        assert "idx_symbol_time" in str(plan), "Index not used"
        
        # 4. Test foreign keys
        try:
            cursor.execute("""
                INSERT INTO trades (strategy_id, user_id, symbol, order_type, side, 
                                  asset_type, quantity, price, status)
                VALUES (999, 999, 'TEST', 'MARKET', 'BUY', 'STOCK', 100, 50, 'PENDING')
            """)
            assert False, "Foreign key constraint failed"
        except mysql.connector.Error as e:
            assert "foreign key constraint fails" in str(e).lower()
            
        # 5. Test transaction rollback
        try:
            cursor.execute("START TRANSACTION")
            cursor.execute("""
                INSERT INTO users (username, email) 
                VALUES ('test_user', 'test@example.com')
            """)
            cursor.execute("""
                INSERT INTO users (username, email) 
                VALUES ('test_user', 'duplicate@example.com')
            """)
            assert False, "Duplicate key constraint failed"
        except mysql.connector.Error:
            cursor.execute("ROLLBACK")
            
        cursor.execute("SELECT COUNT(*) FROM users WHERE username = 'test_user'")
        count = cursor.fetchone()[0]
        assert count == 0, "Transaction rollback failed"
        
        benchmark_logger.info("All schema validations passed")

def test_schema_features(tmp_path):
    """Run all schema feature tests"""
    runner = SchemaRunner()
    test_cases = [
        (test_schema_execution, {'schema_runner': runner}),
        (test_results_verification, {'schema_runner': runner, 'tmp_path': tmp_path}),
        (test_partition_validation, {'schema_runner': runner}),
        (test_transaction_rollback, {'schema_runner': runner}),
        (test_comprehensive_schema_validation, {
            'schema_runner': runner,
            'benchmark_logger': logging.getLogger("benchmark")
        })
    ]
    
    for test, kwargs in test_cases:
        try:
            test(**kwargs)  # Pass kwargs as individual arguments
            print(f"✓ {test.__name__}")
        except Exception as e:
            print(f"✗ {test.__name__}: {str(e)}")
            raise

if __name__ == "__main__":
    pytest.main([__file__, '-v'])