-- db/migrations/test_schema.sql

DELIMITER //

-- Test Data Integrity
CREATE PROCEDURE test_data_constraints()
BEGIN
    -- Test price constraints
    INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
    VALUES ('AAPL', NOW(), 'STOCK', 100, 90, 95, 98, 1000);  -- Should fail: high < low
    
    -- Test volume constraints
    INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
    VALUES ('AAPL', NOW(), 'STOCK', 100, 110, 95, 108, -1000);  -- Should fail: negative volume
    
    -- Test symbol validation
    INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
    VALUES ('INVALID', NOW(), 'STOCK', 100, 110, 95, 108, 1000);  -- Should fail: invalid symbol
END //

-- Test Foreign Key Relationships
CREATE PROCEDURE test_relationships()
BEGIN
    -- Test user-strategy relationship
    INSERT INTO strategies (user_id, strategy_name, parameters)
    VALUES (99999, 'Test Strategy', '{}');  -- Should fail: invalid user_id
    
    -- Test strategy-trade relationship
    INSERT INTO trades (strategy_id, user_id, symbol, order_type, side, asset_type, quantity, price, status)
    VALUES (99999, 1, 'AAPL', 'MARKET', 'BUY', 'STOCK', 100, 150.00, 'PENDING');  -- Should fail: invalid strategy_id
END //

-- Test Stored Procedures
CREATE PROCEDURE test_procedures()
BEGIN
    -- Test market stats calculation
    CALL calculate_market_stats('AAPL', 30);
    
    -- Test data archival
    CALL archive_old_data(DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY));
    
    -- Test health check
    CALL check_database_health();
END //

-- Test Performance
CREATE PROCEDURE test_performance()
BEGIN
    DECLARE i INT DEFAULT 0;
    
    -- Test bulk insert performance
    WHILE i < 10000 DO
        INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
        VALUES ('AAPL', DATE_ADD(NOW(), INTERVAL i SECOND), 'STOCK', 
                100+i, 110+i, 95+i, 108+i, 1000000);
        SET i = i + 1;
    END WHILE;
    
    -- Test query performance
    EXPLAIN ANALYZE
    SELECT * FROM market_data 
    WHERE symbol = 'AAPL' 
    AND timestamp BETWEEN DATE_SUB(NOW(), INTERVAL 30 DAY) AND NOW();
END //

-- Test Performance and Query Plans
CREATE PROCEDURE test_query_performance()
BEGIN
    -- Test index usage
    EXPLAIN ANALYZE
    SELECT * FROM market_data 
    WHERE symbol = 'AAPL' 
    AND timestamp BETWEEN DATE_SUB(NOW(), INTERVAL 30 DAY) AND NOW();
    
    -- Test partition pruning
    EXPLAIN ANALYZE
    SELECT COUNT(*) 
    FROM market_data PARTITION(p_current)
    WHERE timestamp >= '2024-01-01';
    
    -- Test join performance
    EXPLAIN ANALYZE
    SELECT t.*, s.strategy_name
    FROM trades t
    INNER JOIN strategies s ON t.strategy_id = s.id
    WHERE t.symbol = 'AAPL'
    AND t.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);
END //

-- Test Data Validation
CREATE PROCEDURE test_data_validation()
BEGIN
    DECLARE error_count INT DEFAULT 0;
    
    -- Test OHLC price validation
    INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
    SELECT 
        'AAPL',
        NOW(),
        'STOCK',
        100,
        CASE WHEN MOD(id, 2) = 0 THEN 90 ELSE 110 END, -- Should fail when high < open
        95,
        98,
        1000
    FROM market_data LIMIT 1;
    
    -- Test trade quantity validation
    INSERT INTO trades (strategy_id, user_id, symbol, order_type, side, asset_type, quantity, price, status)
    VALUES (1, 1, 'AAPL', 'MARKET', 'BUY', 'STOCK', -100, 150.00, 'PENDING'); -- Should fail: negative quantity
    
    -- Test date range validation
    INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
    VALUES ('AAPL', DATE_ADD(NOW(), INTERVAL 1 DAY), 'STOCK', 100, 110, 95, 108, 1000); -- Should fail: future date
END //

-- Test Database Maintenance
CREATE PROCEDURE test_maintenance()
BEGIN
    -- Test partition rotation
    CALL rotate_partitions();
    
    -- Test data archival
    CALL archive_old_data(DATE_SUB(CURRENT_DATE, INTERVAL 90 DAY));
    
    -- Test cleanup
    CALL cleanup_old_data(30);
    
    -- Verify results
    SELECT 
        COUNT(*) as current_partition_count 
    FROM market_data PARTITION(p_current);
    
    SELECT 
        COUNT(*) as archive_count 
    FROM market_data_archive;
END //

-- Test Error Handling
CREATE PROCEDURE test_error_handling()
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
        @sqlstate = RETURNED_SQLSTATE,
        @errno = MYSQL_ERRNO,
        @text = MESSAGE_TEXT;
        
        INSERT INTO system_logs (level, component, message, metadata)
        VALUES (
            'ERROR',
            'TEST_SUITE',
            CONCAT('Error: ', @text),
            JSON_OBJECT(
                'sqlstate', @sqlstate,
                'errno', @errno,
                'procedure', 'test_error_handling'
            )
        );
    END;
    
    -- Test invalid trade execution
    INSERT INTO trades (strategy_id, user_id, symbol, order_type, side, asset_type, quantity, price, status)
    VALUES (1, 1, 'INVALID', 'MARKET', 'BUY', 'STOCK', 100, 0, 'PENDING');
    
    -- Test invalid market data
    INSERT INTO market_data (symbol, timestamp, data_type, open, high, low, close, volume)
    VALUES ('AAPL', '2099-01-01', 'STOCK', -1, -1, -1, -1, -1);
END //

-- Performance Benchmark Tests
CREATE PROCEDURE test_performance_benchmarks()
BEGIN
    DECLARE start_time, end_time TIMESTAMP;
    
    -- Test bulk insert performance
    SET start_time = NOW();
    INSERT INTO market_data_archive 
    SELECT * FROM market_data 
    WHERE timestamp < DATE_SUB(NOW(), INTERVAL 90 DAY);
    SET end_time = NOW();
    
    INSERT INTO performance_metrics (metric_type, value, window_size)
    VALUES ('BULK_INSERT_TIME', TIMESTAMPDIFF(MICROSECOND, start_time, end_time), 90);
    
    -- Test index scan performance
    SET start_time = NOW();
    SELECT COUNT(*) FROM market_data USE INDEX (idx_symbol_time)
    WHERE symbol = 'AAPL' 
    AND timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY);
    SET end_time = NOW();
    
    INSERT INTO performance_metrics (metric_type, value, window_size)
    VALUES ('INDEX_SCAN_TIME', TIMESTAMPDIFF(MICROSECOND, start_time, end_time), 30);
END //

-- Cleanup Procedures
CREATE PROCEDURE cleanup_test_data()
BEGIN
    -- Remove test data
    DELETE FROM market_data WHERE symbol IN ('TEST', 'INVALID');
    DELETE FROM trades WHERE strategy_id = 99999;
    DELETE FROM performance_metrics WHERE metric_type LIKE 'TEST_%';
    
    -- Reset auto increment values
    ALTER TABLE market_data AUTO_INCREMENT = 1;
    ALTER TABLE trades AUTO_INCREMENT = 1;
    
    -- Optimize tables
    OPTIMIZE TABLE market_data, trades, performance_metrics;
END //

-- Final Validation
CREATE PROCEDURE validate_test_results()
BEGIN
    SELECT 
        (SELECT COUNT(*) FROM system_logs WHERE level = 'ERROR') as error_count,
        (SELECT COUNT(*) FROM performance_metrics WHERE metric_type LIKE 'TEST_%') as benchmark_count,
        (SELECT COUNT(*) FROM market_data_archive) as archive_count;
        
    -- Log test completion
    INSERT INTO system_logs (level, component, message)
    VALUES ('INFO', 'TEST_SUITE', 'Test suite execution completed');
END //

-- Run All Tests
CREATE PROCEDURE run_all_tests()
BEGIN
    -- Run test suites
    CALL test_data_constraints();
    CALL test_relationships();
    CALL test_query_performance();
    CALL test_data_validation();
    CALL test_maintenance();
    
    -- Report results
    SELECT 'Test suite completed' as status;
END //

-- Monitor Database Performance
CREATE PROCEDURE test_monitor_performance()
BEGIN
    -- Monitor connection pool
    SELECT * FROM performance_schema.events_waits_summary_global_by_event_name
    WHERE event_name LIKE 'wait/synch/mutex/innodb/pool%';
    
    -- Monitor query cache
    SELECT * FROM performance_schema.table_io_waits_summary_by_table
    WHERE object_schema = DATABASE()
    ORDER BY count_fetch DESC;
    
    -- Monitor index usage
    SELECT 
        OBJECT_SCHEMA as database_name,
        OBJECT_NAME as table_name,
        INDEX_NAME as index_name,
        COUNT_STAR as total_io,
        COUNT_READ as reads,
        COUNT_WRITE as writes
    FROM performance_schema.table_io_waits_summary_by_index_usage
    WHERE OBJECT_SCHEMA = DATABASE();
END //

-- Analyze Query Plans
CREATE PROCEDURE test_analyze_query_plans()
BEGIN
    -- Test different join strategies
    EXPLAIN FORMAT=JSON
    SELECT t.*, s.strategy_name, m.close
    FROM trades t
    INNER JOIN strategies s ON t.strategy_id = s.id
    INNER JOIN market_data m ON t.symbol = m.symbol AND t.executed_at = m.timestamp;
    
    -- Test index usage patterns
    EXPLAIN FORMAT=TREE
    SELECT symbol, AVG(close) as avg_price
    FROM market_data
    WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
    GROUP BY symbol
    HAVING avg_price > 100;
END //

-- Storage Analysis
CREATE PROCEDURE test_analyze_storage()
BEGIN
    -- Table sizes
    SELECT 
        table_name,
        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)",
        ROUND((data_length / 1024 / 1024), 2) AS "Data Size (MB)",
        ROUND((index_length / 1024 / 1024), 2) AS "Index Size (MB)"
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
    ORDER BY (data_length + index_length) DESC;
    
    -- Partition analysis
    SELECT 
        PARTITION_NAME, 
        TABLE_ROWS,
        ROUND((DATA_LENGTH/1024/1024), 2) as "Data Size (MB)"
    FROM information_schema.partitions
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'market_data';
END //

-- System Configuration Tests
CREATE PROCEDURE test_system_config()
BEGIN
    -- Test InnoDB buffer pool settings
    SELECT @@innodb_buffer_pool_size/1024/1024 AS buffer_pool_mb,
           @@innodb_buffer_pool_instances AS pool_instances;
           
    -- Test thread configurations
    SELECT @@innodb_read_io_threads AS read_threads,
           @@innodb_write_io_threads AS write_threads,
           @@max_connections AS max_conn;
           
    -- Test cache settings
    SELECT @@query_cache_size/1024/1024 AS query_cache_mb,
           @@table_open_cache AS table_cache;
END //

-- Database Verification
CREATE PROCEDURE verify_database_setup()
BEGIN
    -- Check table structures
    SELECT table_name, engine, table_rows, data_length/1024/1024 AS data_mb
    FROM information_schema.tables 
    WHERE table_schema = DATABASE();
    
    -- Verify indexes
    SELECT table_name, index_name, column_name
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
    ORDER BY table_name, index_name;
    
    -- Check foreign keys
    SELECT table_name, constraint_name, referenced_table_name
    FROM information_schema.key_column_usage
    WHERE table_schema = DATABASE()
    AND referenced_table_name IS NOT NULL;
END //

-- Database Optimization Settings
SET GLOBAL innodb_file_per_table = ON;
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
SET GLOBAL innodb_flush_method = 'O_DIRECT';
SET GLOBAL innodb_buffer_pool_dump_at_shutdown = ON;
SET GLOBAL innodb_buffer_pool_load_at_startup = ON;

-- Security Tests
CREATE PROCEDURE test_security()
BEGIN
    -- Test user permissions
    SELECT CURRENT_USER(), USER(), @@hostname;
    
    SHOW GRANTS FOR CURRENT_USER;
    
    -- Test encryption settings
    SELECT @@ssl_cipher, @@have_ssl;
    
    -- Test authentication
    SELECT plugin, authentication_string IS NOT NULL as has_password
    FROM mysql.user
    WHERE user = CURRENT_USER();
END //

-- Backup Verification
CREATE PROCEDURE verify_backup_integrity()
BEGIN
    DECLARE backup_path VARCHAR(255);
    SET backup_path = CONCAT('db/backups/backup_', DATE_FORMAT(NOW(), '%Y%m%d'), '.sql');
    
    -- Test backup creation
    CALL create_backup(backup_path);
    
    -- Verify backup file
    SELECT 
        EXISTS (
            SELECT 1 FROM mysql.backup_progress 
            WHERE backup_type = 'FULL' 
            AND state = 'COMPLETED'
            AND end_time >= NOW() - INTERVAL 1 HOUR
        ) as backup_successful;
        
    -- Check backup size
    SELECT table_name, table_rows, backup_size/1024/1024 as backup_mb
    FROM information_schema.tables t
    JOIN mysql.backup_history h ON t.table_name = h.table_name
    WHERE t.table_schema = DATABASE()
    AND h.backup_id = (SELECT MAX(backup_id) FROM mysql.backup_history);
END //

-- Environment Checks
CREATE PROCEDURE verify_environment()
BEGIN
    -- Check MySQL version
    SELECT VERSION(), @@version_comment;
    
    -- Check system variables
    SHOW VARIABLES WHERE Variable_name LIKE 'innodb%'
    OR Variable_name LIKE 'max%'
    OR Variable_name LIKE 'query_cache%';
    
    -- Check running processes
    SHOW FULL PROCESSLIST;
    
    -- Check storage engine status
    SHOW ENGINE INNODB STATUS;
END //

-- Create Test Execution Sequence
CREATE PROCEDURE execute_test_suite()
BEGIN
    -- Initialize test environment
    SET @test_start = NOW();
    INSERT INTO system_logs (level, component, message)
    VALUES ('INFO', 'TEST_SUITE', 'Starting test execution');

    -- Phase 1: Data Integrity Tests
    CALL test_data_constraints();
    CALL test_relationships();
    CALL test_data_validation();
    
    -- Phase 2: Performance Tests
    CALL test_performance();
    CALL test_query_performance();
    CALL test_performance_benchmarks();
    
    -- Phase 3: Maintenance Tests
    CALL test_maintenance();
    CALL test_analyze_storage();
    
    -- Phase 4: System Tests
    CALL test_monitor_performance();
    CALL test_system_config();
    CALL verify_database_setup();
    
    -- Phase 5: Security and Backup Tests
    CALL test_security();
    CALL verify_backup_integrity();
    
    -- Cleanup and Verification
    CALL cleanup_test_data();
    CALL validate_test_results();
    
    -- Log completion time and duration
    SET @test_end = NOW();
    INSERT INTO system_logs (level, component, message, metadata)
    VALUES (
        'INFO', 
        'TEST_SUITE', 
        'Test execution completed',
        JSON_OBJECT(
            'start_time', @test_start,
            'end_time', @test_end,
            'duration_seconds', TIMESTAMPDIFF(SECOND, @test_start, @test_end)
        )
    );
END //

DELIMITER ;

-- Execute all tests
CALL run_all_tests();

-- Execute final procedures
CALL test_error_handling();
CALL test_performance_benchmarks();
CALL cleanup_test_data();
CALL validate_test_results();

-- Execute monitoring tests
CALL test_monitor_performance();
CALL test_analyze_query_plans();
CALL test_analyze_storage();

-- Execute final verification
CALL test_system_config();
CALL verify_database_setup();

-- Execute final tests
CALL test_security();
CALL verify_backup_integrity();
CALL verify_environment();

-- Execute test suite
CALL execute_test_suite();

-- Final database optimizations
OPTIMIZE TABLE market_data, trades, technical_analysis, news_data;
ANALYZE TABLE market_data, trades, technical_analysis, news_data;

-- Final status verification
SELECT 
    'Test Suite Completed' as status,
    NOW() as completion_time,
    DATABASE() as database_name,
    VERSION() as mysql_version;

-- Set production-ready configuration
SET GLOBAL slow_query_log = ON;
SET GLOBAL long_query_time = 1;
SET GLOBAL log_output = 'TABLE';
SET GLOBAL performance_schema = ON;

-- Final status check
SELECT 'Database setup and verification completed successfully' as status;

-- Log completion
INSERT INTO system_logs (level, component, message)
VALUES ('INFO', 'TEST_SUITE', 'Schema verification completed successfully');

def _validate_sql_statement(self, stmt: str) -> tuple[bool, str]:
    """Validate SQL statement before execution."""
    with self.debug_operation("SQL Statement Validation") as debug:
        # Remove comments and clean whitespace
        cleaned_stmt = re.sub(r'--.*$', '', stmt, flags=re.MULTILINE)
        cleaned_stmt = ' '.join(cleaned_stmt.split())
        
        if 'PARTITION BY RANGE' in stmt:
            with self.debug_operation("Partition Validation") as partition_debug:
                try:
                    # Extract partition block with balanced parentheses
                    partition_start = cleaned_stmt.index('PARTITION BY RANGE')
                    level = 0
                    end_pos = -1
                    
                    # Include the full partition definition by tracking nested parentheses
                    for i in range(partition_start, len(cleaned_stmt)):
                        if cleaned_stmt[i] == '(':
                            level += 1
                        elif cleaned_stmt[i] == ')':
                            level -= 1
                            if level == 0:
                                end_pos = i + 1
                                break
                    
                    if end_pos == -1:
                        return False, "Cannot find complete partition block"
                        
                    partition_def = cleaned_stmt[partition_start:end_pos]
                    self.logger.debug(f"Extracted partition block:\n{partition_def}")

                    # Match partitions including TO_DAYS functions
                    partition_pattern = r'PARTITION\s+p[0-9a-z_]+\s+VALUES\s+LESS\s+THAN\s*\([^)]+\)'
                    maxval_pattern = r'PARTITION\s+p[0-9a-z_]+\s+VALUES\s+LESS\s+THAN\s+MAXVALUE'
                    
                    # Find both regular partitions and MAXVALUE partition
                    regular_partitions = re.findall(partition_pattern, partition_def, re.IGNORECASE)
                    maxval_partitions = re.findall(maxval_pattern, partition_def, re.IGNORECASE)
                    total_partitions = len(regular_partitions) + len(maxval_partitions)
                    
                    self.logger.debug(f"Found {total_partitions} total partitions:")
                    self.logger.debug(f"Regular partitions: {len(regular_partitions)}")
                    self.logger.debug(f"MAXVALUE partitions: {len(maxval_partitions)}")
                    
                    if total_partitions < 2:
                        return False, f"Invalid partition definition - found {total_partitions} partitions, need at least 2"
                    
                    if len(maxval_partitions) == 0:
                        return False, "Missing MAXVALUE partition"
                    
                    self.logger.debug("Partition validation successful")
                    
                except ValueError as e:
                    return False, f"Partition syntax error: {str(e)}"
                    
        return True, ""

PARTITION BY RANGE (TO_DAYS(timestamp)) (
    PARTITION p0 VALUES LESS THAN (TO_DAYS('2025-12-04')),
    PARTITION p1 VALUES LESS THAN (TO_DAYS('2026-12-04')),
    PARTITION p2 VALUES LESS THAN (TO_DAYS('2027-12-04')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
)