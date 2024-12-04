-- db/migrations/schema.sql

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_versions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(20),
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- System Logs Table
CREATE TABLE IF NOT EXISTS system_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(10),
    component VARCHAR(50),
    message TEXT,
    metadata JSON
);

-- Users Table
CREATE TABLE IF NOT EXISTS users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE,
    email VARCHAR(100) UNIQUE,
    password_hash VARCHAR(64),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login DATETIME,
    status VARCHAR(20) DEFAULT 'active',
    settings JSON
);

-- Market Data Table
CREATE TABLE IF NOT EXISTS market_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp DATETIME NOT NULL,
    data_type ENUM('STOCK', 'OPTION', 'FOREX', 'CRYPTO') NOT NULL,
    open DECIMAL(10,4) NOT NULL,
    high DECIMAL(10,4) NOT NULL,
    low DECIMAL(10,4) NOT NULL,
    close DECIMAL(10,4) NOT NULL,
    volume BIGINT NOT NULL,
    vwap DECIMAL(10,4),
    number_of_trades INT,
    source VARCHAR(20) DEFAULT 'POLYGON',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_prices CHECK (
        high >= low AND 
        high >= open AND 
        high >= close AND 
        low <= open AND 
        low <= close
    ),
    CONSTRAINT chk_volume CHECK (volume >= 0),
    UNIQUE KEY unique_record (symbol, timestamp, data_type),
    INDEX idx_symbol_time (symbol, timestamp),
    INDEX idx_type_time (data_type, timestamp),
    INDEX idx_source (source)
) ENGINE=InnoDB;

-- Fixed partitioning using timestamp column
ALTER TABLE market_data
PARTITION BY RANGE (TO_DAYS(timestamp)) (
    PARTITION p0 VALUES LESS THAN (TO_DAYS('2024-01-01')),
    PARTITION p1 VALUES LESS THAN (TO_DAYS('2025-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- Strategies Table
CREATE TABLE IF NOT EXISTS strategies (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    description TEXT,
    parameters JSON NOT NULL,
    is_active BOOLEAN DEFAULT true,
    performance_metrics JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    INDEX idx_user_active (user_id, is_active),
    INDEX idx_strategy_name (strategy_name)
) ENGINE=InnoDB;

-- Trades Table
CREATE TABLE IF NOT EXISTS trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    strategy_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    order_type ENUM('MARKET', 'LIMIT', 'STOP', 'STOP_LIMIT') NOT NULL,
    side ENUM('BUY', 'SELL') NOT NULL,
    asset_type ENUM('STOCK', 'OPTION', 'FOREX', 'CRYPTO') NOT NULL,
    quantity DECIMAL(10,4) NOT NULL,
    price DECIMAL(10,4) NOT NULL,
    status ENUM('PENDING', 'FILLED', 'CANCELLED', 'REJECTED') NOT NULL,
    filled_quantity DECIMAL(10,4) DEFAULT 0,
    filled_price DECIMAL(10,4),
    commission DECIMAL(10,4) DEFAULT 0,
    slippage DECIMAL(10,4),
    execution_details JSON,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    executed_at DATETIME,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_quantity CHECK (quantity > 0),
    CONSTRAINT chk_price CHECK (price > 0),
    CONSTRAINT chk_filled CHECK (filled_quantity <= quantity),
    FOREIGN KEY (strategy_id) REFERENCES strategies(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    INDEX idx_user_symbol (user_id, symbol),
    INDEX idx_strategy_status (strategy_id, status),
    INDEX idx_execution_time (executed_at)
) ENGINE=InnoDB;

-- Technical Analysis Table
CREATE TABLE IF NOT EXISTS technical_analysis (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp DATETIME NOT NULL,
    indicator_type VARCHAR(50) NOT NULL,
    timeframe VARCHAR(20) NOT NULL,
    value DECIMAL(10,4) NOT NULL,
    parameters JSON,
    confidence_score DECIMAL(5,2),
    signal_strength ENUM('WEAK', 'MODERATE', 'STRONG') NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_indicator (symbol, timestamp, indicator_type, timeframe),
    INDEX idx_symbol_indicator (symbol, indicator_type),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB;

-- News Data Table
CREATE TABLE IF NOT EXISTS news_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp DATETIME NOT NULL,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    source VARCHAR(50) NOT NULL,
    url VARCHAR(255) UNIQUE,
    sentiment DECIMAL(5,2),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_time (symbol, timestamp)
) ENGINE=InnoDB;

-- Fundamental Data Table
CREATE TABLE IF NOT EXISTS fundamental_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20),
    report_date DATE,
    report_type VARCHAR(20),
    metrics JSON,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, report_date)
);

-- API Query Cache Table
CREATE TABLE IF NOT EXISTS query_cache (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    query_hash VARCHAR(64),
    endpoint VARCHAR(255),
    parameters JSON,
    response JSON,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    INDEX idx_query (query_hash, expires_at)
);

-- Symbols Table
CREATE TABLE IF NOT EXISTS symbols (
    symbol VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    type VARCHAR(20),
    exchange VARCHAR(20),
    status VARCHAR(10),
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    metadata JSON
);

-- Performance Metrics Table
CREATE TABLE IF NOT EXISTS performance_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    timestamp DATETIME NOT NULL,
    value DECIMAL(10,4) NOT NULL,
    window_size INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_metric (symbol, metric_type, timestamp, window_size),
    INDEX idx_symbol_metric (symbol, metric_type)
);

-- Audit Log Table
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id BIGINT NOT NULL,
    action VARCHAR(10) NOT NULL,
    changed_fields JSON,
    changed_by BIGINT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_table_record (table_name, record_id),
    FOREIGN KEY (changed_by) REFERENCES users(id)
);

-- System Configuration Table
CREATE TABLE IF NOT EXISTS system_config (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    config_key VARCHAR(50) NOT NULL,
    config_value JSON NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_key (config_key)
) ENGINE=InnoDB;

-- User Sessions Table
CREATE TABLE IF NOT EXISTS user_sessions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    session_token VARCHAR(64) NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,
    expires_at DATETIME NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_accessed DATETIME,
    is_valid BOOLEAN DEFAULT true,
    FOREIGN KEY (user_id) REFERENCES users(id),
    UNIQUE KEY unique_token (session_token),
    INDEX idx_user_valid (user_id, is_valid),
    INDEX idx_expiry (expires_at)
) ENGINE=InnoDB;

-- API Keys Table
CREATE TABLE IF NOT EXISTS api_keys (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    api_key VARCHAR(64) UNIQUE NOT NULL,
    description VARCHAR(255),
    permissions JSON,
    expires_at DATETIME,
    is_active BOOLEAN DEFAULT true,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_used DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id),
    INDEX idx_api_key (api_key),
    INDEX idx_user_active (user_id, is_active)
) ENGINE=InnoDB;

-- User Permissions Table
CREATE TABLE IF NOT EXISTS user_permissions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    permission_type VARCHAR(50) NOT NULL,
    resource_type VARCHAR(50),
    resource_id VARCHAR(50),
    granted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    granted_by BIGINT,
    expires_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (granted_by) REFERENCES users(id),
    UNIQUE KEY unique_permission (user_id, permission_type, resource_type, resource_id),
    INDEX idx_user_permission (user_id, permission_type)
) ENGINE=InnoDB;

-- Archive Tables
CREATE TABLE IF NOT EXISTS market_data_archive LIKE market_data;
CREATE TABLE IF NOT EXISTS trades_archive LIKE trades;
CREATE TABLE IF NOT EXISTS technical_analysis_archive LIKE technical_analysis;
CREATE TABLE IF NOT EXISTS news_data_archive LIKE news_data;

-- Database Maintenance Procedures
DELIMITER //

CREATE PROCEDURE cleanup_old_data(IN days_to_keep INT)
BEGIN
    DECLARE cleanup_date DATETIME;
    SET cleanup_date = DATE_SUB(NOW(), INTERVAL days_to_keep DAY);
    
    DELETE FROM market_data WHERE timestamp < cleanup_date;
    DELETE FROM technical_analysis WHERE timestamp < cleanup_date;
    DELETE FROM news_data WHERE timestamp < cleanup_date;
    DELETE FROM audit_log WHERE timestamp < cleanup_date;
END //

CREATE PROCEDURE rotate_partitions()
BEGIN
    -- Add new future partition
    ALTER TABLE market_data REORGANIZE PARTITION p_future INTO (
        PARTITION p_current VALUES LESS THAN (UNIX_TIMESTAMP(DATE_ADD(NOW(), INTERVAL 1 YEAR))),
        PARTITION p_future VALUES LESS THAN MAXVALUE
    );
END //

-- Additional Database Procedures
-- Market Statistics Procedure
CREATE PROCEDURE calculate_market_stats(IN symbol_param VARCHAR(20), IN days INT)
BEGIN
    SELECT 
        symbol,
        AVG(close) as avg_price,
        MIN(low) as period_low,
        MAX(high) as period_high,
        SUM(volume) as total_volume,
        STD(close) as price_volatility
    FROM market_data
    WHERE symbol = symbol_param
    AND timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL days DAY)
    GROUP BY symbol;
END //

-- User Activity Monitor
CREATE PROCEDURE monitor_user_activity()
BEGIN
    SELECT 
        u.username,
        COUNT(t.id) as trade_count,
        COUNT(DISTINCT t.symbol) as symbols_traded,
        SUM(CASE WHEN t.side = 'BUY' THEN t.quantity * t.price ELSE 0 END) as total_bought,
        SUM(CASE WHEN t.side = 'SELL' THEN t.quantity * t.price ELSE 0 END) as total_sold
    FROM users u
    LEFT JOIN trades t ON u.id = t.user_id
    WHERE t.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    GROUP BY u.id, u.username;
END //

-- Database Health Check
CREATE PROCEDURE check_database_health()
BEGIN
    -- Check table sizes
    SELECT 
        table_name,
        table_rows,
        data_length/1024/1024 as data_size_mb,
        index_length/1024/1024 as index_size_mb
    FROM information_schema.tables
    WHERE table_schema = DATABASE();
    
    -- Check connection count
    SELECT COUNT(*) as active_connections
    FROM information_schema.processlist;
    
    -- Check cache hit ratio
    SELECT 
        Variable_name, 
        Value
    FROM performance_schema.global_status
    WHERE Variable_name LIKE 'Qcache%';
END //

-- Data Archival Procedure
CREATE PROCEDURE archive_old_data(IN archive_date DATE)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Archive failed';
    END;
    
    START TRANSACTION;
        -- Archive market data
        INSERT INTO market_data_archive
        SELECT * FROM market_data
        WHERE DATE(timestamp) < archive_date;
        
        -- Delete archived data
        DELETE FROM market_data
        WHERE DATE(timestamp) < archive_date;
    COMMIT;
END //

-- Performance Monitoring Functions
CREATE FUNCTION calculate_vwap(symbol_param VARCHAR(20), start_time DATETIME, end_time DATETIME)
RETURNS DECIMAL(10,4)
DETERMINISTIC
BEGIN
    DECLARE vwap DECIMAL(10,4);
    
    SELECT SUM(price * volume) / SUM(volume) INTO vwap
    FROM market_data 
    WHERE symbol = symbol_param
    AND timestamp BETWEEN start_time AND end_time;
    
    RETURN COALESCE(vwap, 0);
END //

CREATE FUNCTION calculate_volatility(symbol_param VARCHAR(20), days INT)
RETURNS DECIMAL(10,4)
DETERMINISTIC
BEGIN
    DECLARE vol DECIMAL(10,4);
    
    SELECT STD(close) INTO vol
    FROM market_data
    WHERE symbol = symbol_param
    AND timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL days DAY);
    
    RETURN COALESCE(vol, 0);
END //

-- Final Cleanup Triggers
CREATE TRIGGER before_user_delete
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, changed_fields)
    VALUES ('users', OLD.id, 'DELETE', JSON_OBJECT('username', OLD.username));
END //

CREATE TRIGGER after_trade_insert
AFTER INSERT ON trades
FOR EACH ROW
BEGIN
    UPDATE strategies 
    SET performance_metrics = JSON_SET(
        COALESCE(performance_metrics, '{}'),
        '$.last_trade_time', NOW(),
        '$.total_trades', COALESCE(JSON_EXTRACT(performance_metrics, '$.total_trades'), 0) + 1
    )
    WHERE id = NEW.strategy_id;
END //

-- Additional Triggers for Data Consistency
CREATE TRIGGER before_market_data_insert
BEFORE INSERT ON market_data
FOR EACH ROW
BEGIN
    -- Validate symbol exists
    IF NOT EXISTS (SELECT 1 FROM symbols WHERE symbol = NEW.symbol) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid symbol';
    END IF;
    
    -- Ensure timestamp is not in future
    IF NEW.timestamp > NOW() THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Future timestamp not allowed';
    END IF;
END //

-- Additional Statistical Functions
CREATE FUNCTION calculate_sharpe_ratio(symbol_param VARCHAR(20), days INT)
RETURNS DECIMAL(10,4)
DETERMINISTIC
BEGIN
    DECLARE returns DECIMAL(10,4);
    DECLARE std_dev DECIMAL(10,4);
    DECLARE risk_free_rate DECIMAL(10,4) DEFAULT 0.02;  -- Assumed annual rate
    
    SELECT 
        (AVG(daily_return) * 252 - risk_free_rate) / (STD(daily_return) * SQRT(252))
    INTO returns
    FROM (
        SELECT 
            (close / LAG(close) OVER (ORDER BY timestamp) - 1) as daily_return
        FROM market_data
        WHERE symbol = symbol_param
        AND timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL days DAY)
    ) t;
    
    RETURN COALESCE(returns, 0);
END //

-- Database Verification Procedure
CREATE PROCEDURE verify_data_integrity()
BEGIN
    DECLARE error_count INT DEFAULT 0;
    
    -- Check for orphaned records
    SELECT COUNT(*) INTO @orphaned_trades
    FROM trades t 
    LEFT JOIN users u ON t.user_id = u.id
    WHERE u.id IS NULL;
    
    IF @orphaned_trades > 0 THEN
        SET error_count = error_count + 1;
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Orphaned trade records detected';
    END IF;
    
    -- Check for invalid prices
    SELECT COUNT(*) INTO @invalid_prices
    FROM market_data
    WHERE high < low OR close > high OR close < low;
    
    IF @invalid_prices > 0 THEN
        SET error_count = error_count + 1;
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Invalid price data detected';
    END IF;
END //

DELIMITER ;

-- Set database configuration
SET GLOBAL innodb_buffer_pool_size = 4294967296;  -- 4GB
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
SET GLOBAL max_connections = 1000;
SET GLOBAL innodb_buffer_pool_instances = 8;
SET GLOBAL innodb_read_io_threads = 8;
SET GLOBAL innodb_write_io_threads = 8;
SET GLOBAL innodb_io_capacity = 2000;
SET GLOBAL innodb_io_capacity_max = 4000;

-- Final Database Settings
SET GLOBAL innodb_monitor_enable = 'all';
SET GLOBAL performance_schema = 'ON';
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;

-- Create event to run maintenance tasks
CREATE EVENT IF NOT EXISTS daily_maintenance
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_DATE + INTERVAL 1 DAY
DO
BEGIN
    CALL cleanup_old_data(90);
    CALL rotate_partitions();
    CALL check_database_health();
END;