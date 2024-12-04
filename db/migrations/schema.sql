-- Migration file to create initial tables

-- Core Tables (Create these first)
CREATE TABLE IF NOT EXISTS Users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_username (username)
);

-- System Tables (Create second for logging)
CREATE TABLE IF NOT EXISTS SystemLogs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    level VARCHAR(20) NOT NULL,
    component VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trading Tables (Create third)
CREATE TABLE IF NOT EXISTS Trades (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    trade_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol VARCHAR(50),
    quantity DECIMAL(18, 8),
    price DECIMAL(18, 8),
    trade_type ENUM('BUY', 'SELL') NOT NULL,
    asset_type ENUM('STOCK', 'OPTION', 'CRYPTO', 'FOREX') NOT NULL,
    expiration_date DATE NULL,
    strike_price DECIMAL(18, 8) NULL,
    option_type ENUM('CALL', 'PUT') NULL,
    order_type ENUM('MARKET', 'LIMIT', 'STOP', 'STOP_LIMIT') NOT NULL,
    FOREIGN KEY (user_id) REFERENCES Users(id),
    INDEX idx_user_symbol (user_id, symbol),
    INDEX idx_trade_date (trade_date)
);

CREATE TABLE IF NOT EXISTS Strategies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    strategy_name VARCHAR(100) NOT NULL,
    parameters JSON NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(id),
    INDEX idx_user_active (user_id, is_active)
);

-- Market Data Tables (Create fourth)
CREATE TABLE IF NOT EXISTS MarketData (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    data_type ENUM('STOCK', 'CRYPTO', 'FOREX', 'OPTION', 'HISTORICAL') NOT NULL,
    source ENUM('POLYGON', 'FRED', 'SEC', 'INTERNAL') NOT NULL,
    open DECIMAL(18,8),
    high DECIMAL(18,8),
    low DECIMAL(18,8),
    close DECIMAL(18,8),
    volume BIGINT,
    vwap DECIMAL(18,8),
    transactions INT,
    metadata JSON,
    INDEX idx_symbol_timestamp (symbol, timestamp),
    INDEX idx_type_source (data_type, source)
);

-- Analysis Tables (Create fifth)
CREATE TABLE IF NOT EXISTS TechnicalAnalysis (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    analysis_type ENUM('INDICATOR', 'CORRELATION', 'SENTIMENT') NOT NULL,
    value DECIMAL(18,8) NOT NULL,
    parameters JSON,
    metadata JSON,
    INDEX idx_symbol_type (symbol, analysis_type, timestamp)
);

CREATE TABLE IF NOT EXISTS FundamentalData (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    data_type ENUM('SEC', 'FRED', 'FINANCIAL', 'OTHER') NOT NULL,
    report_date DATE NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value DECIMAL(18,8),
    period_type ENUM('Q', 'Y', 'TTM', 'YTD') NOT NULL,
    metadata JSON,
    INDEX idx_symbol_date (symbol, report_date),
    INDEX idx_type_metric (data_type, metric_type)
);

-- AI/ML Tables (Create last)
CREATE TABLE IF NOT EXISTS AI_Models (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    model_name VARCHAR(100) NOT NULL,
    model_type ENUM('PREDICTION', 'CLASSIFICATION', 'SENTIMENT') NOT NULL,
    model_data MEDIUMBLOB NOT NULL,
    parameters JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(id),
    INDEX idx_user_type (user_id, model_type)
);

CREATE TABLE IF NOT EXISTS NewsData (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp BIGINT NOT NULL,
    headline VARCHAR(255) NOT NULL,
    source VARCHAR(50) NOT NULL,
    sentiment FLOAT,
    url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);