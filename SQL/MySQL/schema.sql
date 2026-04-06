-- Tạo Database cho Data Warehouse
CREATE DATABASE IF NOT EXISTS recruitment_dw;

-- Chọn Database vừa tạo
USE recruitment_dw;

-- Tạo bảng tổng hợp Events cho báo cáo (Được ETL từ Cassandra đổ vào)
CREATE TABLE IF NOT EXISTS events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    
    job_id VARCHAR(50) NOT NULL,
    dates DATE NOT NULL,
    hours INT NOT NULL,
    publisher_id VARCHAR(50) DEFAULT '0',
    company_id VARCHAR(50) DEFAULT '0',
    campaign_id VARCHAR(50) DEFAULT '0',
    group_id VARCHAR(50) DEFAULT '0',
    
    
    unqualified_application INT DEFAULT 0,
    qualified_application INT DEFAULT 0,
    conversion INT DEFAULT 0,
    clicks INT DEFAULT 0,
    
    
    bid_set DECIMAL(10, 4) DEFAULT 0.0000,
    spend_hour DECIMAL(10, 4) DEFAULT 0.0000,
    
    
    updated_at TIMESTAMP NULL DEFAULT NULL,
    sources VARCHAR(255) DEFAULT 'Cassandra',
    
    
    INDEX idx_dates (dates),
    INDEX idx_campaign (campaign_id)
);