-- ============================================================================
-- Create daily_sales_summary table
-- Pre-calculated sales data for fast dashboard queries
-- ============================================================================

CREATE TABLE IF NOT EXISTS daily_sales_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Date dimension
    sales_date DATE NOT NULL,
    
    -- Foreign Keys for filtering
    store_code VARCHAR(50) NOT NULL,
    city_id VARCHAR(50) NULL,
    zone VARCHAR(100) NULL,
    
    -- In-Store Sales (from orders table)
    instore_cash DECIMAL(15,2) DEFAULT 0,
    instore_card DECIMAL(15,2) DEFAULT 0,
    instore_upi DECIMAL(15,2) DEFAULT 0,
    instore_other DECIMAL(15,2) DEFAULT 0,  -- INSTORE category
    instore_total DECIMAL(15,2) DEFAULT 0,
    instore_count INT DEFAULT 0,
    
    -- Aggregator Sales (from orders table - quick totals)
    aggregator_zomato DECIMAL(15,2) DEFAULT 0,
    aggregator_swiggy DECIMAL(15,2) DEFAULT 0,
    aggregator_magicpin DECIMAL(15,2) DEFAULT 0,
    aggregator_total DECIMAL(15,2) DEFAULT 0,
    aggregator_count INT DEFAULT 0,
    
    -- Zomato Calculated Values (from zomato table with formula)
    zomato_net_amount DECIMAL(15,2) DEFAULT 0,           -- Using formula: net_amount OR (bill_subtotal - mvd + merchant_pack_charge)
    zomato_calculated_amount DECIMAL(15,2) DEFAULT 0,    -- Always: bill_subtotal - mvd + merchant_pack_charge
    zomato_final_amount DECIMAL(15,2) DEFAULT 0,
    zomato_order_count INT DEFAULT 0,
    
    -- Grand Totals
    total_sales DECIMAL(15,2) DEFAULT 0,                 -- instore_total + aggregator_total
    total_order_count INT DEFAULT 0,
    
    -- Metadata
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Unique constraint to prevent duplicates
    UNIQUE KEY uk_date_store (sales_date, store_code),
    
    -- Indexes for fast filtering
    INDEX idx_sales_date (sales_date),
    INDEX idx_store_code (store_code),
    INDEX idx_city_id (city_id),
    INDEX idx_zone (zone),
    INDEX idx_date_city (sales_date, city_id),
    INDEX idx_date_store (sales_date, store_code)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

