-- ============================================================================
-- Fix zomato table column types to match expected data types
-- ============================================================================
-- This migration ensures column types match what the application expects
-- Run this on the devyani database

USE devyani;

-- Note: Based on current schema analysis:
-- - pg_applied_on is VARCHAR(100) - keeping as VARCHAR for now (can store numeric strings)
-- - order_id, res_id, city_id are VARCHAR(100) - keeping as VARCHAR (can store numeric strings)
-- 
-- If you want to change pg_applied_on to DECIMAL, uncomment the following:
-- ALTER TABLE zomato MODIFY COLUMN pg_applied_on DECIMAL(15,2) NULL;

-- For now, the application code has been updated to treat these as STRING columns
-- which matches the current database schema (VARCHAR columns).

-- No schema changes needed - code now matches database schema

