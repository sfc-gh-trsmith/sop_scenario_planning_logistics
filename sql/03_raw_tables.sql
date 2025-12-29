-- =============================================================================
-- 03_raw_tables.sql
-- RAW layer staging tables for S&OP Scenario Planning & Logistics Optimization
-- 
-- Pattern: All columns are VARCHAR to preserve original data format
-- Metadata columns: _SOURCE_FILE_NAME, _SOURCE_FILE_ROW_NUMBER, _LOADED_TIMESTAMP
--
-- Tables created:
--   - PRODUCT_STAGE
--   - PRODUCT_CATEGORY_STAGE
--   - SITE_STAGE
--   - WORK_CENTER_STAGE
--   - WAREHOUSE_ZONE_STAGE
--   - DEMAND_FORECAST_STAGE
--   - LOGISTICS_COST_STAGE
--   - SCENARIO_DEFINITION_STAGE
--
-- Usage: Run after 02_schema_setup.sql
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);
USE SCHEMA RAW;

-- =============================================================================
-- Product Category Staging
-- =============================================================================
CREATE OR REPLACE TABLE PRODUCT_CATEGORY_STAGE (
    -- Original file columns (all VARCHAR for staging)
    PRODUCT_CATEGORY_ID VARCHAR(50),
    PRODUCT_CATEGORY_CODE VARCHAR(50),
    CATEGORY_NAME VARCHAR(200),
    CATEGORY_DESCRIPTION VARCHAR(500),
    PARENT_CATEGORY_ID VARCHAR(50),
    CATEGORY_LEVEL VARCHAR(10),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE PRODUCT_CATEGORY_STAGE IS 'Staging table for product category hierarchy data';

-- =============================================================================
-- Product Staging
-- =============================================================================
CREATE OR REPLACE TABLE PRODUCT_STAGE (
    -- Original file columns
    PRODUCT_ID VARCHAR(50),
    PRODUCT_CODE VARCHAR(100),
    PRODUCT_NAME VARCHAR(200),
    PRODUCT_DESCRIPTION_SHORT VARCHAR(500),
    PRODUCT_TYPE VARCHAR(50),
    PRODUCT_STATUS VARCHAR(50),
    PRODUCT_CATEGORY_ID VARCHAR(50),
    UNIT_PRICE VARCHAR(20),
    UNIT_COST VARCHAR(20),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE PRODUCT_STAGE IS 'Staging table for product master data';

-- =============================================================================
-- Site Staging (Plants and Warehouses)
-- =============================================================================
CREATE OR REPLACE TABLE SITE_STAGE (
    -- Original file columns
    SITE_ID VARCHAR(50),
    SITE_CODE VARCHAR(50),
    SITE_NAME VARCHAR(200),
    SITE_TYPE VARCHAR(50),  -- 'PLANT' or 'WAREHOUSE'
    REGION VARCHAR(100),
    ADDRESS_LINE_1 VARCHAR(200),
    CITY VARCHAR(100),
    STATE_PROVINCE VARCHAR(100),
    COUNTRY VARCHAR(100),
    OPERATING_STATUS VARCHAR(50),
    CAPACITY_METRIC VARCHAR(20),
    CAPACITY_UNIT_OF_MEASURE VARCHAR(50),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE SITE_STAGE IS 'Staging table for plant and warehouse site data';

-- =============================================================================
-- Work Center Staging (Production Lines)
-- =============================================================================
CREATE OR REPLACE TABLE WORK_CENTER_STAGE (
    -- Original file columns
    WORK_CENTER_ID VARCHAR(50),
    WORK_CENTER_CODE VARCHAR(50),
    WORK_CENTER_NAME VARCHAR(200),
    WORK_CENTER_DESCRIPTION VARCHAR(1000),
    WORK_CENTER_TYPE VARCHAR(50),
    SITE_ID VARCHAR(50),
    CAPACITY_PER_HOUR VARCHAR(20),
    HOURS_PER_DAY VARCHAR(10),
    AVAILABLE_SHIFTS VARCHAR(10),
    EFFICIENCY_FACTOR VARCHAR(10),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE WORK_CENTER_STAGE IS 'Staging table for production work center (line) data';

-- =============================================================================
-- Warehouse Zone Staging
-- =============================================================================
CREATE OR REPLACE TABLE WAREHOUSE_ZONE_STAGE (
    -- Original file columns
    WAREHOUSE_ZONE_ID VARCHAR(50),
    ZONE_CODE VARCHAR(100),
    ZONE_NAME VARCHAR(200),
    SITE_ID VARCHAR(50),
    ZONE_TYPE VARCHAR(50),
    ZONE_TEMPERATURE_CONTROL VARCHAR(50),
    MAX_CAPACITY_PALLETS VARCHAR(20),
    CURRENT_OCCUPANCY_PALLETS VARCHAR(20),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE WAREHOUSE_ZONE_STAGE IS 'Staging table for warehouse zone capacity data';

-- =============================================================================
-- Demand Forecast Staging
-- =============================================================================
CREATE OR REPLACE TABLE DEMAND_FORECAST_STAGE (
    -- Original file columns
    DEMAND_FORECAST_ID VARCHAR(50),
    PRODUCT_ID VARCHAR(50),
    SITE_ID VARCHAR(50),
    FORECAST_DATE VARCHAR(50),
    FORECAST_CREATION_DATE VARCHAR(50),
    FORECAST_QUANTITY VARCHAR(20),
    FORECAST_TYPE VARCHAR(50),
    FORECAST_METHOD VARCHAR(50),
    FORECAST_HORIZON VARCHAR(50),
    CONFIDENCE_LEVEL VARCHAR(10),
    SCENARIO_ID VARCHAR(50),  -- References SCENARIO_DEFINITION
    FISCAL_PERIOD VARCHAR(20),
    FISCAL_MONTH VARCHAR(20),
    FISCAL_QUARTER VARCHAR(10),
    FISCAL_YEAR VARCHAR(10),
    UNIT_REVENUE VARCHAR(20),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE DEMAND_FORECAST_STAGE IS 'Staging table for demand forecast data with scenario versioning';

-- =============================================================================
-- Scenario Definition Staging
-- =============================================================================
CREATE OR REPLACE TABLE SCENARIO_DEFINITION_STAGE (
    -- Original file columns
    SCENARIO_ID VARCHAR(50),
    SCENARIO_CODE VARCHAR(50),
    SCENARIO_NAME VARCHAR(200),
    SCENARIO_DESCRIPTION VARCHAR(1000),
    SCENARIO_TYPE VARCHAR(50),  -- 'BASELINE', 'MARKETING_PUSH', 'CONSERVATIVE'
    IS_OFFICIAL_BASELINE VARCHAR(10),
    CREATED_BY_USER VARCHAR(100),
    CREATED_DATE VARCHAR(50),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE SCENARIO_DEFINITION_STAGE IS 'Staging table for forecast scenario definitions';

-- =============================================================================
-- Logistics Cost Staging
-- =============================================================================
CREATE OR REPLACE TABLE LOGISTICS_COST_STAGE (
    -- Original file columns
    LOGISTICS_COST_ID VARCHAR(50),
    SITE_ID VARCHAR(50),
    WAREHOUSE_ZONE_ID VARCHAR(50),
    STORAGE_COST_PER_PALLET_PER_DAY VARCHAR(20),
    TRANSPORT_COST_PER_UNIT VARCHAR(20),
    HANDLING_COST_PER_PALLET VARCHAR(20),
    OVERFLOW_PENALTY_RATE VARCHAR(20),
    OVERFLOW_PENALTY_THRESHOLD_PCT VARCHAR(10),
    EFFECTIVE_DATE VARCHAR(50),
    EXPIRY_DATE VARCHAR(50),
    VENDOR_NAME VARCHAR(200),
    CONTRACT_REFERENCE VARCHAR(100),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE LOGISTICS_COST_STAGE IS 'Staging table for warehousing and logistics cost data';

-- =============================================================================
-- Inventory Balance Staging
-- =============================================================================
CREATE OR REPLACE TABLE INVENTORY_BALANCE_STAGE (
    -- Original file columns
    INVENTORY_BALANCE_ID VARCHAR(50),
    PRODUCT_ID VARCHAR(50),
    SITE_ID VARCHAR(50),
    WAREHOUSE_ZONE_ID VARCHAR(50),
    BALANCE_DATE VARCHAR(50),
    QUANTITY_ON_HAND VARCHAR(20),
    QUANTITY_RESERVED VARCHAR(20),
    QUANTITY_AVAILABLE VARCHAR(20),
    UNIT_OF_MEASURE VARCHAR(50),
    
    -- RAW metadata columns
    _SOURCE_FILE_NAME VARCHAR(500),
    _SOURCE_FILE_ROW_NUMBER NUMBER,
    _LOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COMMENT ON TABLE INVENTORY_BALANCE_STAGE IS 'Staging table for current inventory positions';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA RAW;

