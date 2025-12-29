-- =============================================================================
-- 04_atomic_tables.sql
-- ATOMIC layer tables for S&OP Scenario Planning & Logistics Optimization
-- 
-- Pattern: Enterprise relational model with proper data types
-- Standard columns: Audit columns (CREATED_BY_USER, CREATED_TIMESTAMP, etc.)
-- Type 2 SCD: VALID_FROM_TIMESTAMP, VALID_TO_TIMESTAMP, IS_CURRENT_FLAG
--
-- Core Entities (from data dictionary):
--   - PRODUCT_CATEGORY, PRODUCT, SITE, WORK_CENTER, WAREHOUSE_ZONE
--
-- Project Extensions:
--   - SCENARIO_DEFINITION, DEMAND_FORECAST_VERSIONS (Hybrid Table), LOGISTICS_COST_FACT
--
-- Note: PRIMARY KEY and FOREIGN KEY are metadata only (not enforced by Snowflake)
-- Note: CHECK constraints are NOT supported - document valid values in comments
--
-- Usage: Run after 03_raw_tables.sql
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);
USE SCHEMA ATOMIC;

-- =============================================================================
-- Product Category (Core Entity)
-- =============================================================================
CREATE OR REPLACE TABLE PRODUCT_CATEGORY (
    -- Primary Key
    PRODUCT_CATEGORY_ID NUMBER(38,0) NOT NULL,
    
    -- Business Keys
    PRODUCT_CATEGORY_CODE VARCHAR(50) NOT NULL,
    CATEGORY_NAME VARCHAR(200) NOT NULL,
    CATEGORY_DESCRIPTION VARCHAR(500),
    
    -- Hierarchy
    PARENT_CATEGORY_ID NUMBER(38,0),
    CATEGORY_LEVEL NUMBER(38,0),
    
    -- Type 2 SCD columns
    VALID_FROM_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    VALID_TO_TIMESTAMP TIMESTAMP_NTZ,
    IS_CURRENT_FLAG BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (PRODUCT_CATEGORY_ID),
    FOREIGN KEY (PARENT_CATEGORY_ID) REFERENCES PRODUCT_CATEGORY(PRODUCT_CATEGORY_ID)
);

COMMENT ON TABLE PRODUCT_CATEGORY IS 'Product category hierarchy for grouping products into families';
COMMENT ON COLUMN PRODUCT_CATEGORY.CATEGORY_LEVEL IS 'Hierarchy level: 1=Top, 2=Family, 3=Subfamily';

-- =============================================================================
-- Product (Core Entity)
-- =============================================================================
CREATE OR REPLACE TABLE PRODUCT (
    -- Primary Key
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    
    -- Business Keys
    PRODUCT_CODE VARCHAR(100) NOT NULL,
    PRODUCT_NAME VARCHAR(200) NOT NULL,
    PRODUCT_DESCRIPTION_SHORT VARCHAR(500),
    
    -- Classification
    PRODUCT_TYPE VARCHAR(50),  -- Valid: 'FINISHED_GOOD', 'RAW_MATERIAL', 'COMPONENT'
    PRODUCT_STATUS VARCHAR(50),  -- Valid: 'ACTIVE', 'INACTIVE', 'DISCONTINUED'
    PRODUCT_CATEGORY_ID NUMBER(38,0),
    
    -- Financials
    UNIT_PRICE NUMBER(18,4),
    UNIT_COST NUMBER(18,4),
    
    -- Type 2 SCD columns
    VALID_FROM_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    VALID_TO_TIMESTAMP TIMESTAMP_NTZ,
    IS_CURRENT_FLAG BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (PRODUCT_ID),
    FOREIGN KEY (PRODUCT_CATEGORY_ID) REFERENCES PRODUCT_CATEGORY(PRODUCT_CATEGORY_ID)
);

COMMENT ON TABLE PRODUCT IS 'Product master data with pricing information';
COMMENT ON COLUMN PRODUCT.PRODUCT_TYPE IS 'Valid values: FINISHED_GOOD, RAW_MATERIAL, COMPONENT';
COMMENT ON COLUMN PRODUCT.PRODUCT_STATUS IS 'Valid values: ACTIVE, INACTIVE, DISCONTINUED';

-- =============================================================================
-- Site (Core Entity) - Plants and Warehouses
-- =============================================================================
CREATE OR REPLACE TABLE SITE (
    -- Primary Key
    SITE_ID NUMBER(38,0) NOT NULL,
    
    -- Business Keys
    SITE_CODE VARCHAR(50) NOT NULL,
    SITE_NAME VARCHAR(200) NOT NULL,
    SITE_TYPE VARCHAR(50) NOT NULL,  -- Valid: 'PLANT', 'WAREHOUSE', 'DISTRIBUTION_CENTER'
    REGION VARCHAR(100),
    
    -- Location
    ADDRESS_LINE_1 VARCHAR(200),
    CITY VARCHAR(100),
    STATE_PROVINCE VARCHAR(100),
    COUNTRY VARCHAR(100),
    
    -- Operations
    OPERATING_STATUS VARCHAR(50),  -- Valid: 'ACTIVE', 'INACTIVE', 'SEASONAL'
    CAPACITY_METRIC NUMBER(18,4),
    CAPACITY_UNIT_OF_MEASURE VARCHAR(50),
    
    -- Type 2 SCD columns
    VALID_FROM_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    VALID_TO_TIMESTAMP TIMESTAMP_NTZ,
    IS_CURRENT_FLAG BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (SITE_ID)
);

COMMENT ON TABLE SITE IS 'Manufacturing plants and warehouse sites';
COMMENT ON COLUMN SITE.SITE_TYPE IS 'Valid values: PLANT, WAREHOUSE, DISTRIBUTION_CENTER';
COMMENT ON COLUMN SITE.OPERATING_STATUS IS 'Valid values: ACTIVE, INACTIVE, SEASONAL';

-- =============================================================================
-- Work Center (Core Entity) - Production Lines
-- =============================================================================
CREATE OR REPLACE TABLE WORK_CENTER (
    -- Primary Key
    WORK_CENTER_ID NUMBER(38,0) NOT NULL,
    
    -- Business Keys
    WORK_CENTER_CODE VARCHAR(50) NOT NULL,
    WORK_CENTER_NAME VARCHAR(200) NOT NULL,
    WORK_CENTER_DESCRIPTION VARCHAR(1000),
    WORK_CENTER_TYPE VARCHAR(50),  -- Valid: 'ASSEMBLY', 'FABRICATION', 'PACKAGING'
    
    -- Location
    SITE_ID NUMBER(38,0) NOT NULL,
    
    -- Capacity
    CAPACITY_PER_HOUR NUMBER(18,4),
    HOURS_PER_DAY NUMBER(8,2),
    AVAILABLE_SHIFTS NUMBER(38,0),
    EFFICIENCY_FACTOR NUMBER(5,4),
    
    -- Type 2 SCD columns
    VALID_FROM_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    VALID_TO_TIMESTAMP TIMESTAMP_NTZ,
    IS_CURRENT_FLAG BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (WORK_CENTER_ID),
    FOREIGN KEY (SITE_ID) REFERENCES SITE(SITE_ID)
);

COMMENT ON TABLE WORK_CENTER IS 'Production work centers (lines) with capacity constraints';
COMMENT ON COLUMN WORK_CENTER.EFFICIENCY_FACTOR IS 'Operational efficiency factor (0.0-1.0)';

-- =============================================================================
-- Warehouse Zone (Core Entity)
-- =============================================================================
CREATE OR REPLACE TABLE WAREHOUSE_ZONE (
    -- Primary Key
    WAREHOUSE_ZONE_ID NUMBER(38,0) NOT NULL,
    
    -- Business Keys
    ZONE_CODE VARCHAR(100) NOT NULL,
    ZONE_NAME VARCHAR(200) NOT NULL,
    
    -- Location
    SITE_ID NUMBER(38,0) NOT NULL,
    
    -- Zone attributes
    ZONE_TYPE VARCHAR(50),  -- Valid: 'STANDARD', 'COLD_STORAGE', 'HAZMAT', 'HIGH_VALUE'
    ZONE_TEMPERATURE_CONTROL VARCHAR(50),
    
    -- Capacity
    MAX_CAPACITY_PALLETS NUMBER(38,0),
    CURRENT_OCCUPANCY_PALLETS NUMBER(38,0),
    
    -- Type 2 SCD columns
    VALID_FROM_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    VALID_TO_TIMESTAMP TIMESTAMP_NTZ,
    IS_CURRENT_FLAG BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (WAREHOUSE_ZONE_ID),
    FOREIGN KEY (SITE_ID) REFERENCES SITE(SITE_ID)
);

COMMENT ON TABLE WAREHOUSE_ZONE IS 'Warehouse storage zones with capacity limits';
COMMENT ON COLUMN WAREHOUSE_ZONE.ZONE_TYPE IS 'Valid values: STANDARD, COLD_STORAGE, HAZMAT, HIGH_VALUE';

-- =============================================================================
-- Scenario Definition (Project Extension)
-- =============================================================================
CREATE OR REPLACE TABLE SCENARIO_DEFINITION (
    -- Primary Key
    SCENARIO_ID NUMBER(38,0) NOT NULL,
    
    -- Business Keys
    SCENARIO_CODE VARCHAR(50) NOT NULL,
    SCENARIO_NAME VARCHAR(200) NOT NULL,
    SCENARIO_DESCRIPTION VARCHAR(1000),
    
    -- Scenario classification
    SCENARIO_TYPE VARCHAR(50) NOT NULL,  -- Valid: 'BASELINE', 'MARKETING_PUSH', 'CONSERVATIVE', 'CUSTOM'
    IS_OFFICIAL_BASELINE BOOLEAN DEFAULT FALSE,
    
    -- Version control
    VERSION_NUMBER NUMBER(38,0) DEFAULT 1,
    PARENT_SCENARIO_ID NUMBER(38,0),
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (SCENARIO_ID),
    FOREIGN KEY (PARENT_SCENARIO_ID) REFERENCES SCENARIO_DEFINITION(SCENARIO_ID)
);

COMMENT ON TABLE SCENARIO_DEFINITION IS 'Forecast scenario definitions for S&OP planning';
COMMENT ON COLUMN SCENARIO_DEFINITION.SCENARIO_TYPE IS 'Valid values: BASELINE, MARKETING_PUSH, CONSERVATIVE, CUSTOM';

-- =============================================================================
-- Demand Forecast Versions (Project Extension)
-- Note: Using standard TABLE instead of HYBRID TABLE for broader account compatibility
-- =============================================================================
CREATE OR REPLACE TABLE DEMAND_FORECAST_VERSIONS (
    -- Primary Key
    DEMAND_FORECAST_ID NUMBER(38,0) NOT NULL,
    
    -- Foreign Keys
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    SITE_ID NUMBER(38,0) NOT NULL,
    SCENARIO_ID NUMBER(38,0) NOT NULL,
    
    -- Forecast data
    FORECAST_DATE DATE NOT NULL,
    FORECAST_CREATION_DATE DATE,
    FORECAST_QUANTITY NUMBER(18,4) NOT NULL,
    FORECAST_TYPE VARCHAR(50),  -- Valid: 'DEMAND', 'SUPPLY', 'INVENTORY'
    FORECAST_METHOD VARCHAR(50),
    FORECAST_HORIZON VARCHAR(50),
    CONFIDENCE_LEVEL NUMBER(5,2),
    
    -- Fiscal calendar
    FISCAL_PERIOD VARCHAR(20),
    FISCAL_MONTH VARCHAR(20),
    FISCAL_QUARTER VARCHAR(10),
    FISCAL_YEAR NUMBER(38,0),
    
    -- Financial metrics
    UNIT_REVENUE NUMBER(18,4),
    TOTAL_REVENUE NUMBER(18,2),
    
    -- Version control
    VERSION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    IS_CURRENT_VERSION BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (DEMAND_FORECAST_ID)
);

COMMENT ON TABLE DEMAND_FORECAST_VERSIONS IS 'Table for versioned demand forecasts - supports write-back from Streamlit';
COMMENT ON COLUMN DEMAND_FORECAST_VERSIONS.VERSION_TIMESTAMP IS 'Timestamp when this version was created';

-- =============================================================================
-- Logistics Cost Fact (Project Extension)
-- =============================================================================
CREATE OR REPLACE TABLE LOGISTICS_COST_FACT (
    -- Primary Key
    LOGISTICS_COST_ID NUMBER(38,0) NOT NULL,
    
    -- Foreign Keys
    SITE_ID NUMBER(38,0) NOT NULL,
    WAREHOUSE_ZONE_ID NUMBER(38,0),
    
    -- Cost metrics
    STORAGE_COST_PER_PALLET_PER_DAY NUMBER(18,4),
    TRANSPORT_COST_PER_UNIT NUMBER(18,4),
    HANDLING_COST_PER_PALLET NUMBER(18,4),
    OVERFLOW_PENALTY_RATE NUMBER(18,4),
    OVERFLOW_PENALTY_THRESHOLD_PCT NUMBER(5,2),  -- Percentage threshold before penalties apply
    
    -- Contract info
    EFFECTIVE_DATE DATE,
    EXPIRY_DATE DATE,
    VENDOR_NAME VARCHAR(200),
    CONTRACT_REFERENCE VARCHAR(100),
    
    -- Type 2 SCD columns
    VALID_FROM_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    VALID_TO_TIMESTAMP TIMESTAMP_NTZ,
    IS_CURRENT_FLAG BOOLEAN DEFAULT TRUE,
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (LOGISTICS_COST_ID),
    FOREIGN KEY (SITE_ID) REFERENCES SITE(SITE_ID),
    FOREIGN KEY (WAREHOUSE_ZONE_ID) REFERENCES WAREHOUSE_ZONE(WAREHOUSE_ZONE_ID)
);

COMMENT ON TABLE LOGISTICS_COST_FACT IS 'Warehousing, storage, and logistics cost data per 3PL contracts';
COMMENT ON COLUMN LOGISTICS_COST_FACT.OVERFLOW_PENALTY_RATE IS 'Additional cost rate when capacity exceeds threshold';

-- =============================================================================
-- Inventory Balance (Core Entity)
-- =============================================================================
CREATE OR REPLACE TABLE INVENTORY_BALANCE (
    -- Primary Key
    INVENTORY_BALANCE_ID NUMBER(38,0) NOT NULL,
    
    -- Foreign Keys
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    SITE_ID NUMBER(38,0) NOT NULL,
    WAREHOUSE_ZONE_ID NUMBER(38,0),
    
    -- Balance data
    BALANCE_DATE DATE NOT NULL,
    QUANTITY_ON_HAND NUMBER(18,4),
    QUANTITY_RESERVED NUMBER(18,4),
    QUANTITY_AVAILABLE NUMBER(18,4),
    UNIT_OF_MEASURE VARCHAR(50),
    
    -- Audit columns
    CREATED_BY_USER VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY_USER VARCHAR(100),
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    
    -- Constraints (metadata only)
    PRIMARY KEY (INVENTORY_BALANCE_ID),
    FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCT(PRODUCT_ID),
    FOREIGN KEY (SITE_ID) REFERENCES SITE(SITE_ID),
    FOREIGN KEY (WAREHOUSE_ZONE_ID) REFERENCES WAREHOUSE_ZONE(WAREHOUSE_ZONE_ID)
);

COMMENT ON TABLE INVENTORY_BALANCE IS 'Current inventory positions by product, site, and zone';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA ATOMIC;

