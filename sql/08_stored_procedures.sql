-- =============================================================================
-- 08_stored_procedures.sql
-- Stored procedures for S&OP Scenario Planning & Logistics
-- 
-- Procedures:
--   - SUBMIT_FORECAST_VERSION: Write-back from Streamlit editable dataframe
--   - CREATE_SCENARIO_COPY: Clone a scenario for editing
--   - LOAD_STAGED_DATA: Load CSVs from stage to RAW tables
--   - TRANSFORM_RAW_TO_ATOMIC: ETL from RAW to ATOMIC layer
--
-- Usage: Run after 07_semantic_model.sql
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);
USE SCHEMA ATOMIC;

-- =============================================================================
-- Procedure: SUBMIT_FORECAST_VERSION
-- Write-back procedure for Streamlit scenario editing
-- Accepts JSON array of forecast updates and applies to hybrid table
-- =============================================================================
CREATE OR REPLACE PROCEDURE SUBMIT_FORECAST_VERSION(
    P_FORECAST_UPDATES VARIANT,  -- JSON array of updates
    P_SCENARIO_ID NUMBER,
    P_USER VARCHAR DEFAULT CURRENT_USER()
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    rows_updated INTEGER DEFAULT 0;
    rows_inserted INTEGER DEFAULT 0;
    result VARIANT;
BEGIN
    -- Mark existing records as non-current for the scenario
    UPDATE ATOMIC.DEMAND_FORECAST_VERSIONS
    SET 
        IS_CURRENT_VERSION = FALSE,
        UPDATED_BY_USER = P_USER,
        UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE SCENARIO_ID = P_SCENARIO_ID
      AND IS_CURRENT_VERSION = TRUE;
    
    rows_updated := SQLROWCOUNT;
    
    -- Insert new version from JSON updates
    INSERT INTO ATOMIC.DEMAND_FORECAST_VERSIONS (
        DEMAND_FORECAST_ID,
        PRODUCT_ID,
        SITE_ID,
        SCENARIO_ID,
        FORECAST_DATE,
        FORECAST_CREATION_DATE,
        FORECAST_QUANTITY,
        FORECAST_TYPE,
        FORECAST_METHOD,
        FORECAST_HORIZON,
        CONFIDENCE_LEVEL,
        FISCAL_PERIOD,
        FISCAL_MONTH,
        FISCAL_QUARTER,
        FISCAL_YEAR,
        UNIT_REVENUE,
        TOTAL_REVENUE,
        VERSION_TIMESTAMP,
        IS_CURRENT_VERSION,
        CREATED_BY_USER,
        CREATED_TIMESTAMP
    )
    SELECT
        -- Generate new IDs for the version
        (SELECT COALESCE(MAX(DEMAND_FORECAST_ID), 0) FROM ATOMIC.DEMAND_FORECAST_VERSIONS) + 
            ROW_NUMBER() OVER (ORDER BY f.value:PRODUCT_ID),
        f.value:PRODUCT_ID::NUMBER,
        f.value:SITE_ID::NUMBER,
        P_SCENARIO_ID,
        f.value:FORECAST_DATE::DATE,
        CURRENT_DATE(),
        f.value:FORECAST_QUANTITY::NUMBER(18,4),
        COALESCE(f.value:FORECAST_TYPE::VARCHAR, 'DEMAND'),
        COALESCE(f.value:FORECAST_METHOD::VARCHAR, 'MANUAL_ADJUSTMENT'),
        COALESCE(f.value:FORECAST_HORIZON::VARCHAR, 'MONTHLY'),
        f.value:CONFIDENCE_LEVEL::NUMBER(5,2),
        f.value:FISCAL_PERIOD::VARCHAR,
        f.value:FISCAL_MONTH::VARCHAR,
        f.value:FISCAL_QUARTER::VARCHAR,
        f.value:FISCAL_YEAR::NUMBER,
        f.value:UNIT_REVENUE::NUMBER(18,4),
        f.value:FORECAST_QUANTITY::NUMBER * f.value:UNIT_REVENUE::NUMBER(18,4),
        CURRENT_TIMESTAMP(),
        TRUE,
        P_USER,
        CURRENT_TIMESTAMP()
    FROM TABLE(FLATTEN(P_FORECAST_UPDATES)) f;
    
    rows_inserted := SQLROWCOUNT;
    
    result := OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'rows_superseded', rows_updated,
        'rows_inserted', rows_inserted,
        'scenario_id', P_SCENARIO_ID,
        'version_timestamp', CURRENT_TIMESTAMP(),
        'submitted_by', P_USER
    );
    
    RETURN result;
END;
$$;

COMMENT ON PROCEDURE SUBMIT_FORECAST_VERSION(VARIANT, NUMBER, VARCHAR) 
IS 'Write-back procedure for Streamlit forecast editing - inserts new version and marks previous as non-current';

-- =============================================================================
-- Procedure: CREATE_SCENARIO_COPY
-- Creates a new scenario by copying from an existing one
-- =============================================================================
CREATE OR REPLACE PROCEDURE CREATE_SCENARIO_COPY(
    P_SOURCE_SCENARIO_ID NUMBER,
    P_NEW_SCENARIO_CODE VARCHAR,
    P_NEW_SCENARIO_NAME VARCHAR,
    P_NEW_SCENARIO_DESCRIPTION VARCHAR DEFAULT NULL,
    P_USER VARCHAR DEFAULT CURRENT_USER()
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    new_scenario_id NUMBER;
    forecasts_copied INTEGER DEFAULT 0;
    result VARIANT;
BEGIN
    -- Create new scenario definition
    INSERT INTO ATOMIC.SCENARIO_DEFINITION (
        SCENARIO_ID,
        SCENARIO_CODE,
        SCENARIO_NAME,
        SCENARIO_DESCRIPTION,
        SCENARIO_TYPE,
        IS_OFFICIAL_BASELINE,
        VERSION_NUMBER,
        PARENT_SCENARIO_ID,
        CREATED_BY_USER,
        CREATED_TIMESTAMP
    )
    SELECT
        (SELECT COALESCE(MAX(SCENARIO_ID), 0) + 1 FROM ATOMIC.SCENARIO_DEFINITION),
        P_NEW_SCENARIO_CODE,
        P_NEW_SCENARIO_NAME,
        COALESCE(P_NEW_SCENARIO_DESCRIPTION, 'Copy of ' || SCENARIO_NAME),
        'CUSTOM',
        FALSE,
        1,
        P_SOURCE_SCENARIO_ID,
        P_USER,
        CURRENT_TIMESTAMP()
    FROM ATOMIC.SCENARIO_DEFINITION
    WHERE SCENARIO_ID = P_SOURCE_SCENARIO_ID;
    
    new_scenario_id := (SELECT MAX(SCENARIO_ID) FROM ATOMIC.SCENARIO_DEFINITION);
    
    -- Copy all forecasts from source scenario
    INSERT INTO ATOMIC.DEMAND_FORECAST_VERSIONS (
        DEMAND_FORECAST_ID,
        PRODUCT_ID,
        SITE_ID,
        SCENARIO_ID,
        FORECAST_DATE,
        FORECAST_CREATION_DATE,
        FORECAST_QUANTITY,
        FORECAST_TYPE,
        FORECAST_METHOD,
        FORECAST_HORIZON,
        CONFIDENCE_LEVEL,
        FISCAL_PERIOD,
        FISCAL_MONTH,
        FISCAL_QUARTER,
        FISCAL_YEAR,
        UNIT_REVENUE,
        TOTAL_REVENUE,
        VERSION_TIMESTAMP,
        IS_CURRENT_VERSION,
        CREATED_BY_USER,
        CREATED_TIMESTAMP
    )
    SELECT
        (SELECT COALESCE(MAX(DEMAND_FORECAST_ID), 0) FROM ATOMIC.DEMAND_FORECAST_VERSIONS) + 
            ROW_NUMBER() OVER (ORDER BY DEMAND_FORECAST_ID),
        PRODUCT_ID,
        SITE_ID,
        new_scenario_id,
        FORECAST_DATE,
        CURRENT_DATE(),
        FORECAST_QUANTITY,
        FORECAST_TYPE,
        FORECAST_METHOD,
        FORECAST_HORIZON,
        CONFIDENCE_LEVEL,
        FISCAL_PERIOD,
        FISCAL_MONTH,
        FISCAL_QUARTER,
        FISCAL_YEAR,
        UNIT_REVENUE,
        TOTAL_REVENUE,
        CURRENT_TIMESTAMP(),
        TRUE,
        P_USER,
        CURRENT_TIMESTAMP()
    FROM ATOMIC.DEMAND_FORECAST_VERSIONS
    WHERE SCENARIO_ID = P_SOURCE_SCENARIO_ID
      AND IS_CURRENT_VERSION = TRUE;
    
    forecasts_copied := SQLROWCOUNT;
    
    result := OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'new_scenario_id', new_scenario_id,
        'new_scenario_code', P_NEW_SCENARIO_CODE,
        'source_scenario_id', P_SOURCE_SCENARIO_ID,
        'forecasts_copied', forecasts_copied,
        'created_by', P_USER
    );
    
    RETURN result;
END;
$$;

COMMENT ON PROCEDURE CREATE_SCENARIO_COPY(NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR) 
IS 'Creates a new scenario by copying forecasts from an existing scenario';

-- =============================================================================
-- Procedure: LOAD_STAGED_DATA
-- Loads CSV files from DATA_STAGE to RAW tables
-- =============================================================================
CREATE OR REPLACE PROCEDURE LOAD_STAGED_DATA()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    total_rows INTEGER DEFAULT 0;
    table_rows INTEGER;
BEGIN
    -- Load Product Categories
    COPY INTO RAW.PRODUCT_CATEGORY_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*product_categories.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Products
    COPY INTO RAW.PRODUCT_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*products.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Sites
    COPY INTO RAW.SITE_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*sites.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Work Centers
    COPY INTO RAW.WORK_CENTER_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*work_centers.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Warehouse Zones
    COPY INTO RAW.WAREHOUSE_ZONE_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*warehouse_zones.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Scenario Definitions
    COPY INTO RAW.SCENARIO_DEFINITION_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*scenario_definitions.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Demand Forecasts
    COPY INTO RAW.DEMAND_FORECAST_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*demand_forecasts.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Logistics Costs
    COPY INTO RAW.LOGISTICS_COST_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*logistics_costs.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    -- Load Inventory Balances
    COPY INTO RAW.INVENTORY_BALANCE_STAGE
    FROM @RAW.DATA_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'RAW.CSV_FORMAT')
    PATTERN = '.*inventory_balances.csv'
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    table_rows := SQLROWCOUNT;
    total_rows := total_rows + table_rows;
    
    RETURN 'Loaded ' || total_rows || ' total rows from staged CSV files';
END;
$$;

COMMENT ON PROCEDURE LOAD_STAGED_DATA() 
IS 'Loads CSV files from DATA_STAGE into RAW schema staging tables';

-- =============================================================================
-- Procedure: TRANSFORM_RAW_TO_ATOMIC
-- ETL procedure to transform RAW data to ATOMIC layer
-- Note: TRUNCATE ensures idempotent loads - running deploy.sh multiple times
--       will not create duplicate records
-- =============================================================================
CREATE OR REPLACE PROCEDURE TRANSFORM_RAW_TO_ATOMIC()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    step_count INTEGER DEFAULT 0;
BEGIN
    -- Truncate all ATOMIC tables to ensure idempotent loads
    -- This prevents duplicate records when deploy.sh is run multiple times
    TRUNCATE TABLE ATOMIC.INVENTORY_BALANCE;
    TRUNCATE TABLE ATOMIC.LOGISTICS_COST_FACT;
    TRUNCATE TABLE ATOMIC.DEMAND_FORECAST_VERSIONS;
    TRUNCATE TABLE ATOMIC.SCENARIO_DEFINITION;
    TRUNCATE TABLE ATOMIC.WAREHOUSE_ZONE;
    TRUNCATE TABLE ATOMIC.WORK_CENTER;
    TRUNCATE TABLE ATOMIC.SITE;
    TRUNCATE TABLE ATOMIC.PRODUCT;
    TRUNCATE TABLE ATOMIC.PRODUCT_CATEGORY;
    
    -- Step 1: Product Categories
    INSERT INTO ATOMIC.PRODUCT_CATEGORY (
        PRODUCT_CATEGORY_ID, PRODUCT_CATEGORY_CODE, CATEGORY_NAME, 
        CATEGORY_DESCRIPTION, PARENT_CATEGORY_ID, CATEGORY_LEVEL,
        VALID_FROM_TIMESTAMP, IS_CURRENT_FLAG
    )
    SELECT
        TRY_TO_NUMBER(PRODUCT_CATEGORY_ID),
        PRODUCT_CATEGORY_CODE,
        CATEGORY_NAME,
        CATEGORY_DESCRIPTION,
        TRY_TO_NUMBER(PARENT_CATEGORY_ID),
        TRY_TO_NUMBER(CATEGORY_LEVEL),
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.PRODUCT_CATEGORY_STAGE
    WHERE TRY_TO_NUMBER(PRODUCT_CATEGORY_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 2: Products
    INSERT INTO ATOMIC.PRODUCT (
        PRODUCT_ID, PRODUCT_CODE, PRODUCT_NAME, PRODUCT_DESCRIPTION_SHORT,
        PRODUCT_TYPE, PRODUCT_STATUS, PRODUCT_CATEGORY_ID, UNIT_PRICE, UNIT_COST,
        VALID_FROM_TIMESTAMP, IS_CURRENT_FLAG
    )
    SELECT
        TRY_TO_NUMBER(PRODUCT_ID),
        PRODUCT_CODE,
        PRODUCT_NAME,
        PRODUCT_DESCRIPTION_SHORT,
        PRODUCT_TYPE,
        PRODUCT_STATUS,
        TRY_TO_NUMBER(PRODUCT_CATEGORY_ID),
        TRY_TO_NUMBER(UNIT_PRICE),
        TRY_TO_NUMBER(UNIT_COST),
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.PRODUCT_STAGE
    WHERE TRY_TO_NUMBER(PRODUCT_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 3: Sites
    INSERT INTO ATOMIC.SITE (
        SITE_ID, SITE_CODE, SITE_NAME, SITE_TYPE, REGION,
        ADDRESS_LINE_1, CITY, STATE_PROVINCE, COUNTRY,
        OPERATING_STATUS, CAPACITY_METRIC, CAPACITY_UNIT_OF_MEASURE,
        VALID_FROM_TIMESTAMP, IS_CURRENT_FLAG
    )
    SELECT
        TRY_TO_NUMBER(SITE_ID),
        SITE_CODE,
        SITE_NAME,
        SITE_TYPE,
        REGION,
        ADDRESS_LINE_1,
        CITY,
        STATE_PROVINCE,
        COUNTRY,
        OPERATING_STATUS,
        TRY_TO_NUMBER(CAPACITY_METRIC),
        CAPACITY_UNIT_OF_MEASURE,
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.SITE_STAGE
    WHERE TRY_TO_NUMBER(SITE_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 4: Work Centers
    INSERT INTO ATOMIC.WORK_CENTER (
        WORK_CENTER_ID, WORK_CENTER_CODE, WORK_CENTER_NAME, WORK_CENTER_DESCRIPTION,
        WORK_CENTER_TYPE, SITE_ID, CAPACITY_PER_HOUR, HOURS_PER_DAY,
        AVAILABLE_SHIFTS, EFFICIENCY_FACTOR,
        VALID_FROM_TIMESTAMP, IS_CURRENT_FLAG
    )
    SELECT
        TRY_TO_NUMBER(WORK_CENTER_ID),
        WORK_CENTER_CODE,
        WORK_CENTER_NAME,
        WORK_CENTER_DESCRIPTION,
        WORK_CENTER_TYPE,
        TRY_TO_NUMBER(SITE_ID),
        TRY_TO_NUMBER(CAPACITY_PER_HOUR),
        TRY_TO_NUMBER(HOURS_PER_DAY),
        TRY_TO_NUMBER(AVAILABLE_SHIFTS),
        TRY_TO_NUMBER(EFFICIENCY_FACTOR),
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.WORK_CENTER_STAGE
    WHERE TRY_TO_NUMBER(WORK_CENTER_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 5: Warehouse Zones
    INSERT INTO ATOMIC.WAREHOUSE_ZONE (
        WAREHOUSE_ZONE_ID, ZONE_CODE, ZONE_NAME, SITE_ID,
        ZONE_TYPE, ZONE_TEMPERATURE_CONTROL,
        MAX_CAPACITY_PALLETS, CURRENT_OCCUPANCY_PALLETS,
        VALID_FROM_TIMESTAMP, IS_CURRENT_FLAG
    )
    SELECT
        TRY_TO_NUMBER(WAREHOUSE_ZONE_ID),
        ZONE_CODE,
        ZONE_NAME,
        TRY_TO_NUMBER(SITE_ID),
        ZONE_TYPE,
        ZONE_TEMPERATURE_CONTROL,
        TRY_TO_NUMBER(MAX_CAPACITY_PALLETS),
        TRY_TO_NUMBER(CURRENT_OCCUPANCY_PALLETS),
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.WAREHOUSE_ZONE_STAGE
    WHERE TRY_TO_NUMBER(WAREHOUSE_ZONE_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 6: Scenario Definitions
    INSERT INTO ATOMIC.SCENARIO_DEFINITION (
        SCENARIO_ID, SCENARIO_CODE, SCENARIO_NAME, SCENARIO_DESCRIPTION,
        SCENARIO_TYPE, IS_OFFICIAL_BASELINE, CREATED_BY_USER
    )
    SELECT
        TRY_TO_NUMBER(SCENARIO_ID),
        SCENARIO_CODE,
        SCENARIO_NAME,
        SCENARIO_DESCRIPTION,
        SCENARIO_TYPE,
        TRY_TO_BOOLEAN(IS_OFFICIAL_BASELINE),
        CREATED_BY_USER
    FROM RAW.SCENARIO_DEFINITION_STAGE
    WHERE TRY_TO_NUMBER(SCENARIO_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 7: Demand Forecasts (into Hybrid Table)
    INSERT INTO ATOMIC.DEMAND_FORECAST_VERSIONS (
        DEMAND_FORECAST_ID, PRODUCT_ID, SITE_ID, SCENARIO_ID,
        FORECAST_DATE, FORECAST_CREATION_DATE, FORECAST_QUANTITY,
        FORECAST_TYPE, FORECAST_METHOD, FORECAST_HORIZON, CONFIDENCE_LEVEL,
        FISCAL_PERIOD, FISCAL_MONTH, FISCAL_QUARTER, FISCAL_YEAR,
        UNIT_REVENUE, TOTAL_REVENUE, VERSION_TIMESTAMP, IS_CURRENT_VERSION
    )
    SELECT
        TRY_TO_NUMBER(DEMAND_FORECAST_ID),
        TRY_TO_NUMBER(PRODUCT_ID),
        TRY_TO_NUMBER(SITE_ID),
        TRY_TO_NUMBER(SCENARIO_ID),
        TRY_TO_DATE(FORECAST_DATE),
        TRY_TO_DATE(FORECAST_CREATION_DATE),
        TRY_TO_NUMBER(FORECAST_QUANTITY),
        FORECAST_TYPE,
        FORECAST_METHOD,
        FORECAST_HORIZON,
        TRY_TO_NUMBER(CONFIDENCE_LEVEL),
        FISCAL_PERIOD,
        FISCAL_MONTH,
        FISCAL_QUARTER,
        TRY_TO_NUMBER(FISCAL_YEAR),
        TRY_TO_NUMBER(UNIT_REVENUE),
        TRY_TO_NUMBER(FORECAST_QUANTITY) * TRY_TO_NUMBER(UNIT_REVENUE),
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.DEMAND_FORECAST_STAGE
    WHERE TRY_TO_NUMBER(DEMAND_FORECAST_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 8: Logistics Costs
    INSERT INTO ATOMIC.LOGISTICS_COST_FACT (
        LOGISTICS_COST_ID, SITE_ID, WAREHOUSE_ZONE_ID,
        STORAGE_COST_PER_PALLET_PER_DAY, TRANSPORT_COST_PER_UNIT,
        HANDLING_COST_PER_PALLET, OVERFLOW_PENALTY_RATE, OVERFLOW_PENALTY_THRESHOLD_PCT,
        EFFECTIVE_DATE, EXPIRY_DATE, VENDOR_NAME, CONTRACT_REFERENCE,
        VALID_FROM_TIMESTAMP, IS_CURRENT_FLAG
    )
    SELECT
        TRY_TO_NUMBER(LOGISTICS_COST_ID),
        TRY_TO_NUMBER(SITE_ID),
        TRY_TO_NUMBER(WAREHOUSE_ZONE_ID),
        TRY_TO_NUMBER(STORAGE_COST_PER_PALLET_PER_DAY),
        TRY_TO_NUMBER(TRANSPORT_COST_PER_UNIT),
        TRY_TO_NUMBER(HANDLING_COST_PER_PALLET),
        TRY_TO_NUMBER(OVERFLOW_PENALTY_RATE),
        TRY_TO_NUMBER(OVERFLOW_PENALTY_THRESHOLD_PCT),
        TRY_TO_DATE(EFFECTIVE_DATE),
        TRY_TO_DATE(EXPIRY_DATE),
        VENDOR_NAME,
        CONTRACT_REFERENCE,
        CURRENT_TIMESTAMP(),
        TRUE
    FROM RAW.LOGISTICS_COST_STAGE
    WHERE TRY_TO_NUMBER(LOGISTICS_COST_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    -- Step 9: Inventory Balances
    INSERT INTO ATOMIC.INVENTORY_BALANCE (
        INVENTORY_BALANCE_ID, PRODUCT_ID, SITE_ID, WAREHOUSE_ZONE_ID,
        BALANCE_DATE, QUANTITY_ON_HAND, QUANTITY_RESERVED, QUANTITY_AVAILABLE,
        UNIT_OF_MEASURE
    )
    SELECT
        TRY_TO_NUMBER(INVENTORY_BALANCE_ID),
        TRY_TO_NUMBER(PRODUCT_ID),
        TRY_TO_NUMBER(SITE_ID),
        TRY_TO_NUMBER(WAREHOUSE_ZONE_ID),
        TRY_TO_DATE(BALANCE_DATE),
        TRY_TO_NUMBER(QUANTITY_ON_HAND),
        TRY_TO_NUMBER(QUANTITY_RESERVED),
        TRY_TO_NUMBER(QUANTITY_AVAILABLE),
        UNIT_OF_MEASURE
    FROM RAW.INVENTORY_BALANCE_STAGE
    WHERE TRY_TO_NUMBER(INVENTORY_BALANCE_ID) IS NOT NULL;
    step_count := step_count + 1;
    
    RETURN 'Completed ' || step_count || ' transformation steps from RAW to ATOMIC';
END;
$$;

COMMENT ON PROCEDURE TRANSFORM_RAW_TO_ATOMIC() 
IS 'ETL procedure to transform RAW staging data to ATOMIC layer - TRUNCATES tables first for idempotent loads';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW PROCEDURES IN SCHEMA ATOMIC;
SHOW PROCEDURES IN SCHEMA SOP_LOGISTICS;

