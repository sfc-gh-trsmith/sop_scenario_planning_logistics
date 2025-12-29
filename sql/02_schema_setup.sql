-- =============================================================================
-- 02_schema_setup.sql
-- Schema setup for S&OP Scenario Planning & Logistics Optimization
-- 
-- Creates three-layer architecture:
--   - RAW: Landing zone for staged file data
--   - ATOMIC: Enterprise relational model (core entities + extensions)
--   - SOP_LOGISTICS: Consumer-facing data mart with views and dynamic tables
--
-- Also creates:
--   - Internal stages for data loading and semantic models
--   - File formats for CSV loading
--
-- Usage: Run after 01_account_setup.sql
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);

-- =============================================================================
-- Step 1: Create Schemas
-- =============================================================================

-- RAW Schema: Landing zone for external data
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Landing zone for staged file data - preserves original format';

-- ATOMIC Schema: Enterprise relational model
CREATE SCHEMA IF NOT EXISTS ATOMIC
    COMMENT = 'Enterprise relational model - normalized canonical entities';

-- SOP_LOGISTICS Schema: Consumer-facing data mart
CREATE SCHEMA IF NOT EXISTS SOP_LOGISTICS
    COMMENT = 'Consumer-facing data products for S&OP scenario planning';

-- =============================================================================
-- Step 2: Create Internal Stages
-- =============================================================================

USE SCHEMA RAW;

-- Stage for loading CSV data files
CREATE STAGE IF NOT EXISTS DATA_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for loading synthetic data CSV files';

-- Stage for PDF documents (for Cortex Search)
CREATE STAGE IF NOT EXISTS DOCS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Internal stage for PDF/DOCX documents for Cortex Search';

USE SCHEMA SOP_LOGISTICS;

-- Stage for semantic model YAML files (Cortex Analyst)
CREATE STAGE IF NOT EXISTS MODELS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for Cortex Analyst semantic model YAML files';

-- =============================================================================
-- Step 3: Create File Formats
-- =============================================================================

USE SCHEMA RAW;

-- CSV file format for data loading
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format for synthetic data loading with header parsing';

-- =============================================================================
-- Step 4: Grant Privileges
-- =============================================================================

-- Grant schema privileges
GRANT ALL PRIVILEGES ON SCHEMA RAW TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT ALL PRIVILEGES ON SCHEMA ATOMIC TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT ALL PRIVILEGES ON SCHEMA SOP_LOGISTICS TO ROLE IDENTIFIER($PROJECT_ROLE);

-- Grant stage privileges
GRANT READ, WRITE ON STAGE RAW.DATA_STAGE TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT READ, WRITE ON STAGE RAW.DOCS_STAGE TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT READ, WRITE ON STAGE SOP_LOGISTICS.MODELS TO ROLE IDENTIFIER($PROJECT_ROLE);

-- Grant future privileges on tables and views
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RAW TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ATOMIC TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA SOP_LOGISTICS TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA SOP_LOGISTICS TO ROLE IDENTIFIER($PROJECT_ROLE);

-- Grant CREATE STREAMLIT on the data mart schema
GRANT CREATE STREAMLIT ON SCHEMA SOP_LOGISTICS TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT CREATE STAGE ON SCHEMA SOP_LOGISTICS TO ROLE IDENTIFIER($PROJECT_ROLE);

-- =============================================================================
-- Verification
-- =============================================================================
SHOW SCHEMAS IN DATABASE IDENTIFIER($FULL_PREFIX);
SHOW STAGES IN SCHEMA RAW;
SHOW STAGES IN SCHEMA SOP_LOGISTICS;
SHOW FILE FORMATS IN SCHEMA RAW;

