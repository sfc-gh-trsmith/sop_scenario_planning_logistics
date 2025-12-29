-- =============================================================================
-- 01_account_setup.sql
-- Account-level setup for S&OP Scenario Planning & Logistics Optimization
-- 
-- Creates:
--   - Role: SOP_SCENARIO_PLANNING_LOGISTICS_ROLE
--   - Warehouse: SOP_SCENARIO_PLANNING_LOGISTICS_WH
--   - Database: SOP_SCENARIO_PLANNING_LOGISTICS
--   - Compute Pool: SOP_SCENARIO_PLANNING_LOGISTICS_COMPUTE_POOL (for notebooks)
--
-- Usage: Run with ACCOUNTADMIN role
-- =============================================================================

-- Use session variables set by deploy.sh
-- Expected variables: FULL_PREFIX, PROJECT_ROLE, PROJECT_WH, PROJECT_COMPUTE_POOL

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- Step 1: Create Role
-- =============================================================================
CREATE ROLE IF NOT EXISTS IDENTIFIER($PROJECT_ROLE);

GRANT ROLE IDENTIFIER($PROJECT_ROLE) TO ROLE SYSADMIN;

-- =============================================================================
-- Step 2: Create Warehouse
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($PROJECT_WH)
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for S&OP Scenario Planning & Logistics demo';

GRANT USAGE ON WAREHOUSE IDENTIFIER($PROJECT_WH) TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT OPERATE ON WAREHOUSE IDENTIFIER($PROJECT_WH) TO ROLE IDENTIFIER($PROJECT_ROLE);

-- =============================================================================
-- Step 3: Create Database
-- =============================================================================
CREATE DATABASE IF NOT EXISTS IDENTIFIER($FULL_PREFIX)
    COMMENT = 'S&OP Integrated Scenario Planning & Logistics Optimization demo';

GRANT OWNERSHIP ON DATABASE IDENTIFIER($FULL_PREFIX) TO ROLE IDENTIFIER($PROJECT_ROLE) COPY CURRENT GRANTS;

-- =============================================================================
-- Step 4: Grant Cortex Privileges
-- =============================================================================
-- Grant Cortex user privileges for Cortex Analyst and Cortex Search
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE IDENTIFIER($PROJECT_ROLE);

-- =============================================================================
-- Step 5: Create Compute Pool for Notebooks (Optional - for ML notebook)
-- =============================================================================
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($PROJECT_COMPUTE_POOL)
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_SUSPEND_SECS = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Compute pool for S&OP optimization notebook';

GRANT USAGE ON COMPUTE POOL IDENTIFIER($PROJECT_COMPUTE_POOL) TO ROLE IDENTIFIER($PROJECT_ROLE);
GRANT MONITOR ON COMPUTE POOL IDENTIFIER($PROJECT_COMPUTE_POOL) TO ROLE IDENTIFIER($PROJECT_ROLE);

-- =============================================================================
-- Step 6: Grant CREATE STREAMLIT privilege
-- =============================================================================
-- This will be granted on the specific schema after schema creation

-- =============================================================================
-- Verification
-- =============================================================================
SHOW ROLES LIKE '%SOP_SCENARIO_PLANNING_LOGISTICS%';
SHOW WAREHOUSES LIKE '%SOP_SCENARIO_PLANNING_LOGISTICS%';
SHOW DATABASES LIKE '%SOP_SCENARIO_PLANNING_LOGISTICS%';

