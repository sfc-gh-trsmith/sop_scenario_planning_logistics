-- =============================================================================
-- 07_semantic_model.sql
-- Cortex Analyst semantic model setup for S&OP Scenario Planning
-- 
-- The semantic model YAML is uploaded to @SOP_LOGISTICS.MODELS stage
-- This script creates helper objects for semantic model testing
--
-- Note: The actual YAML file (sop_analytics_semantic.yaml) should be
-- uploaded to the stage via deploy.sh using PUT command
--
-- Usage: Run after 05_data_mart.sql
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);
USE SCHEMA SOP_LOGISTICS;

-- =============================================================================
-- Step 1: Verify Stage Exists
-- =============================================================================
-- The MODELS stage was created in 02_schema_setup.sql
SHOW STAGES LIKE 'MODELS' IN SCHEMA SOP_LOGISTICS;

-- =============================================================================
-- Step 2: Create Semantic Model Validation View
-- This view helps validate that the semantic model references correct tables
-- =============================================================================
CREATE OR REPLACE VIEW SOP_LOGISTICS.SEMANTIC_MODEL_TABLES_V AS
SELECT 
    'SCENARIO_COMPARISON_V' AS TABLE_NAME,
    'SOP_LOGISTICS' AS SCHEMA_NAME,
    'VIEW' AS OBJECT_TYPE,
    (SELECT COUNT(*) FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V) AS ROW_COUNT,
    'Main fact table for scenario analysis' AS DESCRIPTION
UNION ALL
SELECT 
    'DT_SCENARIO_KPI_SUMMARY' AS TABLE_NAME,
    'SOP_LOGISTICS' AS SCHEMA_NAME,
    'DYNAMIC TABLE' AS OBJECT_TYPE,
    (SELECT COUNT(*) FROM SOP_LOGISTICS.DT_SCENARIO_KPI_SUMMARY) AS ROW_COUNT,
    'Aggregated KPIs by scenario' AS DESCRIPTION
UNION ALL
SELECT 
    'WAREHOUSE_UTILIZATION_PROJECTION' AS TABLE_NAME,
    'SOP_LOGISTICS' AS SCHEMA_NAME,
    'VIEW' AS OBJECT_TYPE,
    (SELECT COUNT(*) FROM SOP_LOGISTICS.WAREHOUSE_UTILIZATION_PROJECTION) AS ROW_COUNT,
    'Warehouse capacity projections' AS DESCRIPTION
UNION ALL
SELECT 
    'INVENTORY_BUILDUP_CURVE' AS TABLE_NAME,
    'SOP_LOGISTICS' AS SCHEMA_NAME,
    'VIEW' AS OBJECT_TYPE,
    (SELECT COUNT(*) FROM SOP_LOGISTICS.INVENTORY_BUILDUP_CURVE) AS ROW_COUNT,
    'Time-series inventory positions' AS DESCRIPTION;

COMMENT ON VIEW SOP_LOGISTICS.SEMANTIC_MODEL_TABLES_V IS 'Lists tables referenced by Cortex Analyst semantic model with row counts';

-- =============================================================================
-- Step 3: Create Golden Query Validation Table
-- Stores expected results for verified queries in the semantic model
-- =============================================================================
CREATE OR REPLACE TABLE SOP_LOGISTICS.SEMANTIC_MODEL_GOLDEN_QUERIES (
    QUERY_ID NUMBER(38,0) AUTOINCREMENT,
    QUERY_NAME VARCHAR(200) NOT NULL,
    NATURAL_LANGUAGE_QUESTION VARCHAR(1000) NOT NULL,
    EXPECTED_SQL VARCHAR(4000) NOT NULL,
    EXPECTED_RESULT_DESCRIPTION VARCHAR(1000),
    LAST_VALIDATED_TIMESTAMP TIMESTAMP_NTZ,
    VALIDATION_STATUS VARCHAR(50),  -- 'PASSED', 'FAILED', 'NOT_RUN'
    
    PRIMARY KEY (QUERY_ID)
);

COMMENT ON TABLE SOP_LOGISTICS.SEMANTIC_MODEL_GOLDEN_QUERIES IS 'Golden queries for validating Cortex Analyst responses';

-- Insert the golden query from DRD
INSERT INTO SOP_LOGISTICS.SEMANTIC_MODEL_GOLDEN_QUERIES (
    QUERY_NAME, 
    NATURAL_LANGUAGE_QUESTION, 
    EXPECTED_SQL, 
    EXPECTED_RESULT_DESCRIPTION,
    VALIDATION_STATUS
)
VALUES (
    'warehousing_cost_by_scenario_october',
    'Compare the total warehousing cost between Baseline and Q4 Push for October',
    'SELECT SCENARIO_CODE, SUM(PROJECTED_WAREHOUSING_COST) AS TOTAL_WAREHOUSING_COST ' ||
    'FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V ' ||
    'WHERE FISCAL_MONTH = ''October'' ' ||
    'GROUP BY SCENARIO_CODE ' ||
    'ORDER BY SCENARIO_CODE;',
    'Returns two rows: BASELINE and Q4_PUSH with their respective warehousing costs for October. Q4_PUSH should be ~20% higher.',
    'NOT_RUN'
);

-- Add more golden queries for validation
INSERT INTO SOP_LOGISTICS.SEMANTIC_MODEL_GOLDEN_QUERIES (
    QUERY_NAME, 
    NATURAL_LANGUAGE_QUESTION, 
    EXPECTED_SQL, 
    EXPECTED_RESULT_DESCRIPTION,
    VALIDATION_STATUS
)
VALUES 
(
    'margin_impact_q4_push',
    'What is the margin impact of the Q4 Marketing Push compared to Baseline?',
    'SELECT SCENARIO_CODE, TOTAL_GROSS_MARGIN, GROSS_MARGIN_PCT ' ||
    'FROM SOP_LOGISTICS.DT_SCENARIO_KPI_SUMMARY ' ||
    'ORDER BY SCENARIO_CODE;',
    'Returns gross margin for each scenario to enable comparison',
    'NOT_RUN'
),
(
    'warehouse_overflow_risk',
    'Which warehouses are at risk of overflow in the Q4 Push scenario?',
    'SELECT ZONE_NAME, SITE_NAME, REGION, PROJECTED_UTILIZATION_PCT ' ||
    'FROM SOP_LOGISTICS.WAREHOUSE_UTILIZATION_PROJECTION ' ||
    'WHERE SCENARIO_CODE = ''Q4_PUSH'' AND IS_OVERFLOW_RISK = TRUE ' ||
    'ORDER BY PROJECTED_UTILIZATION_PCT DESC;',
    'Returns list of warehouse zones with overflow risk, likely including Northeast DC',
    'NOT_RUN'
),
(
    'revenue_by_product_family',
    'Show total revenue by product family for each scenario',
    'SELECT SCENARIO_CODE, PRODUCT_FAMILY, SUM(TOTAL_REVENUE) AS TOTAL_REVENUE ' ||
    'FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V ' ||
    'GROUP BY SCENARIO_CODE, PRODUCT_FAMILY ' ||
    'ORDER BY SCENARIO_CODE, TOTAL_REVENUE DESC;',
    'Returns revenue breakdown by product family for scenario comparison',
    'NOT_RUN'
);

-- =============================================================================
-- Step 4: Create Semantic Model Test Procedure
-- Tests Cortex Analyst against golden queries
-- =============================================================================
CREATE OR REPLACE PROCEDURE SOP_LOGISTICS.TEST_SEMANTIC_MODEL(
    SEMANTIC_MODEL_FILE VARCHAR DEFAULT '@SOP_LOGISTICS.MODELS/sop_analytics_semantic.yaml'
)
RETURNS TABLE (
    QUERY_NAME VARCHAR,
    QUESTION VARCHAR,
    STATUS VARCHAR,
    MESSAGE VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    result_set RESULTSET;
BEGIN
    -- Note: This is a placeholder procedure
    -- Actual Cortex Analyst testing requires the REST API or Streamlit integration
    -- This procedure returns the golden queries for manual validation
    
    result_set := (
        SELECT 
            QUERY_NAME,
            NATURAL_LANGUAGE_QUESTION AS QUESTION,
            VALIDATION_STATUS AS STATUS,
            EXPECTED_RESULT_DESCRIPTION AS MESSAGE
        FROM SOP_LOGISTICS.SEMANTIC_MODEL_GOLDEN_QUERIES
    );
    
    RETURN TABLE(result_set);
END;
$$;

COMMENT ON PROCEDURE SOP_LOGISTICS.TEST_SEMANTIC_MODEL(VARCHAR) IS 'Returns golden queries for semantic model validation';

-- =============================================================================
-- Step 5: Create Helper Function for Semantic Model Path
-- Returns the full path to the semantic model file
-- =============================================================================
CREATE OR REPLACE FUNCTION SOP_LOGISTICS.GET_SEMANTIC_MODEL_PATH()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT CONCAT(
        '@',
        CURRENT_DATABASE(),
        '.SOP_LOGISTICS.MODELS/sop_analytics_semantic.yaml'
    )
$$;

COMMENT ON FUNCTION SOP_LOGISTICS.GET_SEMANTIC_MODEL_PATH() IS 'Returns the stage path for the semantic model YAML';

-- =============================================================================
-- Verification
-- =============================================================================
SELECT SOP_LOGISTICS.GET_SEMANTIC_MODEL_PATH() AS SEMANTIC_MODEL_PATH;
SELECT * FROM SOP_LOGISTICS.SEMANTIC_MODEL_GOLDEN_QUERIES;

