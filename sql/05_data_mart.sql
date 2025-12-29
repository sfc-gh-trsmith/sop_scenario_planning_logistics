-- =============================================================================
-- 05_data_mart.sql
-- SOP_LOGISTICS data mart for S&OP Scenario Planning & Logistics Optimization
-- 
-- Consumer-facing views and tables optimized for:
--   - Executive Dashboard (VP of Supply Chain)
--   - Scenario Builder (Demand Planner)
--   - Capacity Analysis (Logistics Analyst)
--
-- Tables/Views created:
--   - SCENARIO_COMPARISON_V: Denormalized view for scenario analysis
--   - WAREHOUSE_UTILIZATION_PROJECTION: Projected warehouse capacity by scenario
--   - PRODUCTION_CAPACITY_SUMMARY: Work center capacity aggregation
--   - RECOMMENDED_BUILD_PLAN: ML output (empty table, populated by notebook)
--   - INVENTORY_BUILDUP_CURVE: Time-series for "camel hump" visualization
--
-- Dynamic Tables:
--   - DT_SCENARIO_KPI_SUMMARY: Auto-refreshed KPI aggregations
--
-- Usage: Run after 04_atomic_tables.sql
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);
USE SCHEMA SOP_LOGISTICS;

-- =============================================================================
-- Scenario Comparison View (for Executive Dashboard and Cortex Analyst)
-- Denormalized view combining forecasts with scenario and product details
-- =============================================================================
CREATE OR REPLACE VIEW SCENARIO_COMPARISON_V AS
SELECT
    -- Scenario dimensions
    sd.SCENARIO_ID,
    sd.SCENARIO_CODE,
    sd.SCENARIO_NAME,
    sd.SCENARIO_TYPE,
    sd.IS_OFFICIAL_BASELINE,
    
    -- Product dimensions
    df.PRODUCT_ID,
    p.PRODUCT_CODE,
    p.PRODUCT_NAME,
    pc.CATEGORY_NAME AS PRODUCT_FAMILY,
    
    -- Site dimensions
    df.SITE_ID,
    s.SITE_CODE,
    s.SITE_NAME,
    s.SITE_TYPE,
    s.REGION,
    
    -- Time dimensions
    df.FORECAST_DATE,
    df.FISCAL_PERIOD,
    df.FISCAL_MONTH,
    df.FISCAL_QUARTER,
    df.FISCAL_YEAR,
    
    -- Forecast metrics
    df.FORECAST_QUANTITY,
    df.CONFIDENCE_LEVEL,
    df.FORECAST_TYPE,
    
    -- Financial metrics
    df.UNIT_REVENUE,
    df.TOTAL_REVENUE,
    p.UNIT_COST,
    df.FORECAST_QUANTITY * p.UNIT_COST AS TOTAL_COST,
    df.TOTAL_REVENUE - (df.FORECAST_QUANTITY * p.UNIT_COST) AS GROSS_MARGIN,
    
    -- Warehousing cost estimates (join with logistics costs)
    lc.STORAGE_COST_PER_PALLET_PER_DAY,
    lc.OVERFLOW_PENALTY_RATE,
    lc.OVERFLOW_PENALTY_THRESHOLD_PCT,
    
    -- Projected warehousing cost (simplified: assume 30-day storage, 10 units per pallet)
    ROUND(
        (df.FORECAST_QUANTITY / 10) * lc.STORAGE_COST_PER_PALLET_PER_DAY * 30, 
        2
    ) AS PROJECTED_WAREHOUSING_COST,
    
    -- Version tracking
    df.VERSION_TIMESTAMP,
    df.IS_CURRENT_VERSION

FROM ATOMIC.DEMAND_FORECAST_VERSIONS df
JOIN ATOMIC.SCENARIO_DEFINITION sd ON df.SCENARIO_ID = sd.SCENARIO_ID
JOIN ATOMIC.PRODUCT p ON df.PRODUCT_ID = p.PRODUCT_ID AND p.IS_CURRENT_FLAG = TRUE
LEFT JOIN ATOMIC.PRODUCT_CATEGORY pc ON p.PRODUCT_CATEGORY_ID = pc.PRODUCT_CATEGORY_ID AND pc.IS_CURRENT_FLAG = TRUE
JOIN ATOMIC.SITE s ON df.SITE_ID = s.SITE_ID AND s.IS_CURRENT_FLAG = TRUE
LEFT JOIN ATOMIC.LOGISTICS_COST_FACT lc ON s.SITE_ID = lc.SITE_ID AND lc.IS_CURRENT_FLAG = TRUE
WHERE df.IS_CURRENT_VERSION = TRUE;

COMMENT ON VIEW SCENARIO_COMPARISON_V IS 'Denormalized view for S&OP scenario comparison - supports Cortex Analyst queries';

-- =============================================================================
-- Warehouse Utilization Projection View (for Capacity Analysis)
-- =============================================================================
CREATE OR REPLACE VIEW WAREHOUSE_UTILIZATION_PROJECTION AS
SELECT
    -- Warehouse dimensions
    wz.WAREHOUSE_ZONE_ID,
    wz.ZONE_CODE,
    wz.ZONE_NAME,
    wz.ZONE_TYPE,
    s.SITE_ID,
    s.SITE_CODE,
    s.SITE_NAME,
    s.REGION,
    
    -- Capacity metrics
    wz.MAX_CAPACITY_PALLETS,
    wz.CURRENT_OCCUPANCY_PALLETS,
    
    -- Scenario dimensions
    sd.SCENARIO_ID,
    sd.SCENARIO_CODE,
    sd.SCENARIO_NAME,
    
    -- Time dimensions
    df.FISCAL_MONTH,
    df.FISCAL_QUARTER,
    
    -- Projected inventory (aggregate forecasts to warehouse level)
    SUM(df.FORECAST_QUANTITY) AS PROJECTED_UNITS,
    SUM(df.FORECAST_QUANTITY) / 10 AS PROJECTED_PALLETS,  -- Assume 10 units per pallet
    
    -- Utilization metrics
    ROUND(
        (wz.CURRENT_OCCUPANCY_PALLETS + (SUM(df.FORECAST_QUANTITY) / 10)) / 
        NULLIF(wz.MAX_CAPACITY_PALLETS, 0) * 100, 
        2
    ) AS PROJECTED_UTILIZATION_PCT,
    
    -- Overflow flags
    CASE 
        WHEN (wz.CURRENT_OCCUPANCY_PALLETS + (SUM(df.FORECAST_QUANTITY) / 10)) > wz.MAX_CAPACITY_PALLETS 
        THEN TRUE 
        ELSE FALSE 
    END AS IS_OVERFLOW_RISK,
    
    -- Cost impact (overflow penalties)
    lc.OVERFLOW_PENALTY_RATE,
    lc.OVERFLOW_PENALTY_THRESHOLD_PCT,
    CASE 
        WHEN ((wz.CURRENT_OCCUPANCY_PALLETS + (SUM(df.FORECAST_QUANTITY) / 10)) / NULLIF(wz.MAX_CAPACITY_PALLETS, 0) * 100) 
             > lc.OVERFLOW_PENALTY_THRESHOLD_PCT
        THEN ROUND(
            ((wz.CURRENT_OCCUPANCY_PALLETS + (SUM(df.FORECAST_QUANTITY) / 10)) - 
             (wz.MAX_CAPACITY_PALLETS * lc.OVERFLOW_PENALTY_THRESHOLD_PCT / 100)) * 
            lc.OVERFLOW_PENALTY_RATE * 30,  -- 30-day penalty
            2
        )
        ELSE 0
    END AS PROJECTED_OVERFLOW_PENALTY

FROM ATOMIC.WAREHOUSE_ZONE wz
JOIN ATOMIC.SITE s ON wz.SITE_ID = s.SITE_ID AND s.IS_CURRENT_FLAG = TRUE
LEFT JOIN ATOMIC.LOGISTICS_COST_FACT lc ON wz.WAREHOUSE_ZONE_ID = lc.WAREHOUSE_ZONE_ID AND lc.IS_CURRENT_FLAG = TRUE
CROSS JOIN ATOMIC.SCENARIO_DEFINITION sd
LEFT JOIN ATOMIC.DEMAND_FORECAST_VERSIONS df 
    ON df.SITE_ID = s.SITE_ID 
    AND df.SCENARIO_ID = sd.SCENARIO_ID 
    AND df.IS_CURRENT_VERSION = TRUE
WHERE wz.IS_CURRENT_FLAG = TRUE
GROUP BY 
    wz.WAREHOUSE_ZONE_ID, wz.ZONE_CODE, wz.ZONE_NAME, wz.ZONE_TYPE,
    s.SITE_ID, s.SITE_CODE, s.SITE_NAME, s.REGION,
    wz.MAX_CAPACITY_PALLETS, wz.CURRENT_OCCUPANCY_PALLETS,
    sd.SCENARIO_ID, sd.SCENARIO_CODE, sd.SCENARIO_NAME,
    df.FISCAL_MONTH, df.FISCAL_QUARTER,
    lc.OVERFLOW_PENALTY_RATE, lc.OVERFLOW_PENALTY_THRESHOLD_PCT;

COMMENT ON VIEW WAREHOUSE_UTILIZATION_PROJECTION IS 'Projected warehouse capacity utilization by scenario - for Logistics Analyst';

-- =============================================================================
-- Production Capacity Summary View (for planning constraints)
-- =============================================================================
CREATE OR REPLACE VIEW PRODUCTION_CAPACITY_SUMMARY AS
SELECT
    -- Work center dimensions
    wc.WORK_CENTER_ID,
    wc.WORK_CENTER_CODE,
    wc.WORK_CENTER_NAME,
    wc.WORK_CENTER_TYPE,
    
    -- Site dimensions
    s.SITE_ID,
    s.SITE_CODE,
    s.SITE_NAME,
    s.REGION,
    
    -- Capacity metrics
    wc.CAPACITY_PER_HOUR,
    wc.HOURS_PER_DAY,
    wc.AVAILABLE_SHIFTS,
    wc.EFFICIENCY_FACTOR,
    
    -- Calculated daily capacity
    ROUND(
        wc.CAPACITY_PER_HOUR * wc.HOURS_PER_DAY * wc.AVAILABLE_SHIFTS * COALESCE(wc.EFFICIENCY_FACTOR, 1.0),
        2
    ) AS MAX_DAILY_CAPACITY,
    
    -- Monthly capacity (assume 22 working days)
    ROUND(
        wc.CAPACITY_PER_HOUR * wc.HOURS_PER_DAY * wc.AVAILABLE_SHIFTS * COALESCE(wc.EFFICIENCY_FACTOR, 1.0) * 22,
        2
    ) AS MAX_MONTHLY_CAPACITY

FROM ATOMIC.WORK_CENTER wc
JOIN ATOMIC.SITE s ON wc.SITE_ID = s.SITE_ID AND s.IS_CURRENT_FLAG = TRUE
WHERE wc.IS_CURRENT_FLAG = TRUE;

COMMENT ON VIEW PRODUCTION_CAPACITY_SUMMARY IS 'Production work center capacity summary for planning';

-- =============================================================================
-- Recommended Build Plan Table (ML Output - populated by notebook)
-- =============================================================================
CREATE OR REPLACE TABLE RECOMMENDED_BUILD_PLAN (
    -- Primary Key
    PLAN_ID NUMBER(38,0) AUTOINCREMENT,
    
    -- Foreign Keys
    PRODUCT_ID NUMBER(38,0) NOT NULL,
    WORK_CENTER_ID NUMBER(38,0) NOT NULL,
    SCENARIO_ID NUMBER(38,0) NOT NULL,
    
    -- Time dimension
    PRODUCTION_WEEK DATE NOT NULL,
    FISCAL_MONTH VARCHAR(20),
    FISCAL_QUARTER VARCHAR(10),
    
    -- Recommended quantities
    RECOMMENDED_QUANTITY NUMBER(18,4) NOT NULL,
    CURRENT_CAPACITY_AVAILABLE NUMBER(18,4),
    CAPACITY_UTILIZATION_PCT NUMBER(5,2),
    
    -- Inventory projections
    PROJECTED_INVENTORY NUMBER(18,4),
    WAREHOUSE_UTILIZATION_PCT NUMBER(5,2),
    
    -- Cost projections
    PROJECTED_PRODUCTION_COST NUMBER(18,2),
    PROJECTED_STORAGE_COST NUMBER(18,2),
    PROJECTED_TOTAL_COST NUMBER(18,2),
    
    -- Model metadata
    MODEL_VERSION VARCHAR(50),
    MODEL_CONFIDENCE NUMBER(5,4),
    
    -- Audit columns
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints (metadata only)
    PRIMARY KEY (PLAN_ID)
);

COMMENT ON TABLE RECOMMENDED_BUILD_PLAN IS 'ML-optimized production schedule output - populated by prebuild_optimization notebook';

-- =============================================================================
-- Inventory Buildup Curve View (for "camel hump" visualization)
-- =============================================================================
CREATE OR REPLACE VIEW INVENTORY_BUILDUP_CURVE AS
WITH cumulative_forecast AS (
    SELECT
        sd.SCENARIO_ID,
        sd.SCENARIO_CODE,
        sd.SCENARIO_NAME,
        df.FISCAL_MONTH,
        df.FISCAL_QUARTER,
        SUM(df.FORECAST_QUANTITY) AS PERIOD_DEMAND,
        SUM(df.TOTAL_REVENUE) AS PERIOD_REVENUE,
        -- Simplified: assume production leads demand by 1 month for pre-build
        LAG(SUM(df.FORECAST_QUANTITY), 1, 0) OVER (
            PARTITION BY sd.SCENARIO_ID 
            ORDER BY df.FISCAL_MONTH
        ) AS PRIOR_PRODUCTION,
        -- Running inventory balance (simplified model)
        SUM(SUM(df.FORECAST_QUANTITY)) OVER (
            PARTITION BY sd.SCENARIO_ID 
            ORDER BY df.FISCAL_MONTH
            ROWS UNBOUNDED PRECEDING
        ) AS CUMULATIVE_DEMAND
    FROM ATOMIC.DEMAND_FORECAST_VERSIONS df
    JOIN ATOMIC.SCENARIO_DEFINITION sd ON df.SCENARIO_ID = sd.SCENARIO_ID
    WHERE df.IS_CURRENT_VERSION = TRUE
    GROUP BY sd.SCENARIO_ID, sd.SCENARIO_CODE, sd.SCENARIO_NAME, df.FISCAL_MONTH, df.FISCAL_QUARTER
)
SELECT
    SCENARIO_ID,
    SCENARIO_CODE,
    SCENARIO_NAME,
    FISCAL_MONTH,
    FISCAL_QUARTER,
    PERIOD_DEMAND,
    PERIOD_REVENUE,
    PRIOR_PRODUCTION,
    CUMULATIVE_DEMAND,
    -- Inventory curve calculation (pre-build = production ahead of demand)
    CUMULATIVE_DEMAND - PRIOR_PRODUCTION AS INVENTORY_POSITION,
    -- Cost of carry (simplified: $0.50 per unit per month)
    (CUMULATIVE_DEMAND - PRIOR_PRODUCTION) * 0.50 AS COST_OF_CARRY
FROM cumulative_forecast
ORDER BY SCENARIO_ID, FISCAL_MONTH;

COMMENT ON VIEW INVENTORY_BUILDUP_CURVE IS 'Time-series view for inventory buildup visualization (camel hump chart)';

-- =============================================================================
-- Scenario KPI Summary (Dynamic Table for auto-refresh)
-- =============================================================================
CREATE OR REPLACE DYNAMIC TABLE DT_SCENARIO_KPI_SUMMARY
    TARGET_LAG = '5 minutes'
    WAREHOUSE = IDENTIFIER($PROJECT_WH)
AS
SELECT
    -- Scenario dimensions
    sd.SCENARIO_ID,
    sd.SCENARIO_CODE,
    sd.SCENARIO_NAME,
    sd.SCENARIO_TYPE,
    sd.IS_OFFICIAL_BASELINE,
    
    -- Aggregate KPIs
    COUNT(DISTINCT df.PRODUCT_ID) AS PRODUCT_COUNT,
    COUNT(DISTINCT df.SITE_ID) AS SITE_COUNT,
    SUM(df.FORECAST_QUANTITY) AS TOTAL_FORECAST_QUANTITY,
    SUM(df.TOTAL_REVENUE) AS TOTAL_FORECASTED_REVENUE,
    SUM(df.FORECAST_QUANTITY * p.UNIT_COST) AS TOTAL_FORECASTED_COST,
    SUM(df.TOTAL_REVENUE) - SUM(df.FORECAST_QUANTITY * p.UNIT_COST) AS TOTAL_GROSS_MARGIN,
    
    -- Margin percentage
    ROUND(
        (SUM(df.TOTAL_REVENUE) - SUM(df.FORECAST_QUANTITY * p.UNIT_COST)) / 
        NULLIF(SUM(df.TOTAL_REVENUE), 0) * 100,
        2
    ) AS GROSS_MARGIN_PCT,
    
    -- Warehousing cost estimate
    SUM(
        ROUND((df.FORECAST_QUANTITY / 10) * COALESCE(lc.STORAGE_COST_PER_PALLET_PER_DAY, 0.5) * 30, 2)
    ) AS TOTAL_WAREHOUSING_COST,
    
    -- Net margin after warehousing
    SUM(df.TOTAL_REVENUE) - SUM(df.FORECAST_QUANTITY * p.UNIT_COST) - 
    SUM(ROUND((df.FORECAST_QUANTITY / 10) * COALESCE(lc.STORAGE_COST_PER_PALLET_PER_DAY, 0.5) * 30, 2)) 
    AS NET_MARGIN_AFTER_STORAGE,
    
    -- Refresh metadata
    CURRENT_TIMESTAMP() AS _REFRESHED_TIMESTAMP

FROM ATOMIC.DEMAND_FORECAST_VERSIONS df
JOIN ATOMIC.SCENARIO_DEFINITION sd ON df.SCENARIO_ID = sd.SCENARIO_ID
JOIN ATOMIC.PRODUCT p ON df.PRODUCT_ID = p.PRODUCT_ID AND p.IS_CURRENT_FLAG = TRUE
JOIN ATOMIC.SITE s ON df.SITE_ID = s.SITE_ID AND s.IS_CURRENT_FLAG = TRUE
LEFT JOIN ATOMIC.LOGISTICS_COST_FACT lc ON s.SITE_ID = lc.SITE_ID AND lc.IS_CURRENT_FLAG = TRUE
WHERE df.IS_CURRENT_VERSION = TRUE
GROUP BY 
    sd.SCENARIO_ID, sd.SCENARIO_CODE, sd.SCENARIO_NAME, 
    sd.SCENARIO_TYPE, sd.IS_OFFICIAL_BASELINE;

COMMENT ON DYNAMIC TABLE DT_SCENARIO_KPI_SUMMARY IS 'Auto-refreshed KPI aggregations by scenario - refreshes every 5 minutes';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN SCHEMA SOP_LOGISTICS;
SHOW VIEWS IN SCHEMA SOP_LOGISTICS;
SHOW DYNAMIC TABLES IN SCHEMA SOP_LOGISTICS;

