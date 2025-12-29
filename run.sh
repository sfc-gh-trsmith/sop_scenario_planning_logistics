#!/bin/bash
###############################################################################
# run.sh - Runtime operations for S&OP Scenario Planning & Logistics
#
# Commands:
#   main       - Execute the main workflow (optimization notebook)
#   status     - Check status of deployed resources
#   test       - Run query tests to validate data and views
#   streamlit  - Get Streamlit app URL
#   notebook   - Check notebook deployment status
#
# Usage:
#   ./run.sh main
#   ./run.sh status
#   ./run.sh test
#   ./run.sh streamlit
###############################################################################

set -e
set -o pipefail

# Configuration
CONNECTION_NAME="demo"
COMMAND=""
ENV_PREFIX=""
VERBOSE=""

# Project settings
PROJECT_PREFIX="SOP_SCENARIO_PLANNING_LOGISTICS"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Error handler
error_exit() {
    echo -e "${RED}[ERROR] $1${NC}" >&2
    exit 1
}

# Usage
usage() {
    cat << EOF
Usage: $0 COMMAND [OPTIONS]

Runtime operations for S&OP Scenario Planning & Logistics demo.

Commands:
  main        Execute the main workflow (optimization notebook)
  status      Check status of deployed resources (tables, row counts)
  test        Run query tests to validate all views and data
  streamlit   Get Streamlit application URL
  notebook    Check notebook deployment status

Options:
  -c, --connection NAME    Snowflake CLI connection name (default: demo)
  -p, --prefix PREFIX      Environment prefix for resources (e.g., DEV)
  -v, --verbose            Enable verbose output for debugging
  -h, --help               Show this help message

Examples:
  $0 main                  # Execute optimization notebook
  $0 main -v               # Execute with verbose output for debugging
  $0 status                # Check resource status
  $0 test                  # Run query tests
  $0 streamlit             # Get Streamlit URL
  $0 -c prod main          # Use 'prod' connection
EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -c|--connection)
            CONNECTION_NAME="$2"
            shift 2
            ;;
        -p|--prefix)
            ENV_PREFIX="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE="--verbose"
            shift
            ;;
        main|status|test|streamlit|notebook)
            COMMAND="$1"
            shift
            ;;
        *)
            error_exit "Unknown option: $1\nUse --help for usage information"
            ;;
    esac
done

# Require a command
if [ -z "$COMMAND" ]; then
    usage
fi

# Build connection string
SNOW_CONN="-c $CONNECTION_NAME"

# Compute resource names
if [ -n "$ENV_PREFIX" ]; then
    FULL_PREFIX="${ENV_PREFIX}_${PROJECT_PREFIX}"
else
    FULL_PREFIX="${PROJECT_PREFIX}"
fi

DATABASE="${FULL_PREFIX}"
ROLE="${FULL_PREFIX}_ROLE"
WAREHOUSE="${FULL_PREFIX}_WH"
COMPUTE_POOL="${FULL_PREFIX}_COMPUTE_POOL"

###############################################################################
# Command: main - Execute main workflow (optimization notebook)
###############################################################################
cmd_main() {
    echo "=================================================="
    echo "S&OP Scenario Planning - Execute Main Workflow"
    echo "=================================================="
    echo ""
    
    NOTEBOOK_NAME="${PROJECT_PREFIX}_NOTEBOOK"
    
    # Check if notebook exists by counting rows
    echo "Checking for notebook..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE SCHEMA SOP_LOGISTICS;
        SELECT COUNT(*) AS CNT FROM INFORMATION_SCHEMA.NOTEBOOKS WHERE NOTEBOOK_NAME = '${NOTEBOOK_NAME}';
    " 2>&1)
    
    # Check if count is greater than 0
    if ! echo "$RESULT" | grep -qE "\|\s*1\s*\|"; then
        echo -e "${RED}[ERROR]${NC} Notebook '${NOTEBOOK_NAME}' not found in SOP_LOGISTICS schema."
        echo ""
        echo "Deploy the notebook first:"
        echo "  ./deploy.sh --only-notebook"
        exit 1
    fi
    echo -e "${GREEN}[OK]${NC} Notebook found"
    echo ""
    
    # Execute the notebook synchronously using snow notebook execute
    echo "Executing notebook (synchronous mode)..."
    echo -e "${YELLOW}Note: This runs synchronously and waits for completion. May take several minutes.${NC}"
    echo ""
    
    # Use snow notebook execute for proper synchronous execution with full output
    snow notebook execute ${NOTEBOOK_NAME} \
        $SNOW_CONN \
        --database ${DATABASE} \
        --schema SOP_LOGISTICS \
        --role ${ROLE} \
        ${VERBOSE}
    
    EXEC_STATUS=$?
    
    echo ""
    
    if [ $EXEC_STATUS -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} Notebook execution failed (exit code: $EXEC_STATUS)"
        echo ""
        echo "Debug tips:"
        echo "  1. Open the notebook in Snowsight to view cell-by-cell output"
        echo "  2. Check query history for detailed error messages"
        echo "  3. Run: ./run.sh notebook  (to get notebook URL)"
        exit 1
    fi
    
    echo -e "${GREEN}[OK]${NC} Notebook execution completed successfully"
    echo ""
    
    # Verify output table was populated
    echo "Verifying output..."
    VERIFY_RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT COUNT(*) AS ROW_COUNT FROM SOP_LOGISTICS.RECOMMENDED_BUILD_PLAN;
    " 2>&1)
    
    if echo "$VERIFY_RESULT" | grep -q "ROW_COUNT"; then
        ROW_COUNT=$(echo "$VERIFY_RESULT" | grep -E "^\|.*[0-9]" | tail -1 | tr -d '| ')
        echo -e "${GREEN}[OK]${NC} RECOMMENDED_BUILD_PLAN table contains ${ROW_COUNT} rows"
    else
        echo -e "${YELLOW}[WARN]${NC} Could not verify output table"
    fi
    
    echo ""
    echo "=================================================="
    echo -e "${GREEN}Main workflow complete!${NC}"
    echo "=================================================="
    echo ""
    echo "Output table: SOP_LOGISTICS.RECOMMENDED_BUILD_PLAN"
    echo ""
    echo "View results in the Streamlit dashboard:"
    echo "  ./run.sh streamlit"
}

###############################################################################
# Command: status
###############################################################################
cmd_status() {
    echo "=================================================="
    echo "S&OP Scenario Planning - Resource Status"
    echo "=================================================="
    echo ""
    
    echo "Configuration:"
    echo "  Connection: $CONNECTION_NAME"
    echo "  Database: $DATABASE"
    echo "  Role: $ROLE"
    echo ""
    
    echo "Checking resources..."
    echo ""
    
    # Check database exists
    echo -e "${BLUE}Database:${NC}"
    snow sql $SNOW_CONN -q "SHOW DATABASES LIKE '${DATABASE}';" 2>/dev/null || echo "  Not found"
    echo ""
    
    # Check warehouse
    echo -e "${BLUE}Warehouse:${NC}"
    snow sql $SNOW_CONN -q "SHOW WAREHOUSES LIKE '${WAREHOUSE}';" 2>/dev/null || echo "  Not found"
    echo ""
    
    # Check table row counts
    echo -e "${BLUE}Table Row Counts:${NC}"
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT 'ATOMIC.PRODUCT' AS TABLE_NAME, COUNT(*) AS ROWS FROM ATOMIC.PRODUCT
        UNION ALL SELECT 'ATOMIC.PRODUCT_CATEGORY', COUNT(*) FROM ATOMIC.PRODUCT_CATEGORY
        UNION ALL SELECT 'ATOMIC.SITE', COUNT(*) FROM ATOMIC.SITE
        UNION ALL SELECT 'ATOMIC.WORK_CENTER', COUNT(*) FROM ATOMIC.WORK_CENTER
        UNION ALL SELECT 'ATOMIC.WAREHOUSE_ZONE', COUNT(*) FROM ATOMIC.WAREHOUSE_ZONE
        UNION ALL SELECT 'ATOMIC.SCENARIO_DEFINITION', COUNT(*) FROM ATOMIC.SCENARIO_DEFINITION
        UNION ALL SELECT 'ATOMIC.DEMAND_FORECAST_VERSIONS', COUNT(*) FROM ATOMIC.DEMAND_FORECAST_VERSIONS
        UNION ALL SELECT 'ATOMIC.LOGISTICS_COST_FACT', COUNT(*) FROM ATOMIC.LOGISTICS_COST_FACT
        ORDER BY TABLE_NAME;
    " 2>/dev/null || echo "  Error querying tables"
    echo ""
    
    # Check views
    echo -e "${BLUE}Data Mart Views:${NC}"
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT 'SCENARIO_COMPARISON_V' AS VIEW_NAME, COUNT(*) AS ROWS FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
        UNION ALL SELECT 'WAREHOUSE_UTILIZATION_PROJECTION', COUNT(*) FROM SOP_LOGISTICS.WAREHOUSE_UTILIZATION_PROJECTION
        UNION ALL SELECT 'PRODUCTION_CAPACITY_SUMMARY', COUNT(*) FROM SOP_LOGISTICS.PRODUCTION_CAPACITY_SUMMARY
        UNION ALL SELECT 'INVENTORY_BUILDUP_CURVE', COUNT(*) FROM SOP_LOGISTICS.INVENTORY_BUILDUP_CURVE
        UNION ALL SELECT 'DT_SCENARIO_KPI_SUMMARY', COUNT(*) FROM SOP_LOGISTICS.DT_SCENARIO_KPI_SUMMARY
        ORDER BY VIEW_NAME;
    " 2>/dev/null || echo "  Error querying views"
    echo ""
    
    # Check semantic model
    echo -e "${BLUE}Semantic Model:${NC}"
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        LIST @ATOMIC.MODELS;
    " 2>/dev/null || echo "  No semantic model files found"
    echo ""
}

###############################################################################
# Command: test
###############################################################################
cmd_test() {
    echo "=================================================="
    echo "S&OP Scenario Planning - Query Tests"
    echo "=================================================="
    echo ""
    
    TEST_FAILED=0
    
    echo "Running query validation tests..."
    echo ""
    
    # Test 1: Verify scenario comparison view
    echo -e "${BLUE}Test 1:${NC} SCENARIO_COMPARISON_V returns data..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT COUNT(*) AS CNT FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V;
    " 2>&1) 
    
    if echo "$RESULT" | grep -q "CNT"; then
        echo -e "  ${GREEN}[PASS]${NC} View returns data"
    else
        echo -e "  ${RED}[FAIL]${NC} View query failed"
        TEST_FAILED=1
    fi
    
    # Test 2: Verify scenario KPI summary
    echo -e "${BLUE}Test 2:${NC} DT_SCENARIO_KPI_SUMMARY has scenarios..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT COUNT(DISTINCT SCENARIO_CODE) AS SCENARIO_COUNT 
        FROM SOP_LOGISTICS.DT_SCENARIO_KPI_SUMMARY;
    " 2>&1)
    
    if echo "$RESULT" | grep -q "SCENARIO_COUNT"; then
        echo -e "  ${GREEN}[PASS]${NC} Scenarios found"
    else
        echo -e "  ${RED}[FAIL]${NC} No scenarios found"
        TEST_FAILED=1
    fi
    
    # Test 3: Verify golden query for Cortex Analyst
    echo -e "${BLUE}Test 3:${NC} Golden query (warehousing cost by scenario)..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT SCENARIO_CODE, SUM(PROJECTED_WAREHOUSING_COST) AS TOTAL_COST
        FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
        WHERE FISCAL_MONTH = 'October'
        GROUP BY SCENARIO_CODE
        ORDER BY SCENARIO_CODE;
    " 2>&1)
    
    if echo "$RESULT" | grep -q "BASELINE"; then
        echo -e "  ${GREEN}[PASS]${NC} Golden query returns expected data"
    else
        echo -e "  ${RED}[FAIL]${NC} Golden query failed"
        TEST_FAILED=1
    fi
    
    # Test 4: Verify Q4 Push shows higher costs
    echo -e "${BLUE}Test 4:${NC} Q4 Push has higher warehousing costs than Baseline..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        WITH scenario_costs AS (
            SELECT SCENARIO_CODE, SUM(TOTAL_WAREHOUSING_COST) AS TOTAL_COST
            FROM SOP_LOGISTICS.DT_SCENARIO_KPI_SUMMARY
            GROUP BY SCENARIO_CODE
        )
        SELECT 
            CASE WHEN q4.TOTAL_COST > base.TOTAL_COST THEN 'HIGHER' ELSE 'LOWER' END AS Q4_VS_BASELINE
        FROM scenario_costs base, scenario_costs q4
        WHERE base.SCENARIO_CODE = 'BASELINE' AND q4.SCENARIO_CODE = 'Q4_PUSH';
    " 2>&1)
    
    if echo "$RESULT" | grep -q "HIGHER"; then
        echo -e "  ${GREEN}[PASS]${NC} Q4 Push shows higher costs (expected behavior)"
    else
        echo -e "  ${YELLOW}[WARN]${NC} Could not verify Q4 Push cost difference"
    fi
    
    # Test 5: Verify warehouse utilization projection
    echo -e "${BLUE}Test 5:${NC} WAREHOUSE_UTILIZATION_PROJECTION returns data..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        SELECT COUNT(*) AS CNT FROM SOP_LOGISTICS.WAREHOUSE_UTILIZATION_PROJECTION
        WHERE SCENARIO_CODE = 'Q4_PUSH';
    " 2>&1)
    
    if echo "$RESULT" | grep -q "CNT"; then
        echo -e "  ${GREEN}[PASS]${NC} Utilization projections available"
    else
        echo -e "  ${RED}[FAIL]${NC} Utilization projections failed"
        TEST_FAILED=1
    fi
    
    echo ""
    echo "=================================================="
    if [ $TEST_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}Some tests failed. Review output above.${NC}"
        exit 1
    fi
    echo "=================================================="
}

###############################################################################
# Command: streamlit
###############################################################################
cmd_streamlit() {
    echo "=================================================="
    echo "S&OP Scenario Planning - Streamlit Dashboard"
    echo "=================================================="
    echo ""
    
    # Try to get URL
    URL=$(snow streamlit get-url SOP_SCENARIO_PLANNING_LOGISTICS_APP \
        $SNOW_CONN \
        --database $DATABASE \
        --schema SOP_LOGISTICS \
        --role $ROLE 2>/dev/null) || true
    
    if [ -n "$URL" ]; then
        echo "Streamlit Dashboard URL:"
        echo ""
        echo -e "  ${GREEN}$URL${NC}"
        echo ""
    else
        echo -e "${YELLOW}Could not retrieve URL automatically.${NC}"
        echo ""
        echo "To open the dashboard:"
        echo "1. Go to Snowsight (https://app.snowflake.com)"
        echo "2. Navigate to: Projects > Streamlit"
        echo "3. Open: SOP_SCENARIO_PLANNING_LOGISTICS_APP"
        echo ""
        echo "Or run this SQL to get the URL:"
        echo "  SHOW STREAMLITS IN SCHEMA ${DATABASE}.SOP_LOGISTICS;"
    fi
}

###############################################################################
# Command: notebook - Check notebook deployment status
###############################################################################
cmd_notebook() {
    echo "=================================================="
    echo "S&OP Scenario Planning - Notebook Status"
    echo "=================================================="
    echo ""
    
    NOTEBOOK_NAME="${PROJECT_PREFIX}_NOTEBOOK"
    
    # Check if notebook exists
    echo "Checking for notebook..."
    RESULT=$(snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        SHOW NOTEBOOKS LIKE '${NOTEBOOK_NAME}';
    " 2>&1)
    
    if echo "$RESULT" | grep -q "${NOTEBOOK_NAME}"; then
        echo -e "${GREEN}[OK]${NC} Notebook deployed: ${NOTEBOOK_NAME}"
        echo ""
        echo "To execute the notebook programmatically:"
        echo "  ./run.sh main"
        echo ""
        echo "To open in Snowsight:"
        echo "1. Go to https://app.snowflake.com"
        echo "2. Navigate to: Projects > Notebooks"
        echo "3. Open: ${NOTEBOOK_NAME}"
    else
        echo -e "${YELLOW}[WARN]${NC} Notebook not found"
        echo ""
        echo "Run ./deploy.sh to deploy the optimization notebook."
        echo "Source: notebooks/prebuild_optimization.ipynb"
    fi
}

###############################################################################
# Execute command
###############################################################################
case $COMMAND in
    main)
        cmd_main
        ;;
    status)
        cmd_status
        ;;
    test)
        cmd_test
        ;;
    streamlit)
        cmd_streamlit
        ;;
    notebook)
        cmd_notebook
        ;;
    *)
        error_exit "Unknown command: $COMMAND"
        ;;
esac

