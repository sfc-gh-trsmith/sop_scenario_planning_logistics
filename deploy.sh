#!/bin/bash
###############################################################################
# deploy.sh - Deploy S&OP Scenario Planning & Logistics to Snowflake
#
# Creates:
#   - Role, Warehouse, Database, Compute Pool
#   - RAW, ATOMIC, SOP_LOGISTICS schemas
#   - Tables, views, dynamic tables
#   - Cortex Search service
#   - Streamlit application
#   - Optimization notebook
#
# Usage:
#   ./deploy.sh                  # Full deployment
#   ./deploy.sh -c prod          # Use 'prod' connection
#   ./deploy.sh --only-streamlit # Redeploy Streamlit only
###############################################################################

set -e
set -o pipefail

# Configuration
CONNECTION_NAME="demo"
ENV_PREFIX=""
ONLY_COMPONENT=""
SKIP_AGENT_DEPLOY=false

# Project settings
PROJECT_PREFIX="SOP_SCENARIO_PLANNING_LOGISTICS"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Error handler
error_exit() {
    echo -e "${RED}[ERROR] $1${NC}" >&2
    exit 1
}

# Usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy S&OP Scenario Planning & Logistics demo to Snowflake.

Options:
  -c, --connection NAME    Snowflake CLI connection name (default: demo)
  -p, --prefix PREFIX      Environment prefix for resources (e.g., DEV, PROD)
  --only-sql               Run SQL scripts only
  --only-data              Upload and load data only
  --only-streamlit         Deploy Streamlit app only
  --only-notebook          Deploy optimization notebook only
  --only-semantic          Deploy Semantic View only
  --only-search            Deploy Cortex Search service only
  --only-agent             Deploy Cortex Agent only
  --skip-agent             Skip Cortex Agent deployment (use if PAT token unavailable)
  -h, --help               Show this help message

Environment Variables:
  SNOWFLAKE_PAT_TOKEN      Required for Cortex Agent deployment via REST API.
                           Get from Snowsight: Settings > Preferences > Programmatic Access Tokens
                           See .env.example for all configuration options.
                           Use --skip-agent to deploy without the agent.

Examples:
  $0                       # Full deployment
  $0 -c prod               # Use 'prod' connection
  $0 --prefix DEV          # Deploy with DEV_ prefix
  $0 --only-streamlit      # Redeploy Streamlit only
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
        --only-sql)
            ONLY_COMPONENT="sql"
            shift
            ;;
        --only-data)
            ONLY_COMPONENT="data"
            shift
            ;;
        --only-streamlit)
            ONLY_COMPONENT="streamlit"
            shift
            ;;
        --only-notebook)
            ONLY_COMPONENT="notebook"
            shift
            ;;
        --only-semantic)
            ONLY_COMPONENT="semantic"
            shift
            ;;
        --only-search)
            ONLY_COMPONENT="search"
            shift
            ;;
        --only-agent)
            ONLY_COMPONENT="agent"
            shift
            ;;
        --skip-agent)
            SKIP_AGENT_DEPLOY=true
            shift
            ;;
        *)
            error_exit "Unknown option: $1\nUse --help for usage information"
            ;;
    esac
done

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

# Helper function to check if step should run
should_run_step() {
    local step_name="$1"
    if [ -z "$ONLY_COMPONENT" ]; then
        return 0
    fi
    case "$ONLY_COMPONENT" in
        sql)
            [[ "$step_name" == "sql" ]]
            ;;
        data)
            [[ "$step_name" == "data" ]]
            ;;
        streamlit)
            [[ "$step_name" == "streamlit" ]]
            ;;
        notebook)
            [[ "$step_name" == "notebook" ]]
            ;;
        semantic)
            [[ "$step_name" == "semantic" ]]
            ;;
        search)
            [[ "$step_name" == "search" ]]
            ;;
        agent)
            [[ "$step_name" == "agent" ]]
            ;;
        *)
            return 1
            ;;
    esac
}

# Helper function to execute SQL file with variable substitution
# This avoids CLI template parsing issues by:
#   1. Replacing IDENTIFIER($VAR) with actual values
#   2. Escaping S&OP -> SandOP to prevent &OP being parsed as template
# Usage: run_sql_file <sql_file> [role]
#   role: Optional role to use (e.g., ACCOUNTADMIN for setup scripts)
run_sql_file() {
    local sql_file="$1"
    local use_role="${2:-}"
    local temp_sql=$(mktemp)
    
    sed -e "s/IDENTIFIER(\\\$FULL_PREFIX)/${FULL_PREFIX}/g" \
        -e "s/IDENTIFIER(\\\$PROJECT_ROLE)/${ROLE}/g" \
        -e "s/IDENTIFIER(\\\$PROJECT_WH)/${WAREHOUSE}/g" \
        -e "s/IDENTIFIER(\\\$PROJECT_COMPUTE_POOL)/${COMPUTE_POOL}/g" \
        -e "s/S\&OP/SandOP/g" \
        "$sql_file" > "$temp_sql"
    
    if [ -n "$use_role" ]; then
        snow sql $SNOW_CONN --role "$use_role" -f "$temp_sql"
    else
        snow sql $SNOW_CONN -f "$temp_sql"
    fi
    local result=$?
    rm -f "$temp_sql"
    return $result
}

# Display configuration
echo "=================================================="
echo "S&OP Scenario Planning & Logistics - Deployment"
echo "=================================================="
echo ""
echo "Configuration:"
echo "  Connection: $CONNECTION_NAME"
if [ -n "$ENV_PREFIX" ]; then
    echo "  Environment Prefix: $ENV_PREFIX"
fi
if [ -n "$ONLY_COMPONENT" ]; then
    echo "  Deploy Only: $ONLY_COMPONENT"
fi
if [ "$SKIP_AGENT_DEPLOY" = true ]; then
    echo "  Skip Agent: yes"
fi
echo "  Database: $DATABASE"
echo "  Role: $ROLE"
echo "  Warehouse: $WAREHOUSE"
echo ""

###############################################################################
# Step 1: Check prerequisites
###############################################################################
echo "Step 1: Checking prerequisites..."
echo "------------------------------------------------"

# Check for snow CLI
if ! command -v snow &> /dev/null; then
    error_exit "Snowflake CLI (snow) not found. Install with: pip install snowflake-cli"
fi
echo -e "${GREEN}[OK]${NC} Snowflake CLI found"

# Test Snowflake connection
echo "Testing Snowflake connection..."
if ! snow sql $SNOW_CONN -q "SELECT 1" &> /dev/null; then
    echo -e "${RED}[ERROR]${NC} Failed to connect to Snowflake"
    snow connection test $SNOW_CONN 2>&1 || true
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Connection '$CONNECTION_NAME' verified"

# Extract connection parameters for Python utilities using snow CLI JSON output
CONN_PARAMS=$(snow connection list --format json 2>/dev/null | python3 -c "
import sys, json
try:
    conns = json.load(sys.stdin)
    for c in conns:
        if c.get('connection_name') == '$CONNECTION_NAME':
            params = c.get('parameters', {})
            print(f\"CONN_ACCOUNT={params.get('account', '')}\")
            print(f\"CONN_USER={params.get('user', '')}\")
            print(f\"CONN_PRIVATE_KEY={params.get('private_key_file', '')}\")
            print(f\"CONN_ROLE={params.get('role', '')}\")
            break
except Exception as e:
    print(f'ERROR={e}', file=sys.stderr)
    sys.exit(1)
")

if [ -z "$CONN_PARAMS" ]; then
    error_exit "Failed to extract connection parameters from '$CONNECTION_NAME'"
fi

# Parse the output and export environment variables
eval "$CONN_PARAMS"
export SNOWFLAKE_ACCOUNT="$CONN_ACCOUNT"
export SNOWFLAKE_USER="$CONN_USER"
export SNOWFLAKE_PRIVATE_KEY_PATH="$CONN_PRIVATE_KEY"
export SNOWFLAKE_WAREHOUSE="${WAREHOUSE}"
# Default to project role if not set in connection
export SNOWFLAKE_ROLE="${CONN_ROLE:-$ROLE}"

if [ -n "$SNOWFLAKE_ACCOUNT" ] && [ -n "$SNOWFLAKE_USER" ]; then
    echo -e "${GREEN}[OK]${NC} Extracted connection parameters (account: $SNOWFLAKE_ACCOUNT, user: $SNOWFLAKE_USER, role: $SNOWFLAKE_ROLE)"
else
    error_exit "Failed to extract account/user from connection '$CONNECTION_NAME'"
fi

# Check required files
for file in "sql/01_account_setup.sql" "sql/02_schema_setup.sql" "sql/03_raw_tables.sql" \
            "sql/04_atomic_tables.sql" "sql/05_data_mart.sql" "sql/06_cortex_search.sql" \
            "sql/07_semantic_model.sql" "sql/08_stored_procedures.sql"; do
    if [ ! -f "$file" ]; then
        error_exit "Required file not found: $file"
    fi
done
echo -e "${GREEN}[OK]${NC} Required SQL files present"

# Check semantic model and agent files
for file in "semantic_models/sop_analytics_semantic.yaml" "agents/SOP_ANALYST_AGENT.agent.json"; do
    if [ ! -f "$file" ]; then
        error_exit "Required file not found: $file"
    fi
done
echo -e "${GREEN}[OK]${NC} Semantic model and agent files present"

# Check required environment variables for Cortex Agent deployment
if [ "$SKIP_AGENT_DEPLOY" = true ]; then
    echo -e "${YELLOW}[SKIP]${NC} Agent deployment will be skipped (--skip-agent flag set)"
elif [ -z "$SNOWFLAKE_PAT_TOKEN" ]; then
    echo -e "${RED}[ERROR]${NC} SNOWFLAKE_PAT_TOKEN not set"
    echo ""
    echo "This environment variable is required for Cortex Agent deployment."
    echo "Get a PAT token from Snowsight: Settings > Preferences > Programmatic Access Tokens"
    echo ""
    echo "Options:"
    echo "  1. Set the token:  export SNOWFLAKE_PAT_TOKEN=your_token"
    echo "  2. Skip agent:     $0 --skip-agent"
    echo ""
    echo "See .env.example for all configuration options."
    exit 1
else
    echo -e "${GREEN}[OK]${NC} SNOWFLAKE_PAT_TOKEN is set"
fi

# Check data files
if [ ! -d "data/synthetic" ]; then
    error_exit "Synthetic data directory not found: data/synthetic"
fi
echo -e "${GREEN}[OK]${NC} Synthetic data directory found"
echo ""

###############################################################################
# Step 2: Run Account-Level SQL Setup
###############################################################################
if should_run_step "sql"; then
    echo "Step 2: Running account-level SQL setup..."
    echo "------------------------------------------------"
    
    run_sql_file sql/01_account_setup.sql ACCOUNTADMIN
    
    echo -e "${GREEN}[OK]${NC} Account-level setup completed"
    echo ""
fi

###############################################################################
# Step 3: Run Schema-Level SQL Setup
###############################################################################
if should_run_step "sql"; then
    echo "Step 3: Running schema-level SQL setup..."
    echo "------------------------------------------------"
    
    run_sql_file sql/02_schema_setup.sql
    
    echo -e "${GREEN}[OK]${NC} Schema setup completed"
    echo ""
fi

###############################################################################
# Step 4: Create RAW Tables
###############################################################################
if should_run_step "sql"; then
    echo "Step 4: Creating RAW tables..."
    echo "------------------------------------------------"
    
    run_sql_file sql/03_raw_tables.sql
    
    echo -e "${GREEN}[OK]${NC} RAW tables created"
    echo ""
fi

###############################################################################
# Step 5: Create ATOMIC Tables
###############################################################################
if should_run_step "sql"; then
    echo "Step 5: Creating ATOMIC tables..."
    echo "------------------------------------------------"
    
    run_sql_file sql/04_atomic_tables.sql
    
    echo -e "${GREEN}[OK]${NC} ATOMIC tables created"
    echo ""
fi

###############################################################################
# Step 6: Create Data Mart Views and Tables
###############################################################################
if should_run_step "sql"; then
    echo "Step 6: Creating SOP_LOGISTICS data mart..."
    echo "------------------------------------------------"
    
    run_sql_file sql/05_data_mart.sql
    
    echo -e "${GREEN}[OK]${NC} Data mart created"
    echo ""
fi

###############################################################################
# Step 7: Create Stored Procedures
###############################################################################
if should_run_step "sql"; then
    echo "Step 7: Creating stored procedures..."
    echo "------------------------------------------------"
    
    run_sql_file sql/08_stored_procedures.sql
    
    echo -e "${GREEN}[OK]${NC} Stored procedures created"
    echo ""
fi

###############################################################################
# Step 8: Upload Synthetic Data to Stage
###############################################################################
if should_run_step "data"; then
    echo "Step 8: Uploading synthetic data to stage..."
    echo "------------------------------------------------"
    
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        -- Upload CSV files to DATA_STAGE
        PUT file://${SCRIPT_DIR}/data/synthetic/*.csv @RAW.DATA_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
    "
    
    echo -e "${GREEN}[OK]${NC} Data files uploaded"
    echo ""
fi

###############################################################################
# Step 9: Load Data from Stage to RAW Tables
###############################################################################
if should_run_step "data"; then
    echo "Step 9: Loading data into RAW tables..."
    echo "------------------------------------------------"
    
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        CALL ATOMIC.LOAD_STAGED_DATA();
    "
    
    echo -e "${GREEN}[OK]${NC} Data loaded into RAW tables"
    echo ""
fi

###############################################################################
# Step 10: Transform RAW to ATOMIC
###############################################################################
if should_run_step "data"; then
    echo "Step 10: Transforming RAW to ATOMIC..."
    echo "------------------------------------------------"
    
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        CALL ATOMIC.TRANSFORM_RAW_TO_ATOMIC();
    "
    
    echo -e "${GREEN}[OK]${NC} Data transformed to ATOMIC layer"
    echo ""
fi

###############################################################################
# Step 11: Upload Semantic Model YAML to Stage
###############################################################################
if should_run_step "data" || should_run_step "sql" || should_run_step "semantic"; then
    echo "Step 11: Uploading semantic model YAML to stage..."
    echo "------------------------------------------------"
    
    snow sql $SNOW_CONN -q "
        USE ROLE ${ROLE};
        USE DATABASE ${DATABASE};
        USE WAREHOUSE ${WAREHOUSE};
        
        PUT file://${SCRIPT_DIR}/semantic_models/sop_analytics_semantic.yaml @SOP_LOGISTICS.MODELS/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
    "
    
    echo -e "${GREEN}[OK]${NC} Semantic model uploaded to stage"
    echo ""
fi

###############################################################################
# Step 11b: Create Semantic Model Helper Objects
###############################################################################
if should_run_step "data" || should_run_step "sql" || should_run_step "semantic"; then
    echo "Step 11b: Creating semantic model helper objects..."
    echo "------------------------------------------------"
    
    run_sql_file sql/07_semantic_model.sql
    
    echo -e "${GREEN}[OK]${NC} Semantic model helper objects created"
    echo ""
fi

###############################################################################
# Step 11c: Deploy Semantic View (native object)
###############################################################################
if should_run_step "data" || should_run_step "sql" || should_run_step "semantic"; then
    echo "Step 11c: Deploying semantic view..."
    echo "------------------------------------------------"
    
    python utils/sf_cortex_agent_ops.py deploy-semantic-view \
        --input semantic_models/sop_analytics_semantic.yaml \
        --database $DATABASE \
        --schema SOP_LOGISTICS
    
    echo -e "${GREEN}[OK]${NC} Semantic view deployed"
    echo ""
fi

###############################################################################
# Step 12: Upload Documents for Cortex Search (if available)
###############################################################################
if should_run_step "data"; then
    echo "Step 12: Uploading documents for Cortex Search..."
    echo "------------------------------------------------"
    
    if [ -d "data/unstructured" ] && [ "$(ls -A data/unstructured 2>/dev/null)" ]; then
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE WAREHOUSE ${WAREHOUSE};
            
            PUT file://${SCRIPT_DIR}/data/unstructured/*.pdf @RAW.DOCS_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
        "
        echo -e "${GREEN}[OK]${NC} Documents uploaded"
    else
        echo -e "${YELLOW}[WARN]${NC} No documents found in data/unstructured - skipping"
    fi
    echo ""
fi

###############################################################################
# Step 12a: Create Cortex Search Tables and Service
###############################################################################
if should_run_step "data" || should_run_step "sql" || should_run_step "search"; then
    echo "Step 12a: Creating Cortex Search tables and service..."
    echo "------------------------------------------------"
    
    run_sql_file sql/06_cortex_search.sql
    
    echo -e "${GREEN}[OK]${NC} Cortex Search service created"
    echo ""
fi

###############################################################################
# Step 12b: Parse Documents for Cortex Search
###############################################################################
if should_run_step "data" || should_run_step "search"; then
    echo "Step 12b: Parsing documents for Cortex Search..."
    echo "------------------------------------------------"
    
    if [ -d "data/unstructured" ] && [ "$(ls -A data/unstructured 2>/dev/null)" ]; then
        # Refresh stage directory metadata first
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE WAREHOUSE ${WAREHOUSE};
            
            ALTER STAGE RAW.DOCS_STAGE REFRESH;
        " > /dev/null
        
        # Parse documents into chunks (clear existing first to avoid duplicates)
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE WAREHOUSE ${WAREHOUSE};
            USE SCHEMA SOP_LOGISTICS;
            
            -- Clear existing data to avoid duplicates on redeploy
            TRUNCATE TABLE IF EXISTS DOCUMENT_CHUNKS;
            TRUNCATE TABLE IF EXISTS DOCUMENT_METADATA;
            
            -- Insert document metadata
            INSERT INTO DOCUMENT_METADATA (FILE_PATH, FILE_NAME, DOCUMENT_TYPE, VENDOR_NAME, REGION)
            SELECT 
                relative_path AS FILE_PATH,
                SPLIT_PART(relative_path, '/', -1) AS FILE_NAME,
                CASE 
                    WHEN LOWER(relative_path) LIKE '%contract%' THEN 'CONTRACT'
                    WHEN LOWER(relative_path) LIKE '%sla%' THEN 'SLA'
                    WHEN LOWER(relative_path) LIKE '%meeting%' OR LOWER(relative_path) LIKE '%minutes%' THEN 'MEETING_MINUTES'
                    ELSE 'OTHER'
                END AS DOCUMENT_TYPE,
                CASE WHEN LOWER(relative_path) LIKE '%northeast%' THEN 'Northeast 3PL' ELSE 'National' END AS VENDOR_NAME,
                CASE WHEN LOWER(relative_path) LIKE '%northeast%' THEN 'Northeast' ELSE 'National' END AS REGION
            FROM DIRECTORY(@RAW.DOCS_STAGE)
            WHERE LOWER(relative_path) LIKE '%.pdf';

            -- Parse and chunk documents using Cortex PARSE_DOCUMENT
            INSERT INTO DOCUMENT_CHUNKS (DOCUMENT_ID, CHUNK_TEXT, CHUNK_INDEX, RELATIVE_PATH, FILE_URL, DOCUMENT_TYPE, VENDOR_NAME, REGION, PAGE_NUMBER)
            SELECT 
                dm.DOCUMENT_ID,
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@RAW.DOCS_STAGE, d.relative_path, {'mode': 'LAYOUT'}):content::VARCHAR,
                1, d.relative_path, d.file_url, dm.DOCUMENT_TYPE, dm.VENDOR_NAME, dm.REGION, 1
            FROM DIRECTORY(@RAW.DOCS_STAGE) d
            JOIN DOCUMENT_METADATA dm ON dm.FILE_PATH = d.relative_path
            WHERE LOWER(d.relative_path) LIKE '%.pdf';
        "
        
        echo -e "${GREEN}[OK]${NC} Documents parsed and indexed"
    else
        echo -e "${YELLOW}[WARN]${NC} No documents to parse - skipping"
    fi
    echo ""
fi

###############################################################################
# Step 12c: Deploy Cortex Agent (via REST API)
###############################################################################
if should_run_step "data" || should_run_step "sql" || should_run_step "agent"; then
    echo "Step 12c: Deploying Cortex Agent..."
    echo "------------------------------------------------"
    
    if [ "$SKIP_AGENT_DEPLOY" = true ]; then
        echo -e "${YELLOW}[SKIP]${NC} Agent deployment skipped (--skip-agent flag set)"
    else
        python utils/sf_cortex_agent_ops.py import \
            --input agents/SOP_ANALYST_AGENT.agent.json \
            --database $DATABASE \
            --schema SOP_LOGISTICS \
            --replace
        
        echo -e "${GREEN}[OK]${NC} Cortex Agent deployed"
    fi
    echo ""
fi

###############################################################################
# Step 13: Deploy Streamlit Application
###############################################################################
if should_run_step "streamlit"; then
    echo "Step 13: Deploying Streamlit application..."
    echo "------------------------------------------------"
    
    # Check if streamlit directory exists
    if [ ! -d "streamlit" ]; then
        echo -e "${YELLOW}[WARN]${NC} Streamlit directory not found - skipping deployment"
    elif [ ! -f "streamlit/snowflake.yml" ]; then
        echo -e "${YELLOW}[WARN]${NC} snowflake.yml not found - Streamlit not yet configured"
    else
        # Clean up existing deployment
        echo "Cleaning up existing Streamlit deployment..."
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE SCHEMA SOP_LOGISTICS;
            DROP STREAMLIT IF EXISTS SOP_SCENARIO_PLANNING_LOGISTICS_APP;
            REMOVE @streamlit/SOP_SCENARIO_PLANNING_LOGISTICS_APP;
        " 2>/dev/null || true
        
        # Clear local cache
        rm -rf streamlit/output/bundle 2>/dev/null || true
        
        # Deploy
        cd streamlit
        snow streamlit deploy \
            $SNOW_CONN \
            --database $DATABASE \
            --schema SOP_LOGISTICS \
            --role $ROLE \
            --replace
        cd ..
        
        echo -e "${GREEN}[OK]${NC} Streamlit deployment completed"
    fi
    echo ""
fi

###############################################################################
# Step 14: Deploy Optimization Notebook
###############################################################################
if should_run_step "notebook"; then
    echo "Step 14: Deploying optimization notebook..."
    echo "------------------------------------------------"
    
    NOTEBOOK_NAME="${PROJECT_PREFIX}_NOTEBOOK"
    NOTEBOOK_FILE="notebooks/prebuild_optimization.ipynb"
    NOTEBOOK_ENV="notebooks/environment.yml"
    
    # Check if notebook file exists
    if [ ! -f "$NOTEBOOK_FILE" ]; then
        echo -e "${YELLOW}[WARN]${NC} Notebook file not found: $NOTEBOOK_FILE - skipping"
    else
        # Create notebooks stage if it doesn't exist
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE SCHEMA SOP_LOGISTICS;
            
            CREATE STAGE IF NOT EXISTS NOTEBOOKS
                DIRECTORY = (ENABLE = TRUE)
                COMMENT = 'Stage for notebook files';
        " > /dev/null 2>&1
        
        # Upload notebook file to stage
        echo "Uploading notebook file..."
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE SCHEMA SOP_LOGISTICS;
            
            PUT file://${SCRIPT_DIR}/${NOTEBOOK_FILE} @NOTEBOOKS/${NOTEBOOK_NAME}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
        " > /dev/null
        
        # Upload environment.yml if it exists
        if [ -f "$NOTEBOOK_ENV" ]; then
            echo "Uploading environment.yml..."
            snow sql $SNOW_CONN -q "
                USE ROLE ${ROLE};
                USE DATABASE ${DATABASE};
                USE SCHEMA SOP_LOGISTICS;
                
                PUT file://${SCRIPT_DIR}/${NOTEBOOK_ENV} @NOTEBOOKS/${NOTEBOOK_NAME}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
            " > /dev/null
        fi
        
        # Create the notebook object
        echo "Creating notebook object..."
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE SCHEMA SOP_LOGISTICS;
            
            CREATE OR REPLACE NOTEBOOK ${NOTEBOOK_NAME}
                FROM '@NOTEBOOKS/${NOTEBOOK_NAME}/'
                MAIN_FILE = 'prebuild_optimization.ipynb'
                QUERY_WAREHOUSE = '${WAREHOUSE}'
                COMMENT = 'Pre-build production optimization notebook for Q4 demand planning';
        " > /dev/null
        
        # Set live version for headless execution
        echo "Setting live version for headless execution..."
        snow sql $SNOW_CONN -q "
            USE ROLE ${ROLE};
            USE DATABASE ${DATABASE};
            USE SCHEMA SOP_LOGISTICS;
            
            ALTER NOTEBOOK ${NOTEBOOK_NAME} ADD LIVE VERSION FROM LAST;
        " > /dev/null 2>&1 || echo -e "${YELLOW}[WARN]${NC} Could not set live version - notebook may need manual execution first"
        
        echo -e "${GREEN}[OK]${NC} Notebook deployed: ${NOTEBOOK_NAME}"
    fi
    echo ""
fi

###############################################################################
# Deployment Complete
###############################################################################
echo ""
echo "=================================================="
echo -e "${GREEN}Deployment Complete!${NC}"
echo "=================================================="
echo ""

if [ -z "$ONLY_COMPONENT" ]; then
    echo "Resources Created:"
    echo "  - Database: $DATABASE"
    echo "  - Schemas: RAW, ATOMIC, SOP_LOGISTICS"
    echo "  - Role: $ROLE"
    echo "  - Warehouse: $WAREHOUSE"
    echo "  - Semantic View: SOP_ANALYTICS_SEMANTIC_MODEL"
    echo "  - Cortex Agent: SOP_ANALYST_AGENT"
    echo "  - Notebook: ${PROJECT_PREFIX}_NOTEBOOK"
    echo ""
    echo "Next Steps:"
    echo "  1. Check deployment status:"
    echo "     ./run.sh status"
    echo ""
    echo "  2. Run query tests:"
    echo "     ./run.sh test"
    echo ""
    echo "  3. Execute the main workflow:"
    echo "     ./run.sh main"
    echo ""
    echo "  4. Open the Streamlit dashboard:"
    echo "     ./run.sh streamlit"
else
    echo "Deployed component: $ONLY_COMPONENT"
fi
echo ""

