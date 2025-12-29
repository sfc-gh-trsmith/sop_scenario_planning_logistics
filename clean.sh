#!/bin/bash
###############################################################################
# clean.sh - Remove all S&OP Scenario Planning resources from Snowflake
#
# Deletes in dependency order:
#   1. Compute Pools
#   2. Warehouses
#   3. Database (cascades to all tables, views, stages, apps)
#   4. Role
#
# Usage:
#   ./clean.sh              # Interactive confirmation
#   ./clean.sh --force      # Skip confirmation
###############################################################################

set -e
set -o pipefail

# Configuration
CONNECTION_NAME="demo"
FORCE=false
ENV_PREFIX=""

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

Remove all S&OP Scenario Planning resources from Snowflake.

Options:
  -c, --connection NAME    Snowflake CLI connection name (default: demo)
  -p, --prefix PREFIX      Environment prefix for resources (e.g., DEV)
  --force, --yes, -y       Skip confirmation prompt
  -h, --help               Show this help message

Examples:
  $0                       # Interactive cleanup
  $0 --force               # Force cleanup without confirmation
  $0 -c prod --force       # Force cleanup using 'prod' connection
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
        --force|--yes|-y)
            FORCE=true
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

# Display warning
echo "=================================================="
echo "S&OP Scenario Planning - Cleanup"
echo "=================================================="
echo ""
echo -e "${YELLOW}WARNING: This will permanently delete all project resources!${NC}"
echo ""
echo "Resources to be deleted:"
echo "  - Compute Pool: $COMPUTE_POOL"
echo "  - Warehouse: $WAREHOUSE"
echo "  - Database: $DATABASE (includes all tables, views, stages, Streamlit apps)"
echo "  - Role: $ROLE"
echo ""

# Confirmation
if [ "$FORCE" = false ]; then
    read -p "Are you sure you want to delete all resources? (yes/no): " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Cleanup cancelled."
        exit 0
    fi
fi

echo ""
echo "Starting cleanup..."
echo ""

###############################################################################
# Step 1: Drop Compute Pool
###############################################################################
echo "Step 1: Dropping compute pool..."
snow sql $SNOW_CONN -q "
    USE ROLE ACCOUNTADMIN;
    DROP COMPUTE POOL IF EXISTS ${COMPUTE_POOL};
" 2>/dev/null && echo -e "${GREEN}[OK]${NC} Compute pool dropped" \
             || echo -e "${YELLOW}[WARN]${NC} Compute pool not found or already dropped"

###############################################################################
# Step 2: Drop Warehouse
###############################################################################
echo "Step 2: Dropping warehouse..."
snow sql $SNOW_CONN -q "
    USE ROLE ACCOUNTADMIN;
    DROP WAREHOUSE IF EXISTS ${WAREHOUSE};
" 2>/dev/null && echo -e "${GREEN}[OK]${NC} Warehouse dropped" \
             || echo -e "${YELLOW}[WARN]${NC} Warehouse not found or already dropped"

###############################################################################
# Step 3: Drop Database (cascades to all contained objects)
###############################################################################
echo "Step 3: Dropping database..."
snow sql $SNOW_CONN -q "
    USE ROLE ACCOUNTADMIN;
    DROP DATABASE IF EXISTS ${DATABASE};
" 2>/dev/null && echo -e "${GREEN}[OK]${NC} Database dropped" \
             || echo -e "${YELLOW}[WARN]${NC} Database not found or already dropped"

###############################################################################
# Step 4: Drop Role
###############################################################################
echo "Step 4: Dropping role..."
snow sql $SNOW_CONN -q "
    USE ROLE ACCOUNTADMIN;
    DROP ROLE IF EXISTS ${ROLE};
" 2>/dev/null && echo -e "${GREEN}[OK]${NC} Role dropped" \
             || echo -e "${YELLOW}[WARN]${NC} Role not found or already dropped"

###############################################################################
# Cleanup Complete
###############################################################################
echo ""
echo "=================================================="
echo -e "${GREEN}Cleanup Complete!${NC}"
echo "=================================================="
echo ""
echo "All Snowflake resources have been removed."
echo "Local files (data/synthetic/, docs/, etc.) are preserved."
echo ""
echo "To redeploy, run:"
echo "  ./deploy.sh"
echo ""

