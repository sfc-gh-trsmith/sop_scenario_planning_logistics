#!/bin/bash

###############################################################################
# create_user.sh - Create a Snowflake user with access to a demo project
#
# PORTABILITY NOTES:
# ==================
# This script is designed to be portable across projects with minimal changes.
# When copying to a new project:
#
#   1. Ensure .cursor/PROJECT_NAME.md exists with the project name
#   2. Ensure deploy.sh exists with PROJECT_PREFIX variable
#   3. No other changes should be needed!
#
# The script will:
#   - Auto-detect PROJECT_PREFIX from deploy.sh
#   - Auto-detect CONNECTION_NAME from deploy.sh
#   - Query Snowflake for all schemas in the database (no hardcoding needed)
#   - Validate all objects exist before granting
#
###############################################################################
#
# This script automatically infers project configuration from deploy.sh in the
# current project directory. All inferred values can be overridden via CLI.
#
# Usage:
#   ./create_user.sh --user USERNAME [OPTIONS]
#
# Required:
#   --user, -u NAME           Username to create
#
# Optional (auto-inferred from project if not specified):
#   --connection, -c NAME     Snowflake CLI connection name
#   --database, -d NAME       Project database name
#   --role, -r NAME           Role name
#   --warehouse, -w NAME      Warehouse name
#   --compute-pool NAME       Compute pool name (for notebook access)
#   --prefix PREFIX           Environment prefix (e.g., DEV, PROD)
#
# Other Options:
#   --password, -p PASS       Initial password (if not set, user must use SSO)
#   --email EMAIL             User's email address
#   --first-name NAME         User's first name
#   --last-name NAME          User's last name
#   --comment TEXT            Comment for the user
#   --no-change-password      Do NOT force password change on first login
#   --dry-run                 Show SQL without executing
#   --show-config             Show inferred configuration and exit
#   -h, --help                Show this help message
#
# Examples:
#   # Minimal - just specify username (everything else inferred from project)
#   ./create_user.sh -u demo_user -p TempPass123!
#
#   # Override connection
#   ./create_user.sh -u demo_user -c prod -p TempPass123!
#
#   # Use environment prefix (matches deploy.sh --prefix option)
#   ./create_user.sh -u demo_user --prefix DEV -p TempPass123!
#
#   # Show what would be inferred
#   ./create_user.sh --show-config
#
#   # Dry run to see SQL
#   ./create_user.sh -u test_user --dry-run
###############################################################################

set -e
set -o pipefail

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

###############################################################################
# Auto-detect project configuration from deploy.sh
###############################################################################

# Defaults
DEFAULT_CONNECTION_NAME="demo"
DEFAULT_PROJECT_PREFIX=""

# First, try to extract PROJECT_PREFIX from deploy.sh (most reliable source)
if [ -f "$SCRIPT_DIR/deploy.sh" ]; then
    # Extract PROJECT_PREFIX variable from deploy.sh (strip comments first, then extract quoted value)
    DETECTED_PREFIX=$(grep -E '^PROJECT_PREFIX=' "$SCRIPT_DIR/deploy.sh" 2>/dev/null | head -1 | sed 's/#.*//' | sed 's/PROJECT_PREFIX=["'"'"']\{0,1\}\([^"'"'"']*\)["'"'"']\{0,1\}.*/\1/' | tr -d '[:space:]')
    if [ -n "$DETECTED_PREFIX" ]; then
        DEFAULT_PROJECT_PREFIX="$DETECTED_PREFIX"
    fi
    
    # Extract CONNECTION_NAME from deploy.sh (strip comments first, then extract quoted value)
    DETECTED_CONNECTION=$(grep -E '^CONNECTION_NAME=' "$SCRIPT_DIR/deploy.sh" 2>/dev/null | head -1 | sed 's/#.*//' | sed 's/CONNECTION_NAME=["'"'"']\{0,1\}\([^"'"'"']*\)["'"'"']\{0,1\}.*/\1/' | tr -d '[:space:]')
    if [ -n "$DETECTED_CONNECTION" ]; then
        DEFAULT_CONNECTION_NAME="$DETECTED_CONNECTION"
    fi
fi

# Fallback: Read project name from .cursor/PROJECT_NAME.md if not found in deploy.sh
PROJECT_NAME_FILE="${SCRIPT_DIR}/.cursor/PROJECT_NAME.md"
DIR_BASENAME=$(basename "$SCRIPT_DIR")

if [ -z "$DEFAULT_PROJECT_PREFIX" ]; then
    if [ -f "$PROJECT_NAME_FILE" ]; then
        PROJECT_NAME=$(head -1 "$PROJECT_NAME_FILE" | tr -d '[:space:]')
        if [ -n "$PROJECT_NAME" ]; then
            # Convert to uppercase for Snowflake naming
            DEFAULT_PROJECT_PREFIX=$(echo "$PROJECT_NAME" | tr '[:lower:]' '[:upper:]')
        fi
    fi
fi

# Validate project prefix was found
if [ -z "$DEFAULT_PROJECT_PREFIX" ]; then
    echo -e "${YELLOW}[WARN] Could not detect PROJECT_PREFIX from deploy.sh or .cursor/PROJECT_NAME.md${NC}"
    echo "Using directory name: $DIR_BASENAME"
    read -p "Continue? (yes/no): " CONFIRM
    [ "$CONFIRM" != "yes" ] && exit 1
    DEFAULT_PROJECT_PREFIX=$(echo "$DIR_BASENAME" | tr '[:lower:]' '[:upper:]' | tr '-' '_')
fi

# Initialize with defaults (can be overridden by CLI)
CONNECTION_NAME=""
USER_NAME=""
SNOWFLAKE_DATABASE=""
SNOWFLAKE_ROLE=""
SNOWFLAKE_WAREHOUSE=""
COMPUTE_POOL_NAME=""
ENV_PREFIX=""
PASSWORD=""
EMAIL=""
FIRST_NAME=""
LAST_NAME=""
COMMENT=""
MUST_CHANGE_PASSWORD="TRUE"
DRY_RUN=false
SHOW_CONFIG=false

# Function to display usage
usage() {
    cat << EOF
Usage: $0 --user USERNAME [OPTIONS]

Create a Snowflake user with access to a demo project.

This script auto-detects project configuration from deploy.sh in the current
directory. Detected values can be overridden via command line options.

NOTE: This script dynamically discovers ALL schemas in the database and grants
access to each. No hardcoding of schema names is required.

Required:
  -u, --user NAME           Username to create

Auto-Inferred (override with CLI if needed):
  -c, --connection NAME     Snowflake CLI connection name
  -d, --database NAME       Project database name
  -r, --role NAME           Role name
  -w, --warehouse NAME      Warehouse name
  --compute-pool NAME       Compute pool name (for notebook access)
  --prefix PREFIX           Environment prefix (e.g., DEV, PROD)

Other Options:
  -p, --password PASS       Initial password (if not set, user must use SSO)
  --email EMAIL             User's email address
  --first-name NAME         User's first name
  --last-name NAME          User's last name
  --comment TEXT            Comment for the user
  --no-change-password      Do NOT force password change on first login
  --dry-run                 Show SQL without executing
  --show-config             Show inferred configuration and exit
  -h, --help                Show this help message

EOF

    # Show what's auto-detected
    echo "Auto-Detected Configuration (from deploy.sh):"
    if [ -n "$DEFAULT_PROJECT_PREFIX" ]; then
        echo "  Project Prefix:    $DEFAULT_PROJECT_PREFIX"
        echo "  Connection:        $DEFAULT_CONNECTION_NAME"
        echo "  Database:          $DEFAULT_PROJECT_PREFIX"
        echo "  Role:              ${DEFAULT_PROJECT_PREFIX}_ROLE"
        echo "  Warehouse:         ${DEFAULT_PROJECT_PREFIX}_WH"
        echo "  Compute Pool:      ${DEFAULT_PROJECT_PREFIX}_COMPUTE_POOL"
        echo "  Schemas:           (discovered dynamically from Snowflake)"
    else
        echo "  (No deploy.sh found - specify all parameters manually)"
    fi
    echo ""

    cat << EOF
Examples:
  $0 -u demo_user -p TempPass123!           # Minimal - infer everything
  $0 -u demo_user -c prod -p TempPass123!   # Override connection
  $0 -u demo_user --prefix DEV              # Use DEV environment prefix
  $0 --show-config                          # Show detected configuration
  $0 -u test_user --dry-run                 # Preview SQL
EOF
    exit 0
}

# Error exit function
error_exit() {
    echo -e "${RED}[ERROR] $1${NC}" >&2
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -u|--user)
            USER_NAME="$2"
            shift 2
            ;;
        -c|--connection)
            CONNECTION_NAME="$2"
            shift 2
            ;;
        -d|--database)
            SNOWFLAKE_DATABASE="$2"
            shift 2
            ;;
        -r|--role)
            SNOWFLAKE_ROLE="$2"
            shift 2
            ;;
        -w|--warehouse)
            SNOWFLAKE_WAREHOUSE="$2"
            shift 2
            ;;
        --compute-pool)
            COMPUTE_POOL_NAME="$2"
            shift 2
            ;;
        --prefix)
            ENV_PREFIX="$2"
            shift 2
            ;;
        -p|--password)
            PASSWORD="$2"
            shift 2
            ;;
        --email)
            EMAIL="$2"
            shift 2
            ;;
        --first-name)
            FIRST_NAME="$2"
            shift 2
            ;;
        --last-name)
            LAST_NAME="$2"
            shift 2
            ;;
        --comment)
            COMMENT="$2"
            shift 2
            ;;
        --no-change-password)
            MUST_CHANGE_PASSWORD="FALSE"
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --show-config)
            SHOW_CONFIG=true
            shift
            ;;
        *)
            error_exit "Unknown option: $1\nUse --help for usage information"
            ;;
    esac
done

###############################################################################
# Apply Defaults and Build Configuration
###############################################################################

# Apply default connection if not specified
if [ -z "$CONNECTION_NAME" ]; then
    CONNECTION_NAME="$DEFAULT_CONNECTION_NAME"
fi

# Compute the full prefix (may include environment prefix)
if [ -z "$DEFAULT_PROJECT_PREFIX" ]; then
    # No deploy.sh found - require explicit parameters
    if [ -z "$SNOWFLAKE_DATABASE" ]; then
        error_exit "No deploy.sh found. Please specify --database explicitly.\nUse --help for usage information"
    fi
    FULL_PREFIX="$SNOWFLAKE_DATABASE"
else
    # Use project prefix, optionally with environment prefix
    if [ -n "$ENV_PREFIX" ]; then
        FULL_PREFIX="${ENV_PREFIX}_${DEFAULT_PROJECT_PREFIX}"
    else
        FULL_PREFIX="${DEFAULT_PROJECT_PREFIX}"
    fi
fi

# Apply inferred values for any unspecified parameters
if [ -z "$SNOWFLAKE_DATABASE" ]; then
    SNOWFLAKE_DATABASE="$FULL_PREFIX"
fi

if [ -z "$SNOWFLAKE_ROLE" ]; then
    SNOWFLAKE_ROLE="${FULL_PREFIX}_ROLE"
fi

if [ -z "$SNOWFLAKE_WAREHOUSE" ]; then
    SNOWFLAKE_WAREHOUSE="${FULL_PREFIX}_WH"
fi

if [ -z "$COMPUTE_POOL_NAME" ]; then
    # Only set compute pool if we have a project prefix
    if [ -n "$DEFAULT_PROJECT_PREFIX" ]; then
        COMPUTE_POOL_NAME="${FULL_PREFIX}_COMPUTE_POOL"
    fi
fi

if [ -z "$COMMENT" ]; then
    COMMENT="${SNOWFLAKE_DATABASE} Demo User"
fi

###############################################################################
# Show Config Mode
###############################################################################
if [ "$SHOW_CONFIG" = true ]; then
    echo "=================================================="
    echo "Inferred Project Configuration"
    echo "=================================================="
    echo ""
    if [ -f "$SCRIPT_DIR/deploy.sh" ]; then
        echo -e "${GREEN}[OK]${NC} deploy.sh found in project directory"
        echo "  Detected PROJECT_PREFIX: $DEFAULT_PROJECT_PREFIX"
    else
        echo -e "${YELLOW}[WARN]${NC} No deploy.sh found - using defaults"
    fi
    echo ""
    echo "Configuration that will be used:"
    echo "  Connection:     $CONNECTION_NAME"
    echo "  Database:       $SNOWFLAKE_DATABASE"
    echo "  Role:           $SNOWFLAKE_ROLE"
    echo "  Warehouse:      $SNOWFLAKE_WAREHOUSE"
    if [ -n "$COMPUTE_POOL_NAME" ]; then
        echo "  Compute Pool:   $COMPUTE_POOL_NAME"
    fi
    if [ -n "$ENV_PREFIX" ]; then
        echo "  Env Prefix:     $ENV_PREFIX"
    fi
    echo "  Schemas:        (will be discovered dynamically from Snowflake)"
    echo ""
    echo "Override any value with CLI options. Use --help for details."
    echo ""
    exit 0
fi

###############################################################################
# Validate Required Parameters
###############################################################################
if [ -z "$USER_NAME" ]; then
    error_exit "Missing required parameter: --user\nUse --help for usage information"
fi

# Validate username format (alphanumeric and underscore only)
if ! [[ "$USER_NAME" =~ ^[A-Za-z][A-Za-z0-9_]*$ ]]; then
    error_exit "Invalid username format. Must start with a letter and contain only letters, numbers, and underscores."
fi

# Convert username to uppercase (Snowflake convention)
USER_NAME_UPPER=$(echo "$USER_NAME" | tr '[:lower:]' '[:upper:]')

echo "=================================================="
echo "Snowflake Demo - Create User"
echo "=================================================="
echo ""
echo "Configuration (auto-inferred from project):"
echo "  User:       $USER_NAME_UPPER"
echo "  Database:   $SNOWFLAKE_DATABASE"
echo "  Role:       $SNOWFLAKE_ROLE"
echo "  Warehouse:  $SNOWFLAKE_WAREHOUSE"
if [ -n "$COMPUTE_POOL_NAME" ]; then
    echo "  Compute Pool: $COMPUTE_POOL_NAME"
fi
if [ -n "$ENV_PREFIX" ]; then
    echo "  Env Prefix: $ENV_PREFIX"
fi
echo ""
echo -e "${BLUE}[TIP]${NC} Use --show-config to see all inferred values"
echo ""

###############################################################################
# Step 1: Check Prerequisites
###############################################################################
echo "Step 1: Checking prerequisites..."
echo "------------------------------------------------"

# Check if snow CLI is installed
if ! command -v snow &> /dev/null; then
    error_exit "snow CLI not found. Install with: pip install snowflake-cli"
fi
echo -e "${GREEN}[OK]${NC} Snowflake CLI found"

# Test actual connection with a simple query
echo "Testing Snowflake connection..."
CONNECTION_TEST=$(snow sql -c "$CONNECTION_NAME" -q "SELECT CURRENT_USER()" 2>&1)
if [ $? -ne 0 ]; then
    echo -e "${RED}[ERROR]${NC} Failed to connect to Snowflake"
    echo ""
    echo "Connection test output:"
    echo "$CONNECTION_TEST"
    echo ""
    echo "Possible causes:"
    echo "  - JWT private key passphrase not set"
    echo "  - Invalid credentials"
    echo "  - Network connectivity issues"
    echo ""
    echo "For JWT authentication, ensure you've set the passphrase:"
    echo "  export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='your_passphrase'"
    echo ""
    exit 1
fi
echo -e "${GREEN}[OK]${NC} Connection '$CONNECTION_NAME' verified"
echo ""

###############################################################################
# Step 2: Validate Project Resources Exist
###############################################################################
echo "Step 2: Validating project resources..."
echo "------------------------------------------------"

# Verify that the project is deployed (database exists)
echo "Checking database..."
DB_CHECK=$(snow sql -c "$CONNECTION_NAME" -q "SHOW DATABASES LIKE '$SNOWFLAKE_DATABASE'" 2>&1)
if ! echo "$DB_CHECK" | grep -q "$SNOWFLAKE_DATABASE"; then
    error_exit "Database '$SNOWFLAKE_DATABASE' not found. Deploy the project first with ./deploy.sh"
fi
echo -e "${GREEN}[OK]${NC} Database '$SNOWFLAKE_DATABASE' exists"

# Verify role exists
echo "Checking role..."
ROLE_CHECK=$(snow sql -c "$CONNECTION_NAME" -q "SHOW ROLES LIKE '$SNOWFLAKE_ROLE'" 2>&1)
if ! echo "$ROLE_CHECK" | grep -qi "$SNOWFLAKE_ROLE"; then
    error_exit "Role '$SNOWFLAKE_ROLE' not found. Deploy the project first with ./deploy.sh"
fi
echo -e "${GREEN}[OK]${NC} Role '$SNOWFLAKE_ROLE' exists"

# Verify warehouse exists
echo "Checking warehouse..."
WH_CHECK=$(snow sql -c "$CONNECTION_NAME" -q "SHOW WAREHOUSES LIKE '$SNOWFLAKE_WAREHOUSE'" 2>&1)
if ! echo "$WH_CHECK" | grep -qi "$SNOWFLAKE_WAREHOUSE"; then
    error_exit "Warehouse '$SNOWFLAKE_WAREHOUSE' not found. Deploy the project first with ./deploy.sh"
fi
echo -e "${GREEN}[OK]${NC} Warehouse '$SNOWFLAKE_WAREHOUSE' exists"

# Check compute pool if specified (optional - may not exist for all projects)
COMPUTE_POOL_EXISTS=false
if [ -n "$COMPUTE_POOL_NAME" ]; then
    echo "Checking compute pool..."
    CP_CHECK=$(snow sql -c "$CONNECTION_NAME" -q "SHOW COMPUTE POOLS LIKE '$COMPUTE_POOL_NAME'" 2>&1)
    if echo "$CP_CHECK" | grep -qi "$COMPUTE_POOL_NAME"; then
        COMPUTE_POOL_EXISTS=true
        echo -e "${GREEN}[OK]${NC} Compute pool '$COMPUTE_POOL_NAME' exists"
    else
        echo -e "${YELLOW}[INFO]${NC} Compute pool '$COMPUTE_POOL_NAME' not found - skipping compute pool grants"
        COMPUTE_POOL_NAME=""
    fi
fi

# Check if user already exists
USER_EXISTS=false
echo "Checking user..."
USER_CHECK=$(snow sql -c "$CONNECTION_NAME" -q "SHOW USERS LIKE '$USER_NAME_UPPER'" --format json 2>&1)
# Check if JSON result contains actual user data (has "name" field, not just empty array [])
if echo "$USER_CHECK" | grep -q '"name"' && echo "$USER_CHECK" | grep -qi "\"$USER_NAME_UPPER\""; then
    USER_EXISTS=true
    echo -e "${YELLOW}[INFO]${NC} User '$USER_NAME_UPPER' already exists - will grant project access"
else
    echo -e "${GREEN}[OK]${NC} User '$USER_NAME_UPPER' does not exist - will create"
fi

echo ""

###############################################################################
# Step 3: Discover All Schemas in Database
###############################################################################
echo "Step 3: Discovering schemas in database..."
echo "------------------------------------------------"

# Query Snowflake for all schemas (excluding system schemas)
SCHEMA_QUERY="SELECT SCHEMA_NAME FROM ${SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC') ORDER BY SCHEMA_NAME"
SCHEMA_RESULT=$(snow sql -c "$CONNECTION_NAME" -q "$SCHEMA_QUERY" --format json 2>&1)

if [ $? -ne 0 ]; then
    error_exit "Failed to query schemas in database '$SNOWFLAKE_DATABASE'. Error: $SCHEMA_RESULT"
fi

# Parse schema names from JSON result
# The output format is: [{"SCHEMA_NAME": "SCHEMA1"}, {"SCHEMA_NAME": "SCHEMA2"}, ...]
DISCOVERED_SCHEMAS=$(echo "$SCHEMA_RESULT" | grep -o '"SCHEMA_NAME"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/"SCHEMA_NAME"[[:space:]]*:[[:space:]]*"\([^"]*\)"/\1/' | tr '\n' ' ')

if [ -z "$DISCOVERED_SCHEMAS" ]; then
    error_exit "No schemas found in database '$SNOWFLAKE_DATABASE'. Deploy the project first with ./deploy.sh"
fi

# Count and display discovered schemas
SCHEMA_COUNT=$(echo "$DISCOVERED_SCHEMAS" | wc -w | tr -d ' ')
echo -e "${GREEN}[OK]${NC} Found $SCHEMA_COUNT schema(s): $DISCOVERED_SCHEMAS"

# Determine the best default schema for the user
# Priority: 1) Schema matching PROJECT_PREFIX, 2) *_LOGISTICS schema, 3) *_MART schema, 4) First non-RAW/ATOMIC schema
if echo "$DISCOVERED_SCHEMAS" | grep -qw "$DEFAULT_PROJECT_PREFIX"; then
    DEFAULT_SCHEMA="$DEFAULT_PROJECT_PREFIX"
elif echo "$DISCOVERED_SCHEMAS" | grep -qE "_LOGISTICS$"; then
    # Look for any schema ending in _LOGISTICS (data mart pattern for this project)
    DEFAULT_SCHEMA=$(echo "$DISCOVERED_SCHEMAS" | tr ' ' '\n' | grep -E "_LOGISTICS$" | head -1)
elif echo "$DISCOVERED_SCHEMAS" | grep -qE "_MART$"; then
    # Look for any schema ending in _MART (common data mart pattern)
    DEFAULT_SCHEMA=$(echo "$DISCOVERED_SCHEMAS" | tr ' ' '\n' | grep -E "_MART$" | head -1)
else
    # Prefer a non-RAW/ATOMIC schema if available, otherwise use first schema
    NON_STAGING_SCHEMA=$(echo "$DISCOVERED_SCHEMAS" | tr ' ' '\n' | grep -vE "^(RAW|ATOMIC)$" | head -1)
    if [ -n "$NON_STAGING_SCHEMA" ]; then
        DEFAULT_SCHEMA="$NON_STAGING_SCHEMA"
    else
        DEFAULT_SCHEMA=$(echo "$DISCOVERED_SCHEMAS" | awk '{print $1}')
    fi
fi
echo "  Default schema for user: $DEFAULT_SCHEMA"

echo ""

###############################################################################
# Step 4: Build SQL Commands
###############################################################################
echo "Step 4: Building SQL commands..."
echo "------------------------------------------------"

# Build optional user properties (only used for new users)
USER_OPTIONS=""
if [ -n "$PASSWORD" ]; then
    USER_OPTIONS="${USER_OPTIONS} PASSWORD = '${PASSWORD}'"
fi
if [ -n "$EMAIL" ]; then
    USER_OPTIONS="${USER_OPTIONS} EMAIL = '${EMAIL}'"
fi
if [ -n "$FIRST_NAME" ]; then
    USER_OPTIONS="${USER_OPTIONS} FIRST_NAME = '${FIRST_NAME}'"
fi
if [ -n "$LAST_NAME" ]; then
    USER_OPTIONS="${USER_OPTIONS} LAST_NAME = '${LAST_NAME}'"
fi

# Build compute pool grants if specified and exists
COMPUTE_POOL_GRANTS=""
if [ -n "$COMPUTE_POOL_NAME" ]; then
    COMPUTE_POOL_GRANTS="
-- ============================================================
-- GRANT COMPUTE POOL ACCESS (for notebook execution)
-- ============================================================

GRANT USAGE ON COMPUTE POOL ${COMPUTE_POOL_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT MONITOR ON COMPUTE POOL ${COMPUTE_POOL_NAME} TO ROLE ${SNOWFLAKE_ROLE};
"
fi

# Build user creation SQL (only if user doesn't exist)
USER_CREATE_SQL=""
if [ "$USER_EXISTS" = false ]; then
    USER_CREATE_SQL="
-- ============================================================
-- 1. CREATE USER
-- ============================================================

CREATE USER IF NOT EXISTS ${USER_NAME_UPPER}
    ${USER_OPTIONS}
    MUST_CHANGE_PASSWORD = ${MUST_CHANGE_PASSWORD}
    DEFAULT_WAREHOUSE = '${SNOWFLAKE_WAREHOUSE}'
    DEFAULT_NAMESPACE = '${SNOWFLAKE_DATABASE}.${DEFAULT_SCHEMA}'
    DEFAULT_ROLE = '${SNOWFLAKE_ROLE}'
    COMMENT = '${COMMENT}';
"
fi

# Build the SQL header based on whether user exists
if [ "$USER_EXISTS" = true ]; then
    SQL_HEADER="GRANT ACCESS TO USER: ${USER_NAME_UPPER}"
else
    SQL_HEADER="CREATE USER: ${USER_NAME_UPPER}"
fi

# Note: Notebook access is controlled through schema permissions, not direct grants
# GRANT USAGE ON ALL NOTEBOOKS is not supported by Snowflake

# Build per-schema grants dynamically
SCHEMA_GRANTS=""
SCHEMA_NUM=1
for SCHEMA_NAME in $DISCOVERED_SCHEMAS; do
    SCHEMA_GRANTS="${SCHEMA_GRANTS}
-- ============================================================
-- SCHEMA ${SCHEMA_NUM}: ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME}
-- ============================================================

-- Grant schema usage
GRANT USAGE ON SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant table access
GRANT SELECT ON ALL TABLES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT SELECT ON FUTURE TABLES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant view access
GRANT SELECT ON ALL VIEWS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant stage access
GRANT READ ON ALL STAGES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT READ ON FUTURE STAGES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant function access
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant procedure access
GRANT USAGE ON ALL PROCEDURES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant Streamlit access
GRANT USAGE ON ALL STREAMLITS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant file format access
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant Cortex Search service access
GRANT USAGE ON ALL CORTEX SEARCH SERVICES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SCHEMA_NAME} TO ROLE ${SNOWFLAKE_ROLE};
"
    SCHEMA_NUM=$((SCHEMA_NUM + 1))
done

# Build the complete SQL script
SQL_SCRIPT=$(cat << EOF
-- ============================================================
-- ${SQL_HEADER}
-- For: ${SNOWFLAKE_DATABASE} Demo
-- Schemas: ${DISCOVERED_SCHEMAS}
-- Generated: $(date '+%Y-%m-%d %H:%M:%S')
-- ============================================================

USE ROLE ACCOUNTADMIN;
${USER_CREATE_SQL}
-- ============================================================
-- 2. GRANT PROJECT ROLE TO USER
-- ============================================================

GRANT ROLE ${SNOWFLAKE_ROLE} TO USER ${USER_NAME_UPPER};

-- ============================================================
-- 3. GRANT WAREHOUSE USAGE (to role, not user)
-- ============================================================

GRANT USAGE ON WAREHOUSE ${SNOWFLAKE_WAREHOUSE} TO ROLE ${SNOWFLAKE_ROLE};

-- ============================================================
-- 4. GRANT DATABASE ACCESS
-- ============================================================

GRANT USAGE ON DATABASE ${SNOWFLAKE_DATABASE} TO ROLE ${SNOWFLAKE_ROLE};

${SCHEMA_GRANTS}
${COMPUTE_POOL_GRANTS}
-- ============================================================
-- GRANT CORTEX LLM ACCESS
-- ============================================================

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ${SNOWFLAKE_ROLE};

-- ============================================================
-- VERIFICATION
-- ============================================================

DESCRIBE USER ${USER_NAME_UPPER};
SHOW GRANTS TO USER ${USER_NAME_UPPER};

SELECT 'User ${USER_NAME_UPPER} configured successfully with access to ${SCHEMA_COUNT} schema(s)!' AS status;
EOF
)

echo -e "${GREEN}[OK]${NC} SQL commands built for $SCHEMA_COUNT schema(s)"
echo ""

###############################################################################
# Step 5: Execute or Display SQL
###############################################################################
if [ "$DRY_RUN" = true ]; then
    echo ""
    echo -e "${YELLOW}[DRY RUN] The following SQL would be executed:${NC}"
    echo "=================================================="
    echo ""
    echo "$SQL_SCRIPT"
    echo ""
    echo "=================================================="
    echo -e "${YELLOW}[DRY RUN] No changes were made.${NC}"
    echo ""
    exit 0
fi

if [ "$USER_EXISTS" = true ]; then
    echo "Step 5: Granting project access to existing user '${USER_NAME_UPPER}'..."
else
    echo "Step 5: Creating user '${USER_NAME_UPPER}'..."
fi
echo "------------------------------------------------"

# Execute the SQL
snow sql -c "$CONNECTION_NAME" -q "$SQL_SCRIPT"

if [ $? -eq 0 ]; then
    echo ""
    echo "=================================================="
    if [ "$USER_EXISTS" = true ]; then
        echo -e "${GREEN}[OK] Project Access Granted Successfully!${NC}"
    else
        echo -e "${GREEN}[OK] User Created Successfully!${NC}"
    fi
    echo "=================================================="
    echo ""
    
    # Retrieve account information
    echo "Retrieving account information..."
    ACCOUNT_INFO=$(snow sql -c "$CONNECTION_NAME" -q "SELECT CURRENT_ACCOUNT_NAME() AS account, CURRENT_ORGANIZATION_NAME() AS org" --format json 2>/dev/null || echo "[]")
    
    # Parse account info
    ACCOUNT_NAME=$(echo "$ACCOUNT_INFO" | grep -o '"ACCOUNT"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/' | tr '[:upper:]' '[:lower:]' || echo "")
    ORG_NAME=$(echo "$ACCOUNT_INFO" | grep -o '"ORG"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/.*: *"\([^"]*\)".*/\1/' | tr '[:upper:]' '[:lower:]' || echo "")
    
    # Build account URL (lowercase for web URL, uppercase for identifier)
    if [ -n "$ORG_NAME" ] && [ -n "$ACCOUNT_NAME" ]; then
        ACCOUNT_URL="https://app.snowflake.com/${ORG_NAME}/${ACCOUNT_NAME}"
        ACCOUNT_IDENTIFIER=$(echo "${ORG_NAME}-${ACCOUNT_NAME}" | tr '[:lower:]' '[:upper:]')
    elif [ -n "$ACCOUNT_NAME" ]; then
        ACCOUNT_URL="https://${ACCOUNT_NAME}.snowflakecomputing.com"
        ACCOUNT_IDENTIFIER="${ACCOUNT_NAME}"
    else
        ACCOUNT_URL="[Could not retrieve - check connection]"
        ACCOUNT_IDENTIFIER="[Check with administrator]"
    fi
    
    echo ""
    echo "============================================================"
    echo "  USER ACCESS INFORMATION"
    echo "============================================================"
    echo ""
    echo "SNOWFLAKE LOGIN"
    echo "---------------"
    echo "  Web Login URL:      ${ACCOUNT_URL}"
    echo "  Account Identifier: ${ACCOUNT_IDENTIFIER}"
    echo "  Username:           ${USER_NAME_UPPER}"
    if [ "$USER_EXISTS" = true ]; then
        echo "  Password:           [Existing user - use current credentials]"
    elif [ -n "$PASSWORD" ]; then
        echo "  Temporary Password: ${PASSWORD}"
        if [ "$MUST_CHANGE_PASSWORD" = "TRUE" ]; then
            echo "  (Password change required on first login)"
        fi
    else
        echo "  Password:           [Contact administrator for SSO setup]"
    fi
    echo ""
    echo "PROJECT DETAILS"
    echo "---------------"
    echo "  Database:           ${SNOWFLAKE_DATABASE}"
    echo "  Schemas:            ${DISCOVERED_SCHEMAS}"
    echo "  Default Schema:     ${SNOWFLAKE_DATABASE}.${DEFAULT_SCHEMA}"
    echo "  Role:               ${SNOWFLAKE_ROLE}"
    echo "  Warehouse:          ${SNOWFLAKE_WAREHOUSE}"
    if [ -n "$COMPUTE_POOL_NAME" ]; then
        echo "  Compute Pool:       ${COMPUTE_POOL_NAME}"
    fi
    echo ""
    echo "QUICK START SQL"
    echo "---------------"
    echo "  USE ROLE ${SNOWFLAKE_ROLE};"
    echo "  USE DATABASE ${SNOWFLAKE_DATABASE};"
    echo "  USE SCHEMA ${DEFAULT_SCHEMA};"
    echo "  USE WAREHOUSE ${SNOWFLAKE_WAREHOUSE};"
    echo ""
    echo "============================================================"
    echo ""
    echo "Admin Notes:"
    echo "  To remove this user later:"
    echo "    snow sql -c ${CONNECTION_NAME} -q \"DROP USER IF EXISTS ${USER_NAME_UPPER};\""
    echo ""
    echo "  To reset password:"
    echo "    snow sql -c ${CONNECTION_NAME} -q \"ALTER USER ${USER_NAME_UPPER} SET PASSWORD = 'NewPassword!';\""
    echo ""
else
    if [ "$USER_EXISTS" = true ]; then
        error_exit "Failed to grant project access. Check the error messages above."
    else
        error_exit "Failed to create user. Check the error messages above."
    fi
fi
