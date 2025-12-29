#!/usr/bin/env python3
"""
sf_cortex_agent_ops.py - Snowflake Cortex Agent and Semantic View Operations

This script provides commands to:
- Export and import Snowflake Cortex Agent configurations
- Deploy Semantic Views from YAML definitions

AUTHENTICATION:
    RECOMMENDED: Use private key (JWT) authentication for security and reliability.
    Password authentication may experience connection timeouts or failures.

Usage:
    # Export agent using private key authentication (RECOMMENDED)
    python sf_cortex_agent_ops.py export --database MYDB --schema PUBLIC --name my_agent \
        --account myaccount-myorg_cloud --user myuser \
        --private-key-path ~/.ssh/snowflake_key.p8 --warehouse MY_WH
    
    # Export all agents with private key auth
    python sf_cortex_agent_ops.py export-all --database MYDB \
        --account myaccount-myorg_cloud --user myuser \
        --private-key-path ~/.ssh/snowflake_key.p8 --warehouse MY_WH
    
    # Export using .env file (set SNOWFLAKE_PRIVATE_KEY_PATH)
    python sf_cortex_agent_ops.py export --database MYDB --schema PUBLIC --name my_agent
    
    # Import agent (requires PAT token for REST API)
    python sf_cortex_agent_ops.py import --input exports/my_agent.agent.json \
        --account myaccount-myorg_cloud --pat-token mytoken
    
    # Import and replace existing agent
    python sf_cortex_agent_ops.py import --input exports/my_agent.agent.json --replace
    
    # Export semantic view
    python sf_cortex_agent_ops.py export-semantic-view --database MYDB --schema PUBLIC --name my_view \
        --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
    
    # Export all semantic views
    python sf_cortex_agent_ops.py export-all-semantic-views --database MYDB --include-sql \
        --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
    
    # Deploy semantic view from YAML
    python sf_cortex_agent_ops.py deploy-semantic-view --input view.yaml --database MYDB --schema PUBLIC \
        --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
    
    # Help
    python sf_cortex_agent_ops.py --help
    python sf_cortex_agent_ops.py export --help
    python sf_cortex_agent_ops.py import --help
    python sf_cortex_agent_ops.py export-semantic-view --help
    python sf_cortex_agent_ops.py deploy-semantic-view --help

Configuration:
    Option 1 (RECOMMENDED): Create a .env file with private key authentication:
        SNOWFLAKE_ACCOUNT=myaccount-myorg_cloud
        SNOWFLAKE_USER=username
        SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.p8
        SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=passphrase (optional, if key is encrypted)
        SNOWFLAKE_WAREHOUSE=warehouse
        SNOWFLAKE_ROLE=ACCOUNTADMIN
        SNOWFLAKE_PAT_TOKEN=your_token (required for import operations)
    
    Option 2: Pass connection parameters via command-line arguments
        Use --private-key-path for SQL operations (export, deploy)
        Use --pat-token for REST API operations (import)
    
    Option 3 (NOT RECOMMENDED): Password authentication
        Use --password flag (may experience connection issues)
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from decimal import Decimal

import snowflake.connector
from snowflake.connector import DictCursor
import requests
from dotenv import load_dotenv


# ============================================================================
# Configuration
# ============================================================================

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime, date, and Decimal objects."""
    
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        return super().default(obj)


def load_config(env_file: Optional[str] = None) -> None:
    """Load configuration from .env file."""
    if env_file:
        load_dotenv(dotenv_path=env_file)
    else:
        load_dotenv()


def get_snowflake_connection(account=None, user=None, password=None, warehouse=None, role=None, private_key_path=None):
    """Get Snowflake connection using provided parameters or environment variables.
    
    Args:
        account: Snowflake account (overrides env var)
        user: Username (overrides env var)
        password: Password (overrides env var)
        warehouse: Warehouse (overrides env var)
        role: Role (overrides env var)
        private_key_path: Path to private key (overrides env var)
    """
    account = account or os.getenv("SNOWFLAKE_ACCOUNT")
    user = user or os.getenv("SNOWFLAKE_USER")
    password = password or os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    role = role or os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    
    # Check required parameters
    missing = []
    if not account:
        missing.append("account (--account or SNOWFLAKE_ACCOUNT)")
    if not user:
        missing.append("user (--user or SNOWFLAKE_USER)")
    
    if missing:
        raise ValueError(
            f"Required parameters not set: {', '.join(missing)}"
        )
    
    params = {
        "account": account,
        "user": user,
        "role": role,
    }
    
    if warehouse:
        params["warehouse"] = warehouse
    
    # Authentication: password or key-pair
    private_key_path = private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    
    if password:
        params["password"] = password
    elif private_key_path:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        passphrase_bytes = passphrase.encode() if passphrase else None
        
        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase_bytes,
                backend=default_backend()
            )
        
        params["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    else:
        raise ValueError(
            "Authentication not configured.\n"
            "RECOMMENDED: Use private key authentication with --private-key-path or set SNOWFLAKE_PRIVATE_KEY_PATH.\n"
            "Alternative: Use --password or set SNOWFLAKE_PASSWORD (may experience connection issues)."
        )
    
    return snowflake.connector.connect(**params), params["user"]


def get_rest_api_config(host=None, token=None, account=None) -> Dict[str, str]:
    """Get REST API configuration from provided parameters or environment variables.
    
    Args:
        host: Snowflake host (overrides env var)
        token: PAT token (overrides env var)
        account: Account to construct host from (if host not provided)
    """
    host = host or os.getenv("SNOWFLAKE_HOST")
    token = token or os.getenv("SNOWFLAKE_PAT_TOKEN")
    account = account or os.getenv("SNOWFLAKE_ACCOUNT")
    
    if not host:
        if account:
            host = f"{account}.snowflakecomputing.com"
        else:
            raise ValueError(
                "SNOWFLAKE_HOST or SNOWFLAKE_ACCOUNT must be set "
                "(via environment variable or --host/--account argument)"
            )
    
    # Normalize hostname: lowercase and replace underscores with hyphens
    # (Snowflake account identifiers can have underscores, but hostnames use hyphens)
    host = host.lower().replace("_", "-")
    
    if not token:
        raise ValueError(
            "SNOWFLAKE_PAT_TOKEN must be set "
            "(via environment variable or --pat-token argument)"
        )
    
    return {"host": host, "token": token}


# ============================================================================
# Export Functions
# ============================================================================

def describe_agent(
    conn: snowflake.connector.SnowflakeConnection,
    database: str,
    schema: str,
    agent_name: str
) -> List[Dict[str, Any]]:
    """Execute DESCRIBE AGENT and return results."""
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        
        qualified_name = f"{database}.{schema}.{agent_name}"
        sql = f"DESCRIBE AGENT {qualified_name}"
        print(f"Executing: {sql}", file=sys.stderr)
        
        cursor.execute(sql)
        results = cursor.fetchall()
        
        return results
    finally:
        cursor.close()


def parse_create_body(describe_results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Parse the create_body from DESCRIBE AGENT results.
    
    Looks for 'create_body', 'definition', or 'agent_spec' fields.
    """
    for row in describe_results:
        # Check for property-based format (old style)
        property_name = row.get("property", "").lower()
        
        if property_name in ("create_body", "definition"):
            value = row.get("value", "")
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse {property_name}: {e}", file=sys.stderr)
                    return None
        
        # Check for agent_spec field (newer format)
        if "agent_spec" in row:
            value = row.get("agent_spec", "")
            if value:
                if isinstance(value, str):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError as e:
                        print(f"Warning: Failed to parse agent_spec: {e}", file=sys.stderr)
                        return None
                elif isinstance(value, dict):
                    # Already parsed as dict
                    return value
    
    return None


def export_agent(
    database: str,
    schema: str,
    agent_name: str,
    output_file: Path,
    env_file: Optional[str] = None,
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    private_key_path: Optional[str] = None
) -> None:
    """Export an agent configuration to JSON."""
    load_config(env_file)
    
    print(f"Connecting to Snowflake...", file=sys.stderr)
    
    try:
        conn, user_name = get_snowflake_connection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
            private_key_path=private_key_path
        )
        print(f"Connected as {user_name}", file=sys.stderr)
        
        describe_results = describe_agent(conn, database, schema, agent_name)
        
        if not describe_results:
            print(f"Error: Agent {database}.{schema}.{agent_name} not found", 
                  file=sys.stderr)
            sys.exit(1)
        
        print(f"Retrieved {len(describe_results)} properties", file=sys.stderr)
        
        create_body = parse_create_body(describe_results)
        
        export_data = {
            "metadata": {
                "database": database,
                "schema": schema,
                "agent_name": agent_name,
                "exported_by": user_name,
                "tool_version": "0.3.0"
            },
            "describe_results": describe_results,
            "create_body": create_body
        }
        
        # Create parent directory if it doesn't exist (only if path includes subdirectories)
        if output_file.parent != Path("."):
            output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, cls=DateTimeEncoder)
        
        print(f"\n✓ Agent configuration exported to: {output_file}", file=sys.stderr)
        print(f"  Properties: {len(describe_results)}", file=sys.stderr)
        print(f"  Create body: {'Found' if create_body else 'Not found'}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


# ============================================================================
# Import Functions
# ============================================================================

def create_agent_via_rest(
    host: str,
    token: str,
    database: str,
    schema: str,
    agent_name: str,
    create_body: Dict[str, Any],
    replace: bool = False,
    role: Optional[str] = None
) -> Dict[str, Any]:
    """Create or update an agent using the v2 REST API.
    
    Args:
        host: Snowflake host
        token: PAT token
        database: Database name
        schema: Schema name
        agent_name: Agent name
        create_body: Agent specification
        replace: If True, create if not exists OR update if exists. If False, create only.
        role: Optional Snowflake role to use for the request
    
    Returns:
        API response as dict
    """
    # Normalize hostname: lowercase and replace underscores with hyphens
    # (Snowflake account identifiers can have underscores, but hostnames use hyphens)
    host = host.lower().replace("_", "-")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Add role header if specified
    if role:
        headers["X-Snowflake-Role"] = role
        print(f"Using role: {role}", file=sys.stderr)
    
    # Always try POST first to create the agent
    post_url = f"https://{host}/api/v2/databases/{database}/schemas/{schema}/agents"
    
    # Ensure the agent name is in the payload for POST
    create_body_with_name = create_body.copy()
    if "name" not in create_body_with_name:
        create_body_with_name["name"] = agent_name
    
    print(f"Calling REST API: POST {post_url}", file=sys.stderr)
    print(f"Agent name: {agent_name}", file=sys.stderr)
    
    response = requests.post(
        post_url,
        headers=headers,
        json=create_body_with_name,
        timeout=30
    )
    
    # If POST succeeded, we're done
    if response.status_code in (200, 201):
        print(f"Agent created successfully (HTTP {response.status_code})", file=sys.stderr)
    # If POST returned 409 (conflict/already exists) and replace=True, try PUT
    elif response.status_code == 409 and replace:
        print(f"Agent already exists, updating with PUT...", file=sys.stderr)
        put_url = f"https://{host}/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}"
        print(f"Calling REST API: PUT {put_url}", file=sys.stderr)
        
        response = requests.put(
            put_url,
            headers=headers,
            json=create_body,  # PUT doesn't need name in body
            timeout=30
        )
        
        if response.status_code not in (200, 201):
            print(f"Error: HTTP {response.status_code}", file=sys.stderr)
            print(f"Response: {response.text}", file=sys.stderr)
            response.raise_for_status()
        print(f"Agent updated successfully (HTTP {response.status_code})", file=sys.stderr)
    # If POST returned 409 but replace=False, suggest using --replace
    elif response.status_code == 409:
        print(f"Error: Agent already exists. Use --replace to update.", file=sys.stderr)
        response.raise_for_status()
    # Any other error
    else:
        print(f"Error: HTTP {response.status_code}", file=sys.stderr)
        print(f"Response: {response.text}", file=sys.stderr)
        response.raise_for_status()
    
    # Parse JSON response, handle empty responses
    print(f"Response status: {response.status_code}", file=sys.stderr)
    print(f"Response headers: {dict(response.headers)}", file=sys.stderr)
    
    if not response.text or response.text.strip() == "":
        print("Warning: Empty response body", file=sys.stderr)
        return {"status": "success", "message": "Agent operation completed (empty response)"}
    
    try:
        return response.json()
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse JSON response: {e}", file=sys.stderr)
        print(f"Response text: {response.text[:500]}", file=sys.stderr)
        return {"status": "success", "message": "Operation completed", "raw_response": response.text[:500]}


def load_agent_config(input_file: Path) -> Dict[str, Any]:
    """Load agent configuration from JSON file."""
    with open(input_file, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_create_body(
    config: Dict[str, Any],
    database: Optional[str] = None,
    schema: Optional[str] = None,
    agent_name: Optional[str] = None
) -> tuple[str, str, str, Dict[str, Any]]:
    """Extract create_body and identifiers from config."""
    # Check if this is an export artifact
    if "create_body" in config and "metadata" in config:
        create_body = config["create_body"]
        
        if create_body is None:
            raise ValueError(
                "Export artifact does not contain create_body. "
                "Agent may not support this format."
            )
        
        metadata = config["metadata"]
        database = database or metadata.get("database")
        schema = schema or metadata.get("schema")
        agent_name = agent_name or metadata.get("agent_name")
    else:
        create_body = config
    
    if not database or not schema or not agent_name:
        raise ValueError(
            "Database, schema, and agent name must be provided either in the "
            "config file or via command-line arguments (--database, --schema, --name)"
        )
    
    return database, schema, agent_name, create_body


def import_agent(
    input_file: Path,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    agent_name: Optional[str] = None,
    env_file: Optional[str] = None,
    dry_run: bool = False,
    replace: bool = False,
    host: Optional[str] = None,
    pat_token: Optional[str] = None,
    account: Optional[str] = None,
    role: Optional[str] = None
) -> None:
    """Import an agent configuration from JSON."""
    load_config(env_file)
    
    # Get role from environment if not provided
    role = role or os.getenv("SNOWFLAKE_ROLE")
    
    print(f"Loading agent configuration from: {input_file}", file=sys.stderr)
    config = load_agent_config(input_file)
    
    try:
        database, schema, agent_name, create_body = extract_create_body(
            config, database, schema, agent_name
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"\nAgent details:", file=sys.stderr)
    print(f"  Database: {database}", file=sys.stderr)
    print(f"  Schema: {schema}", file=sys.stderr)
    print(f"  Name: {agent_name}", file=sys.stderr)
    print(f"  Mode: {'Replace/Update' if replace else 'Create'}", file=sys.stderr)
    
    if dry_run:
        print("\n[DRY RUN] Would create agent with body:", file=sys.stderr)
        print(json.dumps(create_body, indent=2, cls=DateTimeEncoder), file=sys.stderr)
        return
    
    try:
        api_config = get_rest_api_config(host=host, token=pat_token, account=account)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        result = create_agent_via_rest(
            host=api_config["host"],
            token=api_config["token"],
            database=database,
            schema=schema,
            agent_name=agent_name,
            create_body=create_body,
            replace=replace,
            role=role
        )
        
        action = "updated" if replace else "created"
        print(f"\n✓ Agent {action} successfully!", file=sys.stderr)
        print(f"\nAPI Response:", file=sys.stderr)
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))
        
    except requests.HTTPError as e:
        # If POST failed with 409 (conflict), suggest using --replace
        if not replace and e.response.status_code == 409:
            print(f"\nError: Agent already exists.", file=sys.stderr)
            print(f"Hint: Use --replace flag to update the existing agent", file=sys.stderr)
        else:
            print(f"\nError {e.response.status_code if hasattr(e, 'response') else ''}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}", file=sys.stderr)
        sys.exit(1)


# ============================================================================
# Export All Functions
# ============================================================================

def list_all_agents(
    conn: snowflake.connector.SnowflakeConnection
) -> List[Dict[str, str]]:
    """List all agents accessible to the current account.
    
    Returns:
        List of dicts with keys: database, schema, agent_name
    """
    cursor = conn.cursor(DictCursor)
    agents = []
    
    try:
        # Get all databases
        cursor.execute("SHOW DATABASES")
        databases = [row["name"] for row in cursor.fetchall()]
        
        for database in databases:
            try:
                cursor.execute(f"USE DATABASE {database}")
                cursor.execute("SHOW SCHEMAS")
                schemas = [row["name"] for row in cursor.fetchall()]
                
                for schema in schemas:
                    try:
                        # Try to show agents in this schema
                        cursor.execute(f"SHOW AGENTS IN SCHEMA {database}.{schema}")
                        agent_rows = cursor.fetchall()
                        
                        for row in agent_rows:
                            agents.append({
                                "database": row["database_name"],
                                "schema": row["schema_name"],
                                "agent_name": row["name"]
                            })
                    except Exception as e:
                        # Schema might not have agents or we don't have permission
                        continue
            except Exception as e:
                # Database might not be accessible
                continue
    finally:
        cursor.close()
    
    return agents


def export_all_agents(
    output_dir: Path,
    env_file: Optional[str] = None,
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    private_key_path: Optional[str] = None,
    database_filter: Optional[str] = None,
    schema_filter: Optional[str] = None
) -> None:
    """Export all agents accessible to the account.
    
    Args:
        output_dir: Directory to save exported agents
        database_filter: Optional database name to filter by
        schema_filter: Optional schema name to filter by
        Other args: Same as export_agent
    """
    load_config(env_file)
    
    print(f"Connecting to Snowflake...", file=sys.stderr)
    
    try:
        conn, user_name = get_snowflake_connection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
            private_key_path=private_key_path
        )
        print(f"Connected as {user_name}", file=sys.stderr)
        
        print(f"Discovering agents...", file=sys.stderr)
        agents = list_all_agents(conn)
        
        # Apply filters
        if database_filter:
            agents = [a for a in agents if a["database"].upper() == database_filter.upper()]
        if schema_filter:
            agents = [a for a in agents if a["schema"].upper() == schema_filter.upper()]
        
        if not agents:
            print(f"No agents found", file=sys.stderr)
            if database_filter or schema_filter:
                print(f"Filters applied: database={database_filter}, schema={schema_filter}", 
                      file=sys.stderr)
            return
        
        print(f"Found {len(agents)} agent(s)", file=sys.stderr)
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export each agent
        success_count = 0
        error_count = 0
        
        for i, agent in enumerate(agents, 1):
            db = agent["database"]
            schema = agent["schema"]
            name = agent["agent_name"]
            
            # Use database.schema.name.json format
            output_file = output_dir / f"{db}.{schema}.{name}.json"
            
            print(f"\n[{i}/{len(agents)}] Exporting {db}.{schema}.{name}...", file=sys.stderr)
            
            try:
                describe_results = describe_agent(conn, db, schema, name)
                
                if not describe_results:
                    print(f"  ✗ Agent not found", file=sys.stderr)
                    error_count += 1
                    continue
                
                create_body = parse_create_body(describe_results)
                
                export_data = {
                    "metadata": {
                        "database": db,
                        "schema": schema,
                        "agent_name": name,
                        "exported_by": user_name,
                        "tool_version": "0.3.0"
                    },
                    "describe_results": describe_results,
                    "create_body": create_body
                }
                
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(export_data, f, indent=2, cls=DateTimeEncoder)
                
                print(f"  ✓ Exported to: {output_file}", file=sys.stderr)
                success_count += 1
                
            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)
                error_count += 1
        
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"Export Summary:", file=sys.stderr)
        print(f"  Total agents: {len(agents)}", file=sys.stderr)
        print(f"  Successful: {success_count}", file=sys.stderr)
        print(f"  Failed: {error_count}", file=sys.stderr)
        print(f"  Output directory: {output_dir}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


# ============================================================================
# Semantic View Functions
# ============================================================================

def extract_semantic_view_name(yaml_content: str) -> Optional[str]:
    """Extract semantic view name from YAML content.
    
    Args:
        yaml_content: YAML file content
    
    Returns:
        Semantic view name or None if not found
    """
    # Look for "name: <value>" at the start of a line
    match = re.search(r'^name:\s*(.+)$', yaml_content, re.MULTILINE)
    if match:
        return match.group(1).strip()
    return None


def generate_semantic_view_sql(
    yaml_file: Path,
    database: str,
    schema: str,
    semantic_view_name: Optional[str] = None
) -> str:
    """Generate SQL to create a semantic view from YAML.
    
    Args:
        yaml_file: Path to YAML file
        database: Target database
        schema: Target schema
        semantic_view_name: Override name (if None, extracted from YAML)
    
    Returns:
        SQL statement string
    """
    with open(yaml_file, "r", encoding="utf-8") as f:
        yaml_content = f.read()
    
    # Extract name if not provided
    if not semantic_view_name:
        semantic_view_name = extract_semantic_view_name(yaml_content)
        if not semantic_view_name:
            raise ValueError(f"Could not extract 'name:' from YAML file: {yaml_file}")
    
    # Build SQL
    sql_lines = [
        "-- ============================================================================",
        f"-- Semantic View: {semantic_view_name}",
        "-- ============================================================================",
        f"-- Auto-generated from {yaml_file.name}",
        "-- DO NOT EDIT THIS FILE DIRECTLY - Edit the YAML file instead",
        "-- ============================================================================",
        "",
        "USE ROLE ACCOUNTADMIN;",
        f"USE DATABASE {database};",
        f"USE SCHEMA {schema};",
        "",
        "CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(",
        f"  '{database}.{schema}',",
        "$$",
        yaml_content.rstrip(),
        "",
        "$$",
        ");",
        "",
        "-- Verify semantic view created",
        f"SHOW VIEWS LIKE '{semantic_view_name}' IN SCHEMA {database}.{schema};",
        ""
    ]
    
    return "\n".join(sql_lines)


def export_semantic_view(
    database: str,
    schema: str,
    view_name: str,
    output_yaml: Optional[Path] = None,
    output_sql: Optional[Path] = None,
    env_file: Optional[str] = None,
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    private_key_path: Optional[str] = None
) -> None:
    """Export a semantic view to YAML (and optionally SQL).
    
    Args:
        database: Source database
        schema: Source schema
        view_name: Semantic view name
        output_yaml: Path to save YAML file (default: <view_name>.yaml)
        output_sql: Optional path to save SQL recreation script
        Other args: Snowflake connection parameters
    """
    load_config(env_file)
    
    print(f"Connecting to Snowflake...", file=sys.stderr)
    
    try:
        conn, user_name = get_snowflake_connection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
            private_key_path=private_key_path
        )
        print(f"Connected as {user_name}", file=sys.stderr)
        
        cursor = conn.cursor()
        try:
            # Set context
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
            
            # Export YAML using SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW
            qualified_name = f"{database}.{schema}.{view_name}"
            print(f"\nExporting semantic view: {qualified_name}", file=sys.stderr)
            
            sql = f"SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW('{qualified_name}')"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if not result or not result[0]:
                print(f"Error: Could not export semantic view {qualified_name}", file=sys.stderr)
                sys.exit(1)
            
            yaml_content = result[0]
            
            # Determine output file
            if not output_yaml:
                output_yaml = Path(f"{view_name}.yaml")
            
            # Create parent directory if needed
            if output_yaml.parent != Path("."):
                output_yaml.parent.mkdir(parents=True, exist_ok=True)
            
            # Save YAML
            with open(output_yaml, "w", encoding="utf-8") as f:
                f.write(yaml_content)
            
            print(f"\n✓ Semantic view exported to YAML: {output_yaml}", file=sys.stderr)
            
            # Optionally generate SQL
            if output_sql:
                sql_content = generate_semantic_view_sql(
                    yaml_file=output_yaml,
                    database=database,
                    schema=schema,
                    semantic_view_name=view_name
                )
                
                if output_sql.parent != Path("."):
                    output_sql.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_sql, "w", encoding="utf-8") as f:
                    f.write(sql_content)
                
                print(f"✓ SQL recreation script saved: {output_sql}", file=sys.stderr)
            
        finally:
            cursor.close()
            
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


def deploy_semantic_view(
    yaml_file: Path,
    database: str,
    schema: str,
    output_sql: Optional[Path] = None,
    env_file: Optional[str] = None,
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    private_key_path: Optional[str] = None,
    dry_run: bool = False
) -> None:
    """Deploy a semantic view from YAML to Snowflake.
    
    Args:
        yaml_file: Path to YAML file
        database: Target database
        schema: Target schema
        output_sql: Optional path to save generated SQL
        dry_run: If True, only generate SQL without deploying
        Other args: Snowflake connection parameters
    """
    load_config(env_file)
    
    if not yaml_file.exists():
        print(f"Error: YAML file not found: {yaml_file}", file=sys.stderr)
        sys.exit(1)
    
    # Read YAML and extract name
    with open(yaml_file, "r", encoding="utf-8") as f:
        yaml_content = f.read()
    
    semantic_view_name = extract_semantic_view_name(yaml_content)
    if not semantic_view_name:
        print(f"Error: Could not extract 'name:' from YAML file: {yaml_file}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Building semantic view: {semantic_view_name}", file=sys.stderr)
    print(f"  Input:  {yaml_file}", file=sys.stderr)
    print(f"  Target: {database}.{schema}", file=sys.stderr)
    
    # Generate SQL
    sql = generate_semantic_view_sql(yaml_file, database, schema, semantic_view_name)
    
    # Save to file if requested
    if output_sql:
        if output_sql.parent != Path("."):
            output_sql.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_sql, "w", encoding="utf-8") as f:
            f.write(sql)
        print(f"  Output: {output_sql}", file=sys.stderr)
    
    if dry_run:
        print("\n[DRY RUN] Generated SQL:", file=sys.stderr)
        print(sql)
        return
    
    # Deploy to Snowflake
    print(f"\nConnecting to Snowflake...", file=sys.stderr)
    
    try:
        conn, user_name = get_snowflake_connection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
            private_key_path=private_key_path
        )
        print(f"Connected as {user_name}", file=sys.stderr)
        
        cursor = conn.cursor()
        try:
            # Execute the deployment
            print(f"\nDeploying semantic view...", file=sys.stderr)
            
            # Set context
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")
            
            # Build the CALL statement
            call_sql = f"""
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  '{database}.{schema}',
$$
{yaml_content}
$$
);
"""
            cursor.execute(call_sql)
            result = cursor.fetchall()
            
            print(f"\n✓ Semantic view '{semantic_view_name}' deployed successfully!", file=sys.stderr)
            
            # Verify
            cursor.execute(f"SHOW VIEWS LIKE '{semantic_view_name}' IN SCHEMA {database}.{schema}")
            views = cursor.fetchall()
            
            if views:
                print(f"✓ Verified: View found in {database}.{schema}", file=sys.stderr)
            else:
                print(f"Warning: View not found in SHOW VIEWS output", file=sys.stderr)
            
        finally:
            cursor.close()
            
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


def list_semantic_views(
    conn: snowflake.connector.SnowflakeConnection,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> List[Dict[str, str]]:
    """List all semantic views accessible to the current role.
    
    Args:
        conn: Snowflake connection
        database: Optional database filter
        schema: Optional schema filter
    
    Returns:
        List of dicts with keys: database, schema, view_name
    """
    cursor = conn.cursor(DictCursor)
    views = []
    
    try:
        # Get databases to search
        if database:
            databases = [database]
        else:
            cursor.execute("SHOW DATABASES")
            databases = [row["name"] for row in cursor.fetchall()]
        
        for db in databases:
            try:
                cursor.execute(f"USE DATABASE {db}")
                
                # Get schemas to search
                if schema:
                    schemas = [schema]
                else:
                    cursor.execute("SHOW SCHEMAS")
                    schemas = [row["name"] for row in cursor.fetchall()]
                
                for sch in schemas:
                    try:
                        # Show views and filter for semantic views
                        cursor.execute(f"SHOW VIEWS IN SCHEMA {db}.{sch}")
                        view_rows = cursor.fetchall()
                        
                        for row in view_rows:
                            # Check if this is a semantic view
                            # Semantic views have 'is_semantic' = 'Y' or kind = 'SEMANTIC'
                            is_semantic = (
                                row.get("is_semantic") == "Y" or 
                                row.get("kind") == "SEMANTIC"
                            )
                            
                            if is_semantic:
                                views.append({
                                    "database": row.get("database_name", db),
                                    "schema": row.get("schema_name", sch),
                                    "view_name": row["name"]
                                })
                    except Exception as e:
                        # Schema might not have views or we don't have permission
                        continue
            except Exception as e:
                # Database might not be accessible
                continue
    finally:
        cursor.close()
    
    return views


def export_all_semantic_views(
    output_dir: Path,
    env_file: Optional[str] = None,
    account: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    private_key_path: Optional[str] = None,
    database_filter: Optional[str] = None,
    schema_filter: Optional[str] = None,
    include_sql: bool = False
) -> None:
    """Export all semantic views accessible to the role.
    
    Args:
        output_dir: Directory to save exported views
        database_filter: Optional database name to filter by
        schema_filter: Optional schema name to filter by
        include_sql: If True, also generate SQL recreation scripts
        Other args: Snowflake connection parameters
    """
    load_config(env_file)
    
    print(f"Connecting to Snowflake...", file=sys.stderr)
    
    try:
        conn, user_name = get_snowflake_connection(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
            private_key_path=private_key_path
        )
        print(f"Connected as {user_name}", file=sys.stderr)
        
        print(f"Discovering semantic views...", file=sys.stderr)
        views = list_semantic_views(conn, database=database_filter, schema=schema_filter)
        
        if not views:
            print(f"No semantic views found", file=sys.stderr)
            if database_filter or schema_filter:
                print(f"Filters applied: database={database_filter}, schema={schema_filter}", 
                      file=sys.stderr)
            return
        
        print(f"Found {len(views)} semantic view(s)", file=sys.stderr)
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export each view
        success_count = 0
        error_count = 0
        
        cursor = conn.cursor()
        
        for i, view in enumerate(views, 1):
            db = view["database"]
            sch = view["schema"]
            name = view["view_name"]
            
            # Use database.schema.name.yaml format
            output_yaml = output_dir / f"{db}.{sch}.{name}.yaml"
            output_sql = output_dir / f"{db}.{sch}.{name}.sql" if include_sql else None
            
            print(f"\n[{i}/{len(views)}] Exporting {db}.{sch}.{name}...", file=sys.stderr)
            
            try:
                # Set context
                cursor.execute(f"USE DATABASE {db}")
                cursor.execute(f"USE SCHEMA {sch}")
                
                # Export YAML
                qualified_name = f"{db}.{sch}.{name}"
                sql = f"SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW('{qualified_name}')"
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if not result or not result[0]:
                    print(f"  ✗ Could not export view", file=sys.stderr)
                    error_count += 1
                    continue
                
                yaml_content = result[0]
                
                # Save YAML
                with open(output_yaml, "w", encoding="utf-8") as f:
                    f.write(yaml_content)
                
                print(f"  ✓ YAML: {output_yaml}", file=sys.stderr)
                
                # Optionally save SQL
                if include_sql:
                    sql_content = generate_semantic_view_sql(
                        yaml_file=output_yaml,
                        database=db,
                        schema=sch,
                        semantic_view_name=name
                    )
                    
                    with open(output_sql, "w", encoding="utf-8") as f:
                        f.write(sql_content)
                    
                    print(f"  ✓ SQL: {output_sql}", file=sys.stderr)
                
                success_count += 1
                
            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)
                error_count += 1
        
        cursor.close()
        
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"Export Summary:", file=sys.stderr)
        print(f"  Total views: {len(views)}", file=sys.stderr)
        print(f"  Successful: {success_count}", file=sys.stderr)
        print(f"  Failed: {error_count}", file=sys.stderr)
        print(f"  Output directory: {output_dir}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


# ============================================================================
# CLI
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export and import Snowflake Cortex Agent configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export a single agent (using private key authentication - RECOMMENDED)
  python sf_cortex_agent_ops.py export --database MYDB --schema PUBLIC --name my_agent \\
      --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
  
  # Export all agents (using .env file with SNOWFLAKE_PRIVATE_KEY_PATH set)
  python sf_cortex_agent_ops.py export-all --database MYDB
  
  # Export all agents with explicit authentication
  python sf_cortex_agent_ops.py export-all --database MYDB --schema PUBLIC \\
      --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
  
  # Import an agent (requires PAT token for REST API)
  python sf_cortex_agent_ops.py import --input exports/my_agent.json \\
      --account myaccount-myorg_cloud --pat-token mytoken
  
  # Import and replace existing agent
  python sf_cortex_agent_ops.py import --input exports/my_agent.json --replace --pat-token mytoken
  
  # Export semantic view to YAML
  python sf_cortex_agent_ops.py export-semantic-view --database MYDB --schema PUBLIC --name my_view \\
      --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
  
  # Export all semantic views
  python sf_cortex_agent_ops.py export-all-semantic-views --database MYDB --include-sql \\
      --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8
  
  # Deploy semantic view from YAML
  python sf_cortex_agent_ops.py deploy-semantic-view --input semantic_views/my_view.yaml -d MYDB -s PUBLIC \\
      --account myaccount-myorg_cloud --user myuser --private-key-path ~/.ssh/snowflake_key.p8

Authentication:
  RECOMMENDED: Use private key (JWT) authentication for all SQL operations (export, deploy).
  Set in .env: SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/key.p8
  Or use flag: --private-key-path ~/.ssh/snowflake_key.p8
  
  For import operations (REST API), use: --pat-token or SNOWFLAKE_PAT_TOKEN
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    subparsers.required = True
    
    # Export command
    export_parser = subparsers.add_parser(
        "export",
        help="Export agent configuration to JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    export_parser.add_argument(
        "--database", "-d",
        required=True,
        help="Database name"
    )
    export_parser.add_argument(
        "--schema", "-s",
        required=True,
        help="Schema name"
    )
    export_parser.add_argument(
        "--name", "-n",
        required=True,
        help="Agent name"
    )
    
    # Output options
    export_parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output JSON file path (default: <agent_name>.agent.json)"
    )
    
    # Connection parameters (override .env)
    export_parser.add_argument(
        "--account",
        help="Snowflake account identifier (e.g., myaccount-myorg_cloud)"
    )
    export_parser.add_argument(
        "--user",
        help="Snowflake username"
    )
    export_parser.add_argument(
        "--private-key-path",
        help="Path to private key file for JWT authentication (RECOMMENDED)"
    )
    export_parser.add_argument(
        "--password",
        help="Snowflake password (NOT RECOMMENDED: use --private-key-path instead)"
    )
    export_parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse"
    )
    export_parser.add_argument(
        "--role",
        help="Snowflake role (default: ACCOUNTADMIN)"
    )
    export_parser.add_argument(
        "--env-file",
        help="Path to .env file with Snowflake credentials"
    )
    
    # Export-all command
    export_all_parser = subparsers.add_parser(
        "export-all",
        help="Export all agents accessible to the account",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Output directory
    export_all_parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=Path("exports"),
        help="Output directory for exported agents (default: exports/)"
    )
    
    # Optional filters
    export_all_parser.add_argument(
        "--database", "-d",
        help="Filter by database name (optional)"
    )
    export_all_parser.add_argument(
        "--schema", "-s",
        help="Filter by schema name (optional)"
    )
    
    # Connection parameters (override .env)
    export_all_parser.add_argument(
        "--account",
        help="Snowflake account identifier (e.g., myaccount-myorg_cloud)"
    )
    export_all_parser.add_argument(
        "--user",
        help="Snowflake username"
    )
    export_all_parser.add_argument(
        "--private-key-path",
        help="Path to private key file for JWT authentication (RECOMMENDED)"
    )
    export_all_parser.add_argument(
        "--password",
        help="Snowflake password (NOT RECOMMENDED: use --private-key-path instead)"
    )
    export_all_parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse"
    )
    export_all_parser.add_argument(
        "--role",
        help="Snowflake role (default: ACCOUNTADMIN)"
    )
    export_all_parser.add_argument(
        "--env-file",
        help="Path to .env file with Snowflake credentials"
    )
    
    # Import command
    import_parser = subparsers.add_parser(
        "import",
        help="Import agent configuration from JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    import_parser.add_argument(
        "--input", "-i",
        type=Path,
        required=True,
        help="Input JSON file (export artifact or plain create_body)"
    )
    
    # Override options
    import_parser.add_argument(
        "--database", "-d",
        help="Database name (overrides metadata)"
    )
    import_parser.add_argument(
        "--schema", "-s",
        help="Schema name (overrides metadata)"
    )
    import_parser.add_argument(
        "--name", "-n",
        help="Agent name (overrides metadata)"
    )
    
    # REST API connection parameters (override .env)
    import_parser.add_argument(
        "--account",
        help="Snowflake account identifier (e.g., myaccount-myorg_cloud)"
    )
    import_parser.add_argument(
        "--host",
        help="Snowflake host (optional, constructed from account if not provided)"
    )
    import_parser.add_argument(
        "--pat-token",
        help="Personal Access Token (PAT) for REST API authentication (REQUIRED for import)"
    )
    
    # Other options
    import_parser.add_argument(
        "--env-file",
        help="Path to .env file with Snowflake credentials"
    )
    import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without creating agent"
    )
    import_parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace/update existing agent (use PUT instead of POST)"
    )
    import_parser.add_argument(
        "--role",
        help="Snowflake role to use for the API request (default: SNOWFLAKE_ROLE env var)"
    )
    
    # Export Semantic View command
    export_sv_parser = subparsers.add_parser(
        "export-semantic-view",
        help="Export semantic view to YAML",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    export_sv_parser.add_argument(
        "--database", "-d",
        required=True,
        help="Source database name"
    )
    export_sv_parser.add_argument(
        "--schema", "-s",
        required=True,
        help="Source schema name"
    )
    export_sv_parser.add_argument(
        "--name", "-n",
        required=True,
        help="Semantic view name"
    )
    
    # Optional arguments
    export_sv_parser.add_argument(
        "--output-yaml", "-o",
        type=Path,
        help="Output YAML file path (default: <view_name>.yaml)"
    )
    export_sv_parser.add_argument(
        "--output-sql",
        type=Path,
        help="Optional: Also save SQL recreation script"
    )
    
    # Connection parameters (override .env)
    export_sv_parser.add_argument(
        "--account",
        help="Snowflake account identifier (e.g., myaccount-myorg_cloud)"
    )
    export_sv_parser.add_argument(
        "--user",
        help="Snowflake username"
    )
    export_sv_parser.add_argument(
        "--private-key-path",
        help="Path to private key file for JWT authentication (RECOMMENDED)"
    )
    export_sv_parser.add_argument(
        "--password",
        help="Snowflake password (NOT RECOMMENDED: use --private-key-path instead)"
    )
    export_sv_parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse"
    )
    export_sv_parser.add_argument(
        "--role",
        help="Snowflake role (default: ACCOUNTADMIN)"
    )
    export_sv_parser.add_argument(
        "--env-file",
        help="Path to .env file with Snowflake credentials"
    )
    
    # Export All Semantic Views command
    export_all_sv_parser = subparsers.add_parser(
        "export-all-semantic-views",
        help="Export all semantic views to YAML",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Output directory
    export_all_sv_parser.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=Path("semantic_views"),
        help="Output directory for exported views (default: semantic_views/)"
    )
    
    # Optional filters
    export_all_sv_parser.add_argument(
        "--database", "-d",
        help="Filter by database name (optional)"
    )
    export_all_sv_parser.add_argument(
        "--schema", "-s",
        help="Filter by schema name (optional)"
    )
    export_all_sv_parser.add_argument(
        "--include-sql",
        action="store_true",
        help="Also generate SQL recreation scripts"
    )
    
    # Connection parameters (override .env)
    export_all_sv_parser.add_argument(
        "--account",
        help="Snowflake account identifier (e.g., myaccount-myorg_cloud)"
    )
    export_all_sv_parser.add_argument(
        "--user",
        help="Snowflake username"
    )
    export_all_sv_parser.add_argument(
        "--private-key-path",
        help="Path to private key file for JWT authentication (RECOMMENDED)"
    )
    export_all_sv_parser.add_argument(
        "--password",
        help="Snowflake password (NOT RECOMMENDED: use --private-key-path instead)"
    )
    export_all_sv_parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse"
    )
    export_all_sv_parser.add_argument(
        "--role",
        help="Snowflake role (default: ACCOUNTADMIN)"
    )
    export_all_sv_parser.add_argument(
        "--env-file",
        help="Path to .env file with Snowflake credentials"
    )
    
    # Deploy Semantic View command
    deploy_sv_parser = subparsers.add_parser(
        "deploy-semantic-view",
        help="Deploy semantic view from YAML to Snowflake",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Required arguments
    deploy_sv_parser.add_argument(
        "--input", "-i",
        type=Path,
        required=True,
        help="Input YAML file with semantic view definition"
    )
    deploy_sv_parser.add_argument(
        "--database", "-d",
        required=True,
        help="Target database name"
    )
    deploy_sv_parser.add_argument(
        "--schema", "-s",
        required=True,
        help="Target schema name"
    )
    
    # Optional arguments
    deploy_sv_parser.add_argument(
        "--output-sql", "-o",
        type=Path,
        help="Optional: Save generated SQL to file"
    )
    deploy_sv_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate SQL without deploying to Snowflake"
    )
    
    # Connection parameters (override .env)
    deploy_sv_parser.add_argument(
        "--account",
        help="Snowflake account identifier (e.g., myaccount-myorg_cloud)"
    )
    deploy_sv_parser.add_argument(
        "--user",
        help="Snowflake username"
    )
    deploy_sv_parser.add_argument(
        "--private-key-path",
        help="Path to private key file for JWT authentication (RECOMMENDED)"
    )
    deploy_sv_parser.add_argument(
        "--password",
        help="Snowflake password (NOT RECOMMENDED: use --private-key-path instead)"
    )
    deploy_sv_parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse"
    )
    deploy_sv_parser.add_argument(
        "--role",
        help="Snowflake role (default: ACCOUNTADMIN)"
    )
    deploy_sv_parser.add_argument(
        "--env-file",
        help="Path to .env file with Snowflake credentials"
    )
    
    args = parser.parse_args()
    
    # Execute command
    if args.command == "export":
        output = args.output or Path(f"{args.name}.agent.json")
        export_agent(
            database=args.database,
            schema=args.schema,
            agent_name=args.name,
            output_file=output,
            env_file=args.env_file,
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
            role=args.role,
            private_key_path=args.private_key_path
        )
    
    elif args.command == "export-all":
        export_all_agents(
            output_dir=args.output_dir,
            env_file=args.env_file,
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
            role=args.role,
            private_key_path=args.private_key_path,
            database_filter=args.database,
            schema_filter=args.schema
        )
    
    elif args.command == "import":
        if not args.input.exists():
            print(f"Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        
        import_agent(
            input_file=args.input,
            database=args.database,
            schema=args.schema,
            agent_name=args.name,
            env_file=args.env_file,
            dry_run=args.dry_run,
            replace=args.replace,
            host=args.host,
            pat_token=args.pat_token,
            account=args.account,
            role=args.role
        )
    
    elif args.command == "export-semantic-view":
        export_semantic_view(
            database=args.database,
            schema=args.schema,
            view_name=args.name,
            output_yaml=args.output_yaml,
            output_sql=args.output_sql,
            env_file=args.env_file,
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
            role=args.role,
            private_key_path=args.private_key_path
        )
    
    elif args.command == "export-all-semantic-views":
        export_all_semantic_views(
            output_dir=args.output_dir,
            env_file=args.env_file,
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
            role=args.role,
            private_key_path=args.private_key_path,
            database_filter=args.database,
            schema_filter=args.schema,
            include_sql=args.include_sql
        )
    
    elif args.command == "deploy-semantic-view":
        deploy_semantic_view(
            yaml_file=args.input,
            database=args.database,
            schema=args.schema,
            output_sql=args.output_sql,
            env_file=args.env_file,
            account=args.account,
            user=args.user,
            password=args.password,
            warehouse=args.warehouse,
            role=args.role,
            private_key_path=args.private_key_path,
            dry_run=args.dry_run
        )


if __name__ == "__main__":
    main()

