# S&OP Integrated Scenario Planning & Logistics Optimization

A Snowflake-native demo showcasing how manufacturing organizations can bridge the gap between aggressive Sales/Marketing demand and production capacity through integrated scenario planning and AI-powered analytics.

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat&logo=python&logoColor=white)

## The Problem

> *"If we push marketing in Q4, will our warehouses overflow?"*

Manufacturing S&OP teams struggle with:
- **Disconnected spreadsheets** for demand, production, and logistics planning
- **Hidden costs** of pre-build inventory strategies (warehousing, overflow penalties)
- **Slow planning cycles** that can't keep up with market dynamics
- **Lack of data lineage** and version control for forecast scenarios

## The Solution

This demo shows how Snowflake enables **integrated scenario planning** that connects demand forecasts to production capacity and logistics costs—all in one platform with AI-powered insights.

### The "Wow" Moment

Toggle between **"Baseline Forecast"** and **"Q4 Marketing Push"** scenarios in the dashboard to see:
- Real-time recalculation of Q3 production utilization requirements
- ~20% spike in warehousing costs at the Northeast DC
- AI Analyst answering: *"What is the margin impact of the increased storage costs?"*

## Features

| Component | Description |
|-----------|-------------|
| **Executive Dashboard** | Side-by-side scenario comparison with KPI variance analysis |
| **Scenario Builder** | Create and modify demand scenarios with instant write-back |
| **Capacity Analysis** | Production and warehouse utilization drill-down |
| **AI Analyst** | Cortex Agent with natural language queries (Analyst + Search) |
| **ML Optimization** | Linear Programming notebook for optimal Q3 production scheduling |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Streamlit Application                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │Executive │ │ Scenario │ │ Capacity │ │    AI    │           │
│  │Dashboard │ │ Builder  │ │ Analysis │ │ Analyst  │           │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘           │
└───────┼────────────┼────────────┼────────────┼──────────────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SOP_LOGISTICS Schema                         │
│              (Consumer-Facing Data Mart)                        │
│  ┌────────────────────┐  ┌─────────────────────┐               │
│  │ SCENARIO_COMPARISON│  │ DT_SCENARIO_KPI_    │               │
│  │ _V (View)          │  │ SUMMARY (Dynamic)   │               │
│  └────────────────────┘  └─────────────────────┘               │
│  ┌────────────────────┐  ┌─────────────────────┐               │
│  │ WAREHOUSE_UTILIZATION│ │INVENTORY_BUILDUP_  │               │
│  │ _PROJECTION        │  │CURVE               │               │
│  └────────────────────┘  └─────────────────────┘               │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ATOMIC Schema                              │
│              (Enterprise Relational Model)                      │
│  ┌────────┐ ┌──────────────┐ ┌───────────────────┐             │
│  │PRODUCT │ │ SITE         │ │ DEMAND_FORECAST_  │             │
│  │        │ │ WORK_CENTER  │ │ VERSIONS          │             │
│  └────────┘ └──────────────┘ └───────────────────┘             │
│  ┌──────────────┐ ┌────────────────────────┐                   │
│  │SCENARIO_     │ │LOGISTICS_COST_FACT     │                   │
│  │DEFINITION    │ │                        │                   │
│  └──────────────┘ └────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                       RAW Schema                                │
│              (Landing Zone for Staged Data)                     │
│  ┌────────────┐  ┌────────────┐                                │
│  │ DATA_STAGE │  │ DOCS_STAGE │                                │
│  │ (CSV files)│  │ (PDFs)     │                                │
│  └────────────┘  └────────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

### AI Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Cortex Analyst** | Semantic Model (YAML) | Natural language → SQL for metrics queries |
| **Cortex Search** | RAG over PDFs | Search 3PL contracts, SLAs, meeting minutes |
| **Cortex Agent** | Orchestration | Routes questions to appropriate tool (Analyst or Search) |

## Prerequisites

- **Snowflake Account** with:
  - ACCOUNTADMIN role access (for initial setup)
  - Cortex features enabled (Analyst, Search, Agent)
- **Snowflake CLI** (`snow`) installed: `pip install snowflake-cli`
- **Configured connection** in `~/.snowflake/connections.toml`
- **PAT Token** for Cortex Agent deployment (Settings > Preferences > Programmatic Access Tokens)

## Quick Start

### 1. Clone and Configure

```bash
cd sop_scenario_planning_logistics

# Set PAT token for Agent deployment
export SNOWFLAKE_PAT_TOKEN="your_pat_token"
```

### 2. Deploy Everything

```bash
./deploy.sh
```

This single command:
- Creates the database, schemas, role, and warehouse
- Uploads and loads synthetic data (9 CSV files)
- Creates the three-layer data architecture
- Deploys the Cortex Search service (indexes PDFs)
- Deploys the Semantic Model (for Cortex Analyst)
- Deploys the Cortex Agent (SOP_ANALYST_AGENT)
- Deploys the Streamlit application
- Deploys the optimization notebook

### 3. Verify Deployment

```bash
./run.sh status    # Check resource status
./run.sh test      # Run query validation tests
```

### 4. Open the Dashboard

```bash
./run.sh streamlit  # Get dashboard URL
```

Or navigate in Snowsight: **Projects > Streamlit > SOP_SCENARIO_PLANNING_LOGISTICS_APP**

## Usage

### Deployment Options

```bash
# Full deployment
./deploy.sh

# Deploy specific components
./deploy.sh --only-streamlit   # Redeploy Streamlit app only
./deploy.sh --only-data        # Reload data only
./deploy.sh --only-sql         # Rerun SQL scripts only
./deploy.sh --only-semantic    # Redeploy semantic model
./deploy.sh --only-search      # Redeploy Cortex Search
./deploy.sh --only-agent       # Redeploy Cortex Agent
./deploy.sh --only-notebook    # Redeploy optimization notebook

# Skip agent (if no PAT token)
./deploy.sh --skip-agent

# Use different connection
./deploy.sh -c prod

# Environment prefix for multiple deployments
./deploy.sh -p DEV
```

### Runtime Operations

```bash
# Execute optimization notebook
./run.sh main

# Check resource status
./run.sh status

# Run validation tests
./run.sh test

# Get Streamlit URL
./run.sh streamlit

# Check notebook status
./run.sh notebook
```

### Cleanup

```bash
# Interactive cleanup
./clean.sh

# Force cleanup (no confirmation)
./clean.sh --force
```

## Project Structure

```
sop_scenario_planning_logistics/
├── agents/
│   └── SOP_ANALYST_AGENT.agent.json    # Cortex Agent definition
├── data/
│   ├── synthetic/                       # Source CSV data files
│   │   ├── demand_forecasts.csv
│   │   ├── inventory_balances.csv
│   │   ├── logistics_costs.csv
│   │   ├── product_categories.csv
│   │   ├── products.csv
│   │   ├── scenario_definitions.csv
│   │   ├── sites.csv
│   │   ├── warehouse_zones.csv
│   │   └── work_centers.csv
│   └── unstructured/                    # Documents for Cortex Search
│       ├── northeast_3pl_contract.pdf
│       ├── sop_meeting_minutes_aug2024.pdf
│       └── warehouse_capacity_sla.pdf
├── notebooks/
│   ├── environment.yml                  # Notebook dependencies
│   └── prebuild_optimization.ipynb      # ML optimization notebook
├── semantic_models/
│   └── sop_analytics_semantic.yaml      # Cortex Analyst semantic model
├── sql/
│   ├── 01_account_setup.sql             # Role, warehouse, database
│   ├── 02_schema_setup.sql              # Schemas, stages, file formats
│   ├── 03_raw_tables.sql                # RAW landing tables
│   ├── 04_atomic_tables.sql             # ATOMIC relational model
│   ├── 05_data_mart.sql                 # SOP_LOGISTICS views & dynamic tables
│   ├── 06_cortex_search.sql             # Cortex Search service
│   ├── 07_semantic_model.sql            # Semantic model helpers
│   └── 08_stored_procedures.sql         # Data loading procedures
├── streamlit/
│   ├── environment.yml                  # Streamlit dependencies
│   ├── snowflake.yml                    # Deployment configuration
│   ├── streamlit_app.py                 # Main app entry point
│   ├── pages/
│   │   ├── 1_Executive_Dashboard.py
│   │   ├── 2_Scenario_Builder.py
│   │   ├── 3_Capacity_Analysis.py
│   │   ├── 4_AI_Analyst.py
│   │   └── 5_About.py
│   └── utils/
│       ├── data_loader.py               # Query execution utilities
│       ├── query_registry.py            # SQL query registration
│       └── styles.py                    # UI theming
├── utils/
│   ├── generate_synthetic_data.py       # Data generation script
│   └── sf_cortex_agent_ops.py           # Agent deployment utility
├── deploy.sh                            # Full deployment script
├── run.sh                               # Runtime operations
├── clean.sh                             # Resource cleanup
├── DRD.md                               # Design Requirements Document
└── README.md                            # This file
```

## Sample Queries for AI Analyst

The Cortex Agent can answer both **quantitative** and **qualitative** questions:

### Quantitative (Routes to Cortex Analyst)
- *"Compare the total warehousing cost between Baseline and Q4 Push for October"*
- *"What is the margin impact of Q4 Push compared to Baseline?"*
- *"What is the revenue by product family for each scenario?"*
- *"Show me the forecast quantity by region for Q4"*

### Qualitative (Routes to Cortex Search)
- *"What are the contractual penalty fees if we exceed storage capacity by 10%?"*
- *"What decisions were made in the August S&OP meeting?"*
- *"What is the overflow escalation procedure for Northeast DC?"*

## Key Tables & Views

| Object | Schema | Description |
|--------|--------|-------------|
| `SCENARIO_COMPARISON_V` | SOP_LOGISTICS | Denormalized fact view for scenario analysis |
| `DT_SCENARIO_KPI_SUMMARY` | SOP_LOGISTICS | Dynamic table with pre-aggregated KPIs |
| `WAREHOUSE_UTILIZATION_PROJECTION` | SOP_LOGISTICS | Capacity utilization by scenario/month |
| `INVENTORY_BUILDUP_CURVE` | SOP_LOGISTICS | Q3 inventory build & Q4 depletion ("camel hump") |
| `PRODUCTION_CAPACITY_SUMMARY` | SOP_LOGISTICS | Work center capacity vs. demand |
| `RECOMMENDED_BUILD_PLAN` | SOP_LOGISTICS | ML-optimized production schedule (from notebook) |

## Snowflake Resources Created

| Resource | Name |
|----------|------|
| Database | `SOP_SCENARIO_PLANNING_LOGISTICS` |
| Schemas | `RAW`, `ATOMIC`, `SOP_LOGISTICS` |
| Role | `SOP_SCENARIO_PLANNING_LOGISTICS_ROLE` |
| Warehouse | `SOP_SCENARIO_PLANNING_LOGISTICS_WH` |
| Streamlit | `SOP_SCENARIO_PLANNING_LOGISTICS_APP` |
| Notebook | `SOP_SCENARIO_PLANNING_LOGISTICS_NOTEBOOK` |
| Cortex Search | `SUPPLY_CHAIN_DOCS_SEARCH` |
| Cortex Agent | `SOP_ANALYST_AGENT` |
| Semantic View | `SOP_ANALYTICS_SEMANTIC_MODEL` |

## Business KPIs Demonstrated

- **Net Working Capital**: Balance pre-build inventory costs vs. revenue upside
- **Order Fill Rate**: Ensure Q4 demand satisfaction through Q3 production
- **Planning Cycle Time**: Real-time scenario comparison (seconds vs. weeks)
- **Warehousing Budget Impact**: ~20% increase for Q4 Marketing Push scenario

## Troubleshooting

### Agent Deployment Fails
```bash
# Verify PAT token is set
echo $SNOWFLAKE_PAT_TOKEN

# Deploy without agent
./deploy.sh --skip-agent
```

### Streamlit Won't Load
```bash
# Redeploy Streamlit only
./deploy.sh --only-streamlit

# Check for errors in deployment
./run.sh status
```

### Query Tests Fail
```bash
# Reload data and recreate views
./deploy.sh --only-data
./deploy.sh --only-sql
```

### Connection Issues
```bash
# Test connection
snow connection test -c demo

# List available connections
snow connection list
```

## License

This demo is provided for educational and demonstration purposes.

---

Built with ❄️ Snowflake | Cortex AI | Streamlit

