"""
About Page - Application Information and Documentation

Provides overview, data architecture, and getting started information.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

import sys
sys.path.insert(0, '..')
from utils.styles import get_page_style, COLORS

# Page config
st.set_page_config(page_title="About", page_icon="ℹ️", layout="wide")
st.markdown(get_page_style(), unsafe_allow_html=True)

# Session
session = get_active_session()

# ============================================================================
# PAGE CONTENT
# ============================================================================
st.title("About This Application")

# ============================================================================
# OVERVIEW
# ============================================================================
st.markdown("## Overview")

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    ### The Challenge
    
    Manufacturing organizations face a critical planning question every cycle:
    **"If we push Q4 marketing, will our warehouses overflow?"**
    
    Traditional planning tools operate in silos—demand planning in spreadsheets, 
    production scheduling in ERP, and logistics costing in separate systems. 
    This fragmentation leads to delayed insights and reactive decision-making.
    
    ### The Solution
    
    This application demonstrates **Integrated Scenario Planning** powered by Snowflake:
    
    - **Unified Data Platform**: Demand, production, and logistics data in one place
    - **What-If Analysis**: Create and compare scenarios with instant cost impact
    - **ML Optimization**: Linear programming to balance production and inventory
    - **Natural Language Interface**: Ask questions using Cortex AI
    """)

with col2:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1e293b, #0f172a); 
                padding: 1.5rem; border-radius: 12px; text-align: center;">
        <div style="font-size: 3rem; margin-bottom: 0.5rem;">📊</div>
        <div style="color: #64D2FF; font-size: 1.5rem; font-weight: 700;">S&OP</div>
        <div style="color: #94a3b8; font-size: 0.9rem;">Scenario Planning</div>
        <div style="color: #94a3b8; font-size: 0.9rem; margin-top: 0.5rem;">& Logistics Optimization</div>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# DATA ARCHITECTURE
# ============================================================================
st.markdown("---")
st.markdown("## Data Architecture")

st.markdown("""
This application follows Snowflake's **layered data architecture** pattern:

| Layer | Schema | Purpose |
|-------|--------|---------|
| **RAW** | `RAW` | Landing zone for source data (CSV files) |
| **ATOMIC** | `ATOMIC` | Cleansed, typed tables with Type 2 SCD |
| **DATA MART** | `SOP_LOGISTICS` | Aggregated views for analytics and reporting |
""")

# Data sources
st.markdown("### Data Sources")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="background: rgba(30, 64, 175, 0.2); padding: 1rem; border-radius: 8px; 
                border-left: 3px solid #1e40af;">
        <div style="color: #60a5fa; font-weight: 600; margin-bottom: 0.5rem;">
            📦 Internal Data
        </div>
        <ul style="color: #94a3b8; margin: 0; padding-left: 1.2rem;">
            <li>Demand Forecasts</li>
            <li>Production Capacity</li>
            <li>Warehouse Inventory</li>
            <li>Logistics Costs</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style="background: rgba(180, 83, 9, 0.2); padding: 1rem; border-radius: 8px;
                border-left: 3px solid #b45309;">
        <div style="color: #fbbf24; font-weight: 600; margin-bottom: 0.5rem;">
            🌐 External Data
        </div>
        <ul style="color: #94a3b8; margin: 0; padding-left: 1.2rem;">
            <li>3PL Contract Terms</li>
            <li>SLA Documents</li>
            <li>Meeting Minutes</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="background: rgba(22, 101, 52, 0.2); padding: 1rem; border-radius: 8px;
                border-left: 3px solid #166534;">
        <div style="color: #4ade80; font-weight: 600; margin-bottom: 0.5rem;">
            🤖 Model Outputs
        </div>
        <ul style="color: #94a3b8; margin: 0; padding-left: 1.2rem;">
            <li>Optimized Build Plan</li>
            <li>Capacity Utilization</li>
            <li>Cost Projections</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# HOW IT WORKS (TABBED)
# ============================================================================
st.markdown("---")
st.markdown("## How It Works")

exec_tab, tech_tab = st.tabs(["Executive Overview", "Technical Deep-Dive"])

with exec_tab:
    st.markdown("""
    ### Why Traditional Approaches Fall Short
    
    Traditional S&OP planning suffers from three critical limitations:
    """)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; height: 180px;">
            <div style="color: {COLORS['danger']}; font-weight: 600; margin-bottom: 0.5rem;">Siloed Data</div>
            <p style="color: {COLORS['text_muted']}; font-size: 0.9rem;">
                Demand in spreadsheets, capacity in ERP, costs in finance systems. 
                No single source of truth.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; height: 180px;">
            <div style="color: {COLORS['secondary']}; font-weight: 600; margin-bottom: 0.5rem;">Slow Iterations</div>
            <p style="color: {COLORS['text_muted']}; font-size: 0.9rem;">
                "What if" analysis takes days. By the time you have answers, 
                the business has moved on.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; height: 180px;">
            <div style="color: {COLORS['baseline']}; font-weight: 600; margin-bottom: 0.5rem;">No Causality</div>
            <p style="color: {COLORS['text_muted']}; font-size: 0.9rem;">
                Can't see how a demand change ripples through production 
                and warehouse costs.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("""
    ### How This Solution Works
    
    This application connects the dots between **demand**, **production**, and **logistics** in real-time:
    
    1. **Select a Scenario** - Choose from baseline or alternative demand plans
    2. **See Instant Impact** - Cost changes are calculated immediately as demand propagates
    3. **Identify Bottlenecks** - Visual alerts when capacity thresholds are exceeded
    4. **Make Informed Decisions** - Compare scenarios side-by-side before committing
    
    ### Business Value
    """)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; text-align: center;">
            <div style="color: {COLORS['accent2']}; font-size: 1.5rem; font-weight: 700;">Seconds</div>
            <div style="color: {COLORS['text_muted']}; font-size: 0.8rem;">vs days for scenario analysis</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; text-align: center;">
            <div style="color: {COLORS['primary']}; font-size: 1.5rem; font-weight: 700;">~20%</div>
            <div style="color: {COLORS['text_muted']}; font-size: 0.8rem;">cost variance discovered</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; text-align: center;">
            <div style="color: {COLORS['secondary']}; font-size: 1.5rem; font-weight: 700;">4</div>
            <div style="color: {COLORS['text_muted']}; font-size: 0.8rem;">integrated data domains</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; text-align: center;">
            <div style="color: {COLORS['accent1']}; font-size: 1.5rem; font-weight: 700;">1</div>
            <div style="color: {COLORS['text_muted']}; font-size: 0.8rem;">unified platform</div>
        </div>
        """, unsafe_allow_html=True)

with tech_tab:
    st.markdown("""
    ### Architecture Overview
    
    The solution follows a **Medallion Architecture** pattern with three data layers:
    
    ```
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │    RAW      │ →  │   ATOMIC    │ →  │  SOP_LOGIS  │
    │             │    │             │    │    TICS     │
    │ CSV landing │    │ Type 2 SCD  │    │  Data Mart  │
    │    zone     │    │ cleansed    │    │   Views     │
    └─────────────┘    └─────────────┘    └─────────────┘
    ```
    
    ### Key Technical Components
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        **Data Pipeline**
        - Source: CSV files loaded via Snowflake stages
        - Transformation: SQL-based cleansing and SCD Type 2
        - Refresh: On-demand or scheduled via tasks
        
        **Scenario Modeling**
        - Scenarios stored in `ATOMIC.SCENARIO_DEFINITION`
        - Forecasts linked via `SCENARIO_ID` foreign key
        - Write-back support via Hybrid Tables
        """)
    
    with col2:
        st.markdown(f"""
        **ML Optimization**
        - Algorithm: Linear Programming (PuLP)
        - Objective: Minimize total cost (production + storage + overflow)
        - Constraints: Production capacity, warehouse limits
        - Output: `RECOMMENDED_BUILD_PLAN` table
        
        **AI Integration**
        - Cortex Analyst: Semantic model for NL-to-SQL
        - Cortex Search: RAG over supply chain documents
        - LLM: llama3.1-70b for analysis
        """)
    
    st.markdown("""
    ### Data Model
    
    | Table/View | Schema | Purpose |
    |------------|--------|---------|
    | `DEMAND_FORECAST` | ATOMIC | Forecast quantities by product, site, scenario |
    | `SCENARIO_DEFINITION` | ATOMIC | Scenario metadata (baseline flag, description) |
    | `SITE` / `PRODUCT` | ATOMIC | Master data with Type 2 history |
    | `LOGISTICS_COST` | ATOMIC | Cost parameters (storage, overflow factors) |
    | `SCENARIO_COMPARISON_V` | SOP_LOGISTICS | Flattened view for dashboard queries |
    | `RECOMMENDED_BUILD_PLAN` | SOP_LOGISTICS | ML optimization output |
    
    ### Performance Considerations
    
    - Queries use registered query pattern for testability and fail-fast behavior
    - Parallel query execution via `ThreadPoolExecutor` for dashboard loading
    - `@st.cache_data` with 5-minute TTL for expensive aggregations
    - Plotly visualizations with SiS-compatible data type conversion
    """)

# ============================================================================
# APPLICATION PAGES
# ============================================================================
st.markdown("---")
st.markdown("## Application Pages")

pages_info = [
    {
        "icon": "📈",
        "title": "Executive Dashboard",
        "persona": "VP Supply Chain",
        "description": "Compare scenarios side-by-side, see cost impacts, identify the 'wow' moment where Q4 push triggers overflow costs."
    },
    {
        "icon": "🔧",
        "title": "Scenario Builder",
        "persona": "Demand Planner",
        "description": "Create and modify demand scenarios with bulk adjustments. Changes write back to Snowflake in real-time."
    },
    {
        "icon": "⚡",
        "title": "Capacity Analysis",
        "persona": "Supply Chain Analyst",
        "description": "Deep dive into production and warehouse capacity. View ML optimization results and identify bottlenecks."
    },
    {
        "icon": "🤖",
        "title": "AI Analyst",
        "persona": "All Users",
        "description": "Ask questions in natural language. Cortex Analyst queries structured data; Cortex Search finds relevant documents."
    }
]

for page in pages_info:
    st.markdown(f"""
    <div style="background: {COLORS['surface']}; padding: 1rem; border-radius: 8px; margin-bottom: 0.75rem;">
        <div style="display: flex; align-items: center; gap: 0.75rem;">
            <span style="font-size: 1.5rem;">{page['icon']}</span>
            <div>
                <strong style="color: {COLORS['primary']};">{page['title']}</strong>
                <span style="color: {COLORS['text_muted']}; font-size: 0.85rem;"> — {page['persona']}</span>
            </div>
        </div>
        <p style="color: {COLORS['text']}; margin: 0.5rem 0 0 2.25rem;">{page['description']}</p>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# TECHNOLOGY STACK
# ============================================================================
st.markdown("---")
st.markdown("## Technology Stack")

tech_stack = [
    ("Snowflake", "Data Cloud Platform"),
    ("Snowpark", "Python DataFrame API"),
    ("Cortex Analyst", "Natural Language to SQL"),
    ("Cortex Search", "RAG for Documents"),
    ("Hybrid Tables", "Low-latency Write-back"),
    ("Streamlit", "Interactive Dashboard"),
    ("PuLP", "Linear Programming Optimization"),
    ("Plotly", "Interactive Visualizations")
]

cols = st.columns(4)
for i, (tech, desc) in enumerate(tech_stack):
    with cols[i % 4]:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #334155, #1e293b); 
                    padding: 0.75rem; border-radius: 8px; text-align: center; margin-bottom: 0.5rem;">
            <div style="color: {COLORS['primary']}; font-weight: 600;">{tech}</div>
            <div style="color: {COLORS['text_muted']}; font-size: 0.75rem;">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

# ============================================================================
# GETTING STARTED
# ============================================================================
st.markdown("---")
st.markdown("## Getting Started")

st.markdown("""
### Quick Start

1. **Executive Dashboard** → Start here to see the "wow" moment
   - Select both "Baseline" and "Q4 Marketing Push" scenarios
   - Observe the ~20% warehouse cost increase

2. **Scenario Builder** → Create your own what-if scenarios
   - Select a non-baseline scenario
   - Apply percentage adjustments to demand
   - See changes reflected immediately

3. **Capacity Analysis** → Deep dive into constraints
   - Review production capacity by site
   - Identify warehouse zones at risk
   - View ML optimization recommendations

4. **AI Analyst** → Ask questions naturally
   - Try: "What is the logistics cost for Q4 Push?"
   - Search documents for contract terms

### Demo Data Story

The synthetic data includes a pre-planted scenario:

- **Northeast DC** is operating at 85-90% warehouse capacity
- **Q4 Marketing Push** increases demand by 25-35%
- This combination triggers **overflow storage fees**
- Result: ~20% increase in warehousing budget

This demonstrates the value of integrated planning—seeing cost impacts *before* committing to demand plans.
""")

# ============================================================================
# FOOTER
# ============================================================================
st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    <div style="text-align: center; color: #64a3b8;">
        <div style="font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em;">Built With</div>
        <div style="color: #64D2FF; font-weight: 600;">Snowflake</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style="text-align: center; color: #64a3b8;">
        <div style="font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em;">Version</div>
        <div style="color: #64D2FF; font-weight: 600;">1.0.0</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style="text-align: center; color: #64a3b8;">
        <div style="font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em;">Last Updated</div>
        <div style="color: #64D2FF; font-weight: 600;">December 2024</div>
    </div>
    """, unsafe_allow_html=True)

