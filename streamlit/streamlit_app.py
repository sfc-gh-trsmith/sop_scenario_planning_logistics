"""
S&OP Integrated Scenario Planning & Logistics Optimization

Main Streamlit application entry point.

Personas Served:
- VP Supply Chain (Strategic): Executive Dashboard
- Demand Planner (Operational): Scenario Builder  
- Supply Chain Analyst (Technical): Capacity Analysis
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from utils.styles import get_page_style, COLORS
from utils.query_registry import register_query
from utils.data_loader import run_query

# ============================================================================
# REGISTERED QUERIES
# ============================================================================
HOME_STATS_SQL = register_query(
    "home_planning_stats",
    """
    SELECT 
        COUNT(DISTINCT SCENARIO_CODE) as SCENARIOS,
        COUNT(DISTINCT PRODUCT_ID) as PRODUCTS,
        COUNT(DISTINCT SITE_ID) as SITES,
        SUM(FORECAST_QUANTITY) as TOTAL_FORECAST
    FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
    WHERE IS_CURRENT_VERSION = TRUE
    """,
    "Home page planning statistics",
    min_rows=1
)

# Page configuration
st.set_page_config(
    page_title="S&OP Scenario Planning",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styling
st.markdown(get_page_style(), unsafe_allow_html=True)

# Initialize session
session = get_active_session()

# Sidebar branding
st.sidebar.markdown("""
<div style="text-align: center; padding: 0.5rem 0 1rem 0;">
    <div style="color: #64D2FF; font-weight: 700; font-size: 1.1rem;">SNOWFLAKE</div>
</div>
""", unsafe_allow_html=True)
st.sidebar.markdown("---")
st.sidebar.markdown("### S&OP Planning")
st.sidebar.markdown("*Use the pages above to navigate*")

# Main content - Home page
st.title("S&OP Integrated Scenario Planning")
st.markdown("### Welcome to the Scenario Planning & Logistics Optimization Dashboard")

st.markdown("""
<div style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); 
            border-radius: 16px; padding: 2rem; margin: 1rem 0;">
    <h3 style="color: #64D2FF; margin-top: 0;">The Challenge</h3>
    <p style="color: #e2e8f0; font-size: 1.1rem; line-height: 1.7;">
        Manufacturing organizations face a critical question every planning cycle: 
        <strong>"If we push marketing in Q4, will our warehouses overflow?"</strong>
    </p>
    <p style="color: #94a3b8;">
        This demo shows how Snowflake enables <strong>integrated scenario planning</strong> 
        that connects demand forecasts to production capacity and logistics costs—all in one platform.
    </p>
</div>
""", unsafe_allow_html=True)

# Quick stats
st.markdown("### Current Planning Status")

col1, col2, col3, col4 = st.columns(4)

# Fetch quick stats using registered query
try:
    stats = run_query(session, HOME_STATS_SQL, "home_planning_stats")
    
    with col1:
        st.metric("Active Scenarios", int(stats['SCENARIOS'].iloc[0]))
    with col2:
        st.metric("Products", int(stats['PRODUCTS'].iloc[0]))
    with col3:
        st.metric("Sites", int(stats['SITES'].iloc[0]))
    with col4:
        total_demand = stats['TOTAL_FORECAST'].iloc[0]
        st.metric("Total Forecast (Units)", f"{int(total_demand):,}")
except Exception as e:
    st.error(f"Error loading statistics: {e}")
    raise  # Fail-fast: re-raise to surface the error

# Feature cards
st.markdown("### Explore the Demo")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="metric-card">
        <h4 style="color: #64D2FF; margin-top: 0;">Executive Dashboard</h4>
        <p style="color: #94a3b8;">
            Compare scenarios side-by-side. See the <strong>~20% warehouse budget increase</strong> 
            when Q4 Marketing Push meets Northeast DC capacity constraints.
        </p>
        <p style="color: #A1A1A6; font-size: 0.85rem;">
            <em>For: VP Supply Chain, CFO</em>
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="metric-card">
        <h4 style="color: #FF9F0A; margin-top: 0;">Scenario Builder</h4>
        <p style="color: #94a3b8;">
            Create and modify demand scenarios with <strong>instant write-back</strong> to Snowflake. 
            See how changes propagate through the entire supply chain model.
        </p>
        <p style="color: #A1A1A6; font-size: 0.85rem;">
            <em>For: Demand Planners, S&OP Leads</em>
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="metric-card">
        <h4 style="color: #5AC8FA; margin-top: 0;">Capacity Analysis</h4>
        <p style="color: #94a3b8;">
            Drill into <strong>production and warehouse utilization</strong>. 
            Identify bottlenecks before they become problems.
        </p>
        <p style="color: #A1A1A6; font-size: 0.85rem;">
            <em>For: Supply Chain Analysts, Operations</em>
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="metric-card">
        <h4 style="color: #30D158; margin-top: 0;">AI Analyst</h4>
        <p style="color: #94a3b8;">
            Ask questions in <strong>natural language</strong> using Cortex Analyst. 
            Search supply chain documents with Cortex Search.
        </p>
        <p style="color: #A1A1A6; font-size: 0.85rem;">
            <em>For: All Users</em>
        </p>
    </div>
    """, unsafe_allow_html=True)

# The "Wow" moment teaser
st.markdown("---")
st.markdown("""
<div style="text-align: center; padding: 2rem;">
    <h3 style="color: #FF9F0A;">The New Insight</h3>
    <p style="color: #e2e8f0; font-size: 1.2rem;">
        Select the <strong>"Q4 Marketing Push"</strong> scenario in the Executive Dashboard 
        to see how a 30% demand increase cascades into a <strong>~20% warehousing budget spike</strong>.
    </p>
    <p style="color: #94a3b8;">
        This is the insight that traditionally takes weeks of spreadsheet analysis—delivered in seconds.
    </p>
</div>
""", unsafe_allow_html=True)

