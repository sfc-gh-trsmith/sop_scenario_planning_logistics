"""
Capacity Analysis - Supply Chain Analyst Persona

Technical view for analyzing production and warehouse capacity utilization.
Answers: "Where are the bottlenecks and how do we optimize?"
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from snowflake.snowpark.context import get_active_session

import sys
sys.path.insert(0, '..')
from utils.styles import get_page_style, COLORS, PLOTLY_THEME, metric_card, insight_card, warning_card
from utils.query_registry import register_query
from utils.data_loader import run_query

# Page config
st.set_page_config(page_title="Capacity Analysis", page_icon="⚡", layout="wide")
st.markdown(get_page_style(), unsafe_allow_html=True)

# Session
session = get_active_session()

# ============================================================================
# REGISTERED QUERIES
# ============================================================================
PRODUCTION_CAPACITY_SQL = register_query(
    "capacity_production",
    """
    SELECT 
        SITE_NAME,
        WORK_CENTER_NAME,
        MAX_DAILY_CAPACITY,
        MAX_MONTHLY_CAPACITY,
        HOURS_PER_DAY,
        EFFICIENCY_FACTOR
    FROM SOP_LOGISTICS.PRODUCTION_CAPACITY_SUMMARY
    ORDER BY SITE_NAME, WORK_CENTER_NAME
    """,
    "Production capacity by site"
)

WAREHOUSE_CAPACITY_SQL = register_query(
    "capacity_warehouse",
    """
    SELECT 
        s.SITE_NAME,
        wz.ZONE_NAME,
        wz.MAX_CAPACITY_PALLETS,
        wz.CURRENT_OCCUPANCY_PALLETS,
        wz.ZONE_TYPE,
        lc.STORAGE_COST_PER_PALLET_PER_DAY as STORAGE_COST,
        lc.OVERFLOW_PENALTY_THRESHOLD_PCT as CAPACITY_THRESHOLD_PERCENT,
        lc.OVERFLOW_PENALTY_RATE as OVERFLOW_COST_FACTOR,
        ROUND(wz.CURRENT_OCCUPANCY_PALLETS / NULLIF(wz.MAX_CAPACITY_PALLETS, 0) * 100, 1) as UTILIZATION_PCT
    FROM ATOMIC.WAREHOUSE_ZONE wz
    JOIN ATOMIC.SITE s ON wz.SITE_ID = s.SITE_ID AND s.IS_CURRENT_FLAG = TRUE
    LEFT JOIN ATOMIC.LOGISTICS_COST_FACT lc ON wz.WAREHOUSE_ZONE_ID = lc.WAREHOUSE_ZONE_ID 
        AND lc.IS_CURRENT_FLAG = TRUE
    WHERE wz.IS_CURRENT_FLAG = TRUE
    ORDER BY s.SITE_NAME, wz.ZONE_NAME
    """,
    "Warehouse capacity by zone"
)

OPTIMIZATION_RESULTS_SQL = register_query(
    "capacity_optimization",
    """
    SELECT 
        rbp.FISCAL_MONTH,
        rbp.FISCAL_QUARTER,
        rbp.RECOMMENDED_QUANTITY,
        rbp.CURRENT_CAPACITY_AVAILABLE,
        rbp.CAPACITY_UTILIZATION_PCT,
        rbp.PROJECTED_INVENTORY,
        rbp.WAREHOUSE_UTILIZATION_PCT,
        rbp.PROJECTED_PRODUCTION_COST,
        rbp.PROJECTED_STORAGE_COST,
        rbp.PROJECTED_TOTAL_COST,
        rbp.MODEL_VERSION,
        rbp.MODEL_CONFIDENCE,
        sd.SCENARIO_NAME
    FROM SOP_LOGISTICS.RECOMMENDED_BUILD_PLAN rbp
    JOIN ATOMIC.SCENARIO_DEFINITION sd ON rbp.SCENARIO_ID = sd.SCENARIO_ID
    ORDER BY 
        CASE rbp.FISCAL_MONTH
            WHEN 'July' THEN 1 WHEN 'August' THEN 2 WHEN 'September' THEN 3
            WHEN 'October' THEN 4 WHEN 'November' THEN 5 WHEN 'December' THEN 6
        END
    """,
    "ML optimization results"
)

DEMAND_VS_CAPACITY_SQL = register_query(
    "capacity_demand_comparison",
    """
    SELECT 
        sc.FISCAL_MONTH,
        sc.FISCAL_QUARTER,
        sc.SCENARIO_CODE,
        SUM(sc.FORECAST_QUANTITY) as TOTAL_DEMAND,
        (SELECT SUM(MAX_MONTHLY_CAPACITY) FROM SOP_LOGISTICS.PRODUCTION_CAPACITY_SUMMARY) as TOTAL_CAPACITY
    FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V sc
    GROUP BY sc.FISCAL_MONTH, sc.FISCAL_QUARTER, sc.SCENARIO_CODE
    ORDER BY sc.SCENARIO_CODE,
        CASE sc.FISCAL_MONTH
            WHEN 'July' THEN 1 WHEN 'August' THEN 2 WHEN 'September' THEN 3
            WHEN 'October' THEN 4 WHEN 'November' THEN 5 WHEN 'December' THEN 6
        END
    """,
    "Demand vs capacity comparison"
)

# ============================================================================
# PAGE CONTENT
# ============================================================================
st.title("Capacity Analysis")
st.markdown("**Persona:** Supply Chain Analyst | **Focus:** Capacity utilization and optimization")

# Load data
try:
    production_df = run_query(session, PRODUCTION_CAPACITY_SQL, "production_capacity")
    warehouse_df = run_query(session, WAREHOUSE_CAPACITY_SQL, "warehouse_capacity")
    demand_cap_df = run_query(session, DEMAND_VS_CAPACITY_SQL, "demand_capacity")
except Exception as e:
    st.error(f"Failed to load data: {e}")
    raise

# Try to load optimization results (may not exist yet)
try:
    optimization_df = run_query(session, OPTIMIZATION_RESULTS_SQL, "optimization")
    has_optimization = not optimization_df.empty
except:
    has_optimization = False
    optimization_df = pd.DataFrame()

# ============================================================================
# KPI SUMMARY
# ============================================================================
st.markdown("---")
st.markdown("### Capacity Overview")

col1, col2, col3, col4 = st.columns(4)

total_prod_capacity = production_df['MAX_MONTHLY_CAPACITY'].sum()
total_wh_capacity = warehouse_df['MAX_CAPACITY_PALLETS'].sum()
total_wh_occupied = warehouse_df['CURRENT_OCCUPANCY_PALLETS'].sum()
avg_wh_util = (total_wh_occupied / total_wh_capacity * 100) if total_wh_capacity > 0 else 0

with col1:
    st.markdown(metric_card("Monthly Production Capacity", f"{int(total_prod_capacity):,} units"), unsafe_allow_html=True)

with col2:
    st.markdown(metric_card("Warehouse Capacity", f"{int(total_wh_capacity):,} pallets"), unsafe_allow_html=True)

with col3:
    st.markdown(metric_card("Current WH Utilization", f"{avg_wh_util:.1f}%"), unsafe_allow_html=True)

with col4:
    # Count sites at risk (>80% utilization)
    sites_at_risk = len(warehouse_df[warehouse_df['UTILIZATION_PCT'] > 80]['SITE_NAME'].unique())
    st.markdown(metric_card(
        "Sites at Risk",
        str(sites_at_risk),
        "Above 80% utilization" if sites_at_risk > 0 else None,
        delta_positive=False
    ), unsafe_allow_html=True)

# ============================================================================
# PRODUCTION CAPACITY BY SITE
# ============================================================================
st.markdown("---")
st.markdown("### Production Capacity by Site")

# Aggregate by site
site_capacity = production_df.groupby('SITE_NAME').agg({
    'MAX_MONTHLY_CAPACITY': 'sum',
    'MAX_DAILY_CAPACITY': 'sum'
}).reset_index()

fig1 = go.Figure()

fig1.add_trace(go.Bar(
    x=[str(s) for s in site_capacity['SITE_NAME'].tolist()],
    y=[float(v) for v in site_capacity['MAX_MONTHLY_CAPACITY'].tolist()],
    marker_color=COLORS['primary'],
    text=[f"{int(v):,}" for v in site_capacity['MAX_MONTHLY_CAPACITY'].tolist()],
    textposition='outside',
    name='Monthly Capacity'
))

fig1.update_layout(
    **PLOTLY_THEME,
    xaxis_title="Site",
    yaxis_title="Monthly Capacity (Units)",
    showlegend=False,
    height=400
)

st.plotly_chart(fig1, use_container_width=True)

# ============================================================================
# WAREHOUSE UTILIZATION HEATMAP
# ============================================================================
st.markdown("### Warehouse Utilization by Zone")

fig2 = go.Figure()

# Extract data as plain Python lists
x_labels = (warehouse_df['SITE_NAME'] + ' - ' + warehouse_df['ZONE_NAME']).tolist()
y_values = warehouse_df['UTILIZATION_PCT'].tolist()

# Color based on utilization
colors = []
for util in y_values:
    if util >= 90:
        colors.append(COLORS['danger'])
    elif util >= 80:
        colors.append(COLORS['secondary'])
    else:
        colors.append(COLORS['accent2'])

fig2.add_trace(go.Bar(
    x=x_labels,
    y=y_values,
    marker_color=colors,
    text=[f"{v:.1f}%" for v in y_values],
    textposition='outside'
))

# Add threshold lines
fig2.add_hline(y=80, line_dash="dash", line_color=COLORS['secondary'], 
               annotation_text="Warning (80%)", annotation_position="right")
fig2.add_hline(y=90, line_dash="dash", line_color=COLORS['danger'],
               annotation_text="Critical (90%)", annotation_position="right")

fig2.update_layout(
    **PLOTLY_THEME,
    xaxis_title="Site - Zone",
    yaxis_title="Utilization %",
    yaxis_range=[0, 110],
    showlegend=False,
    height=400
)

st.plotly_chart(fig2, use_container_width=True)

# Highlight at-risk zones
at_risk_zones = warehouse_df[warehouse_df['UTILIZATION_PCT'] >= 80]
if not at_risk_zones.empty:
    st.markdown(warning_card(
        f"<strong>{len(at_risk_zones)} zones</strong> are operating at or above 80% capacity. "
        f"These zones are at risk of overflow under the Q4 Marketing Push scenario:<br/>"
        f"<ul>{''.join([f'<li>{row.SITE_NAME} - {row.ZONE_NAME}: {row.UTILIZATION_PCT:.1f}%</li>' for _, row in at_risk_zones.iterrows()])}</ul>"
    ), unsafe_allow_html=True)

# ============================================================================
# DEMAND VS CAPACITY ANALYSIS
# ============================================================================
st.markdown("---")
st.markdown("### Demand vs Capacity by Scenario")

scenario_select = st.radio(
    "Select Scenario",
    ['BASELINE', 'Q4_PUSH'],
    horizontal=True
)

scenario_data = demand_cap_df[demand_cap_df['SCENARIO_CODE'] == scenario_select]

if not scenario_data.empty:
    fig3 = go.Figure()
    
    # Demand bars - convert to native Python types for SiS compatibility
    fig3.add_trace(go.Bar(
        x=[str(m) for m in scenario_data['FISCAL_MONTH'].tolist()],
        y=[float(d) for d in scenario_data['TOTAL_DEMAND'].tolist()],
        name='Demand',
        marker_color=COLORS['secondary'] if scenario_select == 'Q4_PUSH' else COLORS['baseline']
    ))
    
    # Capacity line - convert to native Python types for SiS compatibility
    fig3.add_trace(go.Scatter(
        x=[str(m) for m in scenario_data['FISCAL_MONTH'].tolist()],
        y=[float(c) for c in scenario_data['TOTAL_CAPACITY'].tolist()],
        mode='lines+markers',
        name='Capacity',
        line=dict(color=COLORS['primary'], width=3, dash='dash'),
        marker=dict(size=10)
    ))
    
    fig3.update_layout(
        **PLOTLY_THEME,
        title=f"Demand vs Capacity: {scenario_select.replace('_', ' ')}",
        xaxis_title="Month",
        yaxis_title="Units",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    st.plotly_chart(fig3, use_container_width=True)
    
    # Calculate capacity gap
    scenario_data['GAP'] = scenario_data['TOTAL_CAPACITY'] - scenario_data['TOTAL_DEMAND']
    scenario_data['GAP_PCT'] = (scenario_data['GAP'] / scenario_data['TOTAL_CAPACITY'] * 100)
    
    # Identify months with capacity shortfall
    shortfall_months = scenario_data[scenario_data['GAP'] < 0]
    if not shortfall_months.empty:
        st.markdown(warning_card(
            f"<strong>Capacity shortfall detected!</strong> "
            f"Demand exceeds production capacity in: "
            f"<strong>{', '.join(shortfall_months['FISCAL_MONTH'].tolist())}</strong>"
        ), unsafe_allow_html=True)

# ============================================================================
# ML OPTIMIZATION RESULTS
# ============================================================================
st.markdown("---")
st.markdown("### ML Optimization Results")

if has_optimization:
    st.markdown(insight_card(
        "Optimization Model",
        f"Model Version: <strong>{optimization_df['MODEL_VERSION'].iloc[0]}</strong> | "
        f"Confidence: <strong>{optimization_df['MODEL_CONFIDENCE'].iloc[0]*100:.0f}%</strong>"
    ), unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Production schedule - extract as plain lists
        months = optimization_df['FISCAL_MONTH'].tolist()
        recommended_qty = optimization_df['RECOMMENDED_QUANTITY'].tolist()
        capacity_available = optimization_df['CURRENT_CAPACITY_AVAILABLE'].tolist()
        
        fig4 = go.Figure()
        
        fig4.add_trace(go.Bar(
            x=months,
            y=recommended_qty,
            name='Recommended Production',
            marker_color=COLORS['primary']
        ))
        
        fig4.add_trace(go.Scatter(
            x=months,
            y=capacity_available,
            mode='lines+markers',
            name='Capacity',
            line=dict(color=COLORS['danger'], dash='dash')
        ))
        
        fig4.update_layout(
            **PLOTLY_THEME,
            title="Optimized Production Schedule",
            xaxis_title="Month",
            yaxis_title="Units",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=350
        )
        
        st.plotly_chart(fig4, use_container_width=True)
    
    with col2:
        # Inventory projection
        fig5 = go.Figure()
        
        # Convert to native Python types for SiS compatibility
        fig5.add_trace(go.Scatter(
            x=[str(m) for m in optimization_df['FISCAL_MONTH'].tolist()],
            y=[float(v) for v in optimization_df['PROJECTED_INVENTORY'].tolist()],
            mode='lines+markers+text',
            fill='tozeroy',
            name='Projected Inventory',
            line=dict(color=COLORS['accent1']),
            fillcolor='rgba(90, 200, 250, 0.2)',
            text=[f"{int(v):,}" for v in optimization_df['PROJECTED_INVENTORY'].tolist()],
            textposition='top center'
        ))
        
        fig5.update_layout(
            **PLOTLY_THEME,
            title="Projected Inventory Levels",
            xaxis_title="Month",
            yaxis_title="Inventory (Units)",
            showlegend=False,
            height=350
        )
        
        st.plotly_chart(fig5, use_container_width=True)
    
    # Cost breakdown
    st.markdown("#### Cost Projection")
    
    cost_df = optimization_df[['FISCAL_MONTH', 'PROJECTED_PRODUCTION_COST', 'PROJECTED_STORAGE_COST', 'PROJECTED_TOTAL_COST']]
    cost_df.columns = ['Month', 'Production Cost', 'Storage Cost', 'Total Cost']
    
    st.dataframe(cost_df, use_container_width=True)
    
    total_cost = optimization_df['PROJECTED_TOTAL_COST'].sum()
    st.markdown(f"**Total Projected Cost:** ${total_cost:,.2f}")

else:
    st.info("🔄 No optimization results available. Run the ML notebook to generate recommendations.")
    st.markdown("""
    To generate optimization results:
    1. Navigate to the ML Notebooks section in Snowflake
    2. Execute the `PREBUILD_OPTIMIZATION` notebook
    3. Return here to view the results
    """)

# ============================================================================
# DETAILED TABLES
# ============================================================================
st.markdown("---")
st.markdown("### Detailed Capacity Data")

tab1, tab2 = st.tabs(["Production Capacity", "Warehouse Zones"])

with tab1:
    st.dataframe(production_df, use_container_width=True)

with tab2:
    display_wh = warehouse_df[['SITE_NAME', 'ZONE_NAME', 'MAX_CAPACITY_PALLETS', 
                               'CURRENT_OCCUPANCY_PALLETS', 'UTILIZATION_PCT', 
                               'STORAGE_COST', 'CAPACITY_THRESHOLD_PERCENT']]
    st.dataframe(display_wh, use_container_width=True)

