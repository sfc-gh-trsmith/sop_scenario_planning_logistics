import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from utils.styles import get_page_style, COLORS, PLOTLY_THEME, metric_card, insight_card, warning_card
from utils.query_registry import register_query
from utils.data_loader import run_query, cached_query

# ============================================================================
# REGISTERED QUERIES
# ============================================================================
SCENARIO_SUMMARY_SQL = register_query(
    "exec_scenario_summary",
    """
    SELECT 
        SCENARIO_CODE,
        SCENARIO_NAME,
        FISCAL_QUARTER,
        SUM(FORECAST_QUANTITY) as TOTAL_DEMAND,
        SUM(TOTAL_REVENUE) as TOTAL_REVENUE,
        COUNT(DISTINCT PRODUCT_ID) as PRODUCT_COUNT,
        COUNT(DISTINCT SITE_ID) as SITE_COUNT
    FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
    GROUP BY SCENARIO_CODE, SCENARIO_NAME, FISCAL_QUARTER
    ORDER BY SCENARIO_CODE, FISCAL_QUARTER
    """,
    "Scenario summary by quarter",
    min_rows=1
)

WAREHOUSE_COST_SQL = register_query(
    "exec_warehouse_cost",
    """
    SELECT 
        SCENARIO_CODE,
        SCENARIO_NAME,
        SITE_NAME,
        SUM(FORECAST_QUANTITY) as DEMAND,
        SUM(PROJECTED_WAREHOUSING_COST) as STORAGE_COST,
        AVG(OVERFLOW_PENALTY_THRESHOLD_PCT) as CAPACITY_THRESHOLD,
        AVG(OVERFLOW_PENALTY_RATE) as OVERFLOW_RATE,
        -- Estimate overflow cost: if demand > threshold proportion of capacity, apply penalty
        -- Assumes sites have ~1M unit capacity, overflow occurs when demand exceeds threshold
        CASE 
            WHEN SUM(FORECAST_QUANTITY) > 1000000 * AVG(OVERFLOW_PENALTY_THRESHOLD_PCT) / 100
            THEN (SUM(FORECAST_QUANTITY) - 1000000 * AVG(OVERFLOW_PENALTY_THRESHOLD_PCT) / 100) 
                 / 10 * AVG(OVERFLOW_PENALTY_RATE) * 30
            ELSE 0
        END as OVERFLOW_COST
    FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
    WHERE FISCAL_QUARTER IN ('Q3', 'Q4')
    GROUP BY SCENARIO_CODE, SCENARIO_NAME, SITE_NAME
    ORDER BY SCENARIO_CODE, STORAGE_COST DESC
    """,
    "Warehouse costs by scenario and site",
    min_rows=1
)

MONTHLY_DEMAND_SQL = register_query(
    "exec_monthly_demand",
    """
    SELECT 
        SCENARIO_CODE,
        FISCAL_MONTH,
        FISCAL_QUARTER,
        SUM(FORECAST_QUANTITY) as TOTAL_DEMAND
    FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
    GROUP BY SCENARIO_CODE, FISCAL_MONTH, FISCAL_QUARTER
    ORDER BY SCENARIO_CODE, 
        CASE FISCAL_MONTH
            WHEN 'July' THEN 1 WHEN 'August' THEN 2 WHEN 'September' THEN 3
            WHEN 'October' THEN 4 WHEN 'November' THEN 5 WHEN 'December' THEN 6
        END
    """,
    "Monthly demand by scenario"
)

AVAILABLE_SCENARIOS_SQL = register_query(
    "exec_available_scenarios",
    """
    SELECT DISTINCT 
        sd.SCENARIO_ID,
        sd.SCENARIO_CODE,
        sd.SCENARIO_NAME,
        sd.IS_OFFICIAL_BASELINE
    FROM ATOMIC.SCENARIO_DEFINITION sd
    JOIN SOP_LOGISTICS.SCENARIO_COMPARISON_V sc ON sd.SCENARIO_CODE = sc.SCENARIO_CODE
    ORDER BY sd.IS_OFFICIAL_BASELINE DESC, sd.SCENARIO_NAME
    """,
    "Available scenarios with data"
)

def render(session):
    """Render the Executive Dashboard"""
    st.title("Executive Dashboard")
    st.markdown("**Persona:** VP Supply Chain | **Focus:** Strategic scenario comparison")
    
    # Load available scenarios dynamically
    try:
        available_scenarios_df = run_query(session, AVAILABLE_SCENARIOS_SQL, "available_scenarios")
    except Exception as e:
        st.error(f"Failed to load scenarios: {e}")
        st.stop()
    
    # Scenario selector
    st.markdown("### Select Scenarios to Compare")
    
    # Create dynamic checkboxes based on available scenarios
    selected_scenarios = []
    num_scenarios = len(available_scenarios_df)
    cols = st.columns(min(num_scenarios, 4))  # Max 4 columns
    
    for i, row in available_scenarios_df.iterrows():
        col_idx = i % min(num_scenarios, 4)
        with cols[col_idx]:
            if st.checkbox(f"{row['SCENARIO_NAME']}", value=(i < 2), key=f"scenario_{row['SCENARIO_CODE']}"):
                selected_scenarios.append(row['SCENARIO_CODE'])
    
    if not selected_scenarios:
        st.warning("Please select at least one scenario to display.")
        st.stop()
    
    # Load data
    try:
        scenario_df = run_query(session, SCENARIO_SUMMARY_SQL, "scenario_summary")
        warehouse_df = run_query(session, WAREHOUSE_COST_SQL, "warehouse_cost")
        monthly_df = run_query(session, MONTHLY_DEMAND_SQL, "monthly_demand")
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        raise
    
    # Filter to selected scenarios
    scenario_df = scenario_df[scenario_df['SCENARIO_CODE'].isin(selected_scenarios)]
    warehouse_df = warehouse_df[warehouse_df['SCENARIO_CODE'].isin(selected_scenarios)]
    monthly_df = monthly_df[monthly_df['SCENARIO_CODE'].isin(selected_scenarios)]
    
    # ============================================================================
    # KPI METRICS
    # ============================================================================
    st.markdown("---")
    st.markdown("### Key Performance Indicators")
    
    # Calculate aggregates
    baseline_cost = warehouse_df[warehouse_df['SCENARIO_CODE'] == 'BASELINE']['STORAGE_COST'].sum() if 'BASELINE' in selected_scenarios else 0
    baseline_overflow = warehouse_df[warehouse_df['SCENARIO_CODE'] == 'BASELINE']['OVERFLOW_COST'].sum() if 'BASELINE' in selected_scenarios else 0
    q4push_cost = warehouse_df[warehouse_df['SCENARIO_CODE'] == 'Q4_PUSH']['STORAGE_COST'].sum() if 'Q4_PUSH' in selected_scenarios else 0
    q4push_overflow = warehouse_df[warehouse_df['SCENARIO_CODE'] == 'Q4_PUSH']['OVERFLOW_COST'].sum() if 'Q4_PUSH' in selected_scenarios else 0
    
    baseline_demand = scenario_df[scenario_df['SCENARIO_CODE'] == 'BASELINE']['TOTAL_DEMAND'].sum() if 'BASELINE' in selected_scenarios else 0
    q4push_demand = scenario_df[scenario_df['SCENARIO_CODE'] == 'Q4_PUSH']['TOTAL_DEMAND'].sum() if 'Q4_PUSH' in selected_scenarios else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'BASELINE' in selected_scenarios:
            st.markdown(metric_card("Baseline Total Demand", f"{int(baseline_demand):,}"), unsafe_allow_html=True)
        
    with col2:
        if 'Q4_PUSH' in selected_scenarios:
            demand_delta = ((q4push_demand - baseline_demand) / baseline_demand * 100) if baseline_demand > 0 else 0
            st.markdown(metric_card(
                "Q4 Push Total Demand", 
                f"{int(q4push_demand):,}",
                f"{demand_delta:.1f}% vs Baseline" if 'BASELINE' in selected_scenarios else None,
                delta_positive=False  # Demand increase = cost pressure
            ), unsafe_allow_html=True)
    
    with col3:
        if 'BASELINE' in selected_scenarios:
            st.markdown(metric_card("Baseline Warehouse Cost", f"${int(baseline_cost + baseline_overflow):,}"), unsafe_allow_html=True)
    
    with col4:
        if 'Q4_PUSH' in selected_scenarios:
            total_q4_cost = q4push_cost + q4push_overflow
            total_baseline_cost = baseline_cost + baseline_overflow
            cost_delta = ((total_q4_cost - total_baseline_cost) / total_baseline_cost * 100) if total_baseline_cost > 0 else 0
            st.markdown(metric_card(
                "Q4 Push Warehouse Cost",
                f"${int(total_q4_cost):,}",
                f"{cost_delta:.1f}% vs Baseline" if 'BASELINE' in selected_scenarios else None,
                delta_positive=False
            ), unsafe_allow_html=True)
    
    # ============================================================================
    # THE "WOW" MOMENT - Cost Increase Highlight
    # ============================================================================
    if 'BASELINE' in selected_scenarios and 'Q4_PUSH' in selected_scenarios and cost_delta > 15:
        st.markdown(warning_card(
            f"The <strong>Q4 Marketing Push</strong> scenario triggers a <strong>{cost_delta:.1f}% increase</strong> "
            f"in warehousing costs. This is primarily driven by the <strong>Northeast DC</strong> exceeding "
            f"its 85% capacity threshold, resulting in <strong>${int(q4push_overflow):,}</strong> in overflow fees."
        ), unsafe_allow_html=True)
    
    # ============================================================================
    # DEMAND COMPARISON CHART
    # ============================================================================
    st.markdown("---")
    st.markdown("### Monthly Demand Comparison")
    
    fig = go.Figure()
    
    color_map = {
        'BASELINE': COLORS['baseline'],
        'Q4_PUSH': COLORS['secondary']
    }
    
    for scenario in selected_scenarios:
        scenario_data = monthly_df[monthly_df['SCENARIO_CODE'] == scenario]
        # Convert to native Python types for SiS compatibility
        fig.add_trace(go.Bar(
            name=scenario.replace('_', ' '),
            x=[str(m) for m in scenario_data['FISCAL_MONTH'].tolist()],
            y=[float(d) for d in scenario_data['TOTAL_DEMAND'].tolist()],
            marker_color=color_map.get(scenario, COLORS['primary']),
            opacity=0.85
        ))
    
    # Add Q3/Q4 boundary
    fig.add_vline(x=2.5, line_dash="dash", line_color=COLORS['danger'], opacity=0.5,
                  annotation_text="Q3 → Q4", annotation_position="top")
    
    fig.update_layout(
        **PLOTLY_THEME,
        barmode='group',
        xaxis_title="Month",
        yaxis_title="Total Demand (Units)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # ============================================================================
    # WAREHOUSE COST BY SITE
    # ============================================================================
    st.markdown("### Warehouse Cost by Site (Q3-Q4)")
    
    # Calculate total cost for each scenario/site
    warehouse_df['TOTAL_WAREHOUSE_COST'] = warehouse_df['STORAGE_COST'] + warehouse_df['OVERFLOW_COST']
    
    fig2 = go.Figure()
    
    # Define scenario colors with better contrast
    scenario_colors = {
        'BASELINE': COLORS['baseline'],
        'Q4_PUSH': COLORS['secondary'],
        'CONSERVATIVE': COLORS['primary']
    }
    
    # Get unique sites sorted by total cost
    sites = warehouse_df.groupby('SITE_NAME')['TOTAL_WAREHOUSE_COST'].sum().sort_values(ascending=False).index.tolist()
    
    for scenario in selected_scenarios:
        scenario_data = warehouse_df[warehouse_df['SCENARIO_CODE'] == scenario]
        
        # Create a lookup dict for this scenario's costs by site
        cost_by_site = dict(zip(scenario_data['SITE_NAME'], scenario_data['TOTAL_WAREHOUSE_COST']))
        
        # Extract values in consistent site order as plain lists
        y_values = [cost_by_site.get(site, 0) for site in sites]
        text_labels = [f"${v/1e6:.1f}M" if v > 0 else "" for v in y_values]
        
        fig2.add_trace(go.Bar(
            name=scenario.replace('_', ' '),
            x=sites,
            y=y_values,
            marker_color=scenario_colors.get(scenario, COLORS['text_muted']),
            text=text_labels,
            textposition='outside',
            hovertemplate=(
                "<b>%{x}</b><br>" +
                "Scenario: " + scenario.replace('_', ' ') + "<br>" +
                "Total Cost: $%{y:,.0f}<br>" +
                "<extra></extra>"
            )
        ))
    
    fig2.update_layout(
        **PLOTLY_THEME,
        barmode='group',
        xaxis_title="Distribution Center",
        yaxis_title="Total Warehouse Cost ($)",
        yaxis_tickformat='$,.0f',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=450,
        bargap=0.15,
        bargroupgap=0.1
    )
    
    st.plotly_chart(fig2, use_container_width=True)
    
    # Show cost breakdown table
    with st.expander("📊 View Detailed Cost Breakdown"):
        cost_pivot = warehouse_df.pivot_table(
            index='SITE_NAME',
            columns='SCENARIO_CODE',
            values=['STORAGE_COST', 'OVERFLOW_COST', 'TOTAL_WAREHOUSE_COST'],
            aggfunc='sum'
        ).round(0)
        
        # Format for display
        display_costs = cost_pivot['TOTAL_WAREHOUSE_COST'].copy()
        display_costs = display_costs.map(lambda x: f"${x:,.0f}" if pd.notna(x) else "-")
        st.dataframe(display_costs, use_container_width=True)
    
    # ============================================================================
    # INSIGHTS
    # ============================================================================
    st.markdown("---")
    st.markdown("### Key Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Find site with highest overflow
        if 'Q4_PUSH' in selected_scenarios:
            q4_warehouse = warehouse_df[warehouse_df['SCENARIO_CODE'] == 'Q4_PUSH']
            if not q4_warehouse.empty:
                max_overflow_site = q4_warehouse.loc[q4_warehouse['OVERFLOW_COST'].idxmax()]
                st.markdown(insight_card(
                    "Capacity Bottleneck",
                    f"<strong>{max_overflow_site['SITE_NAME']}</strong> has the highest overflow cost "
                    f"(${int(max_overflow_site['OVERFLOW_COST']):,}) in the Q4 Push scenario. "
                    f"Consider pre-building inventory in Q3 or negotiating additional 3PL capacity."
                ), unsafe_allow_html=True)
    
    with col2:
        if 'Q4_PUSH' in selected_scenarios and 'BASELINE' in selected_scenarios:
            st.markdown(insight_card(
                "Scenario Impact",
                f"The Q4 Marketing Push increases total demand by <strong>{demand_delta:.1f}%</strong>, "
                f"but warehouse costs increase by <strong>{cost_delta:.1f}%</strong> due to "
                f"non-linear overflow penalties. This demonstrates the value of integrated planning."
            ), unsafe_allow_html=True)
    
    # ============================================================================
    # DATA TABLE
    # ============================================================================
    st.markdown("---")
    with st.container():
        st.markdown("### Scenario Details")
        
        display_df = scenario_df[['SCENARIO_NAME', 'FISCAL_QUARTER', 'TOTAL_DEMAND', 'TOTAL_REVENUE', 'PRODUCT_COUNT', 'SITE_COUNT']]
        display_df.columns = ['Scenario', 'Quarter', 'Total Demand', 'Total Revenue', 'Products', 'Sites']
        
        st.dataframe(display_df, use_container_width=True)
