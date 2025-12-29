import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from utils.styles import get_page_style, COLORS, PLOTLY_THEME, metric_card, insight_card
from utils.query_registry import register_query
from utils.data_loader import run_query

# ============================================================================
# REGISTERED QUERIES
# ============================================================================
SCENARIOS_SQL = register_query(
    "builder_scenarios",
    """
    SELECT SCENARIO_ID, SCENARIO_CODE, SCENARIO_NAME, SCENARIO_DESCRIPTION, IS_OFFICIAL_BASELINE
    FROM ATOMIC.SCENARIO_DEFINITION
    ORDER BY IS_OFFICIAL_BASELINE DESC, SCENARIO_NAME
    """,
    "Available scenarios"
)

PRODUCTS_SQL = register_query(
    "builder_products",
    """
    SELECT DISTINCT p.PRODUCT_ID, p.PRODUCT_NAME, pc.CATEGORY_NAME
    FROM ATOMIC.PRODUCT p
    JOIN ATOMIC.PRODUCT_CATEGORY pc ON p.PRODUCT_CATEGORY_ID = pc.PRODUCT_CATEGORY_ID
    WHERE p.IS_CURRENT_FLAG = TRUE AND pc.IS_CURRENT_FLAG = TRUE
    ORDER BY pc.CATEGORY_NAME, p.PRODUCT_NAME
    """,
    "Available products"
)

SITES_SQL = register_query(
    "builder_sites",
    """
    SELECT DISTINCT SITE_ID, SITE_NAME, SITE_CODE
    FROM ATOMIC.SITE
    WHERE IS_CURRENT_FLAG = TRUE
    ORDER BY SITE_NAME
    """,
    "Available sites"
)

FORECAST_DETAIL_SQL = register_query(
    "builder_forecast_detail",
    """
    SELECT 
        df.DEMAND_FORECAST_ID,
        df.PRODUCT_ID,
        p.PRODUCT_NAME,
        df.SITE_ID,
        s.SITE_NAME,
        df.FORECAST_DATE,
        df.FORECAST_QUANTITY,
        df.SCENARIO_ID,
        sd.SCENARIO_CODE
    FROM ATOMIC.DEMAND_FORECAST_VERSIONS df
    JOIN ATOMIC.PRODUCT p ON df.PRODUCT_ID = p.PRODUCT_ID AND p.IS_CURRENT_FLAG = TRUE
    JOIN ATOMIC.SITE s ON df.SITE_ID = s.SITE_ID AND s.IS_CURRENT_FLAG = TRUE
    JOIN ATOMIC.SCENARIO_DEFINITION sd ON df.SCENARIO_ID = sd.SCENARIO_ID
    WHERE df.IS_CURRENT_VERSION = TRUE
    ORDER BY df.FORECAST_DATE, p.PRODUCT_NAME, s.SITE_NAME
    """,
    "Forecast details"
)

def render(session):
    """Render the Scenario Builder View"""
    st.title("Scenario Builder")
    st.markdown("**Persona:** Demand Planner | **Focus:** Create and modify demand scenarios")
    
    # Load reference data
    try:
        scenarios_df = run_query(session, SCENARIOS_SQL, "scenarios")
        products_df = run_query(session, PRODUCTS_SQL, "products")
        sites_df = run_query(session, SITES_SQL, "sites")
    except Exception as e:
        st.error(f"Failed to load reference data: {e}")
        raise
    
    # ============================================================================
    # SCENARIO SELECTION
    # ============================================================================
    st.markdown("---")
    st.markdown("### Select Scenario to Modify")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        scenario_options = {f"{row['SCENARIO_NAME']} ({row['SCENARIO_CODE']})": row['SCENARIO_ID'] 
                           for _, row in scenarios_df.iterrows()}
        selected_scenario_label = st.selectbox(
            "Scenario",
            options=list(scenario_options.keys()),
            index=1 if len(scenario_options) > 1 else 0  # Default to second (non-baseline)
        )
        selected_scenario_id = scenario_options[selected_scenario_label]
    
    with col2:
        scenario_info = scenarios_df[scenarios_df['SCENARIO_ID'] == selected_scenario_id].iloc[0]
        if scenario_info['IS_OFFICIAL_BASELINE']:
            st.markdown("""
            <div style="background: rgba(255, 159, 10, 0.1); padding: 1rem; border-radius: 8px; margin-top: 1.5rem;">
                ⚠️ <strong>Baseline scenarios are read-only.</strong>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background: rgba(48, 209, 88, 0.1); padding: 1rem; border-radius: 8px; margin-top: 1.5rem;">
                ✅ <strong>This scenario can be modified.</strong>
            </div>
            """, unsafe_allow_html=True)
    
    # ============================================================================
    # BULK ADJUSTMENT TOOL
    # ============================================================================
    st.markdown("---")
    st.markdown("### Bulk Demand Adjustment")
    
    if scenario_info['IS_OFFICIAL_BASELINE']:
        st.info("Select a non-baseline scenario to enable adjustments.")
    else:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            adjustment_type = st.radio(
                "Adjustment Type",
                ["Percentage", "Absolute"],
                horizontal=True
            )
        
        with col2:
            if adjustment_type == "Percentage":
                adjustment_value = st.slider(
                    "Adjustment %",
                    min_value=-50,
                    max_value=100,
                    value=0,
                    step=5,
                    format="%d%%"
                )
            else:
                adjustment_value = st.number_input(
                    "Adjustment (units)",
                    min_value=-10000,
                    max_value=10000,
                    value=0,
                    step=100
                )
        
        with col3:
            # Filter options
            selected_quarter = st.selectbox(
                "Apply to Quarter",
                ["All", "Q3", "Q4"]
            )
        
        # Product/Site filters
        col1, col2 = st.columns(2)
        with col1:
            category_options = ["All Categories"] + products_df['CATEGORY_NAME'].unique().tolist()
            selected_category = st.selectbox("Product Category", category_options)
        
        with col2:
            site_options = ["All Sites"] + sites_df['SITE_NAME'].tolist()
            selected_site = st.selectbox("Site", site_options)
        
        # Preview and Apply
        st.markdown("#### Preview Impact")
        
        # Build preview query
        preview_sql = f"""
        SELECT 
            FISCAL_QUARTER,
            COUNT(*) as AFFECTED_ROWS,
            SUM(FORECAST_QUANTITY) as CURRENT_TOTAL,
            SUM(FORECAST_QUANTITY * {1 + adjustment_value/100 if adjustment_type == 'Percentage' else 1}) 
                {f'+ {adjustment_value}' if adjustment_type == 'Absolute' else ''} as PROJECTED_TOTAL
        FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
        WHERE SCENARIO_ID = {selected_scenario_id}
        {'AND FISCAL_QUARTER = ' + repr(selected_quarter) if selected_quarter != 'All' else ''}
        GROUP BY FISCAL_QUARTER
        ORDER BY FISCAL_QUARTER
        """
        
        try:
            preview_df = run_query(session, preview_sql, "preview")
            
            if not preview_df.empty:
                col1, col2, col3 = st.columns(3)
                
                total_current = preview_df['CURRENT_TOTAL'].sum()
                total_projected = preview_df['PROJECTED_TOTAL'].sum()
                change_pct = ((total_projected - total_current) / total_current * 100) if total_current > 0 else 0
                
                with col1:
                    st.metric("Current Total", f"{int(total_current):,}")
                with col2:
                    st.metric("Projected Total", f"{int(total_projected):,}")
                with col3:
                    st.metric("Change", f"{change_pct:+.1f}%")
                
                st.dataframe(preview_df, use_container_width=True)
        except Exception as e:
            st.warning(f"Preview unavailable: {e}")
        
        # Apply button
        if st.button("💾 Apply Changes", type="primary", disabled=(adjustment_value == 0)):
            with st.spinner("Applying changes to Snowflake..."):
                try:
                    # Build UPDATE statement
                    if adjustment_type == "Percentage":
                        set_clause = f"FORECAST_QUANTITY = FORECAST_QUANTITY * {1 + adjustment_value/100}"
                    else:
                        set_clause = f"FORECAST_QUANTITY = FORECAST_QUANTITY + {adjustment_value}"
                    
                    where_clauses = [f"SCENARIO_ID = {selected_scenario_id}"]
                    
                    if selected_quarter != "All":
                        where_clauses.append(f"MONTH(FORECAST_DATE) IN " + 
                            ("(7, 8, 9)" if selected_quarter == "Q3" else "(10, 11, 12)"))
                    
                    update_sql = f"""
                    UPDATE ATOMIC.DEMAND_FORECAST_VERSIONS
                    SET {set_clause},
                        UPDATED_BY_USER = CURRENT_USER(),
                        UPDATED_TIMESTAMP = CURRENT_TIMESTAMP()
                    WHERE {' AND '.join(where_clauses)}
                        AND IS_CURRENT_VERSION = TRUE
                    """
                    
                    session.sql(update_sql).collect()
                    st.success(f"✅ Successfully applied {adjustment_value}{'%' if adjustment_type == 'Percentage' else ' units'} adjustment!")
                    st.experimental_rerun()
                    
                except Exception as e:
                    st.error(f"Failed to apply changes: {e}")
                    raise
    
    # ============================================================================
    # DETAILED FORECAST VIEW
    # ============================================================================
    st.markdown("---")
    st.markdown("### Detailed Forecast View")
    
    # Load forecast data for selected scenario
    forecast_sql = f"""
    SELECT 
        PRODUCT_NAME,
        SITE_NAME,
        FISCAL_MONTH,
        FISCAL_QUARTER,
        FORECAST_QUANTITY,
        TOTAL_REVENUE
    FROM SOP_LOGISTICS.SCENARIO_COMPARISON_V
    WHERE SCENARIO_ID = {selected_scenario_id}
    ORDER BY 
        CASE FISCAL_MONTH
            WHEN 'July' THEN 1 WHEN 'August' THEN 2 WHEN 'September' THEN 3
            WHEN 'October' THEN 4 WHEN 'November' THEN 5 WHEN 'December' THEN 6
        END,
        PRODUCT_NAME, SITE_NAME
    """
    
    try:
        forecast_df = run_query(session, forecast_sql, "forecast_detail")
        
        # Aggregated view
        agg_df = forecast_df.groupby(['FISCAL_MONTH', 'FISCAL_QUARTER']).agg({
            'FORECAST_QUANTITY': 'sum',
            'TOTAL_REVENUE': 'sum'
        }).reset_index()
        
        month_order = ['July', 'August', 'September', 'October', 'November', 'December']
        agg_df['MONTH_ORDER'] = agg_df['FISCAL_MONTH'].map({m: i for i, m in enumerate(month_order)})
        agg_df = agg_df.sort_values('MONTH_ORDER')
        
        fig = go.Figure()
        
        # Convert to native Python types for SiS compatibility
        fig.add_trace(go.Bar(
            x=[str(m) for m in agg_df['FISCAL_MONTH'].tolist()],
            y=[float(q) for q in agg_df['FORECAST_QUANTITY'].tolist()],
            marker_color=[COLORS['baseline'] if q == 'Q3' else COLORS['secondary'] 
                         for q in agg_df['FISCAL_QUARTER'].tolist()],
            text=[f"{int(v):,}" for v in agg_df['FORECAST_QUANTITY'].tolist()],
            textposition='outside'
        ))
        
        fig.update_layout(
            **PLOTLY_THEME,
            title=f"Monthly Demand: {selected_scenario_label}",
            xaxis_title="Month",
            yaxis_title="Forecast Quantity",
            showlegend=False,
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed data table
        with st.container():
            st.markdown("#### Detailed Data")
            st.dataframe(
                forecast_df[['PRODUCT_NAME', 'SITE_NAME', 'FISCAL_MONTH', 'FORECAST_QUANTITY', 'TOTAL_REVENUE']],
                use_container_width=True,
                height=300
            )
    
    except Exception as e:
        st.error(f"Failed to load forecast data: {e}")
    
    # ============================================================================
    # CREATE NEW SCENARIO
    # ============================================================================
    st.markdown("---")
    st.markdown("### Create New Scenario")
    
    with st.form("new_scenario_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            new_scenario_name = st.text_input("Scenario Name", placeholder="e.g., Supply Shortage Q4")
            new_scenario_code = st.text_input("Scenario Code", placeholder="e.g., SHORTAGE_Q4")
        
        with col2:
            base_scenario = st.selectbox(
                "Copy From",
                options=list(scenario_options.keys())
            )
            new_description = st.text_area("Description", placeholder="Describe this scenario...")
        
        submitted = st.form_submit_button("Create Scenario", type="primary")
        
        if submitted:
            if not new_scenario_name or not new_scenario_code:
                st.error("Please provide both a name and code for the new scenario.")
            else:
                with st.spinner("Creating new scenario..."):
                    try:
                        base_id = scenario_options[base_scenario]
                        
                        # Insert new scenario definition
                        insert_sql = f"""
                        INSERT INTO ATOMIC.SCENARIO_DEFINITION 
                            (SCENARIO_ID, SCENARIO_CODE, SCENARIO_NAME, SCENARIO_DESCRIPTION, SCENARIO_TYPE, IS_OFFICIAL_BASELINE, CREATED_BY_USER, CREATED_TIMESTAMP)
                        SELECT 
                            (SELECT COALESCE(MAX(SCENARIO_ID), 0) + 1 FROM ATOMIC.SCENARIO_DEFINITION),
                            '{new_scenario_code}',
                            '{new_scenario_name}',
                            '{new_description}',
                            'CUSTOM',
                            FALSE,
                            CURRENT_USER(),
                            CURRENT_TIMESTAMP()
                        """
                        session.sql(insert_sql).collect()
                        
                        # Copy forecasts from base scenario
                        copy_sql = f"""
                        INSERT INTO ATOMIC.DEMAND_FORECAST_VERSIONS
                            (DEMAND_FORECAST_ID, PRODUCT_ID, SITE_ID, FORECAST_DATE, FORECAST_QUANTITY,
                             SCENARIO_ID, FORECAST_TYPE, VERSION_TIMESTAMP, IS_CURRENT_VERSION,
                             CREATED_BY_USER, CREATED_TIMESTAMP)
                        SELECT 
                            (SELECT COALESCE(MAX(DEMAND_FORECAST_ID), 0) FROM ATOMIC.DEMAND_FORECAST_VERSIONS) + ROW_NUMBER() OVER (ORDER BY DEMAND_FORECAST_ID),
                            PRODUCT_ID, SITE_ID, FORECAST_DATE, FORECAST_QUANTITY,
                            (SELECT SCENARIO_ID FROM ATOMIC.SCENARIO_DEFINITION WHERE SCENARIO_CODE = '{new_scenario_code}'),
                            'Copied', CURRENT_TIMESTAMP(), TRUE,
                            CURRENT_USER(), CURRENT_TIMESTAMP()
                        FROM ATOMIC.DEMAND_FORECAST_VERSIONS
                        WHERE SCENARIO_ID = {base_id} AND IS_CURRENT_VERSION = TRUE
                        """
                        session.sql(copy_sql).collect()
                        
                        st.success(f"✅ Created scenario '{new_scenario_name}' with forecasts copied from {base_scenario}")
                        st.experimental_rerun()
                        
                    except Exception as e:
                        st.error(f"Failed to create scenario: {e}")
                        raise
