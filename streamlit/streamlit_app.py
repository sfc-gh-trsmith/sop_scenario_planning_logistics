import streamlit as st
from snowflake.snowpark.context import get_active_session
from utils.styles import get_page_style

# Import views
from views import home, executive_dashboard, scenario_builder, capacity_analysis, about

# Import the chat component
from components.chat_agent import render_chat_widget

# 1. Global Config (Must be first)
st.set_page_config(
    page_title="S&OP Scenario Planning",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed" # Hide standard sidebar
)

# 2. Session State Setup
if "current_view" not in st.session_state:
    st.session_state.current_view = "Home"
if "show_chat" not in st.session_state:
    st.session_state.show_chat = False # Default closed

# 3. Static Header / Navbar
def render_navbar():
    with st.container():
        # Create columns: Branding | Nav Items | Chat Toggle
        # Adjust ratios to fit
        col1, col2, col3 = st.columns([1.5, 6, 0.5])
        
        with col1:
            st.markdown("""
            <div style="padding-top: 5px;">
                <span style="font-size: 1.2rem; font-weight: 700;">🏭 S&OP Planner</span>
            </div>
            """, unsafe_allow_html=True)
            
        with col2:
            # Navigation Buttons
            # Use columns inside the nav column for spacing
            nav_cols = st.columns(5)
            
            # Helper to style active button (Streamlit buttons don't support custom class natively easily, 
            # so we just use primary/secondary type)
            def nav_button(label, view_name, col_idx):
                is_active = st.session_state.current_view == view_name
                type_ = "primary" if is_active else "secondary"
                if nav_cols[col_idx].button(label, key=f"nav_{view_name}", type=type_, use_container_width=True):
                    st.session_state.current_view = view_name
                    st.experimental_rerun()

            nav_button("Home", "Home", 0)
            nav_button("Executive", "Executive Dashboard", 1)
            nav_button("Scenarios", "Scenario Builder", 2)
            nav_button("Capacity", "Capacity Analysis", 3)
            nav_button("About", "About", 4)
                
        with col3:
            # Toggle Chat Button - Compact
            # If chat is open, we want to close it (>>). If closed, open it (<<).
            btn_label = "»" if st.session_state.show_chat else "«"
            # Use secondary type for less intrusive look
            if st.button(btn_label, key="toggle_chat", help="Toggle AI Analyst", type="secondary", use_container_width=False):
                st.session_state.show_chat = not st.session_state.show_chat
                st.experimental_rerun()
        
        # Bottom border for navbar
        st.markdown("""
        <div style="border-bottom: 1px solid #334155; margin-top: -10px; margin-bottom: 10px;"></div>
        """, unsafe_allow_html=True)

# 4. Main Application Layout
def main():
    try:
        session = get_active_session()
    except:
        st.warning("Could not get active Snowflake session. Features requiring database access will fail.")
        session = None
    
    # Apply Styles
    st.markdown(get_page_style(), unsafe_allow_html=True)
    
    # Render Navbar (Persistent)
    render_navbar()
    
    # Determine Layout Ratio
    if st.session_state.show_chat:
        # Split: 75% Content, 25% Chat
        main_col, chat_col = st.columns([0.72, 0.28], gap="medium")
    else:
        # Full width content
        main_col = st.container()
        chat_col = None

    # Render Main Content
    with main_col:
        view_name = st.session_state.current_view
        
        if view_name == "Home":
            home.render(session)
        elif view_name == "Executive Dashboard":
            executive_dashboard.render(session)
        elif view_name == "Scenario Builder":
            scenario_builder.render(session)
        elif view_name == "Capacity Analysis":
            capacity_analysis.render(session)
        elif view_name == "About":
            about.render(session)
        else:
            st.error(f"Unknown view: {view_name}")

    # Render Chat Widget (Right Column)
    if st.session_state.show_chat and chat_col:
        with chat_col:
            render_chat_widget(session)

if __name__ == "__main__":
    main()
