"""
Styling utilities for S&OP Streamlit Application

Dark theme with colorblind-safe palette matching the ML notebook.
"""

# Color Palette - Colorblind-safe, dark theme
COLORS = {
    'primary': '#64D2FF',       # Cyan - main accent
    'secondary': '#FF9F0A',     # Orange - secondary accent  
    'accent1': '#5AC8FA',       # Light blue
    'accent2': '#30D158',       # Green - success
    'danger': '#FF6B6B',        # Red - warning/danger
    'baseline': '#A1A1A6',      # Gray - neutral/baseline
    'background': '#0f172a',    # Dark slate
    'surface': '#1e293b',       # Slightly lighter slate
    'text': '#e2e8f0',          # Light text
    'text_muted': '#94a3b8',    # Muted text
}

# Plotly theme configuration
PLOTLY_THEME = {
    'paper_bgcolor': COLORS['background'],
    'plot_bgcolor': COLORS['background'],
    'font': {'color': COLORS['text']},
    'hoverlabel': {
        'bgcolor': COLORS['surface'],
        'bordercolor': '#334155',
        'font': {'color': COLORS['text']}
    }
}


def get_page_style() -> str:
    """Return CSS for consistent page styling."""
    return f"""
    <style>
        /* Hide Streamlit default elements */
        #MainMenu {{visibility: hidden;}}
        footer {{visibility: hidden;}}
        
        /* Fix sidebar navigation - remove fixed height and scroll */
        [data-testid="stSidebarNav"] {{
            max-height: none !important;
            overflow: visible !important;
        }}
        
        [data-testid="stSidebarNav"] > ul {{
            max-height: none !important;
            overflow: visible !important;
        }}
        
        /* Dark theme overrides */
        .stApp {{
            background-color: {COLORS['background']};
        }}
        
        /* Reduce top padding for compact header */
        .block-container {{
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }}

        /* Metric cards */
        .metric-card {{
            background: linear-gradient(135deg, {COLORS['surface']} 0%, #0f172a 100%);
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 1.25rem;
            margin-bottom: 1rem;
        }}
        
        .metric-value {{
            font-size: 2rem;
            font-weight: 700;
            color: {COLORS['primary']};
            margin: 0;
        }}
        
        .metric-label {{
            font-size: 0.875rem;
            color: {COLORS['text_muted']};
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        
        .metric-delta-positive {{
            color: {COLORS['accent2']};
            font-size: 0.875rem;
        }}
        
        .metric-delta-negative {{
            color: {COLORS['danger']};
            font-size: 0.875rem;
        }}
        
        /* Insight cards */
        .insight-card {{
            background: rgba(59, 130, 246, 0.08);
            border-left: 3px solid {COLORS['primary']};
            padding: 0.75rem 1rem;
            margin: 0.5rem 0;
            border-radius: 0 8px 8px 0;
        }}
        
        .insight-card .label {{
            color: {COLORS['primary']};
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.25rem;
        }}
        
        .insight-card .content {{
            color: {COLORS['text']};
            line-height: 1.5;
        }}
        
        /* Warning cards */
        .warning-card {{
            background: rgba(255, 159, 10, 0.1);
            border-left: 3px solid {COLORS['secondary']};
            padding: 0.75rem 1rem;
            margin: 0.5rem 0;
            border-radius: 0 8px 8px 0;
        }}
        
        /* Scrollable containers */
        .scroll-container {{
            max-height: 300px;
            overflow-y: auto;
            padding: 0.5rem;
            background: rgba(15, 23, 42, 0.5);
            border-radius: 8px;
        }}
        
        /* Scenario badges */
        .scenario-badge {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            margin-right: 0.5rem;
        }}
        
        .scenario-baseline {{
            background: rgba(161, 161, 166, 0.2);
            color: {COLORS['baseline']};
        }}
        
        .scenario-q4push {{
            background: rgba(255, 159, 10, 0.2);
            color: {COLORS['secondary']};
        }}
        
        /* KPI highlight */
        .kpi-highlight {{
            font-size: 3rem;
            font-weight: 800;
            background: linear-gradient(90deg, {COLORS['primary']}, {COLORS['accent1']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}
        
        /* ============================================
           CHAT WIDGET - Fixed Layout
           ============================================ */
        
        /* Main container - no fixed height, content determines size */
        .chat-widget-container {{
            background-color: {COLORS['surface']};
            border-radius: 10px;
            border: 1px solid #334155;
            padding: 1rem;
        }}
        
        /* Header section */
        .chat-header {{
            padding-bottom: 0.5rem;
        }}
        
        /* Conversation section - scrolls with max height */
        .chat-conversation {{
            overflow-y: auto;
            max-height: 400px;
            min-height: 100px;
            padding: 0.75rem;
            margin: 0.5rem 0;
            background: rgba(15, 23, 42, 0.4);
            border-radius: 8px;
            border: 1px solid #334155;
        }}
        
        .chat-conversation.chat-empty {{
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100px;
            max-height: 100px;
        }}
        
        /* Individual chat messages */
        .chat-msg {{
            padding: 0.75rem 1rem;
            margin: 0.5rem 0;
            border-radius: 8px;
        }}
        
        .chat-msg-user {{
            background: rgba(100, 210, 255, 0.1);
            border-left: 3px solid {COLORS['primary']};
        }}
        
        .chat-msg-assistant {{
            background: rgba(59, 130, 246, 0.08);
            border-left: 3px solid #3b82f6;
        }}
        
        .chat-msg-label {{
            font-size: 0.7rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.25rem;
        }}
        
        .chat-msg-user .chat-msg-label {{
            color: {COLORS['primary']};
        }}
        
        .chat-msg-assistant .chat-msg-label {{
            color: #60a5fa;
        }}
        
        .chat-msg-content {{
            color: {COLORS['text']};
            line-height: 1.5;
            white-space: pre-wrap;
            word-wrap: break-word;
        }}
        
        /* Input section - fixed to bottom, won't shrink */
        .chat-input-section {{
            flex-shrink: 0;
            border-top: 1px solid #334155;
            padding-top: 0.75rem;
            margin-top: 0.5rem;
        }}
    </style>
    """


def metric_card(label: str, value: str, delta: str = None, delta_positive: bool = True) -> str:
    """Generate HTML for a metric card."""
    delta_html = ""
    if delta:
        delta_class = "metric-delta-positive" if delta_positive else "metric-delta-negative"
        arrow = "↑" if delta_positive else "↓"
        delta_html = f'<div class="{delta_class}">{arrow} {delta}</div>'
    
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """


def insight_card(label: str, content: str) -> str:
    """Generate HTML for an insight card (no expanders)."""
    return f"""
    <div class="insight-card">
        <div class="label">{label}</div>
        <div class="content">{content}</div>
    </div>
    """


def warning_card(content: str) -> str:
    """Generate HTML for a warning card."""
    return f"""
    <div class="warning-card">
        <strong>⚠️ Attention Required</strong><br/>
        {content}
    </div>
    """

