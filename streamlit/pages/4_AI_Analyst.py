"""
AI Analyst - Cortex Agent Interface

Unified natural language interface powered by Snowflake Cortex Agent.
Routes questions automatically to:
  - Cortex Analyst: Structured data queries via semantic model
  - Cortex Search: Document retrieval (contracts, SLAs, meeting minutes)
"""

import streamlit as st
import json
from snowflake.snowpark.context import get_active_session
import _snowflake

import sys
sys.path.insert(0, '..')
from utils.styles import get_page_style, COLORS

# Page config
st.set_page_config(page_title="AI Analyst", page_icon="🤖", layout="wide")
st.markdown(get_page_style(), unsafe_allow_html=True)

# Session
session = get_active_session()

# ============================================================================
# CORTEX AGENT CONFIGURATION
# ============================================================================
AGENT_NAME = "SOP_ANALYST_AGENT"
DATABASE = "SOP_SCENARIO_PLANNING_LOGISTICS"
SCHEMA = "SOP_LOGISTICS"

# Debug mode - set in sidebar, accessed globally
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False

# Sample questions - mix of structured data and document queries
SAMPLE_QUESTIONS = [
    # -------------------------------------------------------------------------
    # MULTIPART QUESTIONS (Force agent to use BOTH Analyst AND Search)
    # These are the showcase questions for the demo
    # -------------------------------------------------------------------------
    
    # Q1: Links numerical cost projection with contract penalty terms
    "The Q4 Push scenario increases warehousing costs at Northeast DC - what are our projected costs there, and what overflow penalties could we face according to the 3PL contract?",
    
    # Q2: Combines forecast data with SLA escalation procedures
    "How much demand increase are we projecting for Q4 Push vs Baseline, and what SLA escalation procedures should we follow if capacity exceeds the threshold?",
    
    # Q3: Links meeting decisions with current scenario financials
    "What contingency plans were discussed in the August S&OP meeting, and how do our current Q4 Push margin projections compare to Baseline?",
    
    # Q4: Northeast-specific analysis with contract terms
    "Show me the warehousing cost difference between scenarios for the Northeast region, and summarize the key terms in our 3PL contract for that facility.",
    
    # Q5: Financial impact with policy context
    "What is the net margin after storage costs for each scenario, and what does our warehouse SLA say about advance notice requirements for capacity changes?",
    
    # -------------------------------------------------------------------------
    # SINGLE-TOOL QUESTIONS (For simpler demos or fallback)
    # -------------------------------------------------------------------------
    
    # Structured data only (Cortex Analyst)
    "Compare the total warehousing cost between Baseline and Q4 Push for October.",
    "What is the gross margin percentage for each scenario?",
    
    # Documents only (Cortex Search)
    "What were the action items from the August S&OP meeting?",
    "What are the storage rates in the warehouse capacity SLA?",
]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def display_message(role: str, content: str):
    """Display a chat message with styled container (compatible with Snowflake Streamlit)."""
    if role == "user":
        st.markdown(f'''
        <div style="background: rgba(100, 210, 255, 0.1); padding: 0.75rem 1rem; 
                    margin: 0.5rem 0; border-radius: 8px; border-left: 3px solid #64d2ff;">
            <div style="color: #64d2ff; font-size: 0.75rem; margin-bottom: 0.25rem;">You</div>
            <p style="color: #e2e8f0; margin: 0; white-space: pre-wrap;">{content}</p>
        </div>
        ''', unsafe_allow_html=True)
    else:
        st.markdown(f'''
        <div style="background: rgba(59, 130, 246, 0.08); padding: 0.75rem 1rem; 
                    margin: 0.5rem 0; border-radius: 8px; border-left: 3px solid #3b82f6;">
            <div style="color: #60a5fa; font-size: 0.75rem; margin-bottom: 0.25rem;">AI Analyst</div>
            <p style="color: #e2e8f0; margin: 0; white-space: pre-wrap;">{content}</p>
        </div>
        ''', unsafe_allow_html=True)


def parse_sse_events(sse_text: str) -> list:
    """Parse Server-Sent Events (SSE) format into a list of events.
    
    SSE format:
        event: <event_type>
        data: <json_data>
        
    Returns:
        List of dicts with 'event' and 'data' keys
    """
    events = []
    current_event = None
    
    for line in sse_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line.startswith('event:'):
            current_event = line[6:].strip()
        elif line.startswith('data:') and current_event:
            data_str = line[5:].strip()
            try:
                data = json.loads(data_str)
            except json.JSONDecodeError:
                data = data_str
            events.append({'event': current_event, 'data': data})
    
    return events


def query_agent(question: str, conversation_history: list = None, debug: bool = False) -> dict:
    """Query the Cortex Agent via REST API.
    
    Uses the _snowflake module to call the Cortex Agent endpoint directly,
    which is the supported method for Streamlit in Snowflake.
    
    Args:
        question: The user's question
        conversation_history: Previous conversation messages
        debug: If True, include debug information in the response
    """
    try:
        # Build the request payload with message history
        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({
            "role": "user", 
            "content": [{"type": "text", "text": question}]
        })
        
        payload = {
            "messages": messages,
            "stream": False
        }
        
        # Call the Cortex Agent via REST API
        response = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/databases/{DATABASE}/schemas/{SCHEMA}/agents/{AGENT_NAME}:run",
            {},  # headers
            {},  # params  
            payload,
            None,  # request_guid
            60000  # timeout_ms
        )
        
        # Parse the response - handle both dict and tuple/list formats
        # _snowflake.send_snow_api_request may return (status, headers, body) or {"status": ..., "content": ...}
        if isinstance(response, (list, tuple)):
            status_code = response[0]
            response_body = response[2] if len(response) > 2 else response[1]
        elif isinstance(response, dict):
            status_code = response.get("status")
            response_body = response.get("content", "{}")
        else:
            return {"error": f"Unexpected response type: {type(response)}"}
        
        if status_code == 200:
            # The response is in Server-Sent Events (SSE) format, not JSON
            # Parse SSE events from the response body
            if isinstance(response_body, str):
                # Check if it looks like SSE format (starts with "event:")
                if response_body.strip().startswith('event:'):
                    events = parse_sse_events(response_body)
                else:
                    # Try parsing as JSON (fallback for other formats)
                    try:
                        result = json.loads(response_body)
                        events = result if isinstance(result, list) else [{"event": "response", "data": result}]
                    except json.JSONDecodeError:
                        events = [{"event": "raw", "data": response_body}]
            else:
                events = response_body if isinstance(response_body, list) else [{"event": "response", "data": response_body}]
            
            # Extract text from the agent's response
            assistant_text = ""
            event_types_found = []
            
            for event in events:
                if not isinstance(event, dict):
                    continue
                    
                event_type = event.get("event", "")
                event_data = event.get("data", {})
                event_types_found.append(event_type)
                
                # Handle error events
                if event_type == "error" or "error" in event_type.lower():
                    error_msg = event_data.get("message", "Unknown error") if isinstance(event_data, dict) else str(event_data)
                    return {"error": error_msg}
                
                # Handle text events - collect complete text blocks (not deltas)
                # response.text contains the full text, response.text.delta contains streaming chunks
                elif event_type == "response.text":
                    if isinstance(event_data, dict) and "text" in event_data:
                        assistant_text += event_data.get("text", "")
                    elif isinstance(event_data, str):
                        assistant_text += event_data
                
                # Fallback: also handle old-style "text" events
                elif event_type == "text":
                    if isinstance(event_data, dict):
                        assistant_text += event_data.get("text", "")
                    elif isinstance(event_data, str):
                        assistant_text += event_data
            
            # Build debug info if requested
            debug_info = None
            if debug:
                debug_info = {
                    "status_code": status_code,
                    "response_format": "SSE" if isinstance(response_body, str) and response_body.strip().startswith('event:') else "JSON",
                    "total_events": len(events),
                    "event_types_found": list(set(event_types_found)),
                    "raw_preview": str(response_body)[:2000] if isinstance(response_body, str) else str(events)[:2000]
                }
            
            # Fallback for other formats
            if not assistant_text:
                # Try to extract from response dict format
                for event in events:
                    if isinstance(event, dict):
                        data = event.get("data", {})
                        if isinstance(data, dict):
                            if "text" in data:
                                assistant_text += data.get("text", "")
                            elif "message" in data and isinstance(data["message"], dict):
                                content = data["message"].get("content", [])
                                for item in content:
                                    if isinstance(item, dict) and item.get("type") == "text":
                                        assistant_text += item.get("text", "")
                
                if not assistant_text:
                    assistant_text = "Response received but no text content found."
            
            response_dict = {"response": assistant_text.strip()}
            if debug_info:
                response_dict["debug"] = debug_info
            return response_dict
        else:
            error_response = {"error": f"API Error (status {status_code}): {response_body}"}
            if debug:
                error_response["debug"] = {
                    "status_code": status_code,
                    "response_body_type": type(response_body).__name__,
                    "response_body_preview": str(response_body)[:2000]
                }
            return error_response
            
    except Exception as e:
        error_response = {"error": str(e)}
        if debug:
            import traceback
            error_response["debug"] = {
                "exception_type": type(e).__name__,
                "traceback": traceback.format_exc()
            }
        return error_response


# ============================================================================
# PAGE CONTENT
# ============================================================================
st.title("AI Analyst")

st.markdown("""
<div style="background: linear-gradient(135deg, rgba(100, 210, 255, 0.1), rgba(255, 159, 10, 0.1)); 
            padding: 1.5rem; border-radius: 12px; margin-bottom: 1.5rem; border: 1px solid rgba(100, 210, 255, 0.2);">
    <h4 style="margin: 0 0 0.5rem 0; color: #f8fafc;">Ask anything about your S&OP data</h4>
    <p style="margin: 0; color: #94a3b8; font-size: 0.95rem;">
        The AI Analyst automatically routes your question to the right source:
        <strong>structured data</strong> (forecasts, costs, scenarios) or 
        <strong>documents</strong> (contracts, SLAs, meeting minutes).
    </p>
</div>
""", unsafe_allow_html=True)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Sample questions section
with st.expander("💡 Sample Questions", expanded=False):
    cols = st.columns(2)
    for i, question in enumerate(SAMPLE_QUESTIONS):
        with cols[i % 2]:
            if st.button(question, key=f"sample_{i}", use_container_width=True):
                st.session_state.pending_question = question
                st.experimental_rerun()

# Create container for chat history (will be populated below, but appears above form)
chat_container = st.container()

# Chat input form - always at the bottom
with st.form(key="chat_form", clear_on_submit=True):
    question = st.text_input("Ask a question about S&OP data or documents...", key="user_question")
    submit_button = st.form_submit_button("Ask", use_container_width=True)
    if not submit_button:
        question = None

# Handle pending question from sample buttons
if "pending_question" in st.session_state:
    question = st.session_state.pending_question
    del st.session_state.pending_question

# Display chat history and process new questions in the container (appears above form)
with chat_container:
    # Display existing chat history
    for message in st.session_state.messages:
        display_message(message["role"], message["content"])
    
    # Process new question
    if question:
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        
        # Display user message
        display_message("user", question)
        
        # Get agent response
        with st.spinner("Analyzing..."):
            result = query_agent(question, debug=st.session_state.debug_mode)
            
            if "error" in result:
                error_msg = result["error"]
                st.error(f"Error: {error_msg}")
                
                # Provide helpful guidance
                if "Agent not available" in error_msg or "not found" in error_msg.lower():
                    st.info("""
                    **Setup Required:** The Cortex Agent needs to be deployed.
                    
                    Run the following to deploy:
                    ```bash
                    ./deploy.sh --only-sql
                    ```
                    
                    Or deploy the agent SQL directly:
                    ```bash
                    snow sql -c demo -f sql/09_cortex_agent.sql
                    ```
                    """)
                
                response_content = f"⚠️ {error_msg}"
            else:
                response_content = result.get("response", "No response received.")
            
            # Display debug info if available and debug mode is enabled
            if st.session_state.debug_mode and "debug" in result:
                with st.expander("🔍 Debug Info - Raw API Response", expanded=True):
                    debug_info = result["debug"]
                    
                    # Display structured debug info
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Response Metadata:**")
                        st.write(f"- Status Code: `{debug_info.get('status_code', 'N/A')}`")
                        st.write(f"- Result Type: `{debug_info.get('result_type', 'N/A')}`")
                        st.write(f"- Result Length: `{debug_info.get('result_length', 'N/A')}`")
                    
                    with col2:
                        st.markdown("**Event Analysis:**")
                        event_types = debug_info.get('event_types_found', [])
                        if isinstance(event_types, list):
                            st.write(f"- Event Types Found: `{event_types}`")
                        else:
                            st.write(f"- Event Types Found: `{event_types}`")
                        st.write(f"- First Event Keys: `{debug_info.get('first_event_keys', 'N/A')}`")
                    
                    # Raw response preview
                    st.markdown("**Raw Response Preview:**")
                    raw_preview = debug_info.get('raw_preview', 'N/A')
                    st.code(raw_preview, language="json")
            
            # Display and store assistant message
            display_message("assistant", response_content)
            st.session_state.messages.append({"role": "assistant", "content": response_content})

# Clear chat button
if st.session_state.messages:
    if st.button("🗑️ Clear Chat", type="secondary"):
        st.session_state.messages = []
        st.experimental_rerun()

# ============================================================================
# SIDEBAR INFO
# ============================================================================
with st.sidebar:
    st.markdown("### About the AI Analyst")
    st.markdown("""
    This assistant uses **Snowflake Cortex Agent** to answer your questions by combining:
    
    📊 **Structured Data**  
    Query forecasts, costs, scenarios, and KPIs through the semantic model.
    
    📄 **Documents**  
    Search contracts, SLAs, and meeting minutes for policies and decisions.
    """)
    
    # Agent info
    st.markdown("---")
    st.markdown("### Agent Configuration")
    st.markdown(f"""
    **Agent:** `{AGENT_NAME}`  
    **Location:** `{DATABASE}.{SCHEMA}`
    
    *Agent availability is verified when you submit a question.*
    """)
    
    # Debug mode toggle
    st.markdown("---")
    st.markdown("### Developer Options")
    st.session_state.debug_mode = st.checkbox("Debug Mode", value=st.session_state.debug_mode, help="Show raw API response structure for debugging")

# ============================================================================
# FOOTER
# ============================================================================
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; color: #94a3b8; font-size: 0.85rem;">
    <p>Powered by <strong>Snowflake Cortex Agent</strong> | 
    Agent: <code>{DATABASE}.{SCHEMA}.{AGENT_NAME}</code></p>
</div>
""", unsafe_allow_html=True)
