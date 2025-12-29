import streamlit as st
import json
import html
import _snowflake
from utils.styles import COLORS

# ============================================================================
# CORTEX AGENT CONFIGURATION
# ============================================================================
AGENT_NAME = "SOP_ANALYST_AGENT"
DATABASE = "SOP_SCENARIO_PLANNING_LOGISTICS"
SCHEMA = "SOP_LOGISTICS"

# Sample questions - mix of structured data and document queries
SAMPLE_QUESTIONS = [
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
    
    # Structured data only (Cortex Analyst)
    "Compare the total warehousing cost between Baseline and Q4 Push for October.",
    "What is the gross margin percentage for each scenario?",
    
    # Documents only (Cortex Search)
    "What were the action items from the August S&OP meeting?",
    "What are the storage rates in the warehouse capacity SLA?",
]

def parse_sse_events(sse_text: str) -> list:
    """Parse Server-Sent Events (SSE) format into a list of events."""
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
    """Query the Cortex Agent via REST API."""
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
        
        # Parse the response
        if isinstance(response, (list, tuple)):
            status_code = response[0]
            response_body = response[2] if len(response) > 2 else response[1]
        elif isinstance(response, dict):
            status_code = response.get("status")
            response_body = response.get("content", "{}")
        else:
            return {"error": f"Unexpected response type: {type(response)}"}
        
        if status_code == 200:
            if isinstance(response_body, str):
                if response_body.strip().startswith('event:'):
                    events = parse_sse_events(response_body)
                else:
                    try:
                        result = json.loads(response_body)
                        events = result if isinstance(result, list) else [{"event": "response", "data": result}]
                    except json.JSONDecodeError:
                        events = [{"event": "raw", "data": response_body}]
            else:
                events = response_body if isinstance(response_body, list) else [{"event": "response", "data": response_body}]
            
            assistant_text = ""
            event_types_found = []
            
            for event in events:
                if not isinstance(event, dict):
                    continue
                    
                event_type = event.get("event", "")
                event_data = event.get("data", {})
                event_types_found.append(event_type)
                
                if event_type == "error" or "error" in event_type.lower():
                    error_msg = event_data.get("message", "Unknown error") if isinstance(event_data, dict) else str(event_data)
                    return {"error": error_msg}
                elif event_type == "response.text":
                    if isinstance(event_data, dict) and "text" in event_data:
                        assistant_text += event_data.get("text", "")
                    elif isinstance(event_data, str):
                        assistant_text += event_data
                elif event_type == "text":
                    if isinstance(event_data, dict):
                        assistant_text += event_data.get("text", "")
                    elif isinstance(event_data, str):
                        assistant_text += event_data
            
            debug_info = None
            if debug:
                debug_info = {
                    "status_code": status_code,
                    "response_format": "SSE" if isinstance(response_body, str) and response_body.strip().startswith('event:') else "JSON",
                    "total_events": len(events),
                    "event_types_found": list(set(event_types_found)),
                    "raw_preview": str(response_body)[:2000] if isinstance(response_body, str) else str(events)[:2000]
                }
            
            if not assistant_text:
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


def render_chat_widget(session):
    """Renders the persistent chat widget with fixed layout."""
    
    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_question" not in st.session_state:
        st.session_state.pending_question = None

    # --- Process pending question FIRST (before rendering UI) ---
    if st.session_state.pending_question:
        with st.spinner("Analyzing..."):
            result = query_agent(st.session_state.pending_question)
            
            if "error" in result:
                error_msg = result["error"]
                if "Agent not available" in error_msg or "not found" in error_msg.lower():
                    response_content = "⚠️ Agent Not Found: Please deploy the agent using `snow sql -c demo -f sql/09_cortex_agent.sql`"
                else:
                    response_content = f"⚠️ Error: {error_msg}"
            else:
                response_content = result.get("response", "No response received.")
            
            st.session_state.messages.append({"role": "assistant", "content": response_content})
            st.session_state.pending_question = None

    # === FIXED LAYOUT STRUCTURE ===
    # Open container wrapper
    st.markdown('<div class="chat-widget-container">', unsafe_allow_html=True)
    
    # --- HEADER SECTION (static) ---
    st.markdown('<div class="chat-header">', unsafe_allow_html=True)
    st.markdown("### AI Analyst")
    st.markdown("Ask anything about your S&OP data or documents.")

    # Sample Questions (collapsed by default)
    with st.expander("💡 Sample Questions", expanded=False):
        for i, question in enumerate(SAMPLE_QUESTIONS):
            if st.button(question, key=f"sample_{i}", use_container_width=True):
                handle_question(question, session)
    
    st.markdown('</div>', unsafe_allow_html=True)  # Close chat-header

    # --- CONVERSATION SECTION (scrollable) ---
    # Build conversation as single HTML block for proper scrolling
    if st.session_state.messages:
        conv_html = '<div class="chat-conversation">'
        for message in st.session_state.messages:
            escaped_content = html.escape(message["content"])
            if message["role"] == "user":
                conv_html += f'''
                <div class="chat-msg chat-msg-user">
                    <div class="chat-msg-label">You</div>
                    <div class="chat-msg-content">{escaped_content}</div>
                </div>
                '''
            else:
                conv_html += f'''
                <div class="chat-msg chat-msg-assistant">
                    <div class="chat-msg-label">AI Analyst</div>
                    <div class="chat-msg-content">{escaped_content}</div>
                </div>
                '''
        conv_html += '</div>'
        st.markdown(conv_html, unsafe_allow_html=True)
    else:
        # Empty state placeholder to maintain layout
        st.markdown('<div class="chat-conversation chat-empty"><p style="color: #64748b; text-align: center; padding: 2rem;">No messages yet. Ask a question below!</p></div>', unsafe_allow_html=True)

    # --- INPUT SECTION (fixed to bottom) ---
    st.markdown('<div class="chat-input-section">', unsafe_allow_html=True)
    
    # Clear chat button (only show if there are messages)
    if st.session_state.messages:
        if st.button("🗑️ Clear Chat", use_container_width=True, key="clear_chat_btn"):
            st.session_state.messages = []
            st.session_state.pending_question = None
    
    # Chat Input form
    with st.form(key="chat_input_form", clear_on_submit=True):
        prompt = st.text_area(
            "Ask a question...",
            key="chat_input_area",
            height=80,
            max_chars=1000,
            label_visibility="collapsed"
        )
        submit = st.form_submit_button("Send", use_container_width=True)
        if submit and prompt:
            handle_question(prompt, session)
            
    # Footer
    st.markdown(f'''
    <div style="text-align: center; color: #94a3b8; font-size: 0.7rem; margin-top: 8px;">
        Agent: <code>{DATABASE}.{SCHEMA}.{AGENT_NAME}</code>
    </div>
    ''', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)  # Close chat-input-section
    st.markdown('</div>', unsafe_allow_html=True)  # Close chat-widget-container


def handle_question(question, session):
    """Queue a question for processing. Processing happens at render time."""
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": question})
    # Queue for processing (will be handled at start of next render)
    st.session_state.pending_question = question

