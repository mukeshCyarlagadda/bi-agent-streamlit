# streamlit_app.py
import streamlit as st
import os
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from dotenv import load_dotenv
from plotly.graph_objects import Figure 
from PIL import Image

# Import modules
from db_connection_ui import render_database_selector, get_database_connection
from sql_agent_langgraph import initialize_dag
from document_generator import create_download_button
from table_utils import display_multi_table_results

# Load environment variables
load_dotenv()

# ------------------------------
# **Page Configuration & Styling**
# ------------------------------
st.set_page_config(
    page_title="Business Intelligence Assistant",
    page_icon="📊",
    layout="wide"
)

st.markdown("""
    <style>
    /* Main Header */
    .main-header {
        font-size: 36px;
        color: #1E90FF;
        text-align: center;
        margin-bottom: 20px;
    }
    /* Sidebar Header */
    .sidebar-header {
        font-size: 24px;
        color: #2F4F4F;
    }
    /* Chat History Container */
    .chat-container {
        max-height: 70vh;
        overflow-y: auto;
        padding: 10px;
    }
    /* Chat Bubbles */
    .user-query {
        font-weight: bold;
        color: white;
        background-color: #007BFF;
        padding: 10px;
        border-radius: 10px;
        width: fit-content;
        max-width: 80%;
        align-self: flex-end;
        margin-bottom: 5px;
    }
    .bot-response {
        font-weight: normal;
        color: black;
        background-color: #f1f1f1;
        padding: 10px;
        border-radius: 10px;
        width: fit-content;
        max-width: 80%;
        align-self: flex-start;
        margin-bottom: 5px;
    }
    /* Fixed Chatbox at Bottom */
    .chatbox-container {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background: #111;
        padding: 15px;
        box-shadow: 0px -2px 10px rgba(255, 255, 255, 0.1);
        display: flex;
        align-items: center;
    }
    .chat-input {
        flex: 1;
        border: 1px solid #ccc;
        padding: 10px;
        font-size: 16px;
        border-radius: 5px;
        background: #222;
        color: white;
    }
    .send-button {
        background: #007BFF;
        border: none;
        color: white;
        padding: 10px 15px;
        border-radius: 5px;
        cursor: pointer;
        font-size: 16px;
        margin-left: 10px;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown("<h1 class='main-header'>🤖 Business Intelligence Assistant</h1>", unsafe_allow_html=True)

# ------------------------------
# **Sidebar for Database Connection (Using the modular component)**
# ------------------------------
is_connected, db_uri = render_database_selector()

# Initialize session state
if "app" not in st.session_state:
    st.session_state["app"] = None
if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []
if "show_sql" not in st.session_state:
    st.session_state["show_sql"] = False

# Initialize DAG if connected to database
if is_connected and db_uri and st.session_state["app"] is None:
    try:
        # Initialize DAG
        app = initialize_dag(db_uri)
        
        # Store in session state
        st.session_state["app"] = app
        
    except Exception as e:
        st.error(f"❌ Error initializing application: {e}")
        st.stop()

# ------------------------------
# **SQL Query Display Toggle**
# ------------------------------
st.sidebar.markdown("---")
st.sidebar.markdown("### Display Options")
st.session_state["show_sql"] = st.sidebar.checkbox(
    "Show SQL Queries", 
    value=st.session_state["show_sql"],
    help="Toggle to show/hide SQL queries in responses"
)

# ------------------------------
# **Add Download Button in Sidebar**
# ------------------------------
if st.session_state["chat_history"]:
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Session Summary")
    create_download_button(st.session_state["chat_history"])

# ------------------------------
# **Display Chat History**
# ------------------------------
st.subheader("💬 Chat History")

chat_container = st.container()
with chat_container:
    for chat in st.session_state["chat_history"]:
        # Display user message
        user_message = f"<div class='user-query'>🟢 You: {chat['query']}</div>"
        st.markdown(user_message, unsafe_allow_html=True)

        result = chat["result"]
        
        # Display SQL query if toggle is enabled and query exists
        if st.session_state["show_sql"] and "sql_query" in chat:
            sql_query = chat["sql_query"]
            if sql_query:
                st.code(sql_query, language="sql")
        
        # Handle the output based on type
        if isinstance(result, dict):
            # Check for multi-table data first
            if "multi_table_data" in result:
                display_multi_table_results(result["multi_table_data"], st.session_state["show_sql"])
            
            # Display single DataFrame if it exists
            elif "dataframe" in result:
                st.dataframe(result["dataframe"])
            
            # Display chart if it exists
            if "chart" in result and isinstance(result["chart"], str) and result["chart"].endswith('.png'):
                try:
                    chart_path = result["chart"]
                    if os.path.exists(chart_path):
                        image = Image.open(chart_path)
                        st.image(image, caption="Generated Chart", use_container_width=True)
                    else:
                        st.error(f"Chart file not found: {chart_path}")
                except Exception as e:
                    st.error(f"Error displaying image: {str(e)}")
        
        # Handle old format results
        elif isinstance(result, pd.DataFrame):
            st.dataframe(result)
        elif isinstance(result, str) and result.endswith('.png'):
            try:
                if os.path.exists(result):
                    image = Image.open(result)
                    st.image(image, caption="Generated Chart", use_container_width=True)
                else:
                    st.error(f"Chart file not found: {result}")
            except Exception as e:
                st.error(f"Error displaying image: {str(e)}")
        elif isinstance(result, str):
            bot_message = f"<div class='bot-response'>{result}</div>"
            st.markdown(bot_message, unsafe_allow_html=True)

# ------------------------------
# **Fixed Chatbox at Bottom**
# ------------------------------
user_query = st.text_input(
    label="Query Input",
    key="user_query", 
    label_visibility="collapsed",
    placeholder="Type your question here..."
)
send_button = st.button("Send", key="send_button")

# ------------------------------
# **Run Query & Update History**
# ------------------------------
if send_button and user_query.strip():
    if st.session_state["app"] is None:
        st.error("⚠️ Please connect to a database first")
    else:
        with st.spinner("Processing your query..."):
            try:
                # Show progress
                progress_bar = st.progress(0)
                
                # Run the DAG
                steps = list(st.session_state["app"].stream({"question": user_query}))
                progress_bar.progress(50)
                
                # Extract Final State
                final_state = steps[-1].get('state_printer', {})
                final_output = final_state.get('final_output')
                
                # Extract SQL query from intermediate steps
                sql_query = None
                for step in steps:
                    if 'generate_sql' in step:
                        sql_query = step['generate_sql'].get('sql_query')
                        break
                
                progress_bar.progress(75)
                
                # Store Query & Response in Chat History
                chat_entry = {
                    "query": user_query,
                    "result": final_output
                }
                if sql_query:
                    chat_entry["sql_query"] = sql_query
                    
                st.session_state["chat_history"].append(chat_entry)
                
                # Clean up matplotlib
                plt.close('all')
                
                # Complete progress
                progress_bar.progress(100)
                
                st.rerun()

            except Exception as e:
                st.error(f"⚠️ Query Processing Error: {e}")
                st.exception(e)