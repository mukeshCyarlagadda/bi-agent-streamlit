"""
Streamlit thin client.

This file's only jobs:
  1. Render the connection sidebar (via component).
  2. Display chat history (via component).
  3. Accept user input, call POST /api/v1/query, store result.
  4. Offer PDF export.

No business logic. No DB connections. No LangGraph.
All heavy lifting happens in the FastAPI backend.
"""
from __future__ import annotations

import streamlit as st
import httpx

from core.config import settings
from ui.components.connection import render_connection_sidebar
from ui.components.chat import render_chat_history

st.set_page_config(
    page_title="Business Intelligence Assistant",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
.main-header { font-size:32px; color:#1E90FF; text-align:center; margin-bottom:20px; }
</style>
""", unsafe_allow_html=True)
st.markdown("<h1 class='main-header'>📊 Business Intelligence Assistant</h1>", unsafe_allow_html=True)

# -- sidebar -----------------------------------------------------------------
render_connection_sidebar()

# Guard: nothing works until the user connects
if not st.session_state.get("session_id"):
    st.info("Connect to a database using the sidebar to get started.")
    st.stop()

# -- display options ---------------------------------------------------------
st.sidebar.markdown("---")
show_sql = st.sidebar.checkbox("Show SQL Queries", value=False)

# -- PDF export --------------------------------------------------------------
if st.session_state.get("chat_history"):
    st.sidebar.markdown("---")
    if st.sidebar.button("📥 Export session as PDF"):
        with st.spinner("Generating PDF…"):
            try:
                resp = httpx.post(
                    f"{settings.api_base_url}/api/v1/export/pdf",
                    headers={"X-Session-ID": st.session_state["session_id"]},
                    timeout=30,
                )
                if resp.status_code == 200:
                    st.sidebar.download_button(
                        "Download PDF",
                        data=resp.content,
                        file_name="bi_session_summary.pdf",
                        mime="application/pdf",
                    )
                else:
                    st.sidebar.error(resp.json().get("detail", "Export failed"))
            except Exception as exc:
                st.sidebar.error(f"Export error: {exc}")

# -- chat history ------------------------------------------------------------
st.subheader("💬 Chat History")
render_chat_history(show_sql=show_sql)

# -- query input -------------------------------------------------------------
with st.form("query_form", clear_on_submit=True):
    question = st.text_input(
        "Your question",
        placeholder="e.g. Show me total sales by region as a bar chart",
        label_visibility="collapsed",
    )
    submitted = st.form_submit_button("Send")

if submitted and question.strip():
    with st.spinner("Thinking…"):
        try:
            resp = httpx.post(
                f"{settings.api_base_url}/api/v1/query",
                json={"question": question},
                headers={"X-Session-ID": st.session_state["session_id"]},
                timeout=120,    # LLM + SQL can be slow
            )
            if resp.status_code == 200:
                result = resp.json()
                st.session_state.setdefault("chat_history", []).append(
                    {"query": question, "result": result}
                )
                st.rerun()
            else:
                st.error(resp.json().get("detail", "Query failed"))
        except httpx.ConnectError:
            st.error(
                f"Cannot reach API at {settings.api_base_url}. "
                "Is the FastAPI server running?"
            )
        except Exception as exc:
            st.error(f"Unexpected error: {exc}")
