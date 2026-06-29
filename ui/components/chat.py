"""
Chat history display component.
Iterates over st.session_state["chat_history"] and renders each entry.
"""
from __future__ import annotations

import streamlit as st

from ui.components.results import render_result


def render_chat_history(show_sql: bool) -> None:
    history = st.session_state.get("chat_history", [])
    if not history:
        st.info("Ask a question below to get started.")
        return

    for entry in history:
        # User bubble
        st.markdown(
            f"<div style='background:#0078FF;color:white;padding:10px;"
            f"border-radius:10px;width:fit-content;max-width:80%;margin-bottom:6px'>"
            f"🟢 {entry['query']}</div>",
            unsafe_allow_html=True,
        )
        # Agent result
        render_result(entry["result"], show_sql=show_sql)
        st.markdown("---")
