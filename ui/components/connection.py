"""
Database connection form — Streamlit UI component.
Calls POST /api/v1/connect and stores session_id in st.session_state.
"""
from __future__ import annotations

import streamlit as st
import httpx

from core.config import settings

_DB_TYPES = [
    "SQLite", "PostgreSQL", "MySQL", "Microsoft SQL Server",
    "Snowflake", "BigQuery", "Oracle", "DuckDB",
]

_DB_KEY_MAP = {
    "SQLite": "sqlite",
    "PostgreSQL": "postgres",
    "MySQL": "mysql",
    "Microsoft SQL Server": "mssql",
    "Snowflake": "snowflake",
    "BigQuery": "bigquery",
    "Oracle": "oracle",
    "DuckDB": "duckdb",
}


def _render_params(db_type: str) -> dict:
    """Render the right form fields for the chosen DB type."""
    p: dict = {}

    if db_type in ("SQLite", "DuckDB"):
        p["db_path"] = st.text_input("Database File Path", value="")

    elif db_type in ("PostgreSQL", "MySQL", "Microsoft SQL Server", "Oracle"):
        c1, c2 = st.columns(2)
        p["host"] = c1.text_input("Host", value="localhost")
        p["port"] = int(c2.number_input("Port", value={"PostgreSQL": 5432, "MySQL": 3306,
                                                         "Microsoft SQL Server": 1433,
                                                         "Oracle": 1521}[db_type]))
        p["user"] = c1.text_input("Username")
        p["password"] = c2.text_input("Password", type="password")
        if db_type != "Oracle":
            p["database"] = st.text_input("Database Name")
        else:
            p["sid"] = st.text_input("SID")

    elif db_type == "Snowflake":
        c1, c2 = st.columns(2)
        p["account"] = c1.text_input("Account", placeholder="orgname-accountname")
        p["user"] = c1.text_input("Username")
        p["password"] = c2.text_input("Password", type="password")
        p["database"] = c1.text_input("Database")
        p["warehouse"] = c2.text_input("Warehouse")
        p["schema_"] = c2.text_input("Schema", value="PUBLIC")

    elif db_type == "BigQuery":
        p["project"] = st.text_input("Project ID")
        p["dataset"] = st.text_input("Dataset")
        creds = st.file_uploader("Service Account JSON Key", type=["json"])
        if creds:
            import os, tempfile
            tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
            tmp.write(creds.getvalue())
            tmp.close()
            p["credentials_path"] = tmp.name

    return p


def render_connection_sidebar() -> None:
    """
    Draw the DB connection panel in the Streamlit sidebar.
    On success, writes session_id and tables to st.session_state.
    """
    st.sidebar.markdown("## Database Connection")

    db_type_label = st.sidebar.selectbox("Database type", _DB_TYPES, key="db_type_label")
    db_type = _DB_KEY_MAP[db_type_label]

    with st.sidebar.form("db_connect_form"):
        params = _render_params(db_type_label)
        submitted = st.form_submit_button("Connect")

    if submitted:
        payload = {"db_type": db_type, **{k: v for k, v in params.items() if v}}
        with st.spinner("Connecting…"):
            try:
                resp = httpx.post(
                    f"{settings.api_base_url}/api/v1/connect",
                    json=payload,
                    timeout=30,
                )
                if resp.status_code == 201:
                    data = resp.json()
                    st.session_state["session_id"] = data["session_id"]
                    st.session_state["tables"] = data["tables"]
                    st.session_state["db_type"] = data["db_type"]
                    st.session_state["chat_history"] = []
                    st.sidebar.success(f"Connected — {len(data['tables'])} tables found")
                    st.rerun()
                else:
                    st.sidebar.error(resp.json().get("detail", "Connection failed"))
            except httpx.ConnectError:
                st.sidebar.error(
                    f"Cannot reach API at {settings.api_base_url}. "
                    "Make sure the FastAPI server is running."
                )

    if st.session_state.get("session_id"):
        st.sidebar.markdown("---")
        st.sidebar.markdown(f"**DB:** `{st.session_state.get('db_type','')}`")
        tables = st.session_state.get("tables", [])
        if tables:
            with st.sidebar.expander(f"{len(tables)} tables"):
                st.write(tables)

        if st.sidebar.button("Disconnect"):
            _disconnect()


def _disconnect() -> None:
    sid = st.session_state.get("session_id")
    if sid:
        try:
            httpx.delete(
                f"{settings.api_base_url}/api/v1/disconnect",
                headers={"X-Session-ID": sid},
                timeout=10,
            )
        except Exception:
            pass
    for key in ("session_id", "tables", "db_type", "chat_history"):
        st.session_state.pop(key, None)
    st.rerun()
