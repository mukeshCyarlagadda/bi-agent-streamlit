"""
Result rendering — turns a QueryResponse payload into Streamlit widgets.
No HTTP calls here. Pure display logic.
"""
from __future__ import annotations

import base64
import io

import pandas as pd
import streamlit as st


def render_result(result: dict, show_sql: bool = False) -> None:
    """
    result is the JSON-decoded QueryResponse dict from the API.
    """
    if show_sql and result.get("sql_query"):
        st.code(result["sql_query"], language="sql")

    result_type = result.get("result_type", "")

    if result_type == "error":
        st.error(result.get("error", "Unknown error"))
        return

    if result_type in ("table", "table_and_chart") and result.get("dataframe"):
        df = pd.DataFrame(result["dataframe"])
        st.dataframe(df, use_container_width=True)
        _download_buttons(df, "result")

    if result_type == "multi_table" and result.get("multi_table_data"):
        _render_multi_table(result["multi_table_data"], show_sql)

    if result.get("chart_base64"):
        img_bytes = base64.b64decode(result["chart_base64"])
        st.image(img_bytes, caption="Generated Chart", use_container_width=True)


def _render_multi_table(tables: list, show_sql: bool) -> None:
    st.subheader(f"Query Results — {len(tables)} tables")
    for t in tables:
        with st.expander(t.get("table_name", "Table"), expanded=True):
            if show_sql and t.get("sql_statement"):
                st.code(t["sql_statement"], language="sql")
            if t.get("error"):
                st.error(t["error"])
            elif t.get("dataframe"):
                df = pd.DataFrame(t["dataframe"])
                st.dataframe(df, use_container_width=True)
                _download_buttons(df, t.get("table_name", "table"))


def _download_buttons(df: pd.DataFrame, name: str) -> None:
    c1, c2, c3 = st.columns([4, 1, 1])
    csv = df.to_csv(index=False).encode()
    c2.download_button("CSV ↓", csv, f"{name}.csv", "text/csv", key=f"csv_{name}_{id(df)}")

    try:
        import openpyxl
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, sheet_name=name[:31], index=False)
        c3.download_button(
            "Excel ↓", buf.getvalue(), f"{name}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"xlsx_{name}_{id(df)}",
        )
    except ImportError:
        pass
