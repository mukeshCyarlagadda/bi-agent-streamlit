"""
Pydantic models for the /query and /export endpoints.

TableResult mirrors the shape produced by convert_dataframe for multi-table queries.
QueryResponse is what the Streamlit client receives and renders.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel


class QueryRequest(BaseModel):
    question: str


class ApproveRequest(BaseModel):
    """Body for POST /query/approve."""
    approved: bool   # True = run the SQL, False = cancel


class TableResult(BaseModel):
    table_name: str
    dataframe: Optional[Dict[str, List[Any]]] = None   # {col: [values]}
    sql_statement: Optional[str] = None
    error: Optional[str] = None


class QueryResponse(BaseModel):
    sql_query: Optional[str] = None

    result_type: Literal[
        "table", "multi_table", "chart", "table_and_chart",
        "error", "message",
        "pending_approval",   # graph is paused at a confirm_sql interrupt
    ]

    # Exactly one of these is populated depending on result_type
    dataframe: Optional[List[Dict[str, Any]]] = None     # rows as list-of-dicts
    multi_table_data: Optional[List[TableResult]] = None

    # Self-contained Plotly HTML (full_html=True, include_plotlyjs="cdn")
    chart_html: Optional[str] = None

    error: Optional[str] = None
    message: Optional[str] = None

    # HITL fields — populated when result_type == "pending_approval"
    pending_sql: Optional[str] = None
    pending_is_ddl: Optional[bool] = None
    pending_reason: Optional[str] = None


class ExportResponse(BaseModel):
    """The PDF is returned as raw bytes in the HTTP body (application/pdf)."""
    pass  # FastAPI returns a Response directly — this model is for documentation only
