"""
LangGraph node functions.

Node contract:
  Every node receives the full GraphState dict and returns a PARTIAL dict.
  LangGraph merges the returned dict into the running state.

Async strategy:
  - LLM calls   → await llm.ainvoke()      (true async I/O)
  - SQL queries  → asyncio.to_thread()     (sync SQLAlchemy in thread pool)
  - Chart render → asyncio.to_thread()     (exec() is sync, run in thread pool)

Error handling strategy:
  - Every node catches its own exceptions and puts them in state
  - Error category (transient vs permanent) drives retry behaviour
  - route_after_sql reads category: permanent → skip retries entirely
"""
from __future__ import annotations

import asyncio
import logging

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import sqlalchemy.exc as sa_exc
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.types import interrupt
from sqlalchemy import create_engine

from agent.prompts import (
    CHART_GENERATION_TEMPLATE,
    CHITCHAT_TEMPLATE,
    CLASSIFY_TEMPLATE,
)
from agent.state import GraphState

# ── Plotly chart constants ────────────────────────────────────────────────────
_CHART_COLORS = [
    "#F59E0B", "#FB923C", "#10B981", "#60A5FA",
    "#F87171", "#A78BFA", "#FBBF24", "#34D399",
]
_CHART_THEME = "plotly_dark"

from core.config import settings
from core.utils import (
    extract_python_code,
    extract_sql_code,
    extract_table_name_from_sql,
    is_multi_statement_sql,
    split_sql_statements,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3

# DDL keywords: queries starting with these will trigger HITL regardless of config
_DDL_FIRST_WORDS = frozenset({
    "insert", "update", "delete", "drop", "truncate",
    "alter", "create", "replace", "merge",
})


# ── Error classification ──────────────────────────────────────────────────────

def classify_sql_error(exc: Exception) -> str:
    """
    Classify a SQL execution error as transient or permanent.

    transient → retry is worthwhile (connection glitch, lock, rate limit)
    permanent → retry with same SQL is pointless; LLM must write different SQL

    Default to "permanent" so we don't loop forever on unknown errors.
    """
    msg = str(exc).lower()

    if isinstance(exc, sa_exc.OperationalError):
        transient_hints = ["database is locked", "connection", "timeout",
                           "server has gone away", "lost connection", "unable to open"]
        if any(h in msg for h in transient_hints):
            return "transient"
        # no such table / no such column → permanent
        return "permanent"

    if isinstance(exc, (sa_exc.ProgrammingError, sa_exc.CompileError,
                        sa_exc.IntegrityError, sa_exc.DataError)):
        return "permanent"

    # openai errors (import lazily to avoid hard dependency)
    try:
        import openai
        if isinstance(exc, openai.RateLimitError):
            return "transient"
        if isinstance(exc, (openai.APIConnectionError, openai.APITimeoutError)):
            return "transient"
        if isinstance(exc, openai.BadRequestError):
            return "llm"   # context too long, content policy, etc.
    except ImportError:
        pass

    return "permanent"


def _is_ddl(sql: str) -> bool:
    first = sql.strip().split()[0].lower() if sql.strip() else ""
    return first in _DDL_FIRST_WORDS


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_history(chat_history: list) -> str:
    if not chat_history:
        return "(no previous messages)"
    lines = []
    for msg in chat_history[-6:]:
        role = "User" if msg.get("role") == "user" else "Assistant"
        lines.append(f"{role}: {msg.get('content', '')}")
    return "\n".join(lines)


def _system_msg(state: GraphState) -> SystemMessage:
    return SystemMessage(content=state.get("system_instructions", "You are a BI assistant."))


# ── Node 1 — classify intent + visualisation type ────────────────────────────

async def classify_question(state: GraphState, llm) -> dict:
    """Single LLM call → intent (data|chitchat) + tag (chart|table)."""
    prompt = CLASSIFY_TEMPLATE.format(question=state["question"])
    try:
        response = await llm.ainvoke([_system_msg(state), HumanMessage(content=prompt)])
    except Exception as exc:
        category = classify_sql_error(exc)
        logger.error(
            "classify_question LLM error: %s",
            exc,
            extra={"error_category": category, "error_node": "classify_question"},
        )
        # Default to data/table so the pipeline still attempts a query
        return {
            "intent": "data", "tag": "table",
            "sql_retry_count": 0, "sql_error": None,
            "is_ddl": False, "hitl_approved": None,
            "error_node": "classify_question", "error_category": category,
        }

    text = response.content.strip()
    intent, tag = "data", "table"
    for line in text.splitlines():
        line = line.strip()
        if line.upper().startswith("INTENT:"):
            v = line.split(":", 1)[1].strip().lower()
            if v in ("data", "chitchat"):
                intent = v
        elif line.upper().startswith("TAG:"):
            v = line.split(":", 1)[1].strip().lower()
            if v in ("chart", "table"):
                tag = v

    logger.debug("classify: intent=%s tag=%s", intent, tag)
    return {
        "intent": intent, "tag": tag,
        "sql_retry_count": 0, "sql_error": None,
        "is_ddl": False, "hitl_approved": None,
        "error_node": None, "error_category": None,
    }


# ── Node 2 — handle chitchat ──────────────────────────────────────────────────

async def handle_chitchat(state: GraphState, llm) -> dict:
    prompt = CHITCHAT_TEMPLATE.format(
        system_instructions=state.get("system_instructions", ""),
        history=_fmt_history(state.get("chat_history", [])),
        question=state["question"],
    )
    try:
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        reply = response.content.strip()
    except Exception as exc:
        logger.error("handle_chitchat LLM error: %s", exc)
        reply = "I'm having trouble responding right now. Please try again."
    logger.debug("chitchat reply: %.60s", reply)
    return {"direct_response": reply, "final_output": {"message": reply}}


# ── SQL retry hint extractor ──────────────────────────────────────────────────

import re as _re

def _error_hint(error_msg: str) -> str:
    """
    Parse a SQL error string and return a targeted natural-language hint
    that helps the LLM avoid repeating the same mistake on retry.
    """
    msg = error_msg.lower()

    # "no such column: alias.Column" — alias/column mismatch
    m = _re.search(r"no such column[:\s]+([`'\"]?)(\w+)\.(\w+)\1", error_msg, _re.IGNORECASE)
    if m:
        alias, col = m.group(2), m.group(3)
        return (
            f"Hint: '{alias}.{col}' is invalid — '{alias}' is an alias for a table "
            f"that does NOT have a column named '{col}'. "
            f"Check which table in your JOIN actually owns '{col}' and use that table's alias instead.\n"
        )

    # "no such column: Column" — column doesn't exist at all
    m = _re.search(r"no such column[:\s]+([`'\"]?)(\w+)\1", error_msg, _re.IGNORECASE)
    if m:
        col = m.group(2)
        return (
            f"Hint: column '{col}' does not exist. "
            f"Check the exact column name in the schema — it may be spelled differently or belong to a different table.\n"
        )

    # "no such table"
    m = _re.search(r"no such table[:\s]+([`'\"]?)(\w+)\1", error_msg, _re.IGNORECASE)
    if m:
        tbl = m.group(2)
        return (
            f"Hint: table '{tbl}' does not exist. "
            f"Use only tables listed in the schema provided.\n"
        )

    # ambiguous column name
    if "ambiguous" in msg:
        m = _re.search(r"ambiguous[^:]*[:\s]+([`'\"]?)(\w+)\1", error_msg, _re.IGNORECASE)
        col = m.group(2) if m else "a column"
        return (
            f"Hint: '{col}' is ambiguous — it exists in more than one joined table. "
            f"Qualify it with the correct table alias (e.g. alias.{col}).\n"
        )

    # syntax error
    if "syntax error" in msg:
        return "Hint: there is a SQL syntax error. Check for missing commas, unmatched parentheses, or invalid keywords.\n"

    return ""   # no specific hint available — generic retry message is enough


# ── Node 3 — generate SQL ─────────────────────────────────────────────────────

async def generate_sql(state: GraphState, sql_generator) -> dict:
    """
    Generates SQL. On retry, prepends the previous error to the question
    so the LLM understands what went wrong and fixes it.
    """
    retry_count = state.get("sql_retry_count", 0)
    sql_error = state.get("sql_error")

    question = state["question"]
    if retry_count > 0 and sql_error:
        hint = _error_hint(sql_error)
        question = (
            f"RETRY ATTEMPT {retry_count + 1}: Your previous SQL failed.\n"
            f"Error: {sql_error}\n"
            f"{hint}"
            f"Rules: do NOT repeat the same query. Re-read the schema carefully before writing SQL.\n\n"
            f"Original question: {question}"
        )

    try:
        raw = await sql_generator.ainvoke({"question": question})
    except Exception as exc:
        category = classify_sql_error(exc)
        logger.error(
            "generate_sql LLM error (attempt %d): %s",
            retry_count + 1, exc,
            extra={"error_node": "generate_sql", "error_category": category},
        )
        # If LLM is down, bail out immediately
        return {
            "sql_query": "",
            "sql_error": f"LLM error: {exc}",
            "sql_retry_count": _MAX_RETRIES,   # skip further retries
            "error_node": "generate_sql",
            "error_category": category,
        }

    sql_query = extract_sql_code(raw)
    is_ddl = _is_ddl(sql_query)
    logger.debug(
        "generate_sql attempt=%d is_ddl=%s sql=%.120s",
        retry_count + 1, is_ddl, sql_query,
    )
    return {"sql_query": sql_query, "is_ddl": is_ddl}


# ── Node 4 — human-in-the-loop gate ──────────────────────────────────────────

def confirm_sql(state: GraphState) -> dict:
    """
    HITL gate between SQL generation and execution.

    Always interrupts if the SQL is DDL (DELETE / DROP / UPDATE / INSERT / …).
    Also interrupts when hitl_sql_preview=True in config (reviews all queries).

    How interrupt() works:
      1. This node calls interrupt(payload) — LangGraph snapshots the graph state
         and suspends execution. The service layer detects the suspension, stores
         the thread_id, and returns a "pending_approval" response to the client.
      2. The React UI shows the SQL and asks "Run this?" — user clicks Approve/Reject.
      3. POST /query/approve sends Command(resume=True/False) with the thread_id.
      4. LangGraph restores the checkpoint and resumes HERE — interrupt() returns
         the value that was passed to Command(resume=...).
      5. If approved=False the node sets sql_query="" so execute_sql returns nothing.

    This node is SYNC (not async) because interrupt() is synchronous in LangGraph.
    """
    sql = state.get("sql_query", "")
    is_ddl = state.get("is_ddl", False)
    needs_review = is_ddl or settings.hitl_sql_preview

    if not needs_review:
        return {"hitl_approved": True}

    reason = "DDL statement detected — this will modify your data" if is_ddl else "SQL preview"
    logger.info(
        "HITL interrupt: %s  sql=%.80s",
        reason, sql,
        extra={"is_ddl": is_ddl, "reason": reason},
    )

    # Pause the graph. The dict here is what the service layer sees as the
    # "interrupt value" — it contains everything the UI needs to render the prompt.
    approved: bool = interrupt({
        "sql": sql,
        "is_ddl": is_ddl,
        "reason": reason,
        "message": (
            "⚠️ This query will modify your database. Approve?" if is_ddl
            else "Review the SQL before execution. Approve?"
        ),
    })

    if not approved:
        logger.info("HITL: SQL rejected by user")
        return {"hitl_approved": False, "sql_query": ""}

    logger.info("HITL: SQL approved by user")
    return {"hitl_approved": True}


# ── Node 5 — execute SQL ──────────────────────────────────────────────────────

def _run_sql_sync(sql_query: str, db_uri: str) -> dict:
    engine = create_engine(db_uri)
    with engine.connect() as conn:
        if is_multi_statement_sql(sql_query):
            tables = []
            for i, stmt in enumerate(split_sql_statements(sql_query)):
                try:
                    df = pd.read_sql(stmt, conn)
                    tables.append({
                        "table_name": extract_table_name_from_sql(stmt),
                        "dataframe": {col: df[col].tolist() for col in df.columns},
                        "sql_statement": stmt,
                    })
                except Exception as exc:
                    tables.append({
                        "table_name": f"Error in statement {i + 1}",
                        "error": str(exc),
                        "sql_statement": stmt,
                    })
            return {"multi_table": True, "tables": tables}
        else:
            df = pd.read_sql(sql_query, conn)
            return {col: df[col].tolist() for col in df.columns}


async def execute_sql(state: GraphState) -> dict:
    """
    Run SQL in a thread pool. On failure:
      - transient error → increment retry count (route_after_sql will retry)
      - permanent error → set retry count to MAX (route_after_sql will give up)
    This way the retry loop never wastes attempts on fundamentally broken SQL.
    """
    sql = state.get("sql_query", "")

    # User rejected SQL at HITL gate
    if state.get("hitl_approved") is False or not sql:
        return {
            "data": {},
            "final_output": {"message": "Query cancelled." if state.get("hitl_approved") is False else "No SQL generated."},
        }

    retry_count = state.get("sql_retry_count", 0)
    try:
        data = await asyncio.to_thread(_run_sql_sync, sql, state["db_uri"])
        logger.info(
            "execute_sql OK  rows=%s",
            len(data.get("tables", data)) if isinstance(data.get("tables"), list) else len(data),
            extra={"error_node": None, "error_category": None},
        )
        return {"data": data, "sql_error": None, "error_node": None, "error_category": None}

    except Exception as exc:
        category = classify_sql_error(exc)
        new_count = _MAX_RETRIES if category == "permanent" else retry_count + 1

        logger.warning(
            "execute_sql FAILED  attempt=%d  category=%s  error=%s",
            retry_count + 1, category, exc,
            extra={
                "error_node": "execute_sql",
                "error_category": category,
                "sql_retry_count": new_count,
            },
        )
        return {
            "data": {},
            "sql_error": str(exc),
            "sql_retry_count": new_count,
            "error_node": "execute_sql",
            "error_category": category,
        }


# ── Routing functions (not nodes) ─────────────────────────────────────────────

def route_by_intent(state: GraphState) -> str:
    return "handle_chitchat" if state.get("intent") == "chitchat" else "generate_sql"


def route_after_sql(state: GraphState) -> str:
    """
    After execute_sql:
      - Final output already set (HITL rejected / no SQL) → state_printer
      - SQL error + retries left → back to generate_sql
      - SQL error + max retries  → state_printer (error message)
      - Success + chart          → generate_chart_instructions
      - Success + table          → state_printer
    """
    if state.get("final_output"):
        return "state_printer"

    if state.get("sql_error"):
        if state.get("sql_retry_count", 0) < _MAX_RETRIES:
            return "generate_sql"
        return "state_printer"

    return (
        "generate_chart_instructions"
        if state.get("tag") == "chart"
        else "state_printer"
    )


# ── Node 6 — generate chart code ─────────────────────────────────────────────

async def generate_chart_instructions(state: GraphState, llm) -> dict:
    data = state.get("data", {})
    if isinstance(data, dict) and data.get("multi_table"):
        tables = data.get("tables", [])
        if not tables or "dataframe" not in tables[0]:
            return {"chart_script": "Error: No chartable data."}
        data = tables[0]["dataframe"]

    df_sample = pd.DataFrame(data).head(5).to_dict(orient="records")
    prompt = CHART_GENERATION_TEMPLATE.format(
        question=state["question"],
        columns=list(data.keys()),
        sample=df_sample,
    )
    try:
        response = await llm.ainvoke([_system_msg(state), HumanMessage(content=prompt)])
        chart_script = extract_python_code(response)
    except Exception as exc:
        logger.error(
            "generate_chart_instructions LLM error: %s", exc,
            extra={"error_node": "generate_chart_instructions"},
        )
        chart_script = "Error: LLM unavailable for chart generation."

    logger.debug("Chart script generated (%d chars)", len(chart_script))
    return {"chart_script": chart_script}


# ── Node 7 — execute chart code ──────────────────────────────────────────────

_EXEC_BUILTINS = {
    "len": len, "range": range, "enumerate": enumerate, "zip": zip,
    "list": list, "dict": dict, "tuple": tuple, "set": set,
    "str": str, "int": int, "float": float, "bool": bool,
    "round": round, "abs": abs, "min": min, "max": max, "sum": sum,
    "sorted": sorted, "reversed": reversed, "print": print,
    "isinstance": isinstance, "hasattr": hasattr, "getattr": getattr,
}


def _render_plotly_sync(chart_script: str, data: dict) -> str:
    df = pd.DataFrame(data)
    local_ns: dict = {
        "__builtins__": _EXEC_BUILTINS,
        "df": df,
        # Common LLM aliases for the dataframe — all point to the same object
        "df_filtered": df, "df_plot": df, "df_chart": df, "data": df,
        "px": px, "go": go, "pd": pd, "np": np,
        "COLORS": _CHART_COLORS, "THEME": _CHART_THEME,
    }
    try:
        exec(chart_script, local_ns)  # noqa: S102
    except Exception as exc:
        logger.error("Chart exec error: %s\nScript:\n%s", exc, chart_script)
        raise

    fig: go.Figure | None = local_ns.get("fig")
    if fig is None:
        logger.error("Chart script produced no 'fig':\n%s", chart_script)
        raise RuntimeError("Chart script did not assign to 'fig'")

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        colorway=_CHART_COLORS,
        font=dict(color="rgba(255,255,255,0.72)", family="Inter, system-ui, sans-serif"),
        margin=dict(l=48, r=24, t=52, b=44),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
            font=dict(size=11, color="rgba(255,255,255,0.60)"),
        ),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.06)",
            linecolor="rgba(255,255,255,0.08)",
            tickfont=dict(size=11, color="rgba(255,255,255,0.50)"),
            title_font=dict(size=12, color="rgba(255,255,255,0.60)"),
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.06)",
            linecolor="rgba(255,255,255,0.08)",
            tickfont=dict(size=11, color="rgba(255,255,255,0.50)"),
            title_font=dict(size=12, color="rgba(255,255,255,0.60)"),
        ),
        title_font=dict(size=14, color="rgba(255,255,255,0.80)"),
    )
    # Thicken lines and enlarge markers so they read well on dark glass
    fig.update_traces(
        selector=dict(type="scatter", mode="lines"),
        line=dict(width=2.5),
    )
    fig.update_traces(
        selector=dict(type="scatter"),
        marker=dict(size=7),
    )
    fig.update_traces(
        selector=dict(type="bar"),
        marker_line_width=0,
        opacity=0.92,
    )
    return pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs=False,
        config={"responsive": True, "displayModeBar": False},
    )


async def execute_chart_code(state: GraphState) -> dict:
    script = state.get("chart_script", "")
    if not script or script.startswith("Error"):
        return {"chart_html": None}

    data = state.get("data", {})
    if isinstance(data, dict) and data.get("multi_table"):
        tables = data.get("tables", [])
        data = tables[0]["dataframe"] if tables and "dataframe" in tables[0] else {}

    try:
        html = await asyncio.to_thread(_render_plotly_sync, script, data)
        logger.info("Chart rendered (%d chars HTML)", len(html))
        return {"chart_html": html, "error_node": None}
    except Exception as exc:
        # Degrade gracefully: log the failure but do NOT set 'error' in state.
        # The SQL data is still valid — state_printer will return a table result.
        logger.error(
            "execute_chart_code FAILED — degrading to table: %s", exc,
            extra={"error_node": "execute_chart_code", "error_category": "permanent"},
        )
        return {"chart_html": None}


# ── Node 8 — assemble final output ────────────────────────────────────────────

def state_printer(state: GraphState) -> dict:
    if state.get("direct_response") or state.get("final_output"):
        return {}

    if state.get("sql_error") and state.get("sql_retry_count", 0) >= _MAX_RETRIES:
        category = state.get("error_category", "unknown")
        node = state.get("error_node", "unknown")
        user_msg = (
            f"Could not generate a working SQL query after {_MAX_RETRIES} attempts. "
            f"Last error: {state['sql_error']}"
            if category == "permanent"
            else f"A temporary error occurred in {node}. Please try again."
        )
        logger.error(
            "DAG giving up: node=%s category=%s error=%s",
            node, category, state.get("sql_error"),
            extra={"error_node": node, "error_category": category},
        )
        return {"final_output": {"error": user_msg}}

    outputs: dict = {}
    data = state.get("data", {})
    if data:
        try:
            if data.get("multi_table"):
                outputs["multi_table_data"] = data["tables"]
            else:
                # Keep as list-of-dicts (JSON/msgpack serializable).
                # DataFrame conversion was causing msgpack errors with MemorySaver.
                n = len(next(iter(data.values()), []))
                outputs["dataframe"] = [
                    {col: vals[i] for col, vals in data.items()}
                    for i in range(n)
                ]
        except Exception as exc:
            outputs["error"] = str(exc)

    if html := state.get("chart_html"):
        outputs["chart_html"] = html
    if err := state.get("error"):
        outputs["error"] = err

    return {"final_output": outputs or {"message": "No data returned for that question."}}
