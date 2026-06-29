"""
LangGraph state definition.

Design rules:
  - Flat and JSON-serialisable — no live DB connections, no objects
  - db_uri travels in state so each node opens its own short-lived connection
  - chat_history uses Annotated[list, operator.add] so multiple nodes can append
    to it within a single graph run without overwriting each other
  - sql_retry_count / sql_error enable the retry loop in the graph
  - error_node / error_category track WHERE and WHAT TYPE of error occurred
  - hitl_approved / is_ddl drive the human-in-the-loop gate
"""
from __future__ import annotations

import operator
from typing import Annotated, List, Optional

from typing_extensions import TypedDict


class ChatMessage(TypedDict):
    role: str      # "user" | "assistant"
    content: str


class GraphState(TypedDict):
    # ── Inputs — set by the service layer on every invocation ────────────────
    question: str
    db_uri: str
    db_type: str
    tables: List[str]
    system_instructions: str

    # ── Conversation context ─────────────────────────────────────────────────
    chat_history: Annotated[List[ChatMessage], operator.add]

    # ── Per-turn routing ─────────────────────────────────────────────────────
    intent: str           # "data" | "chitchat"
    tag: str              # "chart" | "table"

    # ── SQL retry loop ───────────────────────────────────────────────────────
    sql_retry_count: int
    sql_error: Optional[str]

    # ── Human-in-the-loop ────────────────────────────────────────────────────
    is_ddl: bool              # True when generated SQL contains DDL (DELETE/DROP/…)
    hitl_approved: Optional[bool]   # set by confirm_sql after interrupt()

    # ── Error tracking ────────────────────────────────────────────────────────
    # error_node: which node produced the error ("execute_sql", "generate_sql", …)
    # error_category: "transient" | "permanent" | "llm"
    #   transient → retry with backoff (rate limit, DB lock, network)
    #   permanent → stop retrying, LLM needs to produce different SQL
    #   llm       → LLM API itself failed (not a SQL problem)
    error_node: Optional[str]
    error_category: Optional[str]

    # ── Processing ───────────────────────────────────────────────────────────
    sql_query: str
    data: dict
    chart_script: str
    chart_output: Optional[str]  # legacy PNG path (unused in Plotly path)
    chart_html: Optional[str]    # self-contained Plotly HTML string

    # ── Output ───────────────────────────────────────────────────────────────
    final_output: Optional[dict]
    error: Optional[str]
    direct_response: Optional[str]
