"""
Builds and compiles the LangGraph DAG.

Full topology:

  classify_question
      │
      ├─ chitchat → handle_chitchat → END
      │
      └─ data → generate_sql ←────────────────────────┐
                     │                                 │  retry (transient/max not hit)
                 confirm_sql  ← HITL gate             │
                 (interrupt() pauses here for          │
                  DDL or when hitl_sql_preview=True)   │
                     │                                 │
                 execute_sql ────────────────────────→─┘
                     │
                     ├─ sql_error, max retries  → state_printer → END
                     ├─ tag=table               → state_printer → END
                     └─ tag=chart  → generate_chart_instructions
                                          │
                                     execute_chart_code
                                          │
                                     state_printer → END

Human-in-the-loop (HITL):
  The graph is compiled with a MemorySaver checkpointer so its state can be
  persisted across the HTTP boundary.

  Flow:
    1. POST /query   → ainvoke() runs until confirm_sql calls interrupt()
                       → service detects suspension, stores thread_id, returns
                          QueryResponse(result_type="pending_approval", ...)
    2. React shows SQL + Approve/Reject buttons
    3. POST /query/approve → service calls ainvoke(Command(resume=True/False), config)
                           → graph resumes from confirm_sql, continues to execute_sql

Error retry loop:
  execute_sql classifies its exception:
    permanent → sets sql_retry_count=MAX immediately (no more retries)
    transient → increments count; route_after_sql routes back to generate_sql
"""
from __future__ import annotations

import logging
from pathlib import Path

from langchain_classic.chains.sql_database.query import create_sql_query_chain
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from agent.nodes import (
    classify_question,
    confirm_sql,
    execute_chart_code,
    execute_sql,
    generate_chart_instructions,
    generate_sql,
    handle_chitchat,
    route_after_sql,
    route_by_intent,
    state_printer,
)
from agent.prompts import get_sql_generation_prompt
from agent.state import GraphState
from core.config import settings

logger = logging.getLogger(__name__)

_INSTRUCTIONS_PATH = Path(__file__).parent / "instructions.md"

# Shared in-process checkpointer.
# MemorySaver keeps snapshots in a dict — fine for a single-process dev server.
# For production swap with AsyncPostgresSaver or AsyncSqliteSaver.
_checkpointer = MemorySaver()


def _load_instructions(db_type: str, tables: list[str]) -> str:
    try:
        template = _INSTRUCTIONS_PATH.read_text()
        return template.format(
            db_type=db_type or "SQL",
            tables=", ".join(tables) if tables else "unknown",
        )
    except Exception as exc:
        logger.warning("Could not load instructions.md: %s", exc)
        return "You are a helpful BI assistant."


def build_graph(sql_generator) -> object:
    def _llm(model: str, max_tokens: int) -> ChatOpenAI:
        return ChatOpenAI(model=model, api_key=settings.openai_api_key, max_tokens=max_tokens)

    # Each node gets its own model — tune MODEL_CLASSIFY / MODEL_SQL / etc in env vars.
    llm_classify = _llm(settings.model_classify, 32)    # only outputs "INTENT: x\nTAG: y"
    llm_chitchat = _llm(settings.model_chitchat, 256)   # short user-facing replies
    llm_chart    = _llm(settings.model_chart, 1024)     # plotly code needs more room

    workflow = StateGraph(GraphState)

    # Async wrappers — Python has no async lambda
    async def _classify_question(s):
        return await classify_question(s, llm_classify)

    async def _handle_chitchat(s):
        return await handle_chitchat(s, llm_chitchat)

    async def _generate_sql(s):
        return await generate_sql(s, sql_generator)

    async def _generate_chart_instructions(s):
        return await generate_chart_instructions(s, llm_chart)

    # ── Nodes ────────────────────────────────────────────────────────────────
    workflow.add_node("classify_question",           _classify_question)
    workflow.add_node("handle_chitchat",             _handle_chitchat)
    workflow.add_node("generate_sql",                _generate_sql)
    workflow.add_node("confirm_sql",                 confirm_sql)       # sync — interrupt() is sync
    workflow.add_node("execute_sql",                 execute_sql)
    workflow.add_node("generate_chart_instructions", _generate_chart_instructions)
    workflow.add_node("execute_chart_code",          execute_chart_code)
    workflow.add_node("state_printer",               state_printer)

    # ── Entry ────────────────────────────────────────────────────────────────
    workflow.set_entry_point("classify_question")

    # ── Intent branch ────────────────────────────────────────────────────────
    workflow.add_conditional_edges(
        "classify_question",
        route_by_intent,
        {"handle_chitchat": "handle_chitchat", "generate_sql": "generate_sql"},
    )
    workflow.add_edge("handle_chitchat", END)

    # ── SQL pipeline ─────────────────────────────────────────────────────────
    workflow.add_edge("generate_sql", "confirm_sql")    # always goes through HITL gate
    workflow.add_edge("confirm_sql",  "execute_sql")    # gate either passes or sets sql_query=""

    workflow.add_conditional_edges(
        "execute_sql",
        route_after_sql,
        {
            "generate_sql":               "generate_sql",
            "state_printer":              "state_printer",
            "generate_chart_instructions": "generate_chart_instructions",
        },
    )

    # ── Chart branch ─────────────────────────────────────────────────────────
    workflow.add_edge("generate_chart_instructions", "execute_chart_code")
    workflow.add_edge("execute_chart_code",          "state_printer")
    workflow.add_edge("state_printer",               END)

    # Compile WITH checkpointer — required for interrupt() to work.
    return workflow.compile(checkpointer=_checkpointer)


def initialize_dag(db_uri: str, db_type: str = "", tables: list[str] | None = None) -> tuple:
    """
    Returns (compiled_graph, formatted_instructions).
    Called once at /connect; stored in the user's Session.
    """
    tables = tables or []
    logger.info(
        "Initialising DAG  db_type=%s  tables=%d  uri=%.30s…",
        db_type, len(tables), db_uri,
    )
    db = SQLDatabase.from_uri(db_uri)
    # SQL node gets the strongest model + dialect-specific prompt.
    llm_for_sql = ChatOpenAI(model=settings.model_sql, api_key=settings.openai_api_key)
    sql_generator = create_sql_query_chain(
        llm=llm_for_sql, db=db,
        k=settings.sql_result_limit,
        prompt=get_sql_generation_prompt(db_type),
    )
    instructions = _load_instructions(db_type, tables)
    dag = build_graph(sql_generator)
    return dag, instructions
