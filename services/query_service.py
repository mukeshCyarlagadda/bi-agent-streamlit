"""
Query service — orchestrates the LangGraph DAG invocation.

Two entry points:
  run_query()    — starts a new query, returns result or pending-approval state
  resume_query() — resumes a graph suspended at a HITL confirm_sql interrupt

HITL flow:
  1. run_query() calls dag.ainvoke(inputs, config)
  2. When confirm_sql calls interrupt(), ainvoke() returns the snapshot state
  3. We detect suspension via dag.get_state(config).next being non-empty
  4. We store the thread_id in session and return result_type="pending_approval"
  5. Client POSTs /query/approve with approved=True/False
  6. resume_query() calls dag.ainvoke(Command(resume=approved), config)
  7. Graph resumes from inside confirm_sql, continues to execute_sql

Error tracking:
  Every node stores error_node and error_category in state.
  The service layer reads these and includes them in structured log fields.
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import TYPE_CHECKING

from langgraph.types import Command

from api.models.query import QueryResponse, TableResult
from api.session_store import ChatEntry, session_store

if TYPE_CHECKING:
    from api.session_store import Session

logger = logging.getLogger(__name__)

_HISTORY_WINDOW = 6


def _recent_history(session: "Session") -> list:
    entries = session.chat_history[-(_HISTORY_WINDOW // 2):]
    messages = []
    for entry in entries:
        messages.append({"role": "user", "content": entry.query})
        if entry.error:
            messages.append({"role": "assistant", "content": f"Error: {entry.error}"})
        elif entry.dataframe:
            messages.append({"role": "assistant", "content": f"Returned {len(entry.dataframe)} row(s)."})
        elif entry.chart_html:
            messages.append({"role": "assistant", "content": "Generated an interactive chart."})
        else:
            messages.append({"role": "assistant", "content": "Done."})
    return messages


def _blank_inputs(question: str, session: "Session") -> dict:
    """Build the initial GraphState dict for a new invocation."""
    return {
        "question":            question,
        "db_uri":              session.db_uri,
        "db_type":             session.db_type,
        "tables":              session.tables,
        "system_instructions": session.instructions,
        "chat_history":        _recent_history(session),
        "intent": "", "tag": "",
        "sql_retry_count": 0, "sql_error": None,
        "is_ddl": False, "hitl_approved": None,
        "error_node": None, "error_category": None,
        "sql_query": "", "data": {}, "chart_script": "",
        "chart_output": None, "chart_html": None, "final_output": None,
        "error": None, "direct_response": None,
    }


def _extract_result(steps: dict, session: "Session", question: str) -> QueryResponse:
    """
    Turn the graph's final state dict into a QueryResponse.
    Also appends the entry to session history.
    """
    final_output: dict = steps.get("final_output") or {}
    sql_query: str | None = steps.get("sql_query") or None
    error_node = steps.get("error_node")
    error_category = steps.get("error_category")

    # ── chart handling ───────────────────────────────────────────────────────
    chart_html: str | None = final_output.get("chart_html")

    # ── result type ──────────────────────────────────────────────────────────
    has_table = "dataframe" in final_output or "multi_table_data" in final_output
    has_chart = chart_html is not None
    has_error = "error" in final_output
    is_direct = "message" in final_output and not has_table and not has_chart

    if has_error:
        result_type = "error"
    elif has_table and has_chart:
        result_type = "table_and_chart"
    elif has_chart:
        result_type = "chart"
    elif "multi_table_data" in final_output:
        result_type = "multi_table"
    elif is_direct:
        result_type = "message"
    else:
        result_type = "table"

    # ── dataframe — already list[dict] from state_printer ───────────────────
    dataframe_rows: list | None = final_output.get("dataframe")

    multi_table: list[TableResult] | None = None
    if "multi_table_data" in final_output:
        multi_table = [
            TableResult(
                table_name=t.get("table_name", ""),
                dataframe=t.get("dataframe"),
                sql_statement=t.get("sql_statement"),
                error=t.get("error"),
            )
            for t in final_output["multi_table_data"]
        ]

    logger.info(
        "Query result: type=%s  node=%s  category=%s",
        result_type, error_node, error_category,
        extra={
            "session_id": session.session_id[:8],
            "result_type": result_type,
            "error_node": error_node,
            "error_category": error_category,
        },
    )

    session_store.append_history(
        session.session_id,
        ChatEntry(
            query=question,
            sql_query=sql_query,
            result_type=result_type,
            dataframe=dataframe_rows,
            multi_table_data=[t.model_dump() for t in multi_table] if multi_table else None,
            chart_html=chart_html,
            error=final_output.get("error"),
        ),
    )

    return QueryResponse(
        sql_query=sql_query,
        result_type=result_type,
        dataframe=dataframe_rows,
        multi_table_data=multi_table,
        chart_html=chart_html,
        error=final_output.get("error"),
        message=final_output.get("message"),
    )


async def run_query(question: str, session: "Session") -> QueryResponse:
    logger.info("Query start: %.80s", question, extra={"session_id": session.session_id[:8]})

    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    inputs = _blank_inputs(question, session)

    t0 = time.perf_counter()
    steps = await session.dag.ainvoke(inputs, config=config)
    dag_ms = round((time.perf_counter() - t0) * 1000, 1)

    # ── detect HITL interrupt ────────────────────────────────────────────────
    # When interrupt() is called inside confirm_sql, ainvoke() returns the
    # current state. The graph is suspended — snapshot.next will be non-empty.
    snapshot = session.dag.get_state(config)
    if snapshot.next:
        # Extract the interrupt payload (what was passed to interrupt())
        interrupt_data: dict = {}
        for task in snapshot.tasks:
            for intr in task.interrupts:
                interrupt_data = intr.value or {}
                break

        logger.info(
            "HITL suspend: is_ddl=%s  thread=%s  dag=%.0fms",
            interrupt_data.get("is_ddl"), thread_id[:8], dag_ms,
            extra={"session_id": session.session_id[:8], "thread_id": thread_id[:8]},
        )
        # Persist pending state so /query/approve can resume
        session_store.set_pending(session.session_id, thread_id, interrupt_data)

        return QueryResponse(
            sql_query=steps.get("sql_query"),
            result_type="pending_approval",
            pending_sql=interrupt_data.get("sql"),
            pending_is_ddl=interrupt_data.get("is_ddl", False),
            pending_reason=interrupt_data.get("reason", ""),
            message=interrupt_data.get("message"),
        )

    logger.info("Query done: dag=%.0fms", dag_ms, extra={"session_id": session.session_id[:8], "dag_ms": dag_ms})
    return _extract_result(steps, session, question)


async def resume_query(session: "Session", approved: bool) -> QueryResponse:
    """Resume a graph suspended at confirm_sql."""
    thread_id = session.pending_thread_id
    interrupt_data = session.pending_interrupt or {}
    config = {"configurable": {"thread_id": thread_id}}

    logger.info(
        "HITL resume: approved=%s  thread=%s",
        approved, thread_id[:8] if thread_id else "?",
        extra={"session_id": session.session_id[:8]},
    )

    # Command(resume=value) passes `value` back as the return of interrupt()
    # inside the confirm_sql node. The graph continues from that point.
    t0 = time.perf_counter()
    steps = await session.dag.ainvoke(Command(resume=approved), config=config)
    dag_ms = round((time.perf_counter() - t0) * 1000, 1)

    session_store.clear_pending(session.session_id)

    question = steps.get("question", "")
    logger.info("HITL resume done: dag=%.0fms", dag_ms, extra={"session_id": session.session_id[:8]})
    return _extract_result(steps, session, question)
