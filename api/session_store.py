"""
In-memory session store.

Why in-memory and not a database?
  We discussed this: no caching layer yet.  A simple dict is fast, zero-dep,
  and easy to swap for Redis later — just change this file.

Each session holds:
  - the compiled LangGraph DAG (expensive to build, so build once per connect)
  - the db_uri (for nodes that open their own connections)
  - the db_type (for display / audit)
  - chat_history (server-side, used for PDF export without the client re-sending data)

Session ID is a UUID4 — unguessable, used as a Bearer-style header value.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ChatEntry:
    query: str
    sql_query: Optional[str]
    result_type: str            # "table" | "multi_table" | "chart" | "error"
    dataframe: Optional[List[Dict]] = None          # rows as list-of-dicts
    multi_table_data: Optional[List[Dict]] = None
    chart_html: Optional[str] = None
    error: Optional[str] = None


@dataclass
class Session:
    session_id: str
    db_uri: str
    db_type: str
    dag: Any                               # compiled LangGraph CompiledGraph
    user_id: str = ""                      # Supabase user UUID — enforces data isolation
    instructions: str = ""
    tables: List[str] = field(default_factory=list)
    chat_history: List[ChatEntry] = field(default_factory=list)
    # HITL: set when the graph is suspended at a confirm_sql interrupt()
    pending_thread_id: Optional[str] = None
    pending_interrupt: Optional[dict] = None   # the value passed to interrupt()


class SessionStore:
    """Thread-safe enough for a single-process FastAPI app (GIL protects dict ops)."""

    def __init__(self) -> None:
        self._sessions: Dict[str, Session] = {}

    def create(
        self,
        db_uri: str,
        db_type: str,
        dag: Any,
        tables: List[str],
        user_id: str = "",
        instructions: str = "",
    ) -> str:
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = Session(
            session_id=session_id,
            db_uri=db_uri,
            db_type=db_type,
            dag=dag,
            user_id=user_id,
            instructions=instructions,
            tables=tables,
        )
        return session_id

    def get(self, session_id: str) -> Optional[Session]:
        return self._sessions.get(session_id)

    def delete(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    def append_history(self, session_id: str, entry: ChatEntry) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.chat_history.append(entry)

    def set_pending(self, session_id: str, thread_id: str, interrupt_data: dict) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.pending_thread_id = thread_id
            session.pending_interrupt = interrupt_data

    def clear_pending(self, session_id: str) -> None:
        session = self._sessions.get(session_id)
        if session:
            session.pending_thread_id = None
            session.pending_interrupt = None

    def __len__(self) -> int:
        return len(self._sessions)


# Global singleton — FastAPI imports this directly.
# In a multi-process deployment you'd replace this with a Redis-backed store.
session_store = SessionStore()
