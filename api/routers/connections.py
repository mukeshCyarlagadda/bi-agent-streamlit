"""
/connect  — POST: validate DB credentials, initialise DAG, create session
/tables   — GET:  list tables for the active session
/disconnect — DELETE: remove session
"""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect, text

from agent.graph import initialize_dag
from api.dependencies import get_session
from api.models.connection import ConnectRequest, ConnectResponse
from api.session_store import Session, session_store
from core.auth import get_current_user
from core.database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/connect", response_model=ConnectResponse, status_code=status.HTTP_201_CREATED)
async def connect(req: ConnectRequest, _user: dict = Depends(get_current_user)) -> ConnectResponse:
    """
    1. Validate the DB credentials by opening a test connection.
    2. Compile the LangGraph DAG (inspects the DB schema once).
    3. Store both in a new Session and return the session_id.

    Why async?  initialize_dag calls SQLDatabase.from_uri which does I/O.
    Wrapping it in asyncio.to_thread would be correct for a high-traffic API.
    For a BI tool with 1–10 concurrent users, calling it directly from an
    async route is acceptable — it blocks the event loop only briefly.
    """
    logger.debug("connect request: db_type=%r params=%r", req.db_type, req.as_params())
    db_manager = DatabaseManager()
    success, message = db_manager.connect(req.db_type, **req.as_params())

    if not success:
        logger.warning("connect failed: %s", message)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)

    db_uri = db_manager.get_uri()
    tables = db_manager.get_tables()
    db_manager.close()      # we don't keep this connection; nodes open their own

    try:
        dag, instructions = initialize_dag(db_uri, db_type=req.db_type, tables=tables)
    except Exception as exc:
        logger.error("DAG initialisation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initialise agent: {exc}",
        )

    session_id = session_store.create(
        db_uri=db_uri,
        db_type=req.db_type,
        dag=dag,
        tables=tables,
        user_id=_user["id"],
        instructions=instructions,
    )
    logger.info("Session created: %s (%s, %d tables)", session_id[:8], req.db_type, len(tables))

    return ConnectResponse(
        session_id=session_id,
        db_type=req.db_type,
        tables=tables,
        message=message,
    )


@router.get("/tables")
async def list_tables(session: Session = Depends(get_session)) -> dict:
    """Return the table list for the active session."""
    return {"tables": session.tables, "db_type": session.db_type}


@router.get("/preview")
async def preview_data(
    table: str = Query(default=""),
    limit: int = Query(default=100, le=500),
    session: Session = Depends(get_session),
) -> dict:
    """
    Return up to *limit* rows from the session's database for preview.
    Works for any session type — file uploads, SQLite, Postgres, etc.
    """
    try:
        engine = create_engine(session.db_uri)
        with engine.connect() as conn:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            if not tables:
                return {"columns": [], "rows": [], "total": 0, "table": ""}
            tbl = table if table in tables else tables[0]
            result = conn.execute(text(f'SELECT * FROM "{tbl}" LIMIT :lim'), {"lim": limit})
            cols = list(result.keys())
            rows = [list(r) for r in result.fetchall()]
            total = conn.execute(text(f'SELECT COUNT(*) FROM "{tbl}"')).scalar() or 0
        engine.dispose()
        return {"columns": cols, "rows": rows, "total": int(total), "table": tbl}
    except Exception as exc:
        logger.error("Preview failed for session %s: %s", session.session_id[:8], exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


@router.delete("/disconnect", status_code=status.HTTP_204_NO_CONTENT)
async def disconnect(session: Session = Depends(get_session)) -> None:
    """Remove the session from memory."""
    session_store.delete(session.session_id)
    logger.info("Session deleted: %s", session.session_id[:8])


class ReconnectFileRequest(BaseModel):
    db_path: str


@router.post("/reconnect-file", response_model=ConnectResponse, status_code=status.HTTP_201_CREATED)
async def reconnect_file(
    req: ReconnectFileRequest,
    _user: dict = Depends(get_current_user),
) -> ConnectResponse:
    """
    Re-create a backend session from a previously uploaded SQLite file path.
    Called when the in-memory session has expired (e.g. after a server restart)
    but the SQLite file still exists on disk.
    """
    p = Path(req.db_path)
    if not p.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="SQLite file not found on disk. Please re-upload the file.",
        )

    db_uri = f"sqlite:///{req.db_path}"
    try:
        engine = create_engine(db_uri)
        with engine.connect() as conn:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
        engine.dispose()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not open SQLite file: {exc}",
        )

    try:
        dag, instructions = initialize_dag(db_uri, db_type="sqlite", tables=tables)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initialise agent: {exc}",
        )

    session_id = session_store.create(
        db_uri=db_uri,
        db_type="file",
        dag=dag,
        tables=tables,
        user_id=_user["id"],
        instructions=instructions,
    )
    logger.info("File reconnected: %s | user=%s", session_id[:8], _user["id"][:8])

    return ConnectResponse(
        session_id=session_id,
        db_type="file",
        tables=tables,
        message=f"Reconnected to {p.name}",
        db_path=req.db_path,
    )
