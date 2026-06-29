"""
/connect  — POST: validate DB credentials, initialise DAG, create session
/tables   — GET:  list tables for the active session
/disconnect — DELETE: remove session
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status

from agent.graph import initialize_dag
from api.dependencies import get_session
from api.models.connection import ConnectRequest, ConnectResponse
from api.session_store import Session, session_store
from core.database import DatabaseManager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/connect", response_model=ConnectResponse, status_code=status.HTTP_201_CREATED)
async def connect(req: ConnectRequest) -> ConnectResponse:
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


@router.delete("/disconnect", status_code=status.HTTP_204_NO_CONTENT)
async def disconnect(session: Session = Depends(get_session)) -> None:
    """Remove the session from memory."""
    session_store.delete(session.session_id)
    logger.info("Session deleted: %s", session.session_id[:8])
