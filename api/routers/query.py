"""
POST /query        — run the BI agent DAG
POST /query/approve — resume a graph that is waiting at a HITL interrupt
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status

from api.dependencies import get_optional_session, get_session
from api.models.query import ApproveRequest, QueryRequest, QueryResponse
from api.session_store import Session
from core.auth import get_current_user
from services.query_service import resume_query, run_general_query, run_query

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def query(
    req: QueryRequest,
    session: Optional[Session] = Depends(get_optional_session),
    _user: dict = Depends(get_current_user),
) -> QueryResponse:
    if not req.question.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Question cannot be empty.",
        )

    if session is None:
        return await run_general_query(req.question)

    if session.pending_thread_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A query is already waiting for approval. POST /api/v1/query/approve first.",
        )
    try:
        return await run_query(question=req.question, session=session)
    except Exception as exc:
        logger.error("Query failed for session %s: %s", session.session_id[:8], exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent error: {exc}",
        )


@router.post("/query/approve", response_model=QueryResponse)
async def approve(req: ApproveRequest, session: Session = Depends(get_session), _user: dict = Depends(get_current_user)) -> QueryResponse:
    """
    Resume a graph suspended at a confirm_sql interrupt().

    The client sends approved=True (run the SQL) or approved=False (cancel).
    The graph resumes from the exact point it was paused — no state is re-derived.
    """
    if not session.pending_thread_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="No query is waiting for approval in this session.",
        )
    try:
        return await resume_query(session=session, approved=req.approved)
    except Exception as exc:
        logger.error("Resume failed for session %s: %s", session.session_id[:8], exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Resume error: {exc}",
        )
