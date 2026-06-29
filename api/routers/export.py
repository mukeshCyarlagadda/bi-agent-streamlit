"""
POST /export  — generate a PDF of the session's chat history.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import Response

from api.dependencies import get_session
from api.session_store import Session
from services.export_service import generate_session_pdf

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/export/pdf")
async def export_pdf(session: Session = Depends(get_session)) -> Response:
    """
    Generate a PDF of all queries and results in the session.
    Returns raw PDF bytes with the appropriate content-type header.

    FastAPI's Response class lets us return arbitrary binary data.
    We set media_type="application/pdf" so browsers know what to do with it.
    """
    if not session.chat_history:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No chat history to export.",
        )

    try:
        pdf_bytes = generate_session_pdf(session.chat_history)
    except Exception as exc:
        logger.error("PDF export failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"PDF generation failed: {exc}",
        )

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=bi_session_summary.pdf"},
    )
