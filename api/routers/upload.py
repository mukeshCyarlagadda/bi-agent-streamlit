"""
POST /api/v1/upload

Accepts CSV, Excel (.xlsx/.xls), digital PDF, or image (JPG/PNG/WEBP).
Parses into a user-scoped SQLite temp DB, initialises the LangGraph DAG,
and returns a ConnectResponse — identical contract to /connect.

Data isolation: the session is tagged with the authenticated user's ID.
api/dependencies.get_session() verifies that tag on every subsequent call,
so user A cannot access user B's uploaded data.
"""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from agent.graph import initialize_dag
from api.models.connection import ConnectResponse
from api.session_store import session_store
from core.auth import get_current_user
from core.file_parser import ALL_SUFFIXES, file_to_sqlite
from core.storage import upload_sqlite

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    '/upload',
    response_model=ConnectResponse,
    status_code=status.HTTP_201_CREATED,
    summary='Upload a file (CSV / Excel / PDF / image) and open it as a queryable session',
)
async def upload_file(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
) -> ConnectResponse:
    filename = file.filename or 'upload'
    suffix = Path(filename).suffix.lower()

    if suffix not in ALL_SUFFIXES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Unsupported file type '{suffix}'. "
                f"Accepted: {', '.join(sorted(ALL_SUFFIXES))}"
            ),
        )

    content = await file.read()
    user_id = user['id']

    try:
        db_path, tables, message = file_to_sqlite(content, filename, user_id=user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
    except ImportError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))
    except Exception as exc:
        logger.error('Parsing failed for %r (user=%s): %s', filename, user_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to parse file: {exc}',
        )

    db_uri = f'sqlite:///{db_path}'
    db_type = 'file'   # shown in header as "file · connected"

    try:
        dag, instructions = initialize_dag(db_uri, db_type='sqlite', tables=tables)
    except Exception as exc:
        logger.error('DAG init failed for upload %r: %s', filename, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to initialise agent: {exc}',
        )

    session_id = session_store.create(
        db_uri=db_uri,
        db_type=db_type,
        dag=dag,
        tables=tables,
        user_id=user_id,
        instructions=instructions,
    )
    logger.info('Upload session: %s | user=%s | %s', session_id[:8], user_id[:8], message)

    # Back up SQLite to Supabase Storage so it survives server restarts.
    # storage_key is the relative path used to restore it later (e.g. "abc/def.db").
    storage_key = upload_sqlite(db_path)

    return ConnectResponse(
        session_id=session_id,
        db_type=db_type,
        tables=tables,
        message=message,
        db_path=storage_key,
    )
