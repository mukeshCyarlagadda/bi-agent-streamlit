"""
FastAPI dependency functions.

get_session:
  Resolves X-Session-ID → Session, then checks that the session belongs to the
  requesting user (data isolation: user A cannot query user B's uploaded file or
  connected DB).

  Dev mode (Supabase not configured): user_id is "dev-user" and session.user_id
  is also "dev-user", so the ownership check passes.
"""
from __future__ import annotations

from fastapi import Depends, Header, HTTPException, status

from api.session_store import Session, session_store
from core.auth import get_current_user


async def get_session(
    x_session_id: str = Header(...),
    user: dict = Depends(get_current_user),
) -> Session:
    """
    1. Resolve the session by ID.
    2. Verify it belongs to the authenticated user.

    Returns the Session; raises 401/403 otherwise.
    """
    session = session_store.get(x_session_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Session not found or expired. Please reconnect to the database.",
        )

    # Enforce ownership — reject cross-user access
    if session.user_id and session.user_id != user["id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied: this session belongs to a different user.",
        )

    return session
