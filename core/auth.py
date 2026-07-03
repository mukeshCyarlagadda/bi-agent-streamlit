"""
JWT verification for every protected API endpoint.

How it works:
  Every request from the React app carries an Authorization: Bearer <token> header.
  The token is a JWT issued by Supabase when the user signs in.
  We call supabase.auth.get_user(token) which validates the signature + expiry
  server-side and returns the user object.

Dev mode:
  If SUPABASE_URL / SUPABASE_SECRET_KEY are not set in .env, auth is skipped
  and a placeholder dev-user is returned. This keeps local dev working without
  needing a Supabase project.
"""
from __future__ import annotations

import logging

from fastapi import Header, HTTPException, status

from core.config import settings

logger = logging.getLogger(__name__)

_supabase_client = None


def _get_client():
    global _supabase_client
    if _supabase_client is None and settings.supabase_url and settings.supabase_secret_key:
        from supabase import create_client
        _supabase_client = create_client(settings.supabase_url, settings.supabase_secret_key)
    return _supabase_client


async def get_current_user(authorization: str = Header(default=None)) -> dict:
    """
    FastAPI dependency — inject into any route that requires authentication.

    Usage:
        @router.post("/connect")
        async def connect(req: ConnectRequest, user: dict = Depends(get_current_user)):
            user["id"]    # Supabase user UUID
            user["email"] # user's email
    """
    client = _get_client()

    # Dev mode: Supabase not configured → skip auth entirely
    if client is None:
        logger.debug("Auth skipped — Supabase not configured (dev mode)")
        return {"id": "dev-user", "email": "dev@local"}

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated. Please sign in.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization[7:]  # strip "Bearer "
    try:
        response = client.auth.get_user(token)
        return {"id": response.user.id, "email": response.user.email}
    except Exception as exc:
        logger.warning("JWT verification failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired. Please sign in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )
