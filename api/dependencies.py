"""
FastAPI dependency functions.

What is Depends()?
  Instead of every route function repeating "look up session → raise 401 if missing",
  you write that logic once here and declare it as a dependency.
  FastAPI calls it automatically before the route runs, and injects the result.

  If the dependency raises an HTTPException the route never runs —
  FastAPI short-circuits and returns the error response.

  If the dependency yields (see get_db_conn below), FastAPI runs the code after
  `yield` as cleanup *after* the response is sent — like a finally block.
"""
from __future__ import annotations

from fastapi import Header, HTTPException, status

from api.session_store import Session, session_store


async def get_session(x_session_id: str = Header(...)) -> Session:
    """
    Reads the X-Session-ID header and resolves it to a Session object.

    - Header(...) means the header is REQUIRED — FastAPI returns 422 if missing.
    - We return 401 (Unauthorized) if the ID doesn't map to an active session.

    Usage in a route:
        @router.post("/query")
        async def query(req: QueryRequest, session: Session = Depends(get_session)):
            ...
    """
    session = session_store.get(x_session_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session. Please reconnect to the database.",
        )
    return session
