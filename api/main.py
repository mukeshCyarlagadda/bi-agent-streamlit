"""
FastAPI application entry point.

This file has three jobs and only three:
  1. Create the app instance.
  2. Attach middleware (CORS, etc.).
  3. Register routers.

Business logic lives in services/, agent/, core/ — never here.

----
CORS explained
----
Your browser enforces the Same-Origin Policy:
  A page from http://localhost:8501 (Streamlit) cannot call
  http://localhost:8000 (this API) unless the server explicitly permits it.

CORSMiddleware adds the right Access-Control-* headers to every response.
In development we allow Streamlit's origin.
In production you replace allowed_origins with your actual domain(s).

allow_credentials=True is needed when the client sends cookies or custom
headers (we send X-Session-ID).
allow_methods=["*"] permits GET, POST, DELETE, OPTIONS, etc.
allow_headers=["*"] permits X-Session-ID and any other custom header.

----
Lifespan explained
----
The @asynccontextmanager lifespan function replaces the old @app.on_event().
Code before `yield` runs once at startup.
Code after `yield` runs once at shutdown.
Use it to initialise/teardown shared resources (connection pools, ML models).
We don't have shared resources to init, but the pattern is here for when you do.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from dotenv import load_dotenv
load_dotenv()  # sets os.environ BEFORE any LangChain/OpenAI import reads it

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.middleware.request_logging import RequestLoggingMiddleware
from api.routers import chat, connections, export, query, upload
from core.config import settings
from core.logging_config import setup_logging

setup_logging(level=settings.log_level, fmt=settings.log_format)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # ---------- startup ----------
    logger.info("BI Agent API starting (model=%s)", settings.openai_model)
    from core.storage import ensure_bucket
    ensure_bucket()
    yield
    # ---------- shutdown ----------
    logger.info("BI Agent API shutting down")


app = FastAPI(
    title="Business Intelligence Agent API",
    description=(
        "LangGraph-powered natural-language SQL agent. "
        "Connect any SQL database, ask questions in plain English."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",       # Swagger UI — visit http://localhost:8000/docs
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# Middleware — LIFO order: last added = first to run on the way IN
# We add CORS first so it runs second (after request logging has set request_id).
# RequestLoggingMiddleware runs outermost — it sees the true request before CORS
# touches it and captures the real status code after CORS adds its headers.
# ---------------------------------------------------------------------------
_cors_origins = list(settings.allowed_origins)
if settings.extra_allowed_origins:
    _cors_origins += [o.strip() for o in settings.extra_allowed_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestLoggingMiddleware)

# ---------------------------------------------------------------------------
# Routers — group related endpoints under a common prefix
# ---------------------------------------------------------------------------
app.include_router(chat.router,        prefix="/api/v1", tags=["chat"])
app.include_router(connections.router, prefix="/api/v1", tags=["connections"])
app.include_router(query.router,       prefix="/api/v1", tags=["query"])
app.include_router(export.router,      prefix="/api/v1", tags=["export"])
app.include_router(upload.router,      prefix="/api/v1", tags=["upload"])


# ---------------------------------------------------------------------------
# Health check — used by load balancers and Docker health checks
# ---------------------------------------------------------------------------
@app.get("/health", tags=["health"])
async def health() -> dict:
    return {"status": "ok", "model": settings.openai_model}


# ---------------------------------------------------------------------------
# Run directly with:  python -m api.main
# Or (recommended):   uvicorn api.main:app --reload --port 8000
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )
