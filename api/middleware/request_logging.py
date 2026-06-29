"""
RequestLoggingMiddleware

What it does:
  1. Reads X-Request-ID from the incoming request (if a client/gateway sends one)
     or generates a short UUID4 hex prefix — so every log line for this request
     carries the same ID without any manual threading.
  2. Sets the shared ContextVar (request_id_var) so that every logger called
     anywhere inside this request — routers, services, agent nodes — automatically
     includes the request_id field.
  3. Logs an → IN line (method + path) and a ← OUT line (status + timing in ms).
  4. Echoes X-Request-ID back in the response header — so the React frontend can
     capture it and show it in error messages for support debugging.

Paths in _SKIP_PATHS are not logged (health checks, docs) to avoid log spam.
"""
from __future__ import annotations

import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from core.logging_config import request_id_var

logger = logging.getLogger(__name__)

_SKIP_PATHS = frozenset({"/health", "/docs", "/redoc", "/openapi.json", "/favicon.ico"})


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        # Use caller-supplied ID (proxy / API gateway may set this) or generate one
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:10]

        # Set ContextVar — visible to every coroutine in this request's context
        token = request_id_var.set(request_id)

        path = request.url.path
        skip = path in _SKIP_PATHS

        if not skip:
            client_ip = getattr(request.client, "host", "-")
            logger.info(
                "→ %s %s",
                request.method,
                path,
                extra={"client_ip": client_ip},
            )

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("Unhandled exception on %s %s", request.method, path)
            raise
        finally:
            request_id_var.reset(token)

        duration_ms = round((time.perf_counter() - start) * 1000, 1)

        if not skip:
            level = logging.WARNING if response.status_code >= 400 else logging.INFO
            logger.log(
                level,
                "← %s %s  %d  %.1fms",
                request.method,
                path,
                response.status_code,
                duration_ms,
                extra={"status": response.status_code, "duration_ms": duration_ms},
            )

        response.headers["X-Request-ID"] = request_id
        return response
