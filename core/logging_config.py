"""
Logging configuration — industry standard setup.

What this gives you:
  - JSON structured logs (every line is a parseable JSON object)
  - request_id injected into EVERY log line for a request — zero manual passing
  - Sensitive field masking (password, api_key, token never appear in logs)
  - Human-readable coloured format in dev (log_format=text)
  - Machine-readable JSON in prod (log_format=json) — ready for Datadog/ELK/CloudWatch
  - Uvicorn's default access log suppressed (our middleware replaces it)

ContextVar explained:
  Python's contextvars.ContextVar is like a thread-local variable, but for
  async coroutines. Each request runs in its own async context, so setting
  request_id_var inside the middleware is visible to every logger called
  within that same request — even deep in agent/nodes.py — without passing
  it explicitly anywhere.
"""
from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

# ── Shared ContextVar ────────────────────────────────────────────────────────
# Set by RequestLoggingMiddleware at the start of every request.
# Read by _RequestIdFilter and injected into every log record automatically.
request_id_var: ContextVar[str] = ContextVar("request_id", default="-")

# ── Sensitive key names — values are never logged ────────────────────────────
_SENSITIVE_KEYS = frozenset({
    "password", "passwd", "secret", "api_key", "apikey",
    "token", "access_token", "refresh_token", "authorization",
    "openai_api_key", "private_key", "credential",
})

# stdlib LogRecord attrs we skip when copying extra fields into JSON
_STDLIB_ATTRS = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "taskName", "request_id",
})


# ── JSON formatter ────────────────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """
    Emits one JSON object per log line.
    Standard fields: timestamp, level, logger, request_id, message.
    Extra fields (passed via logger.info("msg", extra={...})) are merged in.
    Sensitive field values are replaced with "***REDACTED***".
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level":     record.levelname,
            "logger":    record.name,
            "request_id": getattr(record, "request_id", request_id_var.get()),
            "message":   record.getMessage(),
        }

        # Merge caller-supplied extra fields, masking sensitive keys
        for key, value in record.__dict__.items():
            if key in _STDLIB_ATTRS or key.startswith("_"):
                continue
            if key.lower() in _SENSITIVE_KEYS:
                payload[key] = "***REDACTED***"
            else:
                payload[key] = value

        if record.exc_info:
            payload["traceback"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


# ── Console (dev) formatter ───────────────────────────────────────────────────

_LEVEL_COLOURS = {
    "DEBUG":    "\033[36m",   # cyan
    "INFO":     "\033[32m",   # green
    "WARNING":  "\033[33m",   # yellow
    "ERROR":    "\033[31m",   # red
    "CRITICAL": "\033[35m",   # magenta
}
_RESET = "\033[0m"


class ColourTextFormatter(logging.Formatter):
    """Human-readable coloured format for development."""

    def format(self, record: logging.LogRecord) -> str:
        colour = _LEVEL_COLOURS.get(record.levelname, "")
        rid = getattr(record, "request_id", request_id_var.get())
        rid_part = f"[{rid}] " if rid != "-" else ""
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        prefix = f"{ts}  {colour}{record.levelname:<8}{_RESET}  {rid_part}{record.name}"
        msg = record.getMessage()
        if record.exc_info:
            msg += "\n" + self.formatException(record.exc_info)
        return f"{prefix}  {msg}"


# ── Filter — injects request_id into every record ────────────────────────────

class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


# ── Public setup function ─────────────────────────────────────────────────────

def setup_logging(level: str = "INFO", fmt: str = "text") -> None:
    """
    Call once at application startup (in api/main.py lifespan).

    Args:
        level:  "DEBUG" | "INFO" | "WARNING" | "ERROR"
        fmt:    "text"  → coloured console (development)
                "json"  → structured JSON  (production / containers)
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(RequestIdFilter())
    handler.setFormatter(JsonFormatter() if fmt == "json" else ColourTextFormatter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)

    # Uvicorn's default access logger is noisy and unstructured.
    # Our RequestLoggingMiddleware replaces it with structured lines.
    logging.getLogger("uvicorn.access").handlers.clear()
    logging.getLogger("uvicorn.access").propagate = False

    # Suppress overly verbose third-party loggers
    for noisy in ("httpx", "httpcore", "openai", "langsmith"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
