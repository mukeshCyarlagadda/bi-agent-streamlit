"""
Pure utility functions — no framework imports, no side effects.
Safe to import from any layer.
"""
from __future__ import annotations

import re
from typing import List


# ---------------------------------------------------------------------------
# SQL extraction
# ---------------------------------------------------------------------------

def extract_sql_code(response: object) -> str:
    """
    Pull the SQL statement out of an LLM response string.
    Preserves the original casing of identifiers (case-sensitive DBs care).
    """
    text: str = response.content if hasattr(response, "content") else str(response)
    text = text.replace("SQLQuery:", "").strip()

    # 1. ```sql ... ``` block
    m = re.search(r"```sql\s*([\s\S]+?)\s*```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # 2. Any ``` block whose first token is a SQL keyword
    m = re.search(r"```\s*([\s\S]+?)\s*```", text)
    if m:
        candidate = m.group(1).strip()
        if _starts_with_sql_keyword(candidate):
            return candidate

    # 3. Inline backtick token
    for part in text.split("`"):
        if _starts_with_sql_keyword(part.strip()):
            return part.strip()

    # 4. Line-by-line scan
    for line in text.splitlines():
        if _starts_with_sql_keyword(line.strip()):
            return line.strip()

    return text.strip()


def _starts_with_sql_keyword(s: str) -> bool:
    SQL_KEYWORDS = ("SELECT", "WITH", "INSERT", "UPDATE", "DELETE")
    return any(s.upper().startswith(kw) for kw in SQL_KEYWORDS)


# ---------------------------------------------------------------------------
# Python code extraction (for chart scripts)
# ---------------------------------------------------------------------------

def extract_python_code(response: object) -> str:
    """
    Extract Python code from an LLM response.
    Looks for ```python ... ``` blocks first, then falls back to raw text.
    """
    text: str = response.content if hasattr(response, "content") else str(response)
    text = text.replace("Python Script:", "").strip()

    m = re.search(r"```python\s*([\s\S]+?)\s*```", text, re.DOTALL)
    if m:
        return m.group(1).strip()

    # Heuristic: collect lines that look like Python
    python_keywords = ("def ", "import ", "plt.", "sns.", "px.", "fig.", "ax.")
    lines = [l for l in text.splitlines() if any(kw in l for kw in python_keywords)]
    if lines:
        return "\n".join(lines).strip()

    return text.strip()


# ---------------------------------------------------------------------------
# Multi-statement SQL helpers
# ---------------------------------------------------------------------------

def split_sql_statements(sql_text: str) -> List[str]:
    """Split a block of SQL into individual statements (split on ';')."""
    if not sql_text:
        return []
    lines = [l for l in sql_text.splitlines() if not l.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def is_multi_statement_sql(sql_query: str) -> bool:
    return len(split_sql_statements(sql_query)) > 1


def extract_table_name_from_sql(statement: str) -> str:
    """Best-effort table name extraction from a single SQL statement."""
    m = re.search(r"FROM\s+(\w+)", statement, re.IGNORECASE)
    if m:
        return m.group(1)
    return "Unknown Table"
