"""
Database connection manager — framework-agnostic.
No FastAPI, no Streamlit imports here.
"""
from __future__ import annotations

import os
from typing import Optional, Tuple, Any

import sqlalchemy
from sqlalchemy import create_engine, inspect, text


class DatabaseManager:
    """
    Handles URI construction and connection lifecycle for all supported DBs.
    One instance per user session (stored in SessionStore).
    """

    def __init__(self) -> None:
        self.engine: Optional[sqlalchemy.engine.Engine] = None
        self.conn: Optional[sqlalchemy.engine.Connection] = None
        self.db_type: Optional[str] = None
        self.db_uri: Optional[str] = None
        self._inspector: Optional[sqlalchemy.engine.Inspector] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def connect(self, db_type: str, **params) -> Tuple[bool, str]:
        """
        Build URI, create engine, test connection.
        Returns (success, human-readable message).
        """
        self.close()

        uri = self._build_uri(db_type.lower(), **params)
        if not uri:
            dt = db_type.lower()
            if dt in ("sqlite", "duckdb"):
                return False, f"db_path is required for {db_type}"
            return False, f"Unsupported database type: {db_type}"

        try:
            self.engine = create_engine(uri)
            self.conn = self.engine.connect()
            self.conn.execute(text("SELECT 1"))
            self.db_type = db_type.lower()
            self.db_uri = uri
            self._inspector = inspect(self.engine)
            return True, f"Connected to {db_type}"
        except Exception as exc:
            self.close()
            return False, f"Connection failed: {exc}"

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def get_tables(self) -> list:
        return self._inspector.get_table_names() if self._inspector else []

    def get_uri(self) -> Optional[str]:
        return self.db_uri

    # ------------------------------------------------------------------
    # URI builder — one branch per DB type
    # ------------------------------------------------------------------

    def _build_uri(self, db_type: str, **p) -> Optional[str]:
        if db_type == "sqlite":
            path = p.get("db_path")
            if not path:
                return None   # force a 400: caller must supply db_path
            return f"sqlite:///{path}"

        if db_type == "mysql":
            return (
                f"mysql+pymysql://{p['user']}:{p['password']}"
                f"@{p.get('host') or 'localhost'}:{p.get('port') or 3306}/{p['database']}"
            )

        if db_type in ("postgres", "postgresql"):
            return (
                f"postgresql://{p['user']}:{p['password']}"
                f"@{p.get('host') or 'localhost'}:{p.get('port') or 5432}/{p['database']}"
            )

        if db_type == "mssql":
            return (
                f"mssql+pyodbc://{p['user']}:{p['password']}"
                f"@{p.get('host') or 'localhost'}:{p.get('port') or 1433}/{p['database']}"
                "?driver=ODBC+Driver+17+for+SQL+Server"
            )

        if db_type == "oracle":
            return (
                f"oracle+cx_oracle://{p['user']}:{p['password']}"
                f"@{p.get('host') or 'localhost'}:{p.get('port') or 1521}/{p.get('sid','')}"
            )

        if db_type == "snowflake":
            return (
                f"snowflake://{p['user']}:{p['password']}@{p['account']}"
                f"/{p['database']}/{p.get('schema','PUBLIC')}?warehouse={p['warehouse']}"
            )

        if db_type == "bigquery":
            if creds := p.get("credentials_path"):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds
            return f"bigquery://{p['project']}/{p['dataset']}"

        if db_type == "duckdb":
            path = p.get("db_path")
            if not path:
                return None
            return f"duckdb:///{path}"

        return None
