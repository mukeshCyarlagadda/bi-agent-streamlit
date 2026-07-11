"""
Pydantic models for the /connect and /disconnect endpoints.

Pydantic validates every incoming request body against these models.
Bad data → automatic 422 Unprocessable Entity with field-level detail.
Good data → your route function receives a clean, typed Python object.
"""
from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, field_validator


class ConnectRequest(BaseModel):
    db_type: str                    # "sqlite" | "mysql" | "postgres" | ...

    # File-based DBs
    db_path: Optional[str] = None

    # Network DBs
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

    # Snowflake extras
    account: Optional[str] = None
    warehouse: Optional[str] = None
    schema_: Optional[str] = None  # "schema" is a reserved name in Pydantic v1

    # BigQuery extras
    project: Optional[str] = None
    dataset: Optional[str] = None
    credentials_path: Optional[str] = None

    # Oracle extras
    sid: Optional[str] = None

    @field_validator("db_type")
    @classmethod
    def normalise_db_type(cls, v: str) -> str:
        return v.lower().strip()

    def as_params(self) -> dict:
        """Return connection parameters as a plain dict (strips None values)."""
        mapping = {
            "db_path": self.db_path,
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "account": self.account,
            "warehouse": self.warehouse,
            "schema": self.schema_,
            "project": self.project,
            "dataset": self.dataset,
            "credentials_path": self.credentials_path,
            "sid": self.sid,
        }
        return {k: v for k, v in mapping.items() if v is not None}


class ConnectResponse(BaseModel):
    session_id: str
    db_type: str
    tables: List[str]
    message: str
    db_path: Optional[str] = None   # set for file uploads; lets frontend restore sessions
