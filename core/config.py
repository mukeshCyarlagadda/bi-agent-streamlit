from __future__ import annotations

from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Single source of truth for every config value.
    Reads from environment variables and .env file automatically.
    pydantic-settings validates types at startup — bad config fails fast.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    # SQL
    sql_result_limit: int = 500          # max rows returned per query

    # CORS — list the origins that are allowed to call the API.
    # In dev: Streamlit's port.  In prod: your actual domain.
    allowed_origins: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8501",
        "http://127.0.0.1:8501",
    ]

    # Chart output directory (server-side PNGs, cleaned after encoding)
    charts_dir: str = "generated_charts"

    # Logging
    log_level: str = "INFO"
    # "text" → coloured console (dev)   "json" → structured JSON (prod/containers)
    log_format: str = "text"

    # Human-in-the-loop
    # DDL statements (DELETE/DROP/UPDATE/INSERT) are ALWAYS intercepted.
    # Set hitl_sql_preview=true to also intercept every SELECT for approval.
    hitl_sql_preview: bool = False

    # API — where FastAPI listens and what URL the UI uses to reach it
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_base_url: str = "http://localhost:8000"   # used by Streamlit client


settings = Settings()
