"""
/chat endpoint — works without a database session.

Handles pre-connection chitchat and capability questions using a direct LLM
call. No LangGraph, no SQL, no session required.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel

from core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a Business Intelligence assistant built to help analysts and teams \
query databases using plain English. You are polished, concise, and friendly.

What you can do:
- Connect to SQLite, PostgreSQL, MySQL, Snowflake, BigQuery, DuckDB, and MSSQL
- Translate natural language questions into SQL automatically
- Run complex queries: multi-table JOINs, aggregations, window functions, subqueries
- Visualise results as interactive charts — bar, line, pie, scatter, bubble, area, histogram
- Retry failed SQL queries automatically and explain what went wrong
- Ask for human approval before running any data-modifying query (DELETE, DROP, UPDATE)
- Export results to PDF

When asked what you can do, be specific and enthusiastic but concise — 3-5 sentences max.
When asked a general question, answer it naturally.
Never say you are an AI language model — you are a BI assistant.
If the user asks a data question before connecting a database, tell them to connect \
one from the sidebar and offer to help once they do. Keep it warm, not robotic.
"""

_llm: ChatOpenAI | None = None

def _get_llm() -> ChatOpenAI:
    global _llm
    if _llm is None:
        _llm = ChatOpenAI(
            model=settings.openai_model,
            api_key=settings.openai_api_key,
            max_tokens=256,
            temperature=0.7,
        )
    return _llm


class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []   # [{"role": "user"|"assistant", "content": "..."}]


class ChatResponse(BaseModel):
    reply: str


@router.post("/chat", response_model=ChatResponse)
async def free_chat(req: ChatRequest) -> ChatResponse:
    logger.debug("free_chat: %.80s", req.message)

    messages = [SystemMessage(content=_SYSTEM_PROMPT)]
    for turn in req.history[-6:]:
        if turn.get("role") == "user":
            messages.append(HumanMessage(content=turn["content"]))
        elif turn.get("role") == "assistant":
            from langchain_core.messages import AIMessage
            messages.append(AIMessage(content=turn["content"]))
    messages.append(HumanMessage(content=req.message))

    response = await _get_llm().ainvoke(messages)
    return ChatResponse(reply=response.content.strip())
