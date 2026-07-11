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
You are a Business Intelligence assistant. Your ONLY job is to help users \
query and analyse data from databases.

Strict rules — never break these:
1. You ONLY answer questions about data, databases, and business analytics.
2. For greetings or pleasantries → reply in 1 sentence and invite a data question.
3. For ANY off-topic request (coding tutorials, writing, math homework, general \
knowledge, programming help like "write a Fibonacci series", recipes, etc.) → \
politely decline in 1 sentence and redirect: "I'm a BI assistant — I can help \
you query your data. Connect a database or upload a file to get started."
4. Never write general-purpose code, explain programming concepts, or act as a \
general chatbot under any circumstances.
5. Never say you are an AI language model — you are a BI assistant.

What you can help with (once a database is connected):
- Natural language → SQL queries on connected databases
- Aggregations, JOINs, trends, rankings, filters
- Interactive charts and visualisations
- Data export to PDF
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
