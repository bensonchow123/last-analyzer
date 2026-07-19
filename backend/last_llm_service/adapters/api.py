from typing import Literal

from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from last_llm_service.adapters.openai_agent import run_agent

api = FastAPI()

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # fine on a private network
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str

class ChatRequest(BaseModel):
    messages: list[ChatMessage]

class ChatResponse(BaseModel):
    reply: str

async def chat(request: ChatRequest) -> ChatResponse:
    """Stateless chat, the client resends its whole history each call.

    Cookie stored conversations in the planned notebook UI rely on exactly this.
    Streaming later means turning run_agent into an async generator served over SSE.
    """
    reply = await run_agent([m.model_dump() for m in request.messages])
    return ChatResponse(reply=reply)

# The root path stays free, the planned UI mounts at / after these routes
api.add_api_route("/chat", chat, methods=["POST"])
