import json
import logging
from typing import Literal

from pydantic import BaseModel
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from last_llm_service.adapters.auth import require_admin
from last_llm_service.adapters.openai_agent import run_agent, run_agent_events
from last_llm_service.adapters.settings_routes import get_settings, patch_settings

logger = logging.getLogger(__name__)

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

    Non streaming, kept for simple consumers. The chat UI uses /chat/stream.
    """
    reply = await run_agent([m.model_dump() for m in request.messages])
    return ChatResponse(reply=reply)

async def chat_stream(request: ChatRequest) -> StreamingResponse:
    """Same stateless chat, streamed as SSE. One json event per data: frame."""
    messages = [m.model_dump() for m in request.messages]

    async def frames():
        try:
            async for event in run_agent_events(messages):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            # The 200 header is already sent, an error frame is all we can do
            logger.exception("chat stream failed")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        frames(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

api.add_api_route("/chat", chat, methods=["POST"])
api.add_api_route("/chat/stream", chat_stream, methods=["POST"])

# Settings, the only authenticated surface here. The mcp container runs the same
# image without this route, it picks edits up from the shared overlay file.
admin_only = [Depends(require_admin)]
api.add_api_route("/settings", get_settings, methods=["GET"], dependencies=admin_only)
api.add_api_route("/settings", patch_settings, methods=["PATCH"], dependencies=admin_only)
