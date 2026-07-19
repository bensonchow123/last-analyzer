import json
import logging
from collections.abc import AsyncIterator

from openai import AsyncOpenAI

from last_llm_service.env import env
from last_llm_service.core.tools import OPENAI_TOOLS, dispatch_tool
from last_llm_service.core.prompts import PROMPTS

logger = logging.getLogger(__name__)

async def _run_tool_call(name: str, arguments: str) -> str:
    """Turn one model tool call into a result string, bad input included."""
    try:
        parsed = json.loads(arguments or "{}")
        return await dispatch_tool(name, parsed)
    except (json.JSONDecodeError, TypeError) as e:
        return json.dumps({"error": f"Bad tool arguments: {e}"})

async def run_agent_events(messages: list[dict]) -> AsyncIterator[dict]:
    """Run the tool calling loop, yielding events as they happen.

    Events: {type: delta, text}, {type: tool_call, name, arguments},
    {type: tool_result, name}, and a terminal {type: done}.
    The caller's message list is not mutated, tool call rounds stay internal.
    """
    client = AsyncOpenAI(api_key=env.OPENAI_API_KEY, base_url=env.OPENAI_BASE_URL)
    system_prompt = PROMPTS["chat_system"].replace("{max_tool_rounds}", str(env.MAX_TOOL_ROUNDS))
    messages = [{"role": "system", "content": system_prompt}, *messages]

    for _ in range(env.MAX_TOOL_ROUNDS):
        stream = await client.chat.completions.create(
            model=env.OPENAI_MODEL,
            messages=messages,
            tools=OPENAI_TOOLS,
            stream=True,
        )

        content_parts: list[str] = []
        calls: dict[int, dict] = {}  # tool call fragments accumulate by index
        async for chunk in stream:
            if not chunk.choices:
                continue  # some providers send a trailing usage only chunk
            delta = chunk.choices[0].delta
            if delta.content:
                content_parts.append(delta.content)
                yield {"type": "delta", "text": delta.content}
            for fragment in delta.tool_calls or []:
                call = calls.setdefault(fragment.index, {"id": "", "name": "", "arguments": ""})
                if fragment.id:
                    call["id"] = fragment.id
                if fragment.function:
                    call["name"] += fragment.function.name or ""
                    call["arguments"] += fragment.function.arguments or ""

        if not calls:
            yield {"type": "done"}
            return

        # Rebuild the assistant message by hand, a stream never yields a whole one
        ordered = [calls[i] for i in sorted(calls)]
        messages.append({
            "role": "assistant",
            "content": "".join(content_parts) or None,
            "tool_calls": [
                {"id": c["id"], "type": "function", "function": {"name": c["name"], "arguments": c["arguments"]}}
                for c in ordered
            ],
        })
        for call in ordered:
            logger.info("Tool call: %s(%s)", call["name"], call["arguments"][:200])
            yield {"type": "tool_call", "name": call["name"], "arguments": call["arguments"]}
            result = await _run_tool_call(call["name"], call["arguments"])
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": result})
            yield {"type": "tool_result", "name": call["name"]}

    yield {"type": "delta", "text": PROMPTS["tool_rounds_exhausted"]}
    yield {"type": "done"}

async def run_agent(messages: list[dict]) -> str:
    """Run the loop to completion and return only the final round's text."""
    parts: list[str] = []
    async for event in run_agent_events(messages):
        if event["type"] == "delta":
            parts.append(event["text"])
        elif event["type"] == "tool_call":
            parts.clear()  # text before a tool round is narration, not the reply
    return "".join(parts)
