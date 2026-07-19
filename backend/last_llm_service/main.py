import argparse
import asyncio
import logging

from last_llm_service.env import env

# basicConfig logs to stderr, which keeps the mcp stdio transport clean
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

async def chat_repl():
    """Terminal chat with the agent, a dev convenience that skips the http api."""
    from last_llm_service.adapters.openai_agent import run_agent

    messages = []
    while True:
        try:
            user_input = input("you> ").strip()
        except EOFError:
            break
        if user_input in ("exit", "quit"):
            break
        if not user_input:
            continue

        messages.append({"role": "user", "content": user_input})
        reply = await run_agent(messages)
        print(reply)
        messages.append({"role": "assistant", "content": reply})

def main():
    parser = argparse.ArgumentParser(description="LLM access layer for the scrobble vault")
    parser.add_argument("mode", choices=["mcp", "chat", "api"])
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio",
                        help="mcp transport, http is for running as a service (D1)")
    args = parser.parse_args()

    # Imports live per mode so one mode does not pay for another's dependencies
    if args.mode == "mcp":
        from last_llm_service.adapters.mcp_server import mcp
        if args.transport == "http":
            mcp.run(transport="http", host="0.0.0.0", port=env.LAST_LLM_MCP_PORT)
        else:
            mcp.run()
    # In terminal REPL for debugging the API, never started by docker
    elif args.mode == "chat":
        asyncio.run(chat_repl())
    elif args.mode == "api":
        import uvicorn
        from last_llm_service.adapters.api import api
        config = uvicorn.Config(app=api, host="0.0.0.0", port=env.LAST_LLM_API_PORT, log_level="info")
        asyncio.run(uvicorn.Server(config).serve())

if __name__ == "__main__":
    main()
