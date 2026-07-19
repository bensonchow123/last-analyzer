from fastmcp import FastMCP

from last_llm_service.core import tools

mcp = FastMCP("last-analysis")

# Register the shared tool core, schemas and descriptions come from the signatures and docstrings
mcp.tool(tools.describe_schema)
mcp.tool(tools.query_music_db)
mcp.tool(tools.get_music_summary)
