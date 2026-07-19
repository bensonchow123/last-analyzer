import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # this is project root
load_dotenv(BASE_DIR / ".env")

class Env:
    """Environment variable manager for the last llm service."""
    def __init__(self):
        # Scrobble vault address, the http api is the only contract (ADR-2)
        self.SCROBBLE_VAULT_URL = os.getenv('SCROBBLE_VAULT_URL', 'http://localhost:8000')

        # OpenAI compatible endpoint, a local model server accepts a dummy key
        self.OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        self.OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
        self.OPENAI_MODEL = os.getenv('OPENAI_MODEL')

        # Ports for the mcp and api run modes
        self.LAST_LLM_MCP_PORT = int(os.getenv('LAST_LLM_MCP_PORT', 8001))
        self.LAST_LLM_API_PORT = int(os.getenv('LAST_LLM_API_PORT', 8002))

        # Agent settings
        self.MAX_TOOL_ROUNDS = int(os.getenv('MAX_TOOL_ROUNDS', 10))  # cap so a confused model cannot loop forever

# Initialize the global instance for all files
env = Env()
