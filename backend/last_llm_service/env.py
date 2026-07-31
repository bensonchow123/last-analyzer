import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from last_llm_service import settings_store

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # this is project root
load_dotenv(BASE_DIR / ".env")

class Env:
    """Environment variable manager for the last llm service.

    Two tiers. Values needed before the process can serve a request (ports, the
    vault address, the admin token) are plain attributes read once at startup,
    they only change by editing .env and restarting. Values the settings page can
    edit are properties reading through settings_store, so a save applies to the
    next request with no restart.
    """
    def __init__(self):
        # Scrobble vault address, the http api is the only contract (ADR-2)
        self.SCROBBLE_VAULT_URL = os.getenv('SCROBBLE_VAULT_URL', 'http://localhost:8000')

        # Ports for the mcp and api run modes
        self.LAST_LLM_MCP_PORT = int(os.getenv('LAST_LLM_MCP_PORT', 8001))
        self.LAST_LLM_API_PORT = int(os.getenv('LAST_LLM_API_PORT', 8002))

        # Stamped into the image at build time, 'dev' for a local build
        self.APP_VERSION = os.getenv('APP_VERSION', 'dev')

        # Bearer token for /settings, unset leaves the endpoints disabled.
        # Deliberately not editable through the api, it is what guards the api.
        self.ADMIN_API_TOKEN = os.getenv('ADMIN_API_TOKEN')

    def _get(self, key: str, default=None):
        """Settings page override first, then the env, then the default."""
        stored = settings_store.get(key)
        return stored if stored is not None else os.getenv(key, default)

    def _int(self, key: str, default: int) -> int:
        """A hand mangled override falls back instead of crashing a request."""
        try:
            return int(self._get(key, default))
        except (TypeError, ValueError):
            logger.warning(f"Ignoring non numeric {key}, using {default}")
            return default

    # Editable from the settings page, see settings_spec.json
    @property
    def OPENAI_API_KEY(self) -> str | None:
        # OpenAI compatible endpoint, a local model server accepts a dummy key
        return self._get('OPENAI_API_KEY')

    @property
    def OPENAI_BASE_URL(self) -> str:
        return self._get('OPENAI_BASE_URL', 'https://api.openai.com/v1')

    @property
    def OPENAI_MODEL(self) -> str | None:
        return self._get('OPENAI_MODEL')

    @property
    def MAX_TOOL_ROUNDS(self) -> int:
        return self._int('MAX_TOOL_ROUNDS', 20)  # cap so a confused model cannot loop forever

# Initialize the global instance for all files
env = Env()
