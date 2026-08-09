import logging
import os
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

from scrobble_vault import settings_store

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # this is project root
load_dotenv(BASE_DIR / ".env")

class Env:
    """Environment variable manager for scrobble vault.

    Two tiers. Values needed before the process can serve a request (ports, db
    credentials, the admin token) are plain attributes read once at startup, they
    only change by editing .env and restarting. Values the settings page can edit
    are properties reading through settings_store, so a save applies to the next
    request with no restart.
    """
    def __init__(self):
        # Last.fm username and api key are editable, see the properties below.
        # There is no password or api secret here: those are only needed for
        # last.fm's authenticated write methods, and this vault only ever reads.

        # Sync settings
        self.SCROBBLE_VAULT_PORT = int(os.getenv('SCROBBLE_VAULT_PORT', 8000))

        # Stamped into the image at build time, 'dev' for a local build
        self.APP_VERSION = os.getenv('APP_VERSION', 'dev')

        # Bearer token for /settings, unset leaves the endpoints disabled.
        # Deliberately not editable through the api, it is what guards the api.
        self.ADMIN_API_TOKEN = os.getenv('ADMIN_API_TOKEN')

        # Set false on a machine that cannot spare the ram for the model. Sync
        # keeps working, rows just get a null vector and semantic search 404s.
        # Not on the settings page, loading the model is a restart level choice.
        self.EMBEDDINGS_ENABLED = os.getenv('EMBEDDINGS_ENABLED', 'true').lower() != 'false'

        # PostgreSQL connection (admin)
        self.POSTGRES_USER = os.getenv('POSTGRES_SUPER_USER', 'admin')
        self.POSTGRES_PASSWORD = os.getenv('POSTGRES_SUPER_USER_PASSWORD')
        self.POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
        self.POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
        self.POSTGRES_DB = os.getenv('POSTGRES_DB', 'scrobble_vault')
        self.POSTGRES_MIN_POOL_SIZE = int(os.getenv('POSTGRES_MIN_POOL_SIZE', 1))
        self.POSTGRES_MAX_POOL_SIZE = int(os.getenv('POSTGRES_MAX_POOL_SIZE', 5))

        # PostgreSQL connection (read only)
        self.RO_POSTGRES_USER = os.getenv('POSTGRES_READ_ONLY_USER', 'readonly')
        self.RO_POSTGRES_PASSWORD = os.getenv('POSTGRES_READ_ONLY_USER_PASSWORD')

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
    def LAST_FM_USERNAME(self) -> str | None:
        return self._get('LAST_FM_USERNAME')

    @property
    def LAST_FM_API_KEY(self) -> str | None:
        return self._get('LAST_FM_API_KEY')

    @property
    def SYNC_INTERVAL_MINUTES(self) -> int:
        return self._int('SYNC_INTERVAL_MINUTES', 15)

    @property
    def RATE_LIMIT_MS(self) -> int:
        return self._int('RATE_LIMIT_MS', 200)

    @property
    def LLM_ENDPOINTS_ENABLED(self) -> bool:
        # On by default: a fresh install has no .env, and off would mean the mcp and
        # chat paths 404 out of the box. Turn it off on the settings page when this
        # vault runs alone.
        return str(self._get('LLM_ENDPOINTS_ENABLED', 'true')).lower() == 'true'

    @property
    def DATABASE_URL(self) -> str:
        # URL encode the password to handle special characters
        encoded_password = quote(self.POSTGRES_PASSWORD or '')
        return f"postgresql://{self.POSTGRES_USER}:{encoded_password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def RO_DATABASE_URL(self) -> str:
        # DB URL for the read only user
        encoded_password = quote(self.RO_POSTGRES_PASSWORD or '')
        return f"postgresql://{self.RO_POSTGRES_USER}:{encoded_password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

# Initilize the global instance for all files
env = Env()
