from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from scrobble_vault.env import env
from scrobble_vault.api.auth import require_admin
from scrobble_vault.api.endpoints.vibed_sql_runner import vibed_sql_runner
from scrobble_vault.api.endpoints.music_summary import music_summary
from scrobble_vault.api.endpoints.semantic_search import semantic_search
from scrobble_vault.api.endpoints.settings import get_settings, patch_settings

api = FastAPI()

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allows requests from any origin
    allow_credentials=True,
    allow_methods=["*"],  # Only GET, POST will be used
    allow_headers=["*"],  # allow all headers
)

def require_llm_endpoints():
    """404 when the toggle is off, same as the route not existing."""
    if not env.LLM_ENDPOINTS_ENABLED:
        raise HTTPException(status_code=404, detail="Not Found")

api.add_api_route("/music-summary", music_summary, methods=["GET"])

# These two only exist for the last LLM service, so they 404 unless one uses this
# vault. Registered either way and gated per request, so the settings page can
# flip LLM_ENDPOINTS_ENABLED without a restart.
llm_only = [Depends(require_llm_endpoints)]
api.add_api_route("/vibed-sql-runner", vibed_sql_runner, methods=["POST"], dependencies=llm_only)

def require_embeddings():
    """Semantic search needs the model, so it 404s when embeddings are off."""
    if not env.EMBEDDINGS_ENABLED:
        raise HTTPException(status_code=404, detail="Not Found")

api.add_api_route(
    "/semantic-search",
    semantic_search,
    methods=["POST"],
    dependencies=[*llm_only, Depends(require_embeddings)],
)

# Settings, the only authenticated surface on this service
admin_only = [Depends(require_admin)]
api.add_api_route("/settings", get_settings, methods=["GET"], dependencies=admin_only)
api.add_api_route("/settings", patch_settings, methods=["PATCH"], dependencies=admin_only)
