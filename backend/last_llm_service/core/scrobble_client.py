import json

import httpx

from last_llm_service.env import env

TIMEOUT = 30  # seconds, llm authored queries can be slow

async def _request(method: str, path: str, body: dict | None = None) -> dict:
    """Call the vault, turning every failure into an {"error": reason} a model can read."""
    url = f"{env.SCROBBLE_VAULT_URL}{path}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.request(method, url, json=body)
    except httpx.HTTPError as e:
        return {"error": f"Could not reach the scrobble vault at {url}: {e}"}

    if resp.status_code == 422:
        # The sql validator's detail is a string, fastapi's own body validation is a list
        detail = resp.json().get("detail")
        return {"error": detail if isinstance(detail, str) else json.dumps(detail)}
    if resp.is_error:
        return {"error": f"Vault returned {resp.status_code}: {resp.text}"}
    return resp.json()

async def run_sql(sql: str) -> dict:
    """Execute a read only SELECT through the vault's validator."""
    return await _request("POST", "/vibed-sql-runner", {"sql": sql})

async def get_music_summary() -> dict:
    """Fetch the vault's curated per period listening stats."""
    return await _request("GET", "/music-summary")
