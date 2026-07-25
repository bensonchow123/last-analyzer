import json
from pathlib import Path

from fastapi import HTTPException

from last_llm_service import settings_store
from last_llm_service.env import env

SERVICE = "last_llm_service"
SPEC_PATH = Path(__file__).resolve().parent.parent / "settings_spec.json"
SPEC: list[dict] = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
BY_KEY = {field["key"]: field for field in SPEC}

PREVIEW_TAIL = 4  # trailing characters shown so a key is recognisable, never enough to use

def _mask(value) -> dict:
    """A secret leaves as a hint, the value itself never goes over the wire."""
    if not value:
        return {"set": False, "preview": ""}
    text = str(value)
    tail = text[-PREVIEW_TAIL:] if len(text) > PREVIEW_TAIL else ""
    return {"set": True, "preview": f"...{tail}" if tail else "..."}

def _coerce(field: dict, value) -> str:
    """Validate one value against its spec entry and store it as a string."""
    key = field["key"]
    kind = field.get("type", "string")

    if kind == "bool":
        text = str(value).strip().lower()
        if text not in ("true", "false"):
            raise HTTPException(status_code=422, detail=f"{key} must be true or false.")
        return text

    if kind == "int":
        try:
            number = int(str(value).strip())
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail=f"{key} must be a whole number.")
        low, high = field.get("min"), field.get("max")
        if (low is not None and number < low) or (high is not None and number > high):
            raise HTTPException(status_code=422, detail=f"{key} must be between {low} and {high}.")
        return str(number)

    return str(value).strip()

def _payload() -> dict:
    """The spec, the effective values, and where each one is currently coming from."""
    overrides = settings_store.all_settings()
    values, sources = {}, {}
    for field in SPEC:
        key = field["key"]
        current = getattr(env, key)
        values[key] = _mask(current) if field.get("secret") else current
        sources[key] = "settings" if key in overrides else "env"
    return {"service": SERVICE, "fields": SPEC, "values": values, "sources": sources}

async def get_settings():
    """The editable settings for this service, secrets masked."""
    return _payload()

async def patch_settings(updates: dict):
    """Apply a partial map of settings, null clears one back to the env value."""
    unknown = sorted(set(updates) - set(BY_KEY))
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unknown settings: {', '.join(unknown)}.")

    changes: dict[str, str | None] = {}
    for key, value in updates.items():
        field = BY_KEY[key]

        if value is None:
            changes[key] = None
            continue

        if isinstance(value, str) and not value.strip():
            if field.get("secret"):
                continue  # a blank secret box means leave it alone, not wipe it
            changes[key] = None
            continue

        changes[key] = _coerce(field, value)

    if changes:
        settings_store.save(changes)

    # Nothing to restart, the agent rebuilds its client from env on every request
    return _payload()
