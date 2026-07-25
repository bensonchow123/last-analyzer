import json
import logging
import os
import tempfile
import time
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # this is project root

# env.py imports this module before it calls load_dotenv, so load it here too or
# a SETTINGS_FILE set in .env would be read too late. Without override=True a
# real env var still wins, which is how docker points this at its volume.
load_dotenv(BASE_DIR / ".env")

SETTINGS_FILE = Path(os.getenv('SETTINGS_FILE', BASE_DIR / "settings" / "last_llm_service.json"))

# Overrides set from the settings page, layered on top of the env (see env.py).
# The file is the last word, .env only seeds keys that were never edited.

_TTL_SECONDS = 1.0  # how stale a cached read may get before we stat the file again

_cache: dict[str, str] = {}
_cache_mtime: float | None = None
_cache_checked: float = float('-inf')

def _read() -> dict[str, str]:
    """The overlay, reparsed only when the file changed and at most once a second.

    The stat is what lets the mcp and api containers share one file: only the api
    serves /settings, but both run this image, so the mcp process picks up an edit
    within a second instead of needing its own endpoint or a restart.
    """
    global _cache, _cache_mtime, _cache_checked

    now = time.monotonic()
    if now - _cache_checked < _TTL_SECONDS:
        return _cache
    _cache_checked = now

    try:
        mtime = SETTINGS_FILE.stat().st_mtime
    except OSError:
        # no file yet is the normal case on a fresh machine, not an error
        _cache, _cache_mtime = {}, None
        return _cache

    if mtime == _cache_mtime:
        return _cache

    try:
        loaded = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        # a broken overlay must never take the service down, fall back to the env
        logger.warning(f"Ignoring unreadable settings file {SETTINGS_FILE}: {e}")
        loaded = {}

    _cache = {str(k): str(v) for k, v in loaded.items()} if isinstance(loaded, dict) else {}
    _cache_mtime = mtime
    return _cache

def all_settings() -> dict[str, str]:
    """Every override currently stored, as a copy callers can mutate."""
    return dict(_read())

def get(key: str) -> str | None:
    """The stored override for one key, None when it was never set."""
    return _read().get(key)

def save(updates: dict[str, str | None]) -> None:
    """Merge updates into the overlay, a None value drops back to the env value."""
    global _cache, _cache_mtime, _cache_checked

    merged = all_settings()
    for key, value in updates.items():
        if value is None:
            merged.pop(key, None)
        else:
            merged[key] = str(value)

    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    # mkstemp gives 0600 and sits in the target dir, so os.replace is an atomic
    # swap on the same filesystem, a reader never sees a half written file
    fd, tmp_path = tempfile.mkstemp(dir=SETTINGS_FILE.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, sort_keys=True)
        os.replace(tmp_path, SETTINGS_FILE)
    except BaseException:
        Path(tmp_path).unlink(missing_ok=True)
        raise

    _cache = merged
    _cache_mtime = SETTINGS_FILE.stat().st_mtime
    _cache_checked = time.monotonic()
