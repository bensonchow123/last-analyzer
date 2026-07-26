import asyncio
import logging
import time

from scrobble_vault.env import env
from scrobble_vault.services.sync_scrobbles import init_schema, sync_scrobble_vault

logger = logging.getLogger(__name__)

_TICK_SECONDS = 10  # how often the interval is re-checked, not how often we sync

def _configured() -> bool:
    """Both are needed before last.fm will answer anything."""
    return bool(env.LAST_FM_USERNAME and env.LAST_FM_API_KEY)

async def _sync_once() -> None:
    """One sync, failures logged and swallowed so the loop outlives them."""
    try:
        await sync_scrobble_vault()
    except Exception:
        logger.exception("Sync failed, trying again next interval")

async def _wait_until_configured() -> None:
    """Idle until last.fm details exist, they arrive from the settings page.

    A fresh install has none, and the api has to stay up for the settings page to
    be reachable at all, so this waits instead of raising.
    """
    if _configured():
        return
    logger.warning("No Last.fm username or api key yet, open the settings page to add them. Sync is paused.")
    while not _configured():
        await asyncio.sleep(_TICK_SECONDS)
    logger.info("Last.fm details set, starting sync")

async def run_sync_loop() -> None:
    """Sync on startup, then every SYNC_INTERVAL_MINUTES, forever.

    A plain interval instead of a cron expression: the old '*/n * * * *' only
    reaches 59, above that cronsim silently reads it as hourly, and daily or
    weekly is a fair thing to want. Re-reading the interval each tick is also
    what lets the settings page change it without a restart.
    """
    # Tables first: the schema does not depend on last.fm, and without it
    # /music-summary would error on a missing table instead of coming back empty
    await init_schema()

    await _wait_until_configured()
    await _sync_once()
    last_run = time.monotonic()
    logger.info(f"Syncing every {env.SYNC_INTERVAL_MINUTES} minutes")

    while True:
        await asyncio.sleep(_TICK_SECONDS)

        # Details can be cleared again from the settings page, so re-check
        if not _configured() or time.monotonic() - last_run < env.SYNC_INTERVAL_MINUTES * 60:
            continue

        # last_run moves even on failure, a broken last.fm must not spin the loop
        last_run = time.monotonic()
        await _sync_once()
