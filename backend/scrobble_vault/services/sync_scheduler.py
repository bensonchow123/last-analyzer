import asyncio
import logging
import time

from scrobble_vault.env import env
from scrobble_vault.services.sync_scrobbles import sync_scrobble_vault

logger = logging.getLogger(__name__)

_TICK_SECONDS = 10  # how often the interval is re-checked, not how often we sync

async def run_sync_loop() -> None:
    """Sync on startup, then every SYNC_INTERVAL_MINUTES, forever.

    A plain interval instead of a cron expression: the old '*/n * * * *' only
    reaches 59, above that cronsim silently reads it as hourly, and daily or
    weekly is a fair thing to want. Re-reading the interval each tick is also
    what lets the settings page change it without a restart.
    """
    await sync_scrobble_vault()
    last_run = time.monotonic()
    logger.info(f"Syncing every {env.SYNC_INTERVAL_MINUTES} minutes")

    while True:
        await asyncio.sleep(_TICK_SECONDS)
        if time.monotonic() - last_run < env.SYNC_INTERVAL_MINUTES * 60:
            continue

        # last_run moves even on failure, a broken last.fm must not spin the loop
        last_run = time.monotonic()
        try:
            await sync_scrobble_vault()
        except Exception:
            logger.exception("Scheduled sync failed, retrying next interval")
