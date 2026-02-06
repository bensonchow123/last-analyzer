import asyncio
import logging

import aiocron

from env import env
from services.sync_scrobbles import sync_scrobble_vault

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

async def main():
    # Sync on startup
    await sync_scrobble_vault()
    
    # Register the cron job for the sync
    aiocron.crontab(f'*/{env.SYNC_INTERVAL_MINUTES} * * * *', func=sync_scrobble_vault)
    
    # Keep the event loop running forever
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())