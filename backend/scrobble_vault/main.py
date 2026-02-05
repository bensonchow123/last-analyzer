from pathlib import Path
import asyncio

import aiocron

from env import env
from db import init_sync_table, get_last_sync_time, update_last_sync_time


async def sync_scrobble_vault():
    print("Syncing scrobble vault...")

async def main():
    # Initialize the database schema
    await init_sync_table()
    
    # Sync on startup
    await sync_scrobble_vault()
    
    # Register the cron job for the sync
    aiocron.crontab(f'*/{env.SYNC_INTERVAL_MINUTES} * * * *', func=sync_scrobble_vault)
    
    # Keep the event loop running forever
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())