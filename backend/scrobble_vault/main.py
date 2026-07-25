import asyncio
import logging

import uvicorn

from scrobble_vault.env import env
from scrobble_vault.db.core import init_db, close_db
from scrobble_vault.services.sync_scheduler import run_sync_loop
from scrobble_vault.api.fast_api import api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

async def start_api():
    config = uvicorn.Config(app=api, host="0.0.0.0", port=env.SCROBBLE_VAULT_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    # Initialize database connection pools
    await init_db()
    logging.info("Database connection pools initialized")
    
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(run_sync_loop())
            tg.create_task(start_api())

    finally:
        # Clean up database connection pool on shutdown
        await close_db()
        logging.info("Database connection pool closed")

if __name__ == "__main__":
    asyncio.run(main())