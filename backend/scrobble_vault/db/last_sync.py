import time
import logging

import asyncpg

from scrobble_vault.db import core
from scrobble_vault.db.sql_loader import load_sql

logger = logging.getLogger(__name__)


CREATE_TABLE_SQL = load_sql("last_sync", "create_table")
INSERT_INITIAL_LAST_SYNC_SQL = load_sql("last_sync", "insert_initial_last_sync")
GET_LAST_SYNCED_SCROBBLE_SQL = load_sql("last_sync", "get_last_synced_scrobble")
UPDATE_LAST_SYNCED_SCROBBLE_SQL = load_sql("last_sync", "update_last_synced_scrobble")

async def init_sync_table():
    """
    Initialize the database schema.
    Schema:
        - key: Unique identifier (last_sync_time).
        - value: The Unix timestamp of the last scrobble successfully synced.
        - updated_at: The Unix timestamp of when the database row was last written to.
    """
    try:
        async with core.pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            # Insert initial value if not exists
            await conn.execute(INSERT_INITIAL_LAST_SYNC_SQL)

    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Failed to initialize the syncing table")
        raise

async def get_last_synced_scrobble() -> int | None:
    """
    Get the last successful scrobble sync time from the database.
    Returns:
        - int: The timestamp of the last synced scrobble
        - None: If no sync record exists (first time sync)
    Raises:
        - Exception: If database connection or query fails
    """
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(GET_LAST_SYNCED_SCROBBLE_SQL)
            return row['value'] if row else None
    
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Could not read last sync time")
        raise

async def update_last_synced_scrobble(timestamp: int):
    """Update the last sync time row in the database."""
    try:
        async with core.pool.acquire() as conn:
            await conn.execute(
                UPDATE_LAST_SYNCED_SCROBBLE_SQL,
                timestamp,
                int(time.time()),
            )
        
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Could not update last sync time")
        raise