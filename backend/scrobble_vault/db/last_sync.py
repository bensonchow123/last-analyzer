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

async def touch_last_sync_row():
    """Update only the updated_at field in the last_sync table, leaving value unchanged."""
    try:
        async with core.pool.acquire() as conn:
            await conn.execute(
                "UPDATE last_sync SET updated_at = $1 WHERE key = 'last_sync_time';",
                int(time.time()),
            )
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Could not touch last sync row")
        raise

async def get_last_synced_scrobble() -> dict | None:
    """
    Get the last successful scrobble sync time from the database.
    Returns:
        - dict{"value":int, "updated_at": int}, both are unix timestamps:
            The timestamp of the last synced scrobble as `value` and the last update time as `updated_at`
        - None: If no sync record exists (first time sync)
    Raises:
        - Exception: If database connection or query fails
    """
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(GET_LAST_SYNCED_SCROBBLE_SQL)
            if row:
                return {"value": row["value"], "updated_at": row["updated_at"]}
            else:
                return None
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