import time
import logging

import asyncpg

from env import env

logger = logging.getLogger(__name__)

async def get_db_connection():
    """Create a connection to the PostgreSQL database."""
    return await asyncpg.connect(env.DATABASE_URL)

async def init_sync_table():
    """
    Initialize the database schema.
    Schema:
        - key: Unique identifier (last_sync_time).
        - value: The Unix timestamp of the last scrobble successfully synced.
        - updated_at: The Unix timestamp of when the database row was last written to.
    """
    conn = await get_db_connection()
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS sync_lastfm (
                key VARCHAR(50) PRIMARY KEY,
                value BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            )
        ''')
        # Insert initial value if not exists
        await conn.execute('''
            INSERT INTO sync_lastfm (key, value, updated_at)
            VALUES ('last_sync_time', 0, 0)
            ON CONFLICT (key) DO NOTHING
        ''')

    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Failed to initialize the syncing table")
        raise

    finally:
        await conn.close()

async def get_last_synced_scrobble() -> int | None:
    """
    Get the last successful scrobble sync time from the database.
    Returns:
        - int: The timestamp of the last synced scrobble
        - None: If no sync record exists (first time sync)
    Raises:
        - Exception: If database connection or query fails
    """
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT value FROM sync_lastfm WHERE key = 'last_sync_time'"
        )
        return row['value'] if row else None
    
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Could not read last sync time")
        raise
    
    finally:
        await conn.close()

async def update_last_synced_scrobble(timestamp: int):
    """Update the last sync time row in the database."""
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE sync_lastfm
            SET value = $1, updated_at = $2
            WHERE key = 'last_sync_time'
        ''', timestamp, int(time.time()))
        
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Could not update last sync time")
        raise

    finally:
        await conn.close()

async def init_scrobbles_db():
    pass