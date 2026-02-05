import asyncpg

from env import env

async def get_db_connection():
    """Create a connection to the PostgreSQL database."""
    return await asyncpg.connect(env.DATABASE_URL)

async def init_sync_table():
    """Initialize the database schema."""
    conn = await get_db_connection()
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS sync_lastfm (
                key VARCHAR(50) PRIMARY KEY,
                value BIGINT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        ''')
        # Insert initial value if not exists
        await conn.execute('''
            INSERT INTO sync_lastfm (key, value)
            VALUES ('last_sync_time', 0)
            ON CONFLICT (key) DO NOTHING
        ''')
    finally:
        await conn.close()

async def get_last_sync_time() -> int:
    """Get the last sync time from the database."""
    conn = await get_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT value FROM sync_lastfm WHERE key = 'last_sync_time'"
        )
        return row['value'] if row else 0
    finally:
        await conn.close()

async def update_last_sync_time(timestamp: int):
    """Update the last sync time in the database."""
    conn = await get_db_connection()
    try:
        await conn.execute('''
            UPDATE sync_lastfm 
            SET value = $1, updated_at = NOW() 
            WHERE key = 'last_sync_time'
        ''', timestamp)
    finally:
        await conn.close()