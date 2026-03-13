import asyncpg
from pgvector.asyncpg import register_vector
from unidecode import unidecode

from scrobble_vault.env import env


def normalize(text: str) -> str:
    """Normalize text for consistent DB key comparison.

    Before this random unicode characters cause missing foreign keys in the album and track table.
    """
    return unidecode(text).strip().lower()


pool: asyncpg.Pool | None = None # the global Postgres connection pool (admin)
ro_pool: asyncpg.Pool | None = None # the global Postgres connection pool (read only)

async def _init_connection(conn: asyncpg.Connection):
    """To make sure the pgvector extension is enbaled, before the connection."""
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await register_vector(conn)

async def init_db():
    """Create the Postgres connection pools and enable pgvector."""
    global pool, ro_pool

    # Create the pool
    pool = await asyncpg.create_pool(
        dsn=env.DATABASE_URL,
        min_size=env.POSTGRES_MIN_POOL_SIZE,
        max_size=env.POSTGRES_MAX_POOL_SIZE,
        init=_init_connection,
    )
    
    # Create the read only pool
    ro_pool = await asyncpg.create_pool(
        dsn=env.RO_DATABASE_URL,
        min_size=env.POSTGRES_MIN_POOL_SIZE,
        max_size=env.POSTGRES_MAX_POOL_SIZE,
        init=_init_connection,
    )

async def close_db():
    """Closes the Postgres connection pools"""
    global pool, ro_pool
    if pool:
        await pool.close()
    if ro_pool:
        await ro_pool.close()


