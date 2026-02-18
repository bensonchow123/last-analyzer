import asyncpg
from pgvector.asyncpg import register_vector

from env import env

pool: asyncpg.Pool | None = None # the global Postgres connection pool

async def _init_connection(conn: asyncpg.Connection):
    """To make sure the pgvector extension is enbaled, before the connection."""
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await register_vector(conn)

async def init_db():
    """Create the Postgres connection pool and enable pgvector."""
    global pool
    pool = await asyncpg.create_pool(
        dsn=env.DATABASE_URL,
        min_size=env.POSTGRES_MIN_POOL_SIZE,
        max_size=env.POSTGRES_MAX_POOL_SIZE,
        init=_init_connection,
    )

async def close_db():
    """Closes the Postgres connection pool"""
    await pool.close()


