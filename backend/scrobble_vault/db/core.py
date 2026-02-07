import asyncpg
import env

pool: asyncpg.Pool | None = None # the global Postgres connection pool

async def init_db():
    """Create the Postgres connection pool"""
    global pool
    pool = await asyncpg.create_pool(
        dsn=env.DATABASE_URL,
        min_size=env.POSTGRES_MIN_POOL_SIZE,
        max_size=env.POSTGRES_MAX_POOL_SIZE
    )

async def close_db():
    """Closes the Postgres connection pool"""
    await pool.close()


