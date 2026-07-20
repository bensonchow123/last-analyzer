import logging

from scrobble_vault.db import core
from scrobble_vault.db.sql_loader import load_sql
from scrobble_vault.ai.embeddings import generate_embedding_async

logger = logging.getLogger(__name__)

SEARCH_SQL = {
    "artists": load_sql("semantic_search", "artists"),
    "albums": load_sql("semantic_search", "albums"),
    "tracks": load_sql("semantic_search", "tracks"),
}

async def semantic_search(text: str, kind: str, limit: int) -> list[dict]:
    """Embed the text and rank one table's rows by cosine similarity."""
    embedding = await generate_embedding_async(text)
    async with core.ro_pool.acquire() as conn:
        rows = await conn.fetch(SEARCH_SQL[kind], embedding, limit)
    return [
        {**dict(row), "similarity": round(float(row["similarity"]), 3)}
        for row in rows
    ]
