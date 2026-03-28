from datetime import UTC, datetime

from asyncpg import PostgresError
from fastapi import HTTPException

from scrobble_vault.db.music_summary import get_music_summaries


async def music_summary():
    try:
        summaries = await get_music_summaries()
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "periods": summaries,
        }
    except PostgresError:
        raise HTTPException(status_code=500, detail="The database errored out.")

