from datetime import UTC, datetime

from asyncpg import PostgresError
from fastapi import HTTPException

from scrobble_vault.db import last_sync
from scrobble_vault.db.music_summary import get_music_summaries


async def music_summary():
    try:
        summaries = await get_music_summaries()
        last_synced_at = await last_sync.get_last_synced_scrobble()
        return {
            "generated_at": int(datetime.now(UTC).timestamp()),
            "last_synced_at": last_synced_at,
            "periods": summaries,
        }
    except PostgresError:
        raise HTTPException(status_code=500, detail="The database errored out.")

