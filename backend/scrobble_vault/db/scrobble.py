import logging

import asyncpg

from . import core

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def init_scrobbles_table():
    """
    Initialize the scrobbles table.
    Each row is a single listen event, linked to a track via foreign key.
    Schema:
        - id: Auto-incrementing primary key.
        - track_id: FK to tracks(id) — the track that was scrobbled.
        - listened_at: Unix timestamp of when the scrobble occurred.
        - artist_name / track_name / album_name: Denormalized names from the
          scrobble payload for fast display without JOINs.
    Unique constraint on (track_id, listened_at) prevents duplicate scrobbles.
    """
    try:
        async with core.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS scrobbles (
                    id SERIAL PRIMARY KEY,
                    track_id INTEGER REFERENCES tracks(id),
                    listened_at BIGINT NOT NULL,
                    artist_name TEXT NOT NULL,
                    track_name TEXT NOT NULL,
                    album_name TEXT
                )
            ''')
            await conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS scrobbles_unique_listen
                ON scrobbles (track_id, listened_at)
            ''')
    except (OSError, asyncpg.PostgresError):
        logger.exception("Failed to initialize the scrobbles table")
        raise


async def insert_scrobble(scrobble: dict):
    """
    Insert a single scrobble into the database.

    Resolves the track_id by looking up the track via normalized
    (artist_name, track_name) in the tracks table.

    Args:
        scrobble: A single scrobble object from Last.fm's user.getRecentTracks.
    """
    try:
        artist_name = scrobble.get('artist', {}).get('#text', '')
        track_name = scrobble.get('name', '')
        album_name = scrobble.get('album', {}).get('#text', '') or None
        listened_at = int(scrobble.get('date', {}).get('uts', 0))

        if not artist_name or not track_name or not listened_at:
            logger.warning(f"Skipping scrobble with missing data: {scrobble}")
            return

        async with core.pool.acquire() as conn:
            # Resolve the track foreign key
            track_row = await conn.fetchrow(
                "SELECT id FROM tracks WHERE artist_name_norm = $1 AND track_name_norm = $2",
                normalize(artist_name),
                normalize(track_name),
            )
            track_id = track_row['id'] if track_row else None

            await conn.execute('''
                INSERT INTO scrobbles (
                    track_id, listened_at,
                    artist_name, track_name, album_name
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (track_id, listened_at) DO NOTHING
            ''',
                track_id,
                listened_at,
                artist_name,
                track_name,
                album_name,
            )
    except asyncpg.UniqueViolationError:
        logger.debug(f"Scrobble already exists: {artist_name} - {track_name} @ {listened_at}")
    except (OSError, asyncpg.PostgresError):
        logger.exception(f"Failed to insert scrobble: {artist_name} - {track_name}")
        raise