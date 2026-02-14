import json
import logging

import asyncpg

from . import core

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def init_tracks_table():
    """
    Initialize the tracks table.
    Stores track metadata from Last.fm's track.getInfo endpoint.
    Uses an internal SERIAL id as primary key.
    Unique constraint on normalized (artist_name_norm, track_name_norm) to prevent duplicates.
    Excludes listeners and playcount (global stats that change constantly).
    """
    try:
        async with core.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tracks (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    track_name_norm TEXT NOT NULL,
                    mbid TEXT,
                    url TEXT,
                    duration INTEGER,
                    streamable TEXT,
                    streamable_fulltrack TEXT,
                    artist_id INTEGER REFERENCES artists(id),
                    artist_name TEXT NOT NULL,
                    artist_name_norm TEXT NOT NULL,
                    artist_mbid TEXT,
                    artist_url TEXT,
                    album_id INTEGER REFERENCES albums(id),
                    album_title TEXT,
                    album_artist TEXT,
                    album_mbid TEXT,
                    album_url TEXT,
                    album_position TEXT,
                    album_image_small TEXT,
                    album_image_medium TEXT,
                    album_image_large TEXT,
                    album_image_extralarge TEXT,
                    toptags JSONB,
                    wiki_published TEXT,
                    wiki_summary TEXT,
                    wiki_content TEXT,
                    user_loved BOOLEAN,
                    user_playcount INTEGER
                )
            ''')
            # Create unique index on normalized columns
            await conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS tracks_unique_identity
                ON tracks (artist_name_norm, track_name_norm)
            ''')
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Failed to initialize the tracks table")
        raise


async def track_exists(artist_name: str, track_name: str) -> bool:
    """Check if a track already exists in the database."""
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM tracks WHERE artist_name_norm = $1 AND track_name_norm = $2",
                normalize(artist_name), normalize(track_name)
            )
            return row is not None
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception(f"Failed to check track existence: {artist_name} - {track_name}")
        raise


def _extract_album_image(images: list, size: str) -> str | None:
    """Extract an image URL of the given size from the album images list."""
    for img in images:
        if img.get('size') == size:
            return img.get('#text') or None
    return None


async def insert_track(track_info: dict):
    """
    Insert a track into the database from a track.getInfo JSON response.

    Args:
        track_info: The 'track' object from the Last.fm track.getInfo JSON response.
    """
    try:
        artist = track_info.get('artist', {})
        album = track_info.get('album', {})
        album_images = album.get('image', [])
        toptags = track_info.get('toptags', {}).get('tag', [])
        wiki = track_info.get('wiki', {})

        # Parse user-specific fields (present when username param was used)
        user_loved_raw = track_info.get('userloved')
        user_loved = user_loved_raw == '1' if user_loved_raw is not None else None
        user_playcount_raw = track_info.get('userplaycount')
        user_playcount = int(user_playcount_raw) if user_playcount_raw is not None else None

        async with core.pool.acquire() as conn:
            # Resolve the artist foreign key
            artist_row = await conn.fetchrow(
                "SELECT id FROM artists WHERE artist_name_norm = $1",
                normalize(artist.get('name', '')),
            )
            artist_id = artist_row['id'] if artist_row else None

            # Resolve the album foreign key (nullable as not every track has an album)
            album_id = None
            album_title = album.get('title') or None
            album_artist_name = album.get('artist') or artist.get('name', '')
            if album_title:
                album_row = await conn.fetchrow(
                    "SELECT id FROM albums WHERE artist_name_norm = $1 AND album_name_norm = $2",
                    normalize(album_artist_name),
                    normalize(album_title),
                )
                album_id = album_row['id'] if album_row else None

            await conn.execute('''
                INSERT INTO tracks (
                    name, track_name_norm, mbid, url, duration,
                    streamable, streamable_fulltrack,
                    artist_id, artist_name, artist_name_norm, artist_mbid, artist_url,
                    album_id, album_title, album_artist, album_mbid, album_url, album_position,
                    album_image_small, album_image_medium, album_image_large, album_image_extralarge,
                    toptags,
                    wiki_published, wiki_summary, wiki_content,
                    user_loved, user_playcount
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22,
                    $23,
                    $24, $25, $26,
                    $27, $28
                )
                ON CONFLICT (artist_name_norm, track_name_norm) DO NOTHING
            ''',
                track_info.get('name'),
                normalize(track_info.get('name', '')),
                track_info.get('mbid') or None,
                track_info.get('url') or None,
                int(track_info['duration']) if track_info.get('duration') else None,
                str(track_info.get('streamable', {}).get('#text', '')) if isinstance(track_info.get('streamable'), dict) else str(track_info.get('streamable', '')) or None,
                str(track_info.get('streamable', {}).get('fulltrack', '')) if isinstance(track_info.get('streamable'), dict) else None,
                artist_id,
                artist.get('name'),
                normalize(artist.get('name', '')),
                artist.get('mbid') or None,
                artist.get('url') or None,
                album_id,
                album_title,
                album.get('artist') or None,
                album.get('mbid') or None,
                album.get('url') or None,
                str(album.get('@attr', {}).get('position', '')) or None,
                _extract_album_image(album_images, 'small'),
                _extract_album_image(album_images, 'medium'),
                _extract_album_image(album_images, 'large'),
                _extract_album_image(album_images, 'extralarge'),
                json.dumps(toptags) if toptags else None,
                wiki.get('published') or None,
                wiki.get('summary') or None,
                wiki.get('content') or None,
                user_loved,
                user_playcount,
            )
        logger.info(f"Inserted track: {artist.get('name')} - {track_info.get('name')}")
    except asyncpg.UniqueViolationError:
        logger.debug(f"Track already exists: {track_info.get('artist', {}).get('name')} - {track_info.get('name')}")
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception(f"Failed to insert track: {track_info.get('artist', {}).get('name')} - {track_info.get('name')}")
        raise

