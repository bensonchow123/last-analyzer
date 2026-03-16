import json
import logging

import asyncpg

from scrobble_vault.db import core
from scrobble_vault.db.core import normalize
from scrobble_vault.ai.embeddings import build_track_text, generate_embedding_async
from scrobble_vault.db.sql_loader import load_sql

logger = logging.getLogger(__name__)


CREATE_TABLE_SQL = load_sql("track", "create_table")
CREATE_UNIQUE_IDENTITY_INDEX_SQL = load_sql("track", "create_unique_identity_index")
TRACK_EXISTS_SQL = load_sql("track", "track_exists")
GET_ARTIST_ID_BY_NAME_NORM_SQL = load_sql("track", "get_artist_id_by_name_norm")
GET_ALBUM_ID_BY_NAMES_NORM_SQL = load_sql("track", "get_album_id_by_names_norm")
INSERT_TRACK_SQL = load_sql("track", "insert_track")


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
            await conn.execute(CREATE_TABLE_SQL)
            await conn.execute(CREATE_UNIQUE_IDENTITY_INDEX_SQL)
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Failed to initialize the tracks table")
        raise


async def track_exists(artist_name: str, track_name: str) -> bool:
    """Check if a track already exists in the database."""
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(
                TRACK_EXISTS_SQL,
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
                GET_ARTIST_ID_BY_NAME_NORM_SQL,
                normalize(artist.get('name', '')),
            )
            artist_id = artist_row['id'] if artist_row else None

            # Resolve the album foreign key (nullable as not every track has an album)
            album_id = None
            album_title = album.get('title') or None
            album_artist_name = album.get('artist') or artist.get('name', '')
            if album_title:
                album_row = await conn.fetchrow(
                    GET_ALBUM_ID_BY_NAMES_NORM_SQL,
                    normalize(album_artist_name),
                    normalize(album_title),
                )
                album_id = album_row['id'] if album_row else None

            # Generate embedding
            embedding = await generate_embedding_async(build_track_text({
                'name': track_info.get('name'),
                'artist_name': artist.get('name'),
                'album_title': album_title,
                'toptags': toptags,
                'wiki_content': wiki.get('content'),
                'wiki_summary': wiki.get('summary')
            }))

            await conn.execute(
                INSERT_TRACK_SQL,
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
                embedding,
            )
        logger.info(f"Inserted track: {artist.get('name')} - {track_info.get('name')}")
    except asyncpg.UniqueViolationError:
        logger.debug(f"Track already exists: {track_info.get('artist', {}).get('name')} - {track_info.get('name')}")
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception(f"Failed to insert track: {track_info.get('artist', {}).get('name')} - {track_info.get('name')}")
        raise

