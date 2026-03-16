import json
import logging

import asyncpg

from scrobble_vault.db import core
from scrobble_vault.db.core import normalize
from scrobble_vault.ai.embeddings import build_album_text, generate_embedding_async
from scrobble_vault.db.sql_loader import load_sql

logger = logging.getLogger(__name__)


CREATE_TABLE_SQL = load_sql("album", "create_table")
CREATE_UNIQUE_IDENTITY_INDEX_SQL = load_sql("album", "create_unique_identity_index")
ALBUM_EXISTS_SQL = load_sql("album", "album_exists")
GET_ARTIST_ID_BY_NAME_NORM_SQL = load_sql("album", "get_artist_id_by_name_norm")
INSERT_ALBUM_SQL = load_sql("album", "insert_album")


async def init_albums_table():
    """
    Initialize the albums table.
    Stores album metadata from Last.fm's album.getInfo endpoint.
    Uses an internal SERIAL id as primary key.
    Unique constraint on normalized (artist_name_norm, album_name_norm) to prevent duplicates.
    """
    try:
        async with core.pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            await conn.execute(CREATE_UNIQUE_IDENTITY_INDEX_SQL)
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Failed to initialize the albums table")
        raise


async def album_exists(artist_name: str, album_name: str) -> bool:
    """Check if an album already exists in the database."""
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(
                ALBUM_EXISTS_SQL,
                normalize(artist_name), normalize(album_name)
            )
            return row is not None
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception(f"Failed to check album existence: {artist_name} - {album_name}")
        raise


def _extract_image(images: list, size: str) -> str | None:
    """Extract an image URL of the given size from the images list."""
    for img in images:
        if img.get('size') == size:
            return img.get('#text') or None
    return None


async def insert_album(album_info: dict):
    """
    Insert an album into the database from an album.getInfo JSON response.

    Args:
        album_info: The 'album' object from the Last.fm album.getInfo JSON response.
    """
    try:
        images = album_info.get('image', [])
        tags_raw = album_info.get('tags')
        toptags = tags_raw.get('tag', []) if isinstance(tags_raw, dict) else []
        wiki_raw = album_info.get('wiki')
        wiki = wiki_raw if isinstance(wiki_raw, dict) else {}
        tracks_raw = album_info.get('tracks')
        tracks = tracks_raw.get('track', []) if isinstance(tracks_raw, dict) else []

        # Parse user-specific fields (present when username param was used)
        user_playcount_raw = album_info.get('userplaycount')
        user_playcount = int(user_playcount_raw) if user_playcount_raw is not None else None

        # Parse global stats
        listeners_raw = album_info.get('listeners')
        listeners = int(listeners_raw) if listeners_raw is not None else None
        playcount_raw = album_info.get('playcount')
        playcount = int(playcount_raw) if playcount_raw is not None else None

        artist_name = album_info.get('artist', '')

        # Generate embedding
        embedding = await generate_embedding_async(build_album_text({
            'name': album_info.get('name'),
            'artist_name': artist_name,
            'toptags': toptags,
            'tracks': tracks,
            'wiki_content': wiki.get('content'),
            'wiki_summary': wiki.get('summary')
        }))

        async with core.pool.acquire() as conn:
            # Resolve the artist foreign key
            artist_row = await conn.fetchrow(
                GET_ARTIST_ID_BY_NAME_NORM_SQL,
                normalize(artist_name),
            )
            artist_id = artist_row['id'] if artist_row else None

            await conn.execute(
                INSERT_ALBUM_SQL,
                album_info.get('name'),
                normalize(album_info.get('name', '')),
                album_info.get('mbid') or None,
                album_info.get('url') or None,
                album_info.get('releasedate') or None,
                artist_id,
                artist_name,
                normalize(artist_name),
                _extract_image(images, 'small'),
                _extract_image(images, 'medium'),
                _extract_image(images, 'large'),
                _extract_image(images, 'extralarge'),
                listeners,
                playcount,
                json.dumps(toptags) if toptags else None,
                json.dumps(tracks) if tracks else None,
                wiki.get('published') or None,
                wiki.get('summary') or None,
                wiki.get('content') or None,
                user_playcount,
                embedding,
            )
        logger.info(f"Inserted album: {artist_name} - {album_info.get('name')}")
    except asyncpg.UniqueViolationError:
        logger.debug(f"Album already exists: {album_info.get('artist')} - {album_info.get('name')}")
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception(f"Failed to insert album: {album_info.get('artist')} - {album_info.get('name')}")
        raise