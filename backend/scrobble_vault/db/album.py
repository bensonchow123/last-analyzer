import json
import logging

import asyncpg

from . import core
from ai.embeddings import build_album_text, generate_embedding_async

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def init_albums_table():
    """
    Initialize the albums table.
    Stores album metadata from Last.fm's album.getInfo endpoint.
    Uses an internal SERIAL id as primary key.
    Unique constraint on normalized (artist_name_norm, album_name_norm) to prevent duplicates.
    """
    try:
        async with core.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS albums (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    album_name_norm TEXT NOT NULL,
                    mbid TEXT,
                    url TEXT,
                    release_date TEXT,
                    artist_id INTEGER REFERENCES artists(id),
                    artist_name TEXT NOT NULL,
                    artist_name_norm TEXT NOT NULL,
                    image_small TEXT,
                    image_medium TEXT,
                    image_large TEXT,
                    image_extralarge TEXT,
                    listeners INTEGER,
                    playcount INTEGER,
                    toptags JSONB,
                    tracks JSONB,
                    wiki_published TEXT,
                    wiki_summary TEXT,
                    wiki_content TEXT,
                    user_playcount INTEGER,
                    embedding VECTOR(384)
                )
            ''')
            # Create unique index on normalized columns
            await conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS albums_unique_identity
                ON albums (artist_name_norm, album_name_norm)
            ''')
    except (OSError, asyncpg.PostgresError) as e:
        logger.exception("Failed to initialize the albums table")
        raise


async def album_exists(artist_name: str, album_name: str) -> bool:
    """Check if an album already exists in the database."""
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM albums WHERE artist_name_norm = $1 AND album_name_norm = $2",
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
                "SELECT id FROM artists WHERE artist_name_norm = $1",
                normalize(artist_name),
            )
            artist_id = artist_row['id'] if artist_row else None

            await conn.execute('''
                INSERT INTO albums (
                    name, album_name_norm, mbid, url, release_date,
                    artist_id, artist_name, artist_name_norm,
                    image_small, image_medium, image_large, image_extralarge,
                    listeners, playcount,
                    toptags, tracks,
                    wiki_published, wiki_summary, wiki_content,
                    user_playcount,
                    embedding
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8,
                    $9, $10, $11, $12,
                    $13, $14,
                    $15, $16,
                    $17, $18, $19,
                    $20,
                    $21
                )
                ON CONFLICT (artist_name_norm, album_name_norm) DO NOTHING
            ''',
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