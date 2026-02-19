import json
import logging

import asyncpg

from . import core
from ai.embeddings import build_artist_text, generate_embedding_async

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def init_artists_table():
    """
    Initialize the artists table.
    Stores artist metadata from Last.fm's artist.getInfo endpoint.
    Uses an internal SERIAL id as primary key.
    Unique constraint on normalized artist name to prevent duplicates.
    """
    try:
        async with core.pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS artists (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    artist_name_norm TEXT NOT NULL,
                    mbid TEXT,
                    url TEXT,
                    image_small TEXT,
                    image_medium TEXT,
                    image_large TEXT,
                    image_extralarge TEXT,
                    streamable TEXT,
                    listeners INTEGER,
                    playcount INTEGER,
                    similar_artists JSONB,
                    tags JSONB,
                    bio_published TEXT,
                    bio_summary TEXT,
                    bio_content TEXT,
                    user_playcount INTEGER,
                    embedding VECTOR(384)
                )
            ''')
            await conn.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS artists_unique_identity
                ON artists (artist_name_norm)
            ''')
    except (OSError, asyncpg.PostgresError):
        logger.exception("Failed to initialize the artists table")
        raise


async def artist_exists(artist_name: str) -> bool:
    """Check if an artist already exists in the database."""
    try:
        async with core.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM artists WHERE artist_name_norm = $1",
                normalize(artist_name),
            )
            return row is not None
    except (OSError, asyncpg.PostgresError):
        logger.exception(f"Failed to check artist existence: {artist_name}")
        raise


def _extract_image(images: list, size: str) -> str | None:
    """Extract an image URL of the given size from the images list."""
    for img in images:
        if img.get('size') == size:
            return img.get('#text') or None
    return None


async def insert_artist(artist_info: dict):
    """
    Insert an artist into the database from an artist.getInfo JSON response.

    Args:
        artist_info: The 'artist' object from the Last.fm artist.getInfo JSON response.
    """
    try:
        images = artist_info.get('image', [])
        stats = artist_info.get('stats', {}) if isinstance(artist_info.get('stats'), dict) else {}
        tags_raw = artist_info.get('tags') if isinstance(artist_info.get('tags'), dict) else {}
        tags = tags_raw.get('tag', []) if isinstance(tags_raw, dict) else []
        similar_raw = artist_info.get('similar') if isinstance(artist_info.get('similar'), dict) else {}
        similar = similar_raw.get('artist', []) if isinstance(similar_raw, dict) else []
        bio_raw = artist_info.get('bio') if isinstance(artist_info.get('bio'), dict) else {}

        listeners_raw = stats.get('listeners')
        listeners = int(listeners_raw) if listeners_raw is not None else None
        playcount_raw = stats.get('playcount') or stats.get('plays')
        playcount = int(playcount_raw) if playcount_raw is not None else None
        user_playcount_raw = stats.get('userplaycount')
        user_playcount = int(user_playcount_raw) if user_playcount_raw is not None else None

        artist_name = artist_info.get('name', '')

        # Generate embedding
        embedding = await generate_embedding_async(build_artist_text({
            'name': artist_name,
            'tags': tags,
            'similar_artists': similar,
            'bio_content': bio_raw.get('content'),
            'bio_summary': bio_raw.get('summary')
        }))

        async with core.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO artists (
                    name, artist_name_norm, mbid, url,
                    image_small, image_medium, image_large, image_extralarge,
                    streamable, listeners, playcount,
                    similar_artists, tags,
                    bio_published, bio_summary, bio_content,
                    user_playcount,
                    embedding
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8,
                    $9, $10, $11,
                    $12, $13,
                    $14, $15, $16,
                    $17,
                    $18
                )
                ON CONFLICT (artist_name_norm) DO NOTHING
            ''',
                artist_name,
                normalize(artist_name),
                artist_info.get('mbid') or None,
                artist_info.get('url') or None,
                _extract_image(images, 'small'),
                _extract_image(images, 'medium'),
                _extract_image(images, 'large'),
                _extract_image(images, 'extralarge'),
                str(artist_info.get('streamable', '')) or None,
                listeners,
                playcount,
                json.dumps(similar) if similar else None,
                json.dumps(tags) if tags else None,
                bio_raw.get('published') or None,
                bio_raw.get('summary') or None,
                bio_raw.get('content') or None,
                user_playcount,
                embedding,
            )
        logger.info(f"Inserted artist: {artist_name}")
    except asyncpg.UniqueViolationError:
        logger.debug(f"Artist already exists: {artist_info.get('name')}")
    except (OSError, asyncpg.PostgresError):
        logger.exception(f"Failed to insert artist: {artist_info.get('name')}")
        raise
