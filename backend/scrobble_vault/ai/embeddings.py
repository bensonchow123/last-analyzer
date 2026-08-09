import asyncio
import json
import logging
import os
from typing import Any

import numpy as np

from scrobble_vault.env import env

logger = logging.getLogger(__name__)

# Qdrant's onnx export of all-MiniLM-L6-v2.
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# fastembed defaults to /tmp, which a container wipes on every recreate. Point
# this at a volume so the 87MB download happens once.
CACHE_DIR = os.getenv('EMBEDDING_CACHE_DIR') or None

_model = None


def get_model():
    """Lazy load the embedding model, so the start up time not impacted."""
    global _model
    if _model is None:
        # Imported here so a vault with embeddings off never pulls in onnxruntime.
        from fastembed import TextEmbedding

        logger.info("Loading embedding model: %s", MODEL_NAME)
        _model = TextEmbedding(model_name=MODEL_NAME, cache_dir=CACHE_DIR)
    return _model


def _parse_json_field(value: Any) -> list:
    """Return a Python list from a JSONB field that could be a str or list."""
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
    return value if isinstance(value, list) else []


def _tag_names(tags: Any) -> list[str]:
    """Extract tag name strings from JSONB stored in the db."""
    return [t["name"] for t in _parse_json_field(tags) if isinstance(t, dict) and "name" in t]


def _clean_wiki(text: str | None) -> str | None:
    """Strip the Last.fm attribution link from wiki text."""
    if not text:
        return None

    idx = text.find('<a href="https://www.last.fm')
    if idx != -1:
        text = text[:idx]
    return text.strip() or None


def build_artist_text(row: dict) -> str:
    """
    Build embedding text for an artist.

    Always present: artist name.
    Often present : tags, similar_artists.
    Sometimes     : bio_summary / bio_content.
    """
    parts: list[str] = []
    parts.append(f"Artist: {row['name']}")

    tags = _tag_names(row.get("tags"))
    if tags:
        parts.append(f"Genres/tags: {', '.join(tags)}")

    similar = _parse_json_field(row.get("similar_artists"))
    similar_names = [a["name"] for a in similar if isinstance(a, dict) and "name" in a]
    if similar_names:
        parts.append(f"Similar artists: {', '.join(similar_names)}")

    bio = _clean_wiki(row.get("bio_content")) or _clean_wiki(row.get("bio_summary"))
    if bio:
        parts.append(f"Bio: {bio}")

    return ". ".join(parts)


def build_album_text(row: dict) -> str:
    """
    Build embedding text for an album.

    Always present: album name, artist name.
    Usually present: toptags, track listing.
    Rarely present : wiki_content / wiki_summary.
    """
    parts: list[str] = []
    parts.append(f"Album: {row['name']} by {row['artist_name']}")

    tags = _tag_names(row.get("toptags"))
    if tags:
        parts.append(f"Genres/tags: {', '.join(tags)}")

    tracks = _parse_json_field(row.get("tracks"))
    track_names = [t["name"] for t in tracks if isinstance(t, dict) and "name" in t]
    if track_names:
        parts.append(f"Tracks: {', '.join(track_names)}")

    wiki = _clean_wiki(row.get("wiki_content")) or _clean_wiki(row.get("wiki_summary"))
    if wiki:
        parts.append(f"About: {wiki}")

    return ". ".join(parts)


def build_track_text(row: dict) -> str:
    """
    Build embedding text for a track.

    Always present: track name, artist name.
    Often present : album name.
    Rarely present: toptags, wiki.

    Falls back to {track} from {album} by {artist name} cause track metadata is dogshit.
    """
    parts: list[str] = []
    core_text = f"Track: {row['name']} by {row['artist_name']}"
    if row.get("album_title"):
        core_text += f" from album {row['album_title']}"
    parts.append(core_text)

    tags = _tag_names(row.get("toptags"))
    if tags:
        parts.append(f"Genres/tags: {', '.join(tags)}")

    wiki = _clean_wiki(row.get("wiki_content")) or _clean_wiki(row.get("wiki_summary"))
    if wiki:
        parts.append(f"About: {wiki}")

    return ". ".join(parts)


def generate_embedding(text: str) -> np.ndarray | None:
    """Encode a single text string into a 384-dim float32 vector."""
    if not env.EMBEDDINGS_ENABLED:
        return None
    model = get_model()
    # embed() takes a batch and returns a generator, we only ever want one.
    # float32 to match the vector(384) column, fastembed hands back float64.
    return next(iter(model.embed([text]))).astype(np.float32)


async def generate_embedding_async(text: str) -> np.ndarray | None:
    """
    Use thread pool to run the embedding so the asynic loop is not blocked.

    None when embeddings are off, rows then store a null vector.
    """
    return await asyncio.to_thread(generate_embedding, text)
