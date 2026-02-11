import logging

import aiohttp

from env import env
from db.album import album_exists, insert_album
from services.last_fm import fetch_album_info

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def sync_new_albums(scrobbles: list):
    """
    For each unique album in the scrobbles, check if it already exists in the albums table.
    If not, fetch its info from last.fm and store it.

    Args:
        scrobbles: List of scrobble objects from Last.fm's user.getRecentTracks
    """
    # Extract unique (artist, album) pairs from scrobbles
    # Use normalized keys to group duplicates, but preserve MBIDs
    unique_albums: dict[tuple[str, str], str | None] = {}
    for scrobble in scrobbles:
        artist_name = scrobble.get('artist', {}).get('#text', '')
        album_name = scrobble.get('album', {}).get('#text', '')
        album_mbid = scrobble.get('album', {}).get('mbid') or None
        if artist_name and album_name:
            key = (normalize(artist_name), normalize(album_name))
            if key not in unique_albums:
                unique_albums[key] = album_mbid
            elif unique_albums[key] is None and album_mbid is not None:
                unique_albums[key] = album_mbid

    logger.info(f"Found {len(unique_albums)} unique albums in scrobbles")

    # Filter out albums that already exist in the database
    new_albums = {}
    for (artist_name, album_name), mbid in unique_albums.items():
        if not await album_exists(artist_name, album_name):
            new_albums[(artist_name, album_name)] = mbid

    if not new_albums:
        logger.info("No new albums to fetch info for")
        return

    logger.info(f"Fetching info for {len(new_albums)} new albums from Last.fm")

    # Fetch and store album info for each new album
    # Using a single ClientSession for all requests is more efficient
    async with aiohttp.ClientSession() as session:
        for (artist_name, album_name), mbid in new_albums.items():
            album_info = await fetch_album_info(
                session,
                artist=artist_name,
                album=album_name,
                mbid=mbid,
                username=env.LAST_FM_USERNAME,
            )

            if album_info:
                await insert_album(album_info)
            else:
                logger.warning(f"No album info returned for: {artist_name} - {album_name}")

    logger.info(f"Finished syncing {len(new_albums)} new albums")
