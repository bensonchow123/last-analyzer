import logging

import aiohttp

from env import env
from db.track import track_exists, insert_track
from services.last_fm import fetch_track_info

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def sync_new_tracks(scrobbles: list):
    """
    For each unique song in the scrobbles, check if it already exists in the tracks table.
    If not, fetch its into from last.fm and store it.
    
    Args:
        scrobbles: List of scrobble objects from Last.fm's user.getRecentTracks
    """
    # Extract unique (artist, track) pairs from scrobbles
    # Use normalized keys to group duplicates, but preserve MBIDs
    unique_tracks: dict[tuple[str, str], str | None] = {}
    for scrobble in scrobbles:
        artist_name = scrobble.get('artist', {}).get('#text', '')
        track_name = scrobble.get('name', '')
        mbid = scrobble.get('mbid') or None
        if artist_name and track_name:
            # Use normalized values as the key for deduplication
            key = (normalize(artist_name), normalize(track_name))
            # If key not in dict, add it with the mbid
            # If key exists but has no mbid and this scrobble has one, update it
            if key not in unique_tracks:
                unique_tracks[key] = mbid
            elif unique_tracks[key] is None and mbid is not None:
                unique_tracks[key] = mbid

    logger.info(f"Found {len(unique_tracks)} unique tracks in scrobbles")

    # Filter out tracks that already exist in the database
    new_tracks = {}
    for (artist_name, track_name), mbid in unique_tracks.items():
        if not await track_exists(artist_name, track_name):
            new_tracks[(artist_name, track_name)] = mbid

    if not new_tracks:
        logger.info("No new tracks to fetch info for")
        return

    logger.info(f"Fetching info for {len(new_tracks)} new tracks from Last.fm")

    # Fetch and store track info for each new track
    # Using a single ClientSession for all requests is more efficient
    async with aiohttp.ClientSession() as session:
        for (artist_name, track_name), mbid in new_tracks.items():
            track_info = await fetch_track_info(
                session,
                artist=artist_name,
                track=track_name,
                mbid=mbid,
                username=env.LAST_FM_USERNAME,
            )

            if track_info:
                await insert_track(track_info)
            else:
                logger.warning(f"No track info returned for: {artist_name} - {track_name}")

    logger.info(f"Finished syncing {len(new_tracks)} new tracks")
