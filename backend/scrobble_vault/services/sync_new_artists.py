import logging

import aiohttp

from env import env
from db.artist import artist_exists, insert_artist
from services.last_fm import fetch_artist_info

logger = logging.getLogger(__name__)


def normalize(text: str) -> str:
    """Normalize text for case-insensitive comparison."""
    return text.strip().lower()


async def sync_new_artists(scrobbles: list):
    """
    For each unique artist in the scrobbles, check if it already exists in the artists table.
    If not, fetch its info from last.fm and store it.

    Args:
        scrobbles: List of scrobble objects from Last.fm's user.getRecentTracks
    """
    unique_artists: dict[str, dict[str, str | None]] = {}
    for scrobble in scrobbles:
        artist_data = scrobble.get('artist', {})
        artist_name = artist_data.get('#text', '')
        artist_mbid = artist_data.get('mbid') or None
        if artist_name:
            key = normalize(artist_name)
            if key not in unique_artists:
                unique_artists[key] = {'name': artist_name, 'mbid': artist_mbid}
            elif unique_artists[key].get('mbid') is None and artist_mbid is not None:
                unique_artists[key]['mbid'] = artist_mbid

    logger.info(f"Found {len(unique_artists)} unique artists in scrobbles")

    new_artists: dict[str, dict[str, str | None]] = {}
    for artist_name_norm, artist_payload in unique_artists.items():
        artist_name = artist_payload.get('name') or artist_name_norm
        if not await artist_exists(artist_name):
            new_artists[artist_name_norm] = artist_payload

    if not new_artists:
        logger.info("No new artists to fetch info for")
        return

    logger.info(f"Fetching info for {len(new_artists)} new artists from Last.fm")

    async with aiohttp.ClientSession() as session:
        for artist_name_norm, artist_payload in new_artists.items():
            artist_name = artist_payload.get('name') or artist_name_norm
            mbid = artist_payload.get('mbid')
            artist_info = await fetch_artist_info(
                session,
                artist=artist_name,
                mbid=mbid,
                username=env.LAST_FM_USERNAME,
            )

            if artist_info:
                await insert_artist(artist_info)
            else:
                logger.warning(f"No artist info returned for: {artist_name}")

    logger.info(f"Finished syncing {len(new_artists)} new artists")
