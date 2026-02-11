import asyncio
import logging
import time

import aiohttp

from env import env

logger = logging.getLogger(__name__)

LAST_FM_API_URL = "http://ws.audioscrobbler.com/2.0/"

# Global rate limiter for all Last.fm API calls.
# Last.fm only allows 5 requests per second, so 200ms between requests is recommended.
_rate_limit_lock = asyncio.Lock()
_last_request_time: float = 0.0


async def _rate_limited_get(session: aiohttp.ClientSession, params: dict) -> aiohttp.ClientResponse:
    """
    Perform a GET request to the Last.fm API, respecting the global rate limit.
    Ensures at least RATE_LIMIT_MS milliseconds between any two API requests.
    """
    global _last_request_time
    async with _rate_limit_lock:
        now = time.monotonic()
        elapsed_ms = (now - _last_request_time) * 1000
        wait_ms = env.RATE_LIMIT_MS - elapsed_ms
        if wait_ms > 0:
            await asyncio.sleep(wait_ms / 1000)
        _last_request_time = time.monotonic()
        return await session.get(LAST_FM_API_URL, params=params)

async def fetch_last_fm_data(
        starting_time: int,
        ending_time: int,
        ) -> dict:
    """
    Fetch last.fm scrobble data for a last.fm user within a time range.
    Docs at: https://www.last.fm/api/show/user.getRecentTracks
    """
    all_tracks = []
    
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            params = {
                'method': 'user.getrecenttracks',
                'user': env.LAST_FM_USERNAME,
                'api_key': env.LAST_FM_API_KEY,
                'format': 'json',
                'from': starting_time,
                'to': ending_time,
                'limit': 200,
                'page': page
            }
            
            async with await _rate_limited_get(session, params) as response:
                if response.status != 200:
                    logger.error(f"Last.fm API error: HTTP {response.status}")
                    raise Exception(f"Last.fm API error: {response.status}")
                
                data = await response.json()
                
                # Check if we have recenttracks data
                if 'recenttracks' not in data:
                    break
                    
                tracks = data['recenttracks'].get('track', [])
                
                # If tracks is empty or not a list, break
                if not tracks or not isinstance(tracks, list):
                    break
                    
                # Filter out 'nowplaying' tracks (they have @attr)
                filtered_tracks = [track for track in tracks if '@attr' not in track]
                all_tracks.extend(filtered_tracks)
                
                # Check pagination info
                attr = data['recenttracks'].get('@attr', {})
                total_pages = int(attr.get('totalPages', '1'))
                
                if page >= total_pages:
                    break
                    
                page += 1
    
    return {"scrobbles": all_tracks}

async def fetch_track_info(session, artist=None, track=None, mbid=None, username=None, autocorrect=1):
    """
    Fetch track info from last.fm api.
    Docs at https://www.last.fm/api/show/track.getInfo
    
    Args:
        session: aiohttp ClientSession
        artist (Optional): The artist name
        track (Optional): The track name
        mbid (Optional): The musicbrainz id for the track
        username (Optional): The username for context (includes playcount and loved status)
        autocorrect (Optional): 0 or 1, transforms misspelled artist/track names
    
    Note: Either mbid OR (artist AND track) must be provided
    """
    params = {
        'method': 'track.getInfo',
        'api_key': env.LAST_FM_API_KEY,
        'format': 'json',
        'autocorrect': autocorrect
    }
    
    if mbid:
        params['mbid'] = mbid
    else:
        if not artist or not track:
            logger.error("Either mbid OR (artist AND track) must be provided")
            return {}
        params['artist'] = artist
        params['track'] = track
    
    if username:
        params['username'] = username
    
    try:
        async with await _rate_limited_get(session, params) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('track', {})
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching track info for {artist} - {track}: {e}")
    except ValueError as e:
        logger.error(f"JSON decode error for {artist} - {track}: {e}")
    return {}


async def fetch_album_info(session, artist=None, album=None, mbid=None, username=None, autocorrect=1):
    """
    Fetch album info from last.fm api.
    Docs at https://www.last.fm/api/show/album.getInfo

    Args:
        session: aiohttp ClientSession
        artist (Optional): The artist name
        album (Optional): The album name
        mbid (Optional): The musicbrainz id for the album
        username (Optional): The username for context (includes playcount)
        autocorrect (Optional): 0 or 1, transforms misspelled artist names

    Note: Either mbid OR (artist AND album) must be provided
    """
    params = {
        'method': 'album.getInfo',
        'api_key': env.LAST_FM_API_KEY,
        'format': 'json',
        'autocorrect': autocorrect
    }

    if mbid:
        params['mbid'] = mbid
    else:
        if not artist or not album:
            logger.error("Either mbid OR (artist AND album) must be provided")
            return {}
        params['artist'] = artist
        params['album'] = album

    if username:
        params['username'] = username

    try:
        async with await _rate_limited_get(session, params) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('album', {})
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching album info for {artist} - {album}: {e}")
    except ValueError as e:
        logger.error(f"JSON decode error for {artist} - {album}: {e}")
    return {}


async def fetch_artist_info(session, artist=None, mbid=None, username=None, autocorrect=1, lang=None):
    """
    Fetch artist info from last.fm api.
    Docs at https://www.last.fm/api/show/artist.getInfo

    Args:
        session: aiohttp ClientSession
        artist (Optional): The artist name
        mbid (Optional): The musicbrainz id for the artist
        username (Optional): The username for context (includes playcount)
        autocorrect (Optional): 0 or 1, transforms misspelled artist names
        lang (Optional): ISO 639 alpha-2 language code for biography

    Note: Either mbid OR artist must be provided
    """
    params = {
        'method': 'artist.getInfo',
        'api_key': env.LAST_FM_API_KEY,
        'format': 'json',
        'autocorrect': autocorrect,
    }

    if mbid:
        params['mbid'] = mbid
    else:
        if not artist:
            logger.error("Either mbid OR artist must be provided")
            return {}
        params['artist'] = artist

    if username:
        params['username'] = username

    if lang:
        params['lang'] = lang

    try:
        async with await _rate_limited_get(session, params) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('artist', {})
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching artist info for {artist}: {e}")
    except ValueError as e:
        logger.error(f"JSON decode error for {artist}: {e}")
    return {}