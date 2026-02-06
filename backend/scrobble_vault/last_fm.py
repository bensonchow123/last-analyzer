import asyncio
import logging

import aiohttp

from env import env

logger = logging.getLogger(__name__)

LAST_FM_API_URL = "http://ws.audioscrobbler.com/2.0/"

async def fetch_last_fm_data(
        starting_time: int,
        ending_time: int,
        ) -> dict:
    """Fetch last.fm scrobble data for a last.fm user within a time range."""
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
            
            async with session.get(LAST_FM_API_URL, params=params) as response:
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
                
                # Last.fm only allows 5 request per second, so 200ms is recomended
                await asyncio.sleep(env.RATE_LIMIT_MS / 1000)
    
    return {"scrobbles": all_tracks}

async def fetch_track_tags(session, artist, track, api_key):
    params = {
        'method': 'track.getTopTags',
        'artist': artist,
        'track': track,
        'api_key': env.LAST_FM_API_KEY,
        'format': 'json'
    }
    try:
        async with session.get(LAST_FM_API_URL, params=params) as response:
            if response.status == 200:
                data = await response.json()
                # Extract the names of the top 5-10 tags
                tags = data.get('toptags', {}).get('tag', [])
                tag_list = [t['name'] for t in tags[:10]] 
                return ", ".join(tag_list)
    except Exception as e:
        logger.error(f"Error fetching tags for {track}: {e}")
    return ""


