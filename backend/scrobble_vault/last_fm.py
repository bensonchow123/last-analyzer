import asyncio

import aiohttp

LAST_FM_API_URL = "http://ws.audioscrobbler.com/2.0/"

async def fetch_last_fm_data(
        username: str,
        api_key: str,
        starting_time: int,
        ending_time: int,
        rate_limit: int = 200
        ) -> dict:
    """Fetch last.fm scrobble data for a last.fm user within a time range."""
    all_tracks = []
    
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            params = {
                'method': 'user.getrecenttracks',
                'user': username,
                'api_key': api_key,
                'format': 'json',
                'from': starting_time,
                'to': ending_time,
                'limit': 200,
                'page': page
            }
            
            async with session.get(LAST_FM_API_URL, params=params) as response:
                if response.status != 200:
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
                await asyncio.sleep(rate_limit / 1000)
    
    return {"scrobbles": all_tracks}


