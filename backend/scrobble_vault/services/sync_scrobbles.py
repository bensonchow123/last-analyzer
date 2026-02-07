import time
import logging

from db.last_sync import get_last_synced_scrobble, update_last_synced_scrobble, init_sync_table
from db.track import init_tracks_table
from services.last_fm import fetch_last_fm_data
from services.sync_new_tracks import sync_new_tracks

logger = logging.getLogger(__name__)


async def sync_scrobble_vault():
    """Fetch new scrobbles from last.fm and update the database."""
    # Initialize the database schema, if it is not there yet
    await init_sync_table()
    await init_tracks_table()
    
    logger.info("Starting scrobble sync...")
    
    last_synced_scrobble_timestamp = await get_last_synced_scrobble()
    
    now = int(time.time())

    # If no sync record sync from beginning
    if last_synced_scrobble_timestamp is None:
        starting_time = 0  # Unix epoch
    else:
        starting_time = last_synced_scrobble_timestamp + 1  # Start from next second
    
    scrobble_data = await fetch_last_fm_data(starting_time, now)
    scrobbles = scrobble_data.get('scrobbles', [])
    
    if scrobbles:
        # Fetch and store info for any new unique tracks
        await sync_new_tracks(scrobbles)

        # TODO: Store scrobbles in db
        
        # Update the last synced timestamp to the most recent scrobble
        latest_scrobble_time = max(int(s['date']['uts']) for s in scrobbles)
        await update_last_synced_scrobble(latest_scrobble_time)
        logger.info(f"Sync completed: {len(scrobbles)} scrobbles synced")
    else:
        logger.info("Sync completed: No new scrobbles")
            
