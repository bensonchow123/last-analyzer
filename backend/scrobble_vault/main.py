import asyncio
import os

import aiocron
from dotenv import load_dotenv

from last_fm import fetch_last_fm_data
from db import get_last_sync_time

load_dotenv()

# Load all .env variables into config dictionary
config = {
    'LAST_FM_USERNAME': os.getenv('LAST_FM_USERNAME'),
    'LAST_FM_PASSWORD': os.getenv('LAST_FM_PASSWORD'),
    'LAST_FM_API_KEY': os.getenv('LAST_FM_API_KEY'),
    'LAST_FM_API_SECRET': os.getenv('LAST_FM_API_SECRET'),
    'DB_FILE_PATH': os.getenv('DB_FILE_PATH'),
    'SYNC_INTERVAL_MINUTES': int(os.getenv('SYNC_INTERVAL_MINUTES', 15)),
    'RATE_LIMIT_MS': int(os.getenv('RATE_LIMIT_MS', 200))
}

@aiocron.crontab(f'*/{config["SYNC_INTERVAL_MINUTES"]} * * * *')
async def sync_scrobble_vault():
    pass