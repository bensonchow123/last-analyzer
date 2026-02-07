import os
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # this is project root
load_dotenv(BASE_DIR / ".env")

class Env:
    """Environment variable manager for scrobble vault."""
    def __init__(self):
        # Last.fm configs
        self.LAST_FM_USERNAME = os.getenv('LAST_FM_USERNAME')
        self.LAST_FM_PASSWORD = os.getenv('LAST_FM_PASSWORD')
        self.LAST_FM_API_KEY = os.getenv('LAST_FM_API_KEY')
        self.LAST_FM_API_SECRET = os.getenv('LAST_FM_API_SECRET')

        # Sync settings
        self.SYNC_INTERVAL_MINUTES = int(os.getenv('SYNC_INTERVAL_MINUTES', 15))
        self.RATE_LIMIT_MS = int(os.getenv('RATE_LIMIT_MS', 200))
        
        # PostgreSQL connection
        self.POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
        self.POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
        self.POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
        self.POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
        self.POSTGRES_DB = os.getenv('POSTGRES_DB', 'scrobble_vault')
        self.POSTGRES_MIN_POOL_SIZE = int(os.getenv('POSTGRES_MIN_POOL_SIZE', 1))
        self.POSTGRES_MAX_POOL_SIZE = int(os.getenv('POSTGRES_MAX_POOL_SIZE', 5))
    
    @property
    def DATABASE_URL(self) -> str:
        # URL encode the password to handle special characters
        encoded_password = quote(self.POSTGRES_PASSWORD or '')
        return f"postgresql://{self.POSTGRES_USER}:{encoded_password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

# Initilize the global instance for all files
env = Env()