from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from scrobble_vault.api.endpoints.vibed_sql_runner import vibed_sql_runner
# from scrobble_vault.api.endpoints.music_summary import music_summary

api = FastAPI()

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allows requests from any origin 
    allow_credentials=True,
    allow_methods=["*"],  # Only GET, POST will be used
    allow_headers=["*"],  # allow all headers
)

api.add_api_route("/vibed-sql-runner", vibed_sql_runner, methods=["POST"])
# api.add_api_route("/music-summary", music_summary, methods=["GET"])