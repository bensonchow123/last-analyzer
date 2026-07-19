import json

from last_llm_service.core import scrobble_client

SCHEMA_DOC = """\
# scrobble_vault schema

Postgres, read only access. 5 tables: artists, albums, tracks, scrobbles, last_sync.

## Tables

### artists (one row per artist)
id, name, artist_name_norm, mbid, url,
image_small, image_medium, image_large, image_extralarge,
streamable, listeners, playcount, similar_artists (JSONB), tags (JSONB),
bio_published, bio_summary, bio_content, user_playcount, embedding

### albums (one row per album)
id, name, album_name_norm, mbid, url, release_date,
artist_id -> artists(id), artist_name, artist_name_norm,
image_small, image_medium, image_large, image_extralarge,
listeners, playcount, toptags (JSONB), tracks (JSONB),
wiki_published, wiki_summary, wiki_content, user_playcount, embedding

### tracks (one row per track)
id, name, track_name_norm, mbid, url, duration, streamable, streamable_fulltrack,
artist_id -> artists(id), artist_name, artist_name_norm, artist_mbid, artist_url,
album_id -> albums(id), album_title, album_artist, album_mbid, album_url, album_position,
album_image_small, album_image_medium, album_image_large, album_image_extralarge,
toptags (JSONB), wiki_published, wiki_summary, wiki_content,
user_loved, user_playcount, embedding

### scrobbles (one row per listen, the event log)
id, track_id -> tracks(id), listened_at, artist_name, track_name, album_name

### last_sync (sync bookkeeping, single row)
key, value, updated_at

## Query rules (enforced by the vault, breaking them returns an error to fix and retry)
- A single SELECT statement, no other statement types, only the 5 tables above.
- LIMIT 100 is injected when missing, any limit is capped at 1000.
- Dangerous Postgres functions (pg_sleep, pg_read_file, dblink, ...) are blocked.
- Never select embedding columns, they are scrubbed from results anyway.

## Hints
- scrobbles.listened_at is Unix seconds. Never compute epoch numbers yourself,
  let Postgres convert:
  - last N days: listened_at > extract(epoch from now()) - N*86400
  - one specific day: to_timestamp(listened_at)::date = '2026-07-17'
  - a date range: listened_at >= extract(epoch from timestamp '2026-07-17')
    AND listened_at < extract(epoch from timestamp '2026-07-18')
  - readable timestamps in results: to_timestamp(listened_at)
- tracks.duration is milliseconds.
- Match text on the *_name_norm columns. They are unidecode -> strip -> lower,
  so compare against lowercase ascii, e.g. artist_name_norm = 'beyonce'.
- scrobbles is the source of truth for actual listens. playcount/listeners on
  artists/albums/tracks are Last.fm's global numbers, user_playcount is Last.fm's
  count for this user.
- Denormalised artist_name/track_name/album_name on scrobbles allow display
  without JOINs, but JOIN through the id columns when you need metadata.
"""

async def describe_schema() -> str:
    """Describe the music database schema, its query rules and querying hints. Call this before writing any SQL."""
    return SCHEMA_DOC

DESCRIBE_SCHEMA_PARAMS = {"type": "object", "properties": {}, "required": []}

async def query_music_db(sql: str) -> dict:
    """Run a read only SELECT against the music database. Returns {columns, rows, row_count}, or {error} with the reason the query was rejected so it can be fixed and retried."""
    return await scrobble_client.run_sql(sql)

QUERY_MUSIC_DB_PARAMS = {
    "type": "object",
    "properties": {
        "sql": {"type": "string", "description": "A single SELECT statement."},
    },
    "required": ["sql"],
}

SUMMARY_PERIODS = ("7d", "30d", "365d", "all_time")
NEW_IN_TIMEFRAME_CAP = 20  # the raw 365d discovery lists alone are near 1MB

def _slim(value):
    """Drop image urls and other display only fields, they are dead weight for a model."""
    if isinstance(value, dict):
        return {k: _slim(v) for k, v in value.items() if "_image_" not in k and k != "duration_ms"}
    if isinstance(value, list):
        return [_slim(v) for v in value]
    return value

async def get_music_summary(period: str = "7d") -> dict:
    """Fetch curated listening stats for one period (7d, 30d, 365d or all_time): top artists/albums/tracks, listening clock, most active day, new discoveries and recent tracks. Cheaper than SQL for overview questions."""
    summary = await scrobble_client.get_music_summary()
    if "error" in summary:
        return summary

    picked = next((p for p in summary["periods"] if p["period"] == period), None)
    if picked is None:
        return {"error": f"Unknown period: {period}. Use one of {', '.join(SUMMARY_PERIODS)}."}

    picked = _slim(picked)
    new_in_timeframe = picked["stats"].get("new_in_timeframe") or {}
    for key in ("artists", "albums", "tracks"):
        if isinstance(new_in_timeframe.get(key), list):
            new_in_timeframe[key] = new_in_timeframe[key][:NEW_IN_TIMEFRAME_CAP]

    return {
        "generated_at": summary["generated_at"],
        "last_synced_at": summary["last_synced_at"],
        **picked,
    }

GET_MUSIC_SUMMARY_PARAMS = {
    "type": "object",
    "properties": {
        "period": {
            "type": "string",
            "enum": list(SUMMARY_PERIODS),
            "description": "Time period to fetch, defaults to 7d.",
        },
    },
    "required": [],
}

TOOL_HANDLERS = {
    "describe_schema": describe_schema,
    "query_music_db": query_music_db,
    "get_music_summary": get_music_summary,
}

TOOL_PARAMS = {
    "describe_schema": DESCRIBE_SCHEMA_PARAMS,
    "query_music_db": QUERY_MUSIC_DB_PARAMS,
    "get_music_summary": GET_MUSIC_SUMMARY_PARAMS,
}

# The descriptions come from the docstrings so the two adapters cannot drift apart
OPENAI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": name,
            "description": fn.__doc__,
            "parameters": TOOL_PARAMS[name],
        },
    }
    for name, fn in TOOL_HANDLERS.items()
]

async def dispatch_tool(name: str, arguments: dict) -> str:
    """Run one tool call by name, always handing a string back to the model."""
    handler = TOOL_HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"Unknown tool: {name}"})
    result = await handler(**arguments)
    return result if isinstance(result, str) else json.dumps(result)
