# Summary API Schema

This document describes the response for the `GET /music-summary` endpoint.

## JSON structure
```jsonc
{
  "generated_at": 0, // unix seconds (UTC), request time
  "last_synced_at": 0, // unix seconds (UTC) | null when never synced, last sync time with last.fm API
  "periods": [
    {
      "period": "7d | 30d | 365d | all_time",
      "label": "string",
      "stats": {
        "total_scrobbles": 0,
        "active_days": 0,
        "first_listened_at": 0, // unix seconds (UTC)
        "last_listened_at": 0, // unix seconds (UTC)
        "unique_artists_count": 0,
        "unique_tracks_count": 0,
        "unique_albums_count": 0,
        "listening_time": {}, // see below
        "listening_clock": {}, // see below
        "most_active_day": null, // most_active_day | null, see below
        "new_in_timeframe": {}, // new_in_timeframe (not present in `all_time` period), see below
        "top_artists": [], // see below
        "top_albums": [], // see below
        "top_tracks": [], // see below
        "recent_tracks": [] // see below
      }
    }
  ]
}
```

## Object shapes

```jsonc
{
  "listening_time": {
    "total_seconds": 0,
    "total_hhmmss": "HH:MM:SS",
    "missing_duration_count": 0
  },
  "listening_clock": {
    "peak_hour": { // if no peak_hour null
      "hour": 0,
      "scrobbles": 0,
      "average_duration_seconds": 0,
      "average_duration_hhmmss": "HH:MM:SS"
    },
    "hours": [
      {
        "hour": 0,
        "scrobbles": 0,
        "average_duration_seconds": 0,
        "average_duration_hhmmss": "HH:MM:SS"
      },
      // ... 24 objects, one for each hour (0-23), 0 is 12am
    ]
  },
  "most_active_day": {
    "day": "YYYY-MM-DD", // date string (UTC)
    "scrobbles": 0,
    "average_duration_seconds": 0,
    "average_duration_hhmmss": "HH:MM:SS"
  },
  "new_in_timeframe": {
    "artists_count": 0,
    "artists": [
      {
        "artist_name": "string",
        "plays": 0,
        "first_listened_at": 0,
        "artist_image_extralarge": "string | null",
        "artist_image_large": "string | null",
        "artist_image_medium": "string | null",
        "artist_image_small": "string | null"
      }
    ],
    "albums_count": 0,
    "albums": [
      {
        "artist_name": "string",
        "album_name": "string",
        "plays": 0,
        "first_listened_at": 0,
        "album_image_extralarge": "string | null",
        "album_image_large": "string | null",
        "album_image_medium": "string | null",
        "album_image_small": "string | null"
      }
    ],
    "tracks_count": 0,
    "tracks": [
      {
        "artist_name": "string",
        "track_name": "string",
        "plays": 0,
        "first_listened_at": 0,
        "duration_ms": 0,
        "duration_seconds": 0,
        "duration_hhmmss": "HH:MM:SS",
        "album_name": "string | null",
        "artist_image_extralarge": "string | null",
        "artist_image_large": "string | null",
        "artist_image_medium": "string | null",
        "artist_image_small": "string | null",
        "album_image_extralarge": "string | null",
        "album_image_large": "string | null",
        "album_image_medium": "string | null",
        "album_image_small": "string | null"
      }
    ]
  },
  "recent_tracks": [
    {
      "listened_at": 0, // unix seconds (UTC)
      "artist_name": "string",
      "track_name": "string",
      "album_name": "string | null",
      "duration_ms": 0,
      "duration_seconds": 0,
      "duration_hhmmss": "HH:MM:SS",
      "album_image_extralarge": "string | null",
      "album_image_large": "string | null",
      "album_image_medium": "string | null",
      "album_image_small": "string | null",
      "artist_image_extralarge": "string | null",
      "artist_image_large": "string | null",
      "artist_image_medium": "string | null",
      "artist_image_small": "string | null",
      "listened_at_iso": "ISO-8601" // UTC ISO-8601 timestamp
    },
    // ... (most recent 15 tracks or [])
  ]
}
```

## Conditions and edge cases
- `periods` always returns 4 items in this order: `7d`, `30d`, `365d`, `all_time`.
- `new_in_timeframe` exists only for `period` values `7d`, `30d`, and `365d`.
