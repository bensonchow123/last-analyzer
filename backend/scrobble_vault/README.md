# Summary API Schema

This document describes the response for the `GET /music-summary` endpoint.

## JSON structure
```jsonc
{
  "generated_at": "ISO-8601", // UTC ISO-8601 timestamp
  "periods": [
    {
      "period": "7d | 30d | 365d | all_time",
      "label": "string",
      "template": "string",
      "stats": {
        "total_scrobbles": 0,
        "unique_artists_count": 0,
        "unique_tracks_count": 0,
        "unique_albums_count": 0,
        "active_days": 0,
        "first_listened_at": 0, // unix seconds (UTC)
        "last_listened_at": 0, // unix seconds (UTC)
        "listening_time": {}, // ListeningTime, see below
        "listening_clock": {}, // ListeningClock, see below
        "most_active_day": null, // MostActiveDay | null, see below
        "new_in_timeframe": {}, // NewInTimeframe (not present in `all_time` period), see below
        "top_artists": [], // ArtistSummary[], see below
        "top_albums": [], // AlbumSummary[], see below
        "top_tracks": [], // TrackSummary[], see below
        "recent_tracks": [] // RecentTrack[], see below
      }
    }
  ]
}
```

## Object shapes

```json
{
  "ListeningTime": {
    "total_seconds": 0,
    "total_hhmmss": "HH:MM:SS",
    "missing_duration_count": 0
  },
  "ListeningClock": {
    "peak_hour": "PeakHour | null"
  },
  "PeakHour": {
    "hour": 0,
    "scrobbles": 0,
    "average_duration_seconds": 0,
    "average_duration_hhmmss": "HH:MM:SS"
  },
  "MostActiveDay": {
    "day": "YYYY-MM-DD", // date string (UTC)
    "scrobbles": 0,
    "average_duration_seconds": 0,
    "average_duration_hhmmss": "HH:MM:SS"
  },
  "ArtistSummary": {
    "artist_name": "string",
    "plays": 0,
    "first_listened_at": 0, // unix seconds (UTC)
    "artist_image_extralarge": "string | null",
    "artist_image_large": "string | null",
    "artist_image_medium": "string | null",
    "artist_image_small": "string | null"
  },
  "AlbumSummary": {
    "artist_name": "string",
    "album_name": "string",
    "plays": 0,
    "first_listened_at": 0, // unix seconds (UTC)
    "album_image_extralarge": "string | null",
    "album_image_large": "string | null",
    "album_image_medium": "string | null",
    "album_image_small": "string | null"
  },
  "TrackSummary": {
    "artist_name": "string",
    "track_name": "string",
    "plays": 0,
    "first_listened_at": 0, // unix seconds (UTC)
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
  },
  "RecentTrack": {
    "listened_at": 0, // unix seconds (UTC)
    "listened_at_iso": "ISO-8601", // UTC ISO-8601 timestamp
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
    "artist_image_small": "string | null"
  },
  "NewInTimeframe": {
    "artists_count": 0,
    "artists": "ArtistSummary[]",
    "albums_count": 0,
    "albums": "AlbumSummary[]",
    "tracks_count": 0,
    "tracks": "TrackSummary[]"
  }
}
```

## Conditions and edge cases

- `periods` always returns 4 items in this order: `7d`, `30d`, `365d`, `all_time`.
- `new_in_timeframe` exists only for `period` values `7d`, `30d`, and `365d`.
- `first_listened_at` and `last_listened_at` are null when there are zero scrobbles in a period.
- `most_active_day` is null when there are zero scrobbles in a period.
- `listening_clock.peak_hour` is null when there are zero scrobbles in a period.
- `top_artists`, `top_albums`, `top_tracks`, and `recent_tracks` can be empty arrays.
- Image fields can be null even when names are present.
