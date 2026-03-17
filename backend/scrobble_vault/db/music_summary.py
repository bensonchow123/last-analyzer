import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg

from scrobble_vault.db import core

logger = logging.getLogger(__name__)


AGGREGATE_SQL = """
WITH filtered AS (
	SELECT
		s.listened_at,
		s.artist_name,
		s.track_name,
		s.album_name,
		t.duration AS duration_ms
	FROM scrobbles s
	LEFT JOIN tracks t
		ON t.artist_name_norm = LOWER(TRIM(s.artist_name))
		AND t.track_name_norm = LOWER(TRIM(s.track_name))
	WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
)
SELECT
	COUNT(*)::int AS total_scrobbles,
	COUNT(DISTINCT artist_name)::int AS unique_artists,
	COUNT(DISTINCT (artist_name, track_name))::int AS unique_tracks,
	COUNT(DISTINCT NULLIF(album_name, ''))::int AS unique_albums,
	COUNT(DISTINCT to_timestamp(listened_at)::date)::int AS active_days,
	COALESCE(SUM(duration_ms), 0)::bigint AS total_duration_ms,
	COUNT(*) FILTER (WHERE duration_ms IS NULL OR duration_ms <= 0)::int AS missing_duration_count,
	MIN(listened_at)::bigint AS first_listened_at,
	MAX(listened_at)::bigint AS last_listened_at
FROM filtered;
"""

ARTIST_PLAYS_SQL = """
SELECT
	s.artist_name,
	COUNT(*)::int AS plays,
	MAX(a.image_extralarge) AS artist_image_extralarge,
	MAX(a.image_large) AS artist_image_large,
	MAX(a.image_medium) AS artist_image_medium,
	MAX(a.image_small) AS artist_image_small
FROM scrobbles s
LEFT JOIN artists a
	ON a.artist_name_norm = LOWER(TRIM(s.artist_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
GROUP BY s.artist_name
ORDER BY plays DESC, s.artist_name ASC;
"""

NEW_ARTISTS_IN_TIMEFRAME_SQL = """
WITH enriched_scrobbles AS (
	SELECT
		s.listened_at,
		s.artist_name,
		t.artist_id,
		COALESCE(
			CASE WHEN t.artist_id IS NOT NULL THEN 'artist_id:' || t.artist_id::text END,
			'name:' || LOWER(TRIM(s.artist_name))
		) AS artist_key
	FROM scrobbles s
	LEFT JOIN tracks t
		ON t.id = s.track_id
),
first_seen AS (
	SELECT
		artist_key,
		MIN(listened_at)::bigint AS first_listened_at
	FROM enriched_scrobbles
	GROUP BY artist_key
),
new_artist_keys AS (
	SELECT
		fs.artist_key,
		fs.first_listened_at
	FROM first_seen fs
	WHERE ($1::bigint IS NULL OR fs.first_listened_at >= $1)
)
SELECT
	COALESCE(
		MAX(a_by_id.name),
		MAX(a_by_name.name),
		MIN(es.artist_name)
	) AS artist_name,
	COUNT(*)::int AS plays,
	nak.first_listened_at,
	COALESCE(MAX(a_by_id.image_extralarge), MAX(a_by_name.image_extralarge)) AS artist_image_extralarge,
	COALESCE(MAX(a_by_id.image_large), MAX(a_by_name.image_large)) AS artist_image_large,
	COALESCE(MAX(a_by_id.image_medium), MAX(a_by_name.image_medium)) AS artist_image_medium,
	COALESCE(MAX(a_by_id.image_small), MAX(a_by_name.image_small)) AS artist_image_small
FROM enriched_scrobbles es
JOIN new_artist_keys nak
	ON nak.artist_key = es.artist_key
LEFT JOIN artists a_by_id
	ON a_by_id.id = es.artist_id
LEFT JOIN artists a_by_name
	ON es.artist_id IS NULL
	AND a_by_name.artist_name_norm = LOWER(TRIM(es.artist_name))
WHERE ($1::bigint IS NULL OR es.listened_at >= $1)
GROUP BY nak.artist_key, nak.first_listened_at
ORDER BY nak.first_listened_at DESC, plays DESC, artist_name ASC;
"""

TRACK_PLAYS_SQL = """
SELECT
	s.artist_name,
	s.track_name,
	COUNT(*)::int AS plays,
	COALESCE(MAX(t.duration), 0)::bigint AS duration_ms,
	MAX(t.album_title) AS album_name,
	MAX(a.image_extralarge) AS artist_image_extralarge,
	MAX(a.image_large) AS artist_image_large,
	MAX(a.image_medium) AS artist_image_medium,
	MAX(a.image_small) AS artist_image_small,
	MAX(t.album_image_extralarge) AS album_image_extralarge,
	MAX(t.album_image_large) AS album_image_large,
	MAX(t.album_image_medium) AS album_image_medium,
	MAX(t.album_image_small) AS album_image_small
FROM scrobbles s
LEFT JOIN tracks t
	ON t.artist_name_norm = LOWER(TRIM(s.artist_name))
	AND t.track_name_norm = LOWER(TRIM(s.track_name))
LEFT JOIN artists a
	ON a.artist_name_norm = LOWER(TRIM(s.artist_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
GROUP BY s.artist_name, s.track_name
ORDER BY plays DESC, s.artist_name ASC, s.track_name ASC;
"""

NEW_TRACKS_IN_TIMEFRAME_SQL = """
WITH enriched_scrobbles AS (
	SELECT
		s.listened_at,
		s.artist_name,
		s.track_name,
		t.id AS track_id,
		t.artist_id,
		COALESCE(
			CASE WHEN t.id IS NOT NULL THEN 'track_id:' || t.id::text END,
			'pair:' || LOWER(TRIM(s.artist_name)) || '||' || LOWER(TRIM(s.track_name))
		) AS track_key
	FROM scrobbles s
	LEFT JOIN tracks t
		ON t.id = s.track_id
),
first_seen AS (
	SELECT
		track_key,
		MIN(listened_at)::bigint AS first_listened_at
	FROM enriched_scrobbles
	GROUP BY track_key
),
new_track_keys AS (
	SELECT
		fs.track_key,
		fs.first_listened_at
	FROM first_seen fs
	WHERE ($1::bigint IS NULL OR fs.first_listened_at >= $1)
)
SELECT
	COALESCE(MAX(t_by_id.artist_name), MIN(es.artist_name)) AS artist_name,
	COALESCE(MAX(t_by_id.name), MIN(es.track_name)) AS track_name,
	COUNT(*)::int AS plays,
	nk.first_listened_at,
	COALESCE(MAX(t_by_id.duration), 0)::bigint AS duration_ms,
	MAX(t_by_id.album_title) AS album_name,
	COALESCE(MAX(a_by_id.image_extralarge), MAX(a_by_name.image_extralarge)) AS artist_image_extralarge,
	COALESCE(MAX(a_by_id.image_large), MAX(a_by_name.image_large)) AS artist_image_large,
	COALESCE(MAX(a_by_id.image_medium), MAX(a_by_name.image_medium)) AS artist_image_medium,
	COALESCE(MAX(a_by_id.image_small), MAX(a_by_name.image_small)) AS artist_image_small,
	MAX(t_by_id.album_image_extralarge) AS album_image_extralarge,
	MAX(t_by_id.album_image_large) AS album_image_large,
	MAX(t_by_id.album_image_medium) AS album_image_medium,
	MAX(t_by_id.album_image_small) AS album_image_small
FROM enriched_scrobbles es
JOIN new_track_keys nk
	ON nk.track_key = es.track_key
LEFT JOIN tracks t_by_id
	ON t_by_id.id = es.track_id
LEFT JOIN artists a_by_id
	ON a_by_id.id = es.artist_id
LEFT JOIN artists a_by_name
	ON es.artist_id IS NULL
	AND a_by_name.artist_name_norm = LOWER(TRIM(es.artist_name))
WHERE ($1::bigint IS NULL OR es.listened_at >= $1)
GROUP BY nk.track_key, nk.first_listened_at
ORDER BY nk.first_listened_at DESC, plays DESC, artist_name ASC, track_name ASC;
"""

ALBUM_PLAYS_SQL = """
SELECT
	s.artist_name,
	s.album_name,
	COUNT(*)::int AS plays,
	MAX(al.image_extralarge) AS album_image_extralarge,
	MAX(al.image_large) AS album_image_large,
	MAX(al.image_medium) AS album_image_medium,
	MAX(al.image_small) AS album_image_small
FROM scrobbles s
LEFT JOIN albums al
	ON al.artist_name_norm = LOWER(TRIM(s.artist_name))
	AND al.album_name_norm = LOWER(TRIM(s.album_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
  AND COALESCE(s.album_name, '') <> ''
GROUP BY s.artist_name, s.album_name
ORDER BY plays DESC, s.artist_name ASC, s.album_name ASC;
"""

NEW_ALBUMS_IN_TIMEFRAME_SQL = """
WITH enriched_scrobbles AS (
	SELECT
		s.listened_at,
		s.artist_name,
		s.album_name,
		t.album_id,
		COALESCE(
			CASE WHEN t.album_id IS NOT NULL THEN 'album_id:' || t.album_id::text END,
			'pair:' || LOWER(TRIM(s.artist_name)) || '||' || LOWER(TRIM(s.album_name))
		) AS album_key
	FROM scrobbles s
	LEFT JOIN tracks t
		ON t.id = s.track_id
	WHERE COALESCE(s.album_name, '') <> ''
),
first_seen AS (
	SELECT
		album_key,
		MIN(listened_at)::bigint AS first_listened_at
	FROM enriched_scrobbles
	GROUP BY album_key
),
new_album_keys AS (
	SELECT
		fs.album_key,
		fs.first_listened_at
	FROM first_seen fs
	WHERE ($1::bigint IS NULL OR fs.first_listened_at >= $1)
)
SELECT
	COALESCE(MAX(al_by_id.artist_name), MIN(es.artist_name)) AS artist_name,
	COALESCE(MAX(al_by_id.name), MIN(es.album_name)) AS album_name,
	COUNT(*)::int AS plays,
	nak.first_listened_at,
	MAX(al_by_id.image_extralarge) AS album_image_extralarge,
	MAX(al_by_id.image_large) AS album_image_large,
	MAX(al_by_id.image_medium) AS album_image_medium,
	MAX(al_by_id.image_small) AS album_image_small
FROM enriched_scrobbles es
JOIN new_album_keys nak
	ON nak.album_key = es.album_key
LEFT JOIN albums al_by_id
	ON al_by_id.id = es.album_id
WHERE ($1::bigint IS NULL OR es.listened_at >= $1)
GROUP BY nak.album_key, nak.first_listened_at
ORDER BY nak.first_listened_at DESC, plays DESC, artist_name ASC, album_name ASC;
"""

LISTENING_CLOCK_SQL = """
SELECT
	EXTRACT(HOUR FROM to_timestamp(s.listened_at))::int AS hour,
	COUNT(*)::int AS scrobbles,
	COALESCE(SUM(t.duration), 0)::bigint AS duration_ms
FROM scrobbles s
LEFT JOIN tracks t
	ON t.artist_name_norm = LOWER(TRIM(s.artist_name))
	AND t.track_name_norm = LOWER(TRIM(s.track_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
GROUP BY hour
ORDER BY hour ASC;
"""

MOST_ACTIVE_DAY_SQL = """
SELECT
	to_timestamp(s.listened_at)::date AS day,
	COUNT(*)::int AS scrobbles,
	COALESCE(SUM(t.duration), 0)::bigint AS duration_ms
FROM scrobbles s
LEFT JOIN tracks t
	ON t.artist_name_norm = LOWER(TRIM(s.artist_name))
	AND t.track_name_norm = LOWER(TRIM(s.track_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
GROUP BY day
ORDER BY duration_ms DESC, scrobbles DESC, day DESC
LIMIT 1;
"""

RECENT_TRACKS_SQL = """
SELECT
	s.listened_at,
	s.artist_name,
	s.track_name,
	s.album_name,
	COALESCE(t.duration, 0)::bigint AS duration_ms,
	MAX(t.album_image_extralarge) AS album_image_extralarge,
	MAX(t.album_image_large) AS album_image_large,
	MAX(t.album_image_medium) AS album_image_medium,
	MAX(t.album_image_small) AS album_image_small,
	MAX(a.image_extralarge) AS artist_image_extralarge,
	MAX(a.image_large) AS artist_image_large,
	MAX(a.image_medium) AS artist_image_medium,
	MAX(a.image_small) AS artist_image_small
FROM scrobbles s
LEFT JOIN tracks t
	ON t.artist_name_norm = LOWER(TRIM(s.artist_name))
	AND t.track_name_norm = LOWER(TRIM(s.track_name))
LEFT JOIN artists a
	ON a.artist_name_norm = LOWER(TRIM(s.artist_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
GROUP BY s.listened_at, s.artist_name, s.track_name, s.album_name, t.duration
ORDER BY s.listened_at DESC
LIMIT 15;
"""


def _window_start_ts(days: int | None) -> int | None:
	if days is None:
		return None
	now = datetime.now(UTC)
	return int(now.timestamp()) - days * 24 * 60 * 60


def _fmt_ts(unix_ts: int | None) -> str:
	if not unix_ts:
		return "n/a"
	return datetime.fromtimestamp(unix_ts, UTC).strftime("%Y-%m-%d")


def _ms_to_seconds(duration_ms: int | None) -> int:
	if not duration_ms or duration_ms <= 0:
		return 0
	return int(duration_ms / 1000)


def _seconds_to_hhmmss(total_seconds: int) -> str:
	hours = total_seconds // 3600
	minutes = (total_seconds % 3600) // 60
	seconds = total_seconds % 60
	return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _with_duration_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
	updated_rows: list[dict[str, Any]] = []
	for row in rows:
		item = dict(row)
		seconds = _ms_to_seconds(item.get("duration_ms"))
		item["duration_seconds"] = seconds
		item["duration_hhmmss"] = _seconds_to_hhmmss(seconds)
		updated_rows.append(item)
	return updated_rows


def _build_template(period_label: str, stats: dict[str, Any]) -> str:
	active_days = stats["active_days"] or 0
	total_scrobbles = stats["total_scrobbles"] or 0
	avg_daily = (total_scrobbles / active_days) if active_days else 0.0
	total_listening_seconds = stats["listening_time"]["total_seconds"]

	top_artist = stats.get("top_artists", [None])[0]
	top_artist_line = (
		f"{top_artist['artist_name']} ({top_artist['plays']} plays)"
		if top_artist
		else "n/a"
	)

	top_track = stats.get("top_tracks", [None])[0]
	top_track_line = (
		f"{top_track['artist_name']} — {top_track['track_name']} ({top_track['plays']} plays)"
		if top_track
		else "n/a"
	)

	top_album = stats.get("top_albums", [None])[0]
	top_album_line = (
		f"{top_album['artist_name']} — {top_album['album_name']} ({top_album['plays']} plays)"
		if top_album
		else "n/a"
	)

	most_active_day = stats.get("most_active_day")
	most_active_day_line = (
		f"{most_active_day['day']} ({most_active_day['duration_hhmmss']}, {most_active_day['scrobbles']} scrobbles)"
		if most_active_day
		else "n/a"
	)

	peak_hour = stats["listening_clock"].get("peak_hour")
	peak_hour_line = (
		f"{peak_hour['hour']:02d}:00 ({peak_hour['scrobbles']} scrobbles)"
		if peak_hour
		else "n/a"
	)

	return (
		f"Music Summary ({period_label})\n"
		f"- Range: {_fmt_ts(stats['first_listened_at'])} to {_fmt_ts(stats['last_listened_at'])}\n"
		f"- Total scrobbles: {total_scrobbles}\n"
		f"- Unique artists: {stats['unique_artists_count']}\n"
		f"- Unique tracks: {stats['unique_tracks_count']}\n"
		f"- Unique albums: {stats['unique_albums_count']}\n"
		f"- Active days: {active_days}\n"
		f"- Average scrobbles/day: {avg_daily:.2f}\n"
		f"- Listening time: {stats['listening_time']['total_hhmmss']} ({total_listening_seconds} sec)\n"
		f"- Listening clock peak: {peak_hour_line}\n"
		f"- Most active day: {most_active_day_line}\n"
		f"- Top artist: {top_artist_line}\n"
		f"- Top track: {top_track_line}\n"
		f"- Top album: {top_album_line}"
	)


async def _summary_for_period(period_key: str, days: int | None) -> dict[str, Any]:
	lower_bound = _window_start_ts(days)
	period_label = "all time" if days is None else f"last {days} days"

	try:
		async with core.ro_pool.acquire() as conn:
			aggregate = await conn.fetchrow(AGGREGATE_SQL, lower_bound)
			if not aggregate or aggregate["total_scrobbles"] == 0:
				empty_stats: dict[str, Any] = {
					"total_scrobbles": 0,
					"unique_artists_count": 0,
					"unique_tracks_count": 0,
					"unique_albums_count": 0,
					"active_days": 0,
					"listening_time": {
						"total_seconds": 0,
						"total_hhmmss": "00:00:00",
						"missing_duration_count": 0,
					},
					"listening_clock": {
						"peak_hour": None,
					},
					"most_active_day": None,
					"play_source": "scrobbles",
					"top_artists": [],
					"top_albums": [],
					"top_tracks": [],
					"recent_tracks": [],
					"first_listened_at": None,
					"last_listened_at": None,
				}

				if days is not None:
					empty_stats["new_in_timeframe"] = {
						"artists_count": 0,
						"artists": [],
						"albums_count": 0,
						"albums": [],
						"tracks_count": 0,
						"tracks": [],
					}

				return {
					"period": period_key,
					"label": period_label,
					"stats": empty_stats,
					"template": f"Music Summary ({period_label})\n- No scrobbles found in this period.",
				}

			artist_rows = await conn.fetch(ARTIST_PLAYS_SQL, lower_bound)
			new_artist_rows = await conn.fetch(NEW_ARTISTS_IN_TIMEFRAME_SQL, lower_bound)
			track_rows = await conn.fetch(TRACK_PLAYS_SQL, lower_bound)
			new_track_rows = await conn.fetch(NEW_TRACKS_IN_TIMEFRAME_SQL, lower_bound)
			album_rows = await conn.fetch(ALBUM_PLAYS_SQL, lower_bound)
			new_album_rows = await conn.fetch(NEW_ALBUMS_IN_TIMEFRAME_SQL, lower_bound)
			clock_rows = await conn.fetch(LISTENING_CLOCK_SQL, lower_bound)
			active_day = await conn.fetchrow(MOST_ACTIVE_DAY_SQL, lower_bound)
			recent_rows = await conn.fetch(RECENT_TRACKS_SQL, lower_bound)

			stats = dict(aggregate)
			stats["unique_artists_count"] = stats.pop("unique_artists")
			stats["unique_tracks_count"] = stats.pop("unique_tracks")
			stats["unique_albums_count"] = stats.pop("unique_albums")

			total_seconds = _ms_to_seconds(stats["total_duration_ms"])
			stats["listening_time"] = {
				"total_seconds": total_seconds,
				"total_hhmmss": _seconds_to_hhmmss(total_seconds),
				"missing_duration_count": stats["missing_duration_count"],
			}
			stats["play_source"] = "scrobbles"

			clock_hours = []
			for row in clock_rows:
				hour_seconds = _ms_to_seconds(row["duration_ms"])
				clock_hours.append(
					{
						"hour": row["hour"],
						"scrobbles": row["scrobbles"],
						"duration_seconds": hour_seconds,
						"duration_hhmmss": _seconds_to_hhmmss(hour_seconds),
					}
				)

			peak_hour = max(clock_hours, key=lambda item: item["scrobbles"]) if clock_hours else None
			stats["listening_clock"] = {
				"peak_hour": peak_hour,
			}

			if active_day:
				active_day_seconds = _ms_to_seconds(active_day["duration_ms"])
				stats["most_active_day"] = {
					"day": str(active_day["day"]),
					"scrobbles": active_day["scrobbles"],
					"duration_seconds": active_day_seconds,
					"duration_hhmmss": _seconds_to_hhmmss(active_day_seconds),
				}
			else:
				stats["most_active_day"] = None

			unique_artist_list = [dict(row) for row in new_artist_rows]
			new_album_list = [dict(row) for row in new_album_rows]
			new_track_list = _with_duration_fields([dict(row) for row in new_track_rows])

			if days is not None:
				stats["new_in_timeframe"] = {
					"artists_count": len(unique_artist_list),
					"artists": unique_artist_list,
					"albums_count": len(new_album_list),
					"albums": new_album_list,
					"tracks_count": len(new_track_list),
					"tracks": new_track_list,
				}

			stats["top_artists"] = [dict(row) for row in artist_rows[:10]]
			stats["top_albums"] = [dict(row) for row in album_rows[:10]]
			stats["top_tracks"] = _with_duration_fields([dict(row) for row in track_rows[:10]])

			recent_tracks: list[dict[str, Any]] = []
			for row in recent_rows:
				recent_item = dict(row)
				duration_seconds = _ms_to_seconds(recent_item.get("duration_ms"))
				recent_item["duration_seconds"] = duration_seconds
				recent_item["duration_hhmmss"] = _seconds_to_hhmmss(duration_seconds)
				recent_item["listened_at_iso"] = datetime.fromtimestamp(
					recent_item["listened_at"], UTC
				).isoformat()
				recent_tracks.append(recent_item)
			stats["recent_tracks"] = recent_tracks

			return {
				"period": period_key,
				"label": period_label,
				"stats": stats,
				"template": _build_template(period_label, stats),
			}
	except (OSError, asyncpg.PostgresError):
		logger.exception("Failed generating music summary for period=%s", period_key)
		raise


async def get_music_summaries() -> list[dict[str, Any]]:
	windows: list[tuple[str, int | None]] = [
		("7d", 7),
		("30d", 30),
		("365d", 365),
		("all_time", None),
	]
	return [await _summary_for_period(period_key, days) for period_key, days in windows]

