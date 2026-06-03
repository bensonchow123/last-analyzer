import logging
from datetime import UTC, datetime
from typing import Any

import asyncpg

from scrobble_vault.db import core
from scrobble_vault.db.sql_loader import load_sql

logger = logging.getLogger(__name__)

# Load SQL queries with the SQL loader to make it only load once
AGGREGATE_SQL = load_sql("music_summary", "aggregate")
ARTIST_PLAYS_SQL = load_sql("music_summary", "artist_plays")
NEW_ARTISTS_IN_TIMEFRAME_SQL = load_sql("music_summary", "new_artists_in_timeframe")
TRACK_PLAYS_SQL = load_sql("music_summary", "track_plays")
NEW_TRACKS_IN_TIMEFRAME_SQL = load_sql("music_summary", "new_tracks_in_timeframe")
ALBUM_PLAYS_SQL = load_sql("music_summary", "album_plays")
NEW_ALBUMS_IN_TIMEFRAME_SQL = load_sql("music_summary", "new_albums_in_timeframe")
LISTENING_CLOCK_SQL = load_sql("music_summary", "listening_clock")
MOST_ACTIVE_DAY_SQL = load_sql("music_summary", "most_active_day")
RECENT_TRACKS_SQL = load_sql("music_summary", "recent_tracks")

# Helper functions for the create summary function
def _window_start_ts(days: int | None) -> int | None:
	if days is None:
		return None
	now = datetime.now(UTC)
	return int(now.timestamp()) - days * 24 * 60 * 60


def _fmt_ts(unix_ts: int | None) -> str:
	if not unix_ts:
		return "n/a"
	return str(int(unix_ts))


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


async def _summary_for_period(period_key: str, days: int | None) -> dict[str, Any]:
	lower_bound = _window_start_ts(days)
	period_label = "All Time" if days is None else f"Last {days} Days"

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

			total_seconds = _ms_to_seconds(stats.pop("total_duration_ms"))
			missing_duration_count = stats.pop("missing_duration_count")
			stats["listening_time"] = {
				"total_seconds": total_seconds,
				"total_hhmmss": _seconds_to_hhmmss(total_seconds),
				"missing_duration_count": missing_duration_count,
			}

			clock_hours = []
			for row in clock_rows:
				total_seconds = _ms_to_seconds(row["duration_ms"])
				avg_seconds = int(total_seconds / row["scrobbles"]) if row["scrobbles"] > 0 else 0
				clock_hours.append(
					{
						"hour": row["hour"],
						"scrobbles": row["scrobbles"],
						"average_duration_seconds": avg_seconds,
						"average_duration_hhmmss": _seconds_to_hhmmss(avg_seconds),
					}
				)

			# Fill in missing hours with zeroes
			all_hours = {h: {"hour": h, "scrobbles": 0, "average_duration_seconds": 0, "average_duration_hhmmss": "00:00:00"} for h in range(24)}
			for hour_stat in clock_hours:
				all_hours[hour_stat["hour"]] = hour_stat
			clock_hours_full = [all_hours[h] for h in range(24)]

			peak_hour = max(clock_hours_full, key=lambda item: item["scrobbles"]) if clock_hours_full else None
			stats["listening_clock"] = {
				"peak_hour": peak_hour,
				"hours": clock_hours_full
			}

			if active_day:
				total_active_day_seconds = _ms_to_seconds(active_day["duration_ms"])
				avg_active_day_seconds = int(total_active_day_seconds / active_day["scrobbles"]) if active_day["scrobbles"] > 0 else 0
				stats["most_active_day"] = {
					"day": str(active_day["day"]),
					"scrobbles": active_day["scrobbles"],
					"average_duration_seconds": avg_active_day_seconds,
					"average_duration_hhmmss": _seconds_to_hhmmss(avg_active_day_seconds),
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
				"stats": stats
			}
	except (OSError, asyncpg.PostgresError):
		logger.exception("Failed generating music summary for period=%s", period_key)
		raise

# For each timeframe create a summary, skipping new in timeframe section for the 'all_time' time period
async def get_music_summaries() -> list[dict[str, Any]]:
	windows: list[tuple[str, int | None]] = [
		("7d", 7),
		("30d", 30),
		("365d", 365),
		("all_time", None),
	]
	return [await _summary_for_period(period_key, days) for period_key, days in windows]

