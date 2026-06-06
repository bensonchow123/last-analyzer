import logging
from datetime import UTC, datetime
from typing import Any
import math

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
LISTENING_WEEKDAY_SQL = load_sql("music_summary", "most_active_weekday")
MOST_ACTIVE_DAY_SQL = load_sql("music_summary", "most_active_day")
RECENT_TRACKS_SQL = load_sql("music_summary", "recent_tracks")

# Helper functions for the create summary function
def _window_start_ts(days: int | None) -> int | None:
	if days is None:
		return None
	# Calculate days including the current day
	now = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
	return int(now.timestamp()) - (days - 1) * 24 * 60 * 60

def _ms_to_seconds(duration_ms: int | None) -> int:
	if not duration_ms or duration_ms <= 0:
		return 0
	return int(duration_ms / 1000)


def _weekday_name(weekday_index: int) -> str:
	return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][weekday_index - 1]


def _format_duration_string(total_seconds: int) -> str:
	"""Return a compact human duration string, where Zero -> "0s".

	Examples:
	- 7200 -> "2h"
	- 7380 -> "2h 3m"
	- 7382 -> "2h 3m 2s"
	- 62 -> "1m 2s"
	- 45 -> "45s"
	- 0 -> "0s"
	"""
	hours = total_seconds // 3600
	minutes = (total_seconds % 3600) // 60
	seconds = total_seconds % 60

	if hours == 0 and minutes == 0 and seconds == 0:
		return "0s"

	parts: list[str] = []
	if hours:
		parts.append(f"{hours}h")
	if minutes:
		parts.append(f"{minutes}m")
	if seconds:
		parts.append(f"{seconds}s")

	return " ".join(parts)

# Add seconds/string duration fields for rows with duration_ms
def _with_duration_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
	updated_rows: list[dict[str, Any]] = []
	for row in rows:
		item = dict(row)
		seconds = _ms_to_seconds(item.get("duration_ms"))
		item["duration_seconds"] = seconds
		item["duration_string"] = _format_duration_string(seconds)
		updated_rows.append(item)
	return updated_rows


async def _summary_for_period(period_key: str, days: int | None) -> dict[str, Any]:
	lower_bound = _window_start_ts(days)
	period_label = "All Time" if days is None else f"Last {days} Days"

	try:
		async with core.ro_pool.acquire() as conn:
			# Fetch the aggregate row that drives most top level stats
			aggregate = await conn.fetchrow(AGGREGATE_SQL, lower_bound)
			if not aggregate or aggregate["total_scrobbles"] == 0:
				# build minimal JSON response for an empty timeframe
				empty_stats: dict[str, Any] = {
					"total_scrobbles": 0,
					"unique_artists_count": 0,
					"unique_tracks_count": 0,
					"unique_albums_count": 0,
					"active_days": 0,
					"listening_time": None,
					"listening_clock": None,
					"listening_weekday": None,
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

			# Fetch rows used to compose the JSON response sections below
			artist_rows = await conn.fetch(ARTIST_PLAYS_SQL, lower_bound)
			new_artist_rows = await conn.fetch(NEW_ARTISTS_IN_TIMEFRAME_SQL, lower_bound)
			track_rows = await conn.fetch(TRACK_PLAYS_SQL, lower_bound)
			new_track_rows = await conn.fetch(NEW_TRACKS_IN_TIMEFRAME_SQL, lower_bound)
			album_rows = await conn.fetch(ALBUM_PLAYS_SQL, lower_bound)
			new_album_rows = await conn.fetch(NEW_ALBUMS_IN_TIMEFRAME_SQL, lower_bound)
			clock_rows = await conn.fetch(LISTENING_CLOCK_SQL, lower_bound)
			weekday_rows = await conn.fetch(LISTENING_WEEKDAY_SQL, lower_bound)
			active_day = await conn.fetchrow(MOST_ACTIVE_DAY_SQL, lower_bound)
			recent_rows = await conn.fetch(RECENT_TRACKS_SQL, lower_bound)

			# Map aggregate DB columns into the `stats` JSON object which is per period
			stats = dict(aggregate)
			stats["unique_artists_count"] = stats.pop("unique_artists")
			stats["unique_tracks_count"] = stats.pop("unique_tracks")
			stats["unique_albums_count"] = stats.pop("unique_albums")

			total_seconds = _ms_to_seconds(stats.pop("total_duration_ms"))
			missing_duration_count = stats.pop("missing_duration_count")
			# Listening time section caculation, totals, formatted string and missing count
			stats["listening_time"] = {
				"total_seconds": total_seconds,
				"total_string": _format_duration_string(total_seconds),
				"missing_duration_count": missing_duration_count,
			}

			# Listening clock calculation: compute per-hour totals and average listening time per day
			# Denominator: number of days in the window, or active_days when all-time
			denom_days = days if days is not None else max(1, stats.get("active_days", 1))
			clock_hours = []
			for row in clock_rows:
				total_seconds = _ms_to_seconds(row["duration_ms"])
				# average listening seconds for this hour across the timeframe (seconds/day)
				avg_seconds = int(total_seconds / denom_days) if denom_days > 0 else 0
				avg_scrobbles = int(row["scrobbles"] / denom_days) if denom_days > 0 else 0
				clock_hours.append(
					{
						"hour": row["hour"],
						"average_scrobbles": avg_scrobbles,
						"average_listening_seconds": avg_seconds,
						"average_listening_string": _format_duration_string(avg_seconds),
					}
				)

			# Fill missing hours so the API always returns 24 hour entries
			all_hours = {h: {"hour": h, "average_scrobbles": 0, "average_listening_seconds": 0} for h in range(24)}
			for hour_stat in clock_hours:
				all_hours[hour_stat["hour"]] = hour_stat
			clock_hours_full = [all_hours[h] for h in range(24)]

			peak_hour = max(clock_hours_full, key=lambda item: item["average_listening_seconds"]) if clock_hours_full else None
			stats["listening_clock"] = {
				"peak_hour": peak_hour,
				"hours": clock_hours_full
			}

			# Listening weekday calculations, compute average listening time for the weekday across weeks
			# Estimate number of weeks in window by rounding up days/7, uses `active_days` for all_time
			weekday_stats = []
			weeks = math.ceil((days if days is not None else max(1, stats.get("active_days", 1))) / 7)
			for row in weekday_rows:
				total_seconds = _ms_to_seconds(row["duration_ms"])
				# average listening seconds for this weekday across the timeframe (seconds/weekday occurrence)
				avg_seconds = int(total_seconds / weeks) if weeks > 0 else 0
				avg_scrobbles = int(row["scrobbles"] / weeks) if weeks > 0 else 0
				weekday_stats.append(
					{
						"weekday_index": row["weekday_index"],
						"weekday": _weekday_name(row["weekday_index"]),
						"average_scrobbles": avg_scrobbles,
						"average_listening_seconds": avg_seconds,
						"average_listening_string": _format_duration_string(avg_seconds),
					}
				)

			# Ensure we always return 7 weekday entries
			all_weekdays = {
				weekday_index: {
					"weekday_index": weekday_index,
					"weekday": _weekday_name(weekday_index),
					"average_scrobbles": 0,
					"average_listening_seconds": 0,
					"average_listening_string": _format_duration_string(0),
				}
				for weekday_index in range(1, 8)
			}
			for weekday_stat in weekday_stats:
				all_weekdays[weekday_stat["weekday_index"]] = weekday_stat
			weekday_list_full = [all_weekdays[weekday_index] for weekday_index in range(1, 8)]
			peak_weekday = max(weekday_list_full, key=lambda item: item["average_listening_seconds"]) if weekday_list_full else None
			stats["listening_weekday"] = {
				"peak_day": peak_weekday,
				"days": weekday_list_full,
			}

			# Most active day calculation
			if active_day:
				total_active_day_seconds = _ms_to_seconds(active_day["duration_ms"])
				stats["most_active_day"] = {
					"day": str(active_day["day"]),
					"scrobbles": active_day["scrobbles"],
					"total_listening_seconds": total_active_day_seconds,
					"total_listening_string": _format_duration_string(total_active_day_seconds),
				}
			else:
				stats["most_active_day"] = None

			# New in timeframe calculation for artists/albums/tracks
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

			# Top lists calculation
			stats["top_artists"] = [dict(row) for row in artist_rows[:10]]
			stats["top_albums"] = [dict(row) for row in album_rows[:10]]
			stats["top_tracks"] = _with_duration_fields([dict(row) for row in track_rows[:10]])

			# Recent tracks preserve ordering and add duration fields
			recent_tracks: list[dict[str, Any]] = []
			for row in recent_rows:
				recent_item = dict(row)
				duration_seconds = _ms_to_seconds(recent_item.get("duration_ms"))
				recent_item["duration_seconds"] = duration_seconds
				recent_item["duration_string"] = _format_duration_string(duration_seconds)
				recent_tracks.append(recent_item)
			stats["recent_tracks"] = recent_tracks

			# Final JSON payload for this period
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
