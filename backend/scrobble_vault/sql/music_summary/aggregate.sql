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
