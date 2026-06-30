SELECT
	EXTRACT(ISODOW FROM to_timestamp(s.listened_at))::int AS weekday_index,
	COUNT(*)::int AS scrobbles,
	COALESCE(SUM(t.duration), 0)::bigint AS duration_ms
FROM scrobbles s
LEFT JOIN tracks t
	ON t.artist_name_norm = LOWER(TRIM(s.artist_name))
	AND t.track_name_norm = LOWER(TRIM(s.track_name))
WHERE ($1::bigint IS NULL OR s.listened_at >= $1)
GROUP BY weekday_index
ORDER BY weekday_index ASC;