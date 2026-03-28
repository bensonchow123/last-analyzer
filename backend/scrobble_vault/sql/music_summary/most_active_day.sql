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
