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
