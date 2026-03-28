SELECT
	s.artist_name,
	s.album_name,
	COUNT(*)::int AS plays,
	MIN(s.listened_at)::bigint AS first_listened_at,
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
