SELECT
	s.artist_name,
	COUNT(*)::int AS plays,
	MIN(s.listened_at)::bigint AS first_listened_at,
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
