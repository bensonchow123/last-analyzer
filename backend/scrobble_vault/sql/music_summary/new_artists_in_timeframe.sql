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
