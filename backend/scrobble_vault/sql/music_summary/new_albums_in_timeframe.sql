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
