WITH enriched_scrobbles AS (
	SELECT
		s.listened_at,
		s.artist_name,
		s.track_name,
		t.id AS track_id,
		t.artist_id,
		COALESCE(
			CASE WHEN t.id IS NOT NULL THEN 'track_id:' || t.id::text END,
			'pair:' || LOWER(TRIM(s.artist_name)) || '||' || LOWER(TRIM(s.track_name))
		) AS track_key
	FROM scrobbles s
	LEFT JOIN tracks t
		ON t.id = s.track_id
),
first_seen AS (
	SELECT
		track_key,
		MIN(listened_at)::bigint AS first_listened_at
	FROM enriched_scrobbles
	GROUP BY track_key
),
new_track_keys AS (
	SELECT
		fs.track_key,
		fs.first_listened_at
	FROM first_seen fs
	WHERE ($1::bigint IS NULL OR fs.first_listened_at >= $1)
)
SELECT
	COALESCE(MAX(t_by_id.artist_name), MIN(es.artist_name)) AS artist_name,
	COALESCE(MAX(t_by_id.name), MIN(es.track_name)) AS track_name,
	COUNT(*)::int AS plays,
	nk.first_listened_at,
	COALESCE(MAX(t_by_id.duration), 0)::bigint AS duration_ms,
	MAX(t_by_id.album_title) AS album_name,
	COALESCE(MAX(a_by_id.image_extralarge), MAX(a_by_name.image_extralarge)) AS artist_image_extralarge,
	COALESCE(MAX(a_by_id.image_large), MAX(a_by_name.image_large)) AS artist_image_large,
	COALESCE(MAX(a_by_id.image_medium), MAX(a_by_name.image_medium)) AS artist_image_medium,
	COALESCE(MAX(a_by_id.image_small), MAX(a_by_name.image_small)) AS artist_image_small,
	MAX(t_by_id.album_image_extralarge) AS album_image_extralarge,
	MAX(t_by_id.album_image_large) AS album_image_large,
	MAX(t_by_id.album_image_medium) AS album_image_medium,
	MAX(t_by_id.album_image_small) AS album_image_small
FROM enriched_scrobbles es
JOIN new_track_keys nk
	ON nk.track_key = es.track_key
LEFT JOIN tracks t_by_id
	ON t_by_id.id = es.track_id
LEFT JOIN artists a_by_id
	ON a_by_id.id = es.artist_id
LEFT JOIN artists a_by_name
	ON es.artist_id IS NULL
	AND a_by_name.artist_name_norm = LOWER(TRIM(es.artist_name))
WHERE ($1::bigint IS NULL OR es.listened_at >= $1)
GROUP BY nk.track_key, nk.first_listened_at
ORDER BY plays DESC, nk.first_listened_at DESC, artist_name ASC, track_name ASC;
