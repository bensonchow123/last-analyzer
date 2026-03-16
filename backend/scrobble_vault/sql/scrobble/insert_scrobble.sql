INSERT INTO scrobbles (
    track_id, listened_at,
    artist_name, track_name, album_name
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (track_id, listened_at) DO NOTHING;