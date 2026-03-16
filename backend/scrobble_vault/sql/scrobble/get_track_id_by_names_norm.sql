SELECT id
FROM tracks
WHERE artist_name_norm = $1 AND track_name_norm = $2;