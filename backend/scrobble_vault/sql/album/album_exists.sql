SELECT 1
FROM albums
WHERE artist_name_norm = $1 AND album_name_norm = $2;