CREATE UNIQUE INDEX IF NOT EXISTS albums_unique_identity
ON albums (artist_name_norm, album_name_norm);