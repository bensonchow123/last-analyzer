CREATE UNIQUE INDEX IF NOT EXISTS artists_unique_identity
ON artists (artist_name_norm);