CREATE UNIQUE INDEX IF NOT EXISTS tracks_unique_identity
ON tracks (artist_name_norm, track_name_norm);