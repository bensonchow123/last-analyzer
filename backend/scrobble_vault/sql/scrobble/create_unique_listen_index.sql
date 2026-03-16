CREATE UNIQUE INDEX IF NOT EXISTS scrobbles_unique_listen
ON scrobbles (track_id, listened_at);