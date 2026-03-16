CREATE TABLE IF NOT EXISTS scrobbles (
    id SERIAL PRIMARY KEY,
    track_id INTEGER REFERENCES tracks(id),
    listened_at BIGINT NOT NULL,
    artist_name TEXT NOT NULL,
    track_name TEXT NOT NULL,
    album_name TEXT
);