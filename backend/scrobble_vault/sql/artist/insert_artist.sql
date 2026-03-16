INSERT INTO artists (
    name, artist_name_norm, mbid, url,
    image_small, image_medium, image_large, image_extralarge,
    streamable, listeners, playcount,
    similar_artists, tags,
    bio_published, bio_summary, bio_content,
    user_playcount,
    embedding
) VALUES (
    $1, $2, $3, $4,
    $5, $6, $7, $8,
    $9, $10, $11,
    $12, $13,
    $14, $15, $16,
    $17,
    $18
)
ON CONFLICT (artist_name_norm) DO NOTHING;