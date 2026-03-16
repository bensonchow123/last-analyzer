INSERT INTO albums (
    name, album_name_norm, mbid, url, release_date,
    artist_id, artist_name, artist_name_norm,
    image_small, image_medium, image_large, image_extralarge,
    listeners, playcount,
    toptags, tracks,
    wiki_published, wiki_summary, wiki_content,
    user_playcount,
    embedding
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8,
    $9, $10, $11, $12,
    $13, $14,
    $15, $16,
    $17, $18, $19,
    $20,
    $21
)
ON CONFLICT (artist_name_norm, album_name_norm) DO NOTHING;