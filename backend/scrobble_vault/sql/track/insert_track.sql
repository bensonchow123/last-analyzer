INSERT INTO tracks (
    name, track_name_norm, mbid, url, duration,
    streamable, streamable_fulltrack,
    artist_id, artist_name, artist_name_norm, artist_mbid, artist_url,
    album_id, album_title, album_artist, album_mbid, album_url, album_position,
    album_image_small, album_image_medium, album_image_large, album_image_extralarge,
    toptags,
    wiki_published, wiki_summary, wiki_content,
    user_loved, user_playcount,
    embedding
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7,
    $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18,
    $19, $20, $21, $22,
    $23,
    $24, $25, $26,
    $27, $28,
    $29
)
ON CONFLICT (artist_name_norm, track_name_norm) DO NOTHING;