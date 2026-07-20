SELECT name, artist_name, album_title, user_playcount,
       1 - (embedding <=> $1) AS similarity
FROM tracks
WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1
LIMIT $2;
