SELECT name, artist_name, release_date, user_playcount,
       1 - (embedding <=> $1) AS similarity
FROM albums
WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1
LIMIT $2;
