SELECT name, listeners, playcount, user_playcount,
       1 - (embedding <=> $1) AS similarity
FROM artists
WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1
LIMIT $2;
