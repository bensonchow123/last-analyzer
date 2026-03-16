UPDATE last_sync
SET value = $1, updated_at = $2
WHERE key = 'last_sync_time';