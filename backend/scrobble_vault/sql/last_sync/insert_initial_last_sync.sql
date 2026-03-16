INSERT INTO last_sync (key, value, updated_at)
VALUES ('last_sync_time', 0, 0)
ON CONFLICT (key) DO NOTHING;