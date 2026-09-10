-- auth_token is the opaque bearer credential a runner presents on Heartbeat (and future
-- authenticated RPCs). M0 mints it in RegisterRunner and stores the raw value here so the
-- server can validate it even across restarts (the in-memory token map is best-effort only).
-- Hashing this column is a later hardening task (see T-M0-05 notes).
ALTER TABLE runners ADD COLUMN auth_token text;
