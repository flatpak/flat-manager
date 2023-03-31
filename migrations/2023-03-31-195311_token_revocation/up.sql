CREATE TABLE tokens (
    token_id TEXT NOT NULL PRIMARY KEY,
    expires TIMESTAMP,
    last_used TIMESTAMP,
    revoked_at TIMESTAMP
);