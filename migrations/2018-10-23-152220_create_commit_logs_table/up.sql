CREATE TABLE commit_logs (
    id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    text TEXT NOT NULL);
