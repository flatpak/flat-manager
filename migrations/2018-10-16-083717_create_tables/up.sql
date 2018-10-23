CREATE TABLE builds (
    id SERIAL PRIMARY KEY,
    is_published BOOL NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    repo_state SMALLINT NOT NULL DEFAULT 0);

CREATE TABLE commit_logs (
    id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    text TEXT NOT NULL);

CREATE TABLE build_refs (
    id SERIAL PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    ref_name TEXT NOT NULL,
    commit TEXT NOT NULL);

CREATE TABLE published_refs (
    id SERIAL PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    ref_name TEXT NOT NULL,
    commit TEXT NOT NULL);

CREATE UNIQUE INDEX build_refs_index ON build_refs (ref_name, build_id);
CREATE UNIQUE INDEX published_refs_index ON published_refs (ref_name, build_id);
