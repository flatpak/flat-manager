CREATE TABLE builds (
    id SERIAL PRIMARY KEY,
    is_published BOOL NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    repo_state SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    log_type SMALLINT NOT NULL,
    log_text TEXT NOT NULL
);

CREATE TABLE refs (
    id SERIAL PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    ref_type SMALLINT  NOT NULL,
    ref_name TEXT NOT NULL,
    commit TEXT NOT NULL);

CREATE UNIQUE INDEX refs_ref_name ON refs (ref_name, ref_type, build_id);
