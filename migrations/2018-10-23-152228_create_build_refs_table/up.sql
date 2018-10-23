CREATE TABLE build_refs (
    id SERIAL PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    ref_name TEXT NOT NULL,
    commit TEXT NOT NULL);

CREATE UNIQUE INDEX build_refs_index ON build_refs (ref_name, build_id);
