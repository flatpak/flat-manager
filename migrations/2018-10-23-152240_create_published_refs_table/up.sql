CREATE TABLE published_refs (
    id SERIAL PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id),
    ref_name TEXT NOT NULL,
    commit TEXT NOT NULL);

CREATE UNIQUE INDEX published_refs_index ON published_refs (ref_name, build_id);
