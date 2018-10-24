CREATE TABLE builds (
    id SERIAL PRIMARY KEY,
    is_published BOOL NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    repo_state SMALLINT NOT NULL DEFAULT 0,
    repo_state_reason Text);
