CREATE TABLE builds (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    repo_state SMALLINT NOT NULL DEFAULT 0,
    repo_state_reason Text,
    published_state SMALLINT NOT NULL DEFAULT 0,
    published_state_reason Text
);
