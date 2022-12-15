CREATE TABLE checks (
    check_name TEXT NOT NULL,
    build_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0,
    status_reason TEXT,
    results TEXT,

    PRIMARY KEY (check_name, build_id),
    FOREIGN KEY (build_id) REFERENCES builds(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    UNIQUE (job_id)
);

CREATE INDEX checks_build_id ON checks (build_id);