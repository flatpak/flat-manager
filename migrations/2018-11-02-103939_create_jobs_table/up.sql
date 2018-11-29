CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    kind SMALLINT NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL DEFAULT 0, -- 0 == unstarted, 1 == in progress, 2 == completed, 2 == broken
    contents TEXT NOT NULL,
    results TEXT,
    log TEXT NOT NULL DEFAULT ''
);

CREATE TABLE job_dependencies (
    job_id INTEGER NOT NULL,
    depends_on INTEGER NOT NULL,
    PRIMARY KEY (job_id, depends_on),
    FOREIGN KEY (job_id)  REFERENCES  jobs (id),
    FOREIGN KEY (depends_on) REFERENCES jobs (id)
);

CREATE VIEW job_dependencies_with_status AS
  SELECT
        job_dependencies.job_id as job_id,
        job_dependencies.depends_on AS depends_on,
        jobs.status as dependant_status
  FROM job_dependencies
       INNER JOIN jobs
               ON job_dependencies.depends_on = jobs.id;


ALTER TABLE builds ADD commit_job_id INTEGER REFERENCES jobs (id);
ALTER TABLE builds ADD publish_job_id INTEGER REFERENCES jobs (id);
