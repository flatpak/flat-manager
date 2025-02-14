-- Migration to add the "prune" job type to the jobs table

ALTER TABLE jobs
ADD CONSTRAINT chk_job_kind CHECK (kind IN (0, 1, 2, 3, 4)); -- Assuming 0-3 are existing job types, 4 is for "prune"
