-- Migration to add the "prune" job type to the jobs table

ALTER TABLE jobs
ADD CONSTRAINT chk_job_kind CHECK (kind IN (0, 1, 2, 3, 4, 5)); -- 0=commit, 1=publish, 2=updaterepo, 3=republish, 4=check, 5=prune
