-- Remove the constraint for job kinds
ALTER TABLE jobs DROP CONSTRAINT chk_job_kind;

-- Add back the original constraint without the prune job type
ALTER TABLE jobs ADD CONSTRAINT chk_job_kind CHECK (kind IN (0, 1, 2, 3, 4)); -- Excluding 5 (Prune)
