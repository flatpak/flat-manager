ALTER TABLE builds DROP COLUMN commit_job_id;
ALTER TABLE builds DROP COLUMN publish_job_id;

drop view job_dependencies_with_status;
drop table job_dependencies;
drop table jobs;
