mod check_job;
mod commit_job;
mod job_executor;
mod job_instance;
mod job_queue;
mod publish_job;
mod republish_job;
mod update_repo_job;
mod utils;

pub use job_executor::start_job_executor;
pub use job_queue::{cleanup_started_jobs, JobQueue, ProcessJobs, StopJobQueue};

/**************************************************************************
 * Job handling - theory of operations.
 *
 * The job queue is stored in the database, and we regularly (on a
 * timer and triggered via in-process ProcessJobs messages) check for new
 * jobs and execute them.
 *
 * All jobs have a status which is:
 *   New - queued but not started
 *   Started - set when we start working on a job
 *   Ended - set when the job is done
 *   Broken - set when we get some internal error working on a job,
 *            or if an old job was marked "Started" already on startup.
 *
 * All jobs are run on single-threaded blocking actors, but we have
 * multiple of those. One per configured repo, and one for the builds.
 * This way we avoid any races with multiple things modifying a single
 * repo, but still allow concurrent build commits with a repo update.
 *
 * There is also an async JobQueue actor which manages the sync ones,
 * handling the queue of jobs and other messages such as StopJobs to
 * shut down things.
 *
 * Since job status is changed on multiple thread all the
 * modification/access to that is serialized in the db using serialized
 * transactions.
 *
 ************************************************************************/
