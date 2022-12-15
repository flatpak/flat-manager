use crate::errors::{JobError, JobResult};
use crate::models::{Job, JobKind};
use diesel::pg::PgConnection;

use super::check_job::CheckJobInstance;
use super::commit_job::CommitJobInstance;
use super::job_executor::JobExecutor;
use super::publish_job::PublishJobInstance;
use super::republish_job::RepublishJobInstance;
use super::update_repo_job::UpdateRepoJobInstance;

pub fn new_job_instance(executor: &JobExecutor, job: Job) -> Box<dyn JobInstance> {
    match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => CommitJobInstance::new(job),
        Some(JobKind::Publish) => PublishJobInstance::new(job),
        Some(JobKind::UpdateRepo) => {
            UpdateRepoJobInstance::new(job, executor.delta_generator.clone())
        }
        Some(JobKind::Republish) => RepublishJobInstance::new(job),
        Some(JobKind::Check) => CheckJobInstance::new(job),
        _ => InvalidJobInstance::new(job, JobError::new("Unknown job type")),
    }
}

pub trait JobInstance {
    fn get_job_id(&self) -> i32;
    fn order(&self) -> i32 {
        0
    }
    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &PgConnection,
    ) -> JobResult<serde_json::Value>;
}

pub struct InvalidJobInstance {
    pub job_id: i32,
    pub error: JobError,
}

impl InvalidJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job, error: JobError) -> Box<dyn JobInstance> {
        Box::new(InvalidJobInstance {
            job_id: job.id,
            error,
        })
    }
}

impl JobInstance for InvalidJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn handle_job(
        &mut self,
        _executor: &JobExecutor,
        _conn: &PgConnection,
    ) -> JobResult<serde_json::Value> {
        Err(self.error.clone())
    }
}
