use crate::errors::{JobError, JobResult};
use crate::jobs::job_executor::JobExecutor;
use crate::jobs::job_instance::JobInstance;
use crate::models::Job;
use diesel::pg::PgConnection;
use log::info;
use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, Serialize, Deserialize)]
pub struct PruneJob {}

pub struct PruneJobInstance {
    job: Job,
}

impl PruneJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        match serde_json::from_str::<PruneJob>(&job.contents) {
            Ok(_) => Box::new(PruneJobInstance { job }),
            Err(e) => super::job_instance::InvalidJobInstance::new(
                job,
                JobError::new(&format!("Invalid prune job contents: {}", e)),
            ),
        }
    }
}

impl JobInstance for PruneJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job.id
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &mut PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!("#{}: Handling Job Prune", &self.job.id);

        // Get repo config
        let config = &executor.config;
        let repo = self
            .job
            .repo
            .as_ref()
            .ok_or_else(|| JobError::new("No repo specified"))?;
        let repoconfig = config
            .get_repoconfig(repo)
            .map_err(|_e| JobError::new(&format!("Can't find repo {}", repo)))?;

        let repo_path = repoconfig.get_abs_repo_path();

        // First do a dry run
        job_log_and_info!(self.job.id, conn, "Running prune dry-run");
        let mut cmd = Command::new("flatpak");
        cmd.arg("build-update-repo")
            .arg("-v")
            .arg("--no-update-summary")
            .arg("--no-update-appstream")
            .arg("--prune-dry-run")
            .arg("--prune-depth=3")
            .arg(&repo_path);

        super::utils::do_command(cmd)?;

        // Then do the actual prune
        job_log_and_info!(self.job.id, conn, "Running actual prune");
        let mut cmd = Command::new("flatpak");
        cmd.arg("build-update-repo")
            .arg("-v")
            .arg("--no-update-summary")
            .arg("--no-update-appstream")
            .arg("--prune")
            .arg("--prune-depth=3")
            .arg(&repo_path);

        super::utils::do_command(cmd)?;

        Ok(serde_json::json!({}))
    }

    // Higher order than Publish/UpdateRepo to prevent them from running while prune is in queue
    fn order(&self) -> i32 {
        100
    }
}
