use crate::errors::{JobError, JobResult};
use crate::jobs::job_executor::JobExecutor;
use crate::jobs::job_instance::JobInstance;
use crate::models::Job;
use diesel::pg::PgConnection;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::process::Command;

#[derive(Debug, Serialize, Deserialize)]
pub struct PruneJob {}

pub struct PruneJobInstance {
    pub job: Job,
}

impl PruneJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        Box::new(PruneJobInstance { job })
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
            .map_err(|_e| JobError::new(&format!("Can't find repo {repo}")))?;

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

        let output = super::utils::do_command_with_output(&mut cmd)?;
        job_log_and_info!(
            self.job.id,
            conn,
            &format!(
                "Dry-run stdout:\n{}",
                String::from_utf8_lossy(&output.stdout)
            )
        );
        job_log_and_info!(
            self.job.id,
            conn,
            &format!(
                "Dry-run stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        );

        if !output.status.success() {
            return Err(JobError::new(&format!(
                "Prune dry-run failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

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

        let output = super::utils::do_command_with_output(&mut cmd)?;
        job_log_and_info!(
            self.job.id,
            conn,
            &format!("Prune stdout:\n{}", String::from_utf8_lossy(&output.stdout))
        );
        job_log_and_info!(
            self.job.id,
            conn,
            &format!("Prune stderr:\n{}", String::from_utf8_lossy(&output.stderr))
        );

        if !output.status.success() {
            return Err(JobError::new(&format!(
                "Prune failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        Ok(json!({}))
    }

    // Higher order than Publish/UpdateRepo to prevent them from running while prune is in queue
    fn order(&self) -> i32 {
        100
    }
}
