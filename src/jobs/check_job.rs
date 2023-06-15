use diesel::pg::PgConnection;
use diesel::prelude::*;
use log::info;
use serde_json::json;

use crate::errors::{JobError, JobResult};
use crate::models::{self, Build, Check, CheckJob, CheckStatus, Job, RepoState};
use crate::schema::{builds, checks};

use super::job_executor::JobExecutor;
use super::job_instance::{InvalidJobInstance, JobInstance};
use super::utils::do_command_with_output;

#[derive(Debug)]
pub struct CheckJobInstance {
    pub job_id: i32,
    pub build_id: i32,
    pub name: String,
}

impl CheckJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        if let Ok(check_job) = serde_json::from_str::<CheckJob>(&job.contents) {
            Box::new(CheckJobInstance {
                job_id: job.id,
                build_id: check_job.build,
                name: check_job.name,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse check job"))
        }
    }
}

impl JobInstance for CheckJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn order(&self) -> i32 {
        1 /* Same priority as regular publish jobs */
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &mut PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!(
            "#{}: Handling Job Check: name: {}, build: {}",
            &self.job_id, self.name, &self.build_id,
        );

        let config = &executor.config;

        // Get build details
        let build_data = builds::table
            .filter(builds::id.eq(self.build_id))
            .get_result::<models::Build>(conn)
            .map_err(|_e| JobError::new("Can't load build"))?;

        // Get repo config
        let repoconfig = config
            .get_repoconfig(&build_data.repo)
            .map_err(|_e| JobError::new(&format!("Can't find repo {}", &build_data.repo)))?;

        let build_repo_path = config.build_repo_base.join(self.build_id.to_string());

        let check_hook = if let Some(hook) = repoconfig.hooks.checks.get(&self.name) {
            hook
        } else {
            return Err(JobError::new(&format!(
                "The '{}' check hook is not defined",
                self.name
            )));
        };

        // Run the hook
        job_log_and_info!(self.job_id, conn, "Running check command");

        let new_status = if let Some(mut cmd) = check_hook.command.build_command(build_repo_path) {
            cmd.env("FLAT_MANAGER_BUILD_ID", self.build_id.to_string())
                .env("FLAT_MANAGER_JOB_ID", self.job_id.to_string());

            let output = do_command_with_output(&mut cmd)?;

            if output.status.success() {
                job_log_and_info!(self.job_id, conn, "Check command exited successfully");
                CheckStatus::Passed
            } else {
                let msg = format!(
                    "Check command {:?} exited unsuccessfully: {}, stderr: {}",
                    cmd,
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );

                job_log_and_info!(self.job_id, conn, &msg);

                if check_hook.reviewable {
                    CheckStatus::ReviewRequired(msg)
                } else {
                    CheckStatus::Failed(msg)
                }
            }
        } else {
            return Err(JobError::new(&format!(
                "The '{}' check hook is defined, but it has no command",
                self.name
            )));
        };

        conn.transaction::<_, JobError, _>(|conn| {
            let check = checks::table
                .filter(checks::job_id.eq(self.job_id))
                .for_update()
                .get_result::<Check>(conn)?;

            // The command can edit its own check status by calling the API. If that happened and the command exited
            // successfully, don't change it.
            if matches!(new_status, CheckStatus::Failed(_))
                || check.status == CheckStatus::Pending.to_db().0
            {
                let (status, status_reason) = new_status.to_db();
                diesel::update(checks::table)
                    .filter(checks::job_id.eq(self.job_id))
                    .set((
                        checks::status.eq(status),
                        checks::status_reason.eq(status_reason),
                    ))
                    .get_result::<Check>(conn)?;

                job_log_and_info!(
                    self.job_id,
                    conn,
                    format!("Check status updated to {new_status:?}")
                );
            } else {
                job_log_and_info!(
                    self.job_id,
                    conn,
                    format!("Check status updated to {:?}", check.status)
                );
            }

            update_build_status_after_check(self.build_id, conn)?;

            Ok(())
        })?;

        Ok(json! {()})
    }
}

/// When all checks have completed, the build should be moved to the Ready state if the checks all passed or Failed
/// if any did not.
pub fn update_build_status_after_check(
    build_id: i32,
    conn: &mut PgConnection,
) -> Result<(), JobError> {
    conn.transaction(|conn| {
        // Get all the checks for the given build
        let check_statuses = checks::table
            .filter(checks::build_id.eq(build_id))
            .get_results::<Check>(conn)?
            .into_iter()
            .map(|check| {
                (
                    check.check_name,
                    CheckStatus::from_db(check.status, check.status_reason)
                        .unwrap_or_else(|| CheckStatus::Failed("Invalid status".to_string())),
                )
            })
            .collect::<Vec<_>>();

        // If they're not all finished, nothing to do yet
        if !check_statuses
            .iter()
            .all(|(_name, status)| status.is_finished())
        {
            return Ok(());
        }

        let build = builds::table
            .filter(builds::id.eq(build_id))
            .for_update()
            .get_result::<Build>(conn)?;

        // Sanity check--make sure the build is still in Validating state
        if !RepoState::from_db(build.repo_state, &build.repo_state_reason)
            .same_state_as(&RepoState::Validating)
        {
            return Err(JobError::new(&format!(
                "Expected repo to be in {:?} state upon check completion, but it was in {:?}",
                RepoState::Committing,
                RepoState::from_db(build.repo_state, &build.repo_state_reason)
            )));
        }

        let failing = check_statuses
            .iter()
            .filter(|(_name, status)| status.is_failed())
            .collect::<Vec<_>>();

        // If any check failed, then the build fails, otherwise it is now ready for publishing
        let (new_state, new_state_reason) = if failing.is_empty() {
            RepoState::Ready
        } else {
            RepoState::Failed(format!(
                "{} out of {} checks failed ({})",
                failing.len(),
                check_statuses.len(),
                failing
                    .iter()
                    .map(|(name, _)| name)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
        .to_db();

        diesel::update(builds::table)
            .filter(builds::id.eq(build_id))
            .set((
                builds::repo_state.eq(new_state),
                builds::repo_state_reason.eq(new_state_reason),
            ))
            .get_result::<Build>(conn)?;

        Ok(())
    })
}
