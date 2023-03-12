use diesel::pg::PgConnection;
use diesel::prelude::*;
use log::info;
use serde_json::json;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::process::Command;
use std::str;

use crate::config::{Config, RepoConfig};
use crate::errors::{JobError, JobResult};
use crate::models::{
    self, Check, CheckJob, CheckStatus, CommitJob, Job, JobKind, NewJob, RepoState,
};
use crate::ostree;
use crate::schema::*;

use super::job_executor::JobExecutor;
use super::job_instance::{InvalidJobInstance, JobInstance};
use super::utils::{add_gpg_args, do_command, generate_flatpakref};

#[derive(Debug)]
pub struct CommitJobInstance {
    pub job_id: i32,
    pub build_id: i32,
    pub endoflife: Option<String>,
    pub endoflife_rebase: Option<String>,
    pub token_type: Option<i32>,
}

impl CommitJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        if let Ok(commit_job) = serde_json::from_str::<CommitJob>(&job.contents) {
            Box::new(CommitJobInstance {
                job_id: job.id,
                build_id: commit_job.build,
                endoflife: commit_job.endoflife,
                endoflife_rebase: commit_job.endoflife_rebase,
                token_type: commit_job.token_type,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse commit job"))
        }
    }

    fn do_commit_build_refs(
        &self,
        build_refs: &[models::BuildRef],
        config: &Config,
        repoconfig: &RepoConfig,
        conn: &mut PgConnection,
    ) -> JobResult<serde_json::Value> {
        let build_repo_path = config.build_repo_base.join(self.build_id.to_string());
        let upload_path = build_repo_path.join("upload");

        let mut src_repo_arg = OsString::from("--src-repo=");
        src_repo_arg.push(&upload_path);

        let mut commits = HashMap::new();

        let endoflife_rebase_arg = if let Some(endoflife_rebase) = &self.endoflife_rebase {
            build_refs
                .iter()
                .find(|app_ref| app_ref.ref_name.starts_with("app/"))
                .map(|app_ref| {
                    format!(
                        "--end-of-life-rebase={}={}",
                        app_ref.ref_name.split('/').nth(1).unwrap(),
                        endoflife_rebase
                    )
                })
        } else {
            None
        };

        for build_ref in build_refs.iter() {
            let mut cmd = Command::new("flatpak");
            cmd.arg("build-commit-from")
                .arg("--timestamp=NOW") // All builds have the same timestamp, not when the individual builds finished
                .arg("--no-update-summary") // We update it once at the end
                .arg("--untrusted") // Verify that the uploaded objects are correct
                .arg("--force") // Always generate a new commit even if nothing changed
                .arg("--disable-fsync"); // There is a sync in flatpak build-update-repo, so avoid it here

            add_gpg_args(&mut cmd, &config.build_gpg_key, &config.gpg_homedir);

            if let Some(endoflife) = &self.endoflife {
                cmd.arg(format!("--end-of-life={endoflife}"));
            };

            if let Some(endoflife_rebase_arg) = &endoflife_rebase_arg {
                cmd.arg(endoflife_rebase_arg);
            };

            if let Some(token_type) = self.token_type {
                cmd.arg(format!("--token-type={token_type}"));
            }

            let src_ref_arg = format!("--src-ref={}", build_ref.commit);

            cmd.arg(&src_repo_arg)
                .arg(&src_ref_arg)
                .arg(&build_repo_path)
                .arg(&build_ref.ref_name);

            job_log_and_info!(
                self.job_id,
                conn,
                &format!(
                    "Committing ref {} ({})",
                    build_ref.ref_name, build_ref.commit,
                ),
            );
            do_command(cmd)?;

            let commit = ostree::parse_ref(&build_repo_path, &build_ref.ref_name)?;
            commits.insert(build_ref.ref_name.to_string(), commit);

            let unwanted_exts = [".Debug", ".Locale", ".Sources", ".Docs"];
            let ref_id_parts: Vec<&str> = build_ref.ref_name.split('/').collect();

            if build_ref.ref_name.starts_with("app/")
                || (build_ref.ref_name.starts_with("runtime/")
                    && !unwanted_exts
                        .iter()
                        .any(|&ext| ref_id_parts[1].ends_with(ext)))
            {
                let (filename, contents) = generate_flatpakref(
                    &build_ref.ref_name,
                    Some(self.build_id),
                    config,
                    repoconfig,
                );
                let path = build_repo_path.join(filename);
                File::create(path)?.write_all(contents.as_bytes())?;
            }
        }

        let mut cmd = Command::new("flatpak");
        cmd.arg("build-update-repo").arg(&build_repo_path);

        add_gpg_args(&mut cmd, &config.build_gpg_key, &config.gpg_homedir);

        job_log_and_info!(self.job_id, conn, "running build-update-repo");
        do_command(cmd)?;

        job_log_and_info!(self.job_id, conn, "Removing upload directory");
        fs::remove_dir_all(&upload_path)?;

        Ok(json!({ "refs": commits }))
    }
}

impl JobInstance for CommitJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &mut PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!("#{}: Handling Job Commit: build: {}, end-of-life: {}, eol-rebase: {}, token-type: {:?}",
              &self.job_id, &self.build_id, self.endoflife.as_ref().unwrap_or(&"".to_string()), self.endoflife_rebase.as_ref().unwrap_or(&"".to_string()), self.token_type);

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

        // Get the uploaded refs from db
        let build_refs = build_refs::table
            .filter(build_refs::build_id.eq(self.build_id))
            .get_results::<models::BuildRef>(conn)
            .map_err(|_e| JobError::new("Can't load build refs"))?;

        if build_refs.is_empty() {
            return Err(JobError::new("No refs in build"));
        }

        // Do the actual work
        let res = self.do_commit_build_refs(&build_refs, config, repoconfig, conn);

        // Update the build repo state in db
        conn.transaction::<_, JobError, _>(|conn| {
            let current_build = builds::table
                .filter(builds::id.eq(self.build_id))
                .for_update()
                .get_result::<models::Build>(conn)?;
            let current_repo_state =
                RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);

            if !current_repo_state.same_state_as(&RepoState::Committing) {
                // Something weird was happening, we expected this build to be in the verifying state
                return Err(JobError::new(&format!(
                    "Expected repo to be in {:?} state upon commit job completion, but it was in {:?}",
                    RepoState::Committing,
                    RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason)
                )));
            };

            let new_repo_state = match &res {
                Ok(_) => {
                    if repoconfig.hooks.checks.is_empty() {
                        RepoState::Ready
                    } else {
                        // Create a check job for each configured check hook
                        let check_jobs = diesel::insert_into(jobs::table)
                            .values(
                                repoconfig
                                    .hooks
                                    .checks
                                    .keys()
                                    .map(|name| NewJob {
                                        kind: JobKind::Check.to_db(),
                                        start_after: None,
                                        repo: None,
                                        contents: json!(CheckJob {
                                            build: self.build_id,
                                            name: name.to_owned()
                                        })
                                        .to_string(),
                                    })
                                    .collect::<Vec<_>>(),
                            )
                            .get_results::<Job>(conn)?;

                        // Create a check row for each new check job. This row ties the job to the build and records its status.
                        let (pending_status, pending_status_msg) = CheckStatus::Pending.to_db();
                        let checks = repoconfig
                            .hooks
                            .checks
                            .keys()
                            .zip(check_jobs.iter())
                            .map(|(name, job)| Check {
                                check_name: name.to_owned(),
                                build_id: self.build_id,
                                job_id: job.id,
                                status: pending_status,
                                status_reason: pending_status_msg.cloned(),
                                results: None,
                            })
                            .collect::<Vec<_>>();

                        diesel::insert_into(checks::table)
                            .values(checks)
                            .get_result::<Check>(conn)?;

                        // Put the build in the Validating state. The last check job to finish will move the build
                        // into the Ready or Failed state.
                        RepoState::Validating
                    }
                }
                Err(e) => RepoState::Failed(e.to_string()),
            };

            let (val, reason) = RepoState::to_db(&new_repo_state);
            diesel::update(builds::table)
                .filter(builds::id.eq(self.build_id))
                .set((
                    builds::repo_state.eq(val),
                    builds::repo_state_reason.eq(reason),
                ))
                .get_result::<models::Build>(conn)?;

            Ok(())
        })?;

        res
    }
}
