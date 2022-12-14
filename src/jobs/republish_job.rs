use diesel::pg::PgConnection;
use libostree::gio::NONE_CANCELLABLE;
use log::info;
use serde_json::json;
use std::ffi::OsString;
use std::fs::{self};
use std::process::Command;
use tempfile::TempDir;

use crate::errors::{JobError, JobResult};
use crate::jobs::utils::schedule_update_job;
use crate::models::Job;
use crate::models::RepublishJob;
use crate::ostree::init_ostree_repo;

use super::job_executor::JobExecutor;
use super::job_instance::{InvalidJobInstance, JobInstance};
use super::utils::{add_gpg_args, do_command, job_log_and_info};

#[derive(Debug)]
pub struct RepublishJobInstance {
    pub job_id: i32,
    pub app_id: String,
    pub repo: String,
}

impl RepublishJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        if let Ok(publish_job) = serde_json::from_str::<RepublishJob>(&job.contents) {
            let repo = if let Some(repo) = job.repo {
                repo
            } else {
                return InvalidJobInstance::new(
                    job,
                    JobError::new("Republish job requires a repo"),
                );
            };

            Box::new(RepublishJobInstance {
                job_id: job.id,
                repo,
                app_id: publish_job.app,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse republish job"))
        }
    }
}

impl JobInstance for RepublishJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn order(&self) -> i32 {
        1 /* Same priority as regular publish jobs */
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!(
            "#{}: Handling Job Republish: repo: {}, app: {}",
            &self.job_id, &self.repo, &self.app_id,
        );

        // Get repo config
        let config = &executor.config;
        let repoconfig = config
            .get_repoconfig(&self.repo)
            .map_err(|_e| JobError::new(&format!("Can't find repo {}", &self.repo)))?;

        let repo = libostree::Repo::new_for_path(repoconfig.get_abs_repo_path());
        repo.open(NONE_CANCELLABLE)
            .map_err(|e| JobError::new(&format!("Failed to open repo {}: {}", &self.repo, e)))?;

        /* Create a temporary repo to use while editing commits, so that intermediate commits don't clutter the main
        repo. */
        let tmp_dir = &repoconfig
            .get_abs_repo_path()
            .join("tmp")
            .join("republish-repos");
        fs::create_dir_all(tmp_dir)?;
        let tmp_repo_dir = TempDir::new_in(tmp_dir).map_err(|e| {
            JobError::new(&format!("Failed to create temporary repo directory: {}", e))
        })?;

        init_ostree_repo(tmp_repo_dir.path(), &repoconfig.get_abs_repo_path(), &None)?;

        /* Find all refs that match the app */
        let refs = repo
            .list_refs_ext(
                Some(&format!("app/{}", self.app_id)),
                libostree::RepoListRefsExtFlags::NONE,
                NONE_CANCELLABLE,
            )
            .map_err(|e| JobError::new(&format!("Failed to load repo {}: {}", &self.repo, e)))?;

        // Import commits to the temporary repo
        for (ref_name, _checksum) in refs {
            job_log_and_info(self.job_id, conn, &format!("Re-publishing {}", &ref_name));

            let mut cmd = Command::new("flatpak");
            cmd.arg("build-commit-from")
                .arg("--force") // Always generate a new commit even if nothing changed
                .arg("--no-update-summary")
                .arg("--disable-fsync"); // No need for fsync in intermediate steps

            cmd.arg(&format!(
                "--src-repo={}",
                repoconfig
                    .get_abs_repo_path()
                    .to_str()
                    .expect("repo paths should be valid unicode")
            ))
            .arg(&format!("--src-ref={}", ref_name))
            .arg(tmp_repo_dir.path())
            .arg(&ref_name);

            do_command(cmd)?;
        }

        // Run the publish hook, if any
        if let Some(hook) = repoconfig.hooks.publish.build_command(tmp_repo_dir.path()) {
            job_log_and_info(self.job_id, conn, "Running publish hook");
            do_command(hook)?;
        }

        // Publish the potentially edited commits back to the main repo
        let mut cmd = Command::new("flatpak");
        cmd.arg("build-commit-from").arg("--no-update-summary"); // We update it separately

        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);

        let mut src_repo_arg = OsString::from("--src-repo=");
        src_repo_arg.push(tmp_repo_dir.path());
        cmd.arg(&src_repo_arg).arg(&repoconfig.path);

        job_log_and_info(
            self.job_id,
            conn,
            &format!("Republishing refs to repo {}", repoconfig.name),
        );
        do_command(cmd)?;

        /* The repo summary may need to be updated */
        let update_job = schedule_update_job(config, repoconfig, conn, self.job_id)?;

        Ok(json!({
            "update-repo-job": update_job.id,
        }))
    }
}
