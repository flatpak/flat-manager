use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use log::{error, info};
use serde_json::json;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::process::Command;

use crate::config::{Config, RepoConfig};
use crate::errors::{JobError, JobResult};
use crate::models;
use crate::models::{Job, PublishJob, PublishedState};
use crate::ostree;
use crate::schema::*;

use super::job_executor::JobExecutor;
use super::job_instance::{InvalidJobInstance, JobInstance};
use super::utils::{
    add_gpg_args, do_command, generate_flatpakref, job_log_and_info, schedule_update_job,
};

#[derive(Debug)]
pub struct PublishJobInstance {
    pub job_id: i32,
    pub build_id: i32,
}

impl PublishJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job) -> Box<dyn JobInstance> {
        if let Ok(publish_job) = serde_json::from_str::<PublishJob>(&job.contents) {
            Box::new(PublishJobInstance {
                job_id: job.id,
                build_id: publish_job.build,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse publish job"))
        }
    }

    fn do_publish(
        &self,
        build: &models::Build,
        build_refs: &[models::BuildRef],
        config: &Config,
        repoconfig: &RepoConfig,
        conn: &PgConnection,
    ) -> JobResult<serde_json::Value> {
        let build_repo_path = config.build_repo_base.join(self.build_id.to_string());

        // Run the publish hook, if any
        if let Some(hook) = repoconfig.hooks.publish.build_command(&build_repo_path) {
            job_log_and_info(self.job_id, conn, "Running publish hook");
            do_command(hook)?;
        }

        let mut src_repo_arg = OsString::from("--src-repo=");
        src_repo_arg.push(&build_repo_path);

        // Import commit and modify refs

        let mut cmd = Command::new("flatpak");
        cmd.arg("build-commit-from")
            .arg("--force") // Always generate a new commit even if nothing changed
            .arg("--no-update-summary"); // We update it separately

        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);

        if let Some(collection_id) = &repoconfig.collection_id {
            for ref extra_id in build.extra_ids.iter() {
                cmd.arg(format!("--extra-collection-id={collection_id}.{extra_id}"));
            }
        }

        cmd.arg(&src_repo_arg).arg(&repoconfig.path);

        job_log_and_info(
            self.job_id,
            conn,
            &format!("Importing build to repo {}", repoconfig.name),
        );
        do_command(cmd)?;

        let appstream_dir = repoconfig.path.join("appstream");
        fs::create_dir_all(&appstream_dir)?;

        let screenshots_dir = repoconfig.path.join("screenshots");
        fs::create_dir_all(&screenshots_dir)?;

        let mut commits = HashMap::new();
        for build_ref in build_refs.iter() {
            if build_ref.ref_name.starts_with("app/") || build_ref.ref_name.starts_with("runtime/")
            {
                let commit = ostree::parse_ref(&repoconfig.path, &build_ref.ref_name)?;
                commits.insert(build_ref.ref_name.to_string(), commit);
            }

            if build_ref.ref_name.starts_with("app/") {
                let (filename, contents) =
                    generate_flatpakref(&build_ref.ref_name, None, config, repoconfig);
                let path = appstream_dir.join(&filename);
                job_log_and_info(self.job_id, conn, &format!("generating {}", &filename));
                let old_contents = fs::read_to_string(&path).unwrap_or_default();
                if contents != old_contents {
                    File::create(&path)?.write_all(contents.as_bytes())?;
                }
            }
        }

        for build_ref in build_refs.iter() {
            if build_ref.ref_name.starts_with("screenshots/") {
                job_log_and_info(
                    self.job_id,
                    conn,
                    &format!("extracting {}", build_ref.ref_name),
                );
                let mut cmd = Command::new("ostree");
                cmd.arg(&format!("--repo={}", &build_repo_path.to_str().unwrap()))
                    .arg("checkout")
                    .arg("--user-mode")
                    .arg("--bareuseronly-dirs")
                    .arg("--union")
                    .arg(&build_ref.ref_name)
                    .arg(&screenshots_dir);
                do_command(cmd)?;
            }
        }

        let update_job = schedule_update_job(config, repoconfig, conn, self.job_id)?;

        Ok(json!({
            "refs": commits,
            "update-repo-job": update_job.id,
        }))
    }
}

impl JobInstance for PublishJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn order(&self) -> i32 {
        1 /* Delay publish after commits (and other normal ops). because the
          commits may generate more publishes. */
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!(
            "#{}: Handling Job Publish: build: {}",
            &self.job_id, &self.build_id
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

        // Get the uploaded refs from db
        let build_refs = build_refs::table
            .filter(build_refs::build_id.eq(self.build_id))
            .get_results::<models::BuildRef>(conn)
            .map_err(|_e| JobError::new("Can't load build refs"))?;
        if build_refs.is_empty() {
            return Err(JobError::new("No refs in build"));
        }

        // Do the actual work
        let res = self.do_publish(&build_data, &build_refs, config, repoconfig, conn);

        // Update the publish repo state in db

        let new_published_state = match &res {
            Ok(_) => PublishedState::Published,
            Err(e) => PublishedState::Failed(e.to_string()),
        };

        conn.transaction::<models::Build, DieselError, _>(|| {
            let current_build = builds::table
                .filter(builds::id.eq(self.build_id))
                .get_result::<models::Build>(conn)?;
            let current_published_state = PublishedState::from_db(
                current_build.published_state,
                &current_build.published_state_reason,
            );
            if !current_published_state.same_state_as(&PublishedState::Publishing) {
                // Something weird was happening, we expected this build to be in the publishing state
                error!("Unexpected publishing state {:?}", current_published_state);
                return Err(DieselError::RollbackTransaction);
            };
            let (val, reason) = PublishedState::to_db(&new_published_state);
            diesel::update(builds::table)
                .filter(builds::id.eq(self.build_id))
                .set((
                    builds::published_state.eq(val),
                    builds::published_state_reason.eq(reason),
                ))
                .get_result::<models::Build>(conn)
        })?;

        res
    }
}
