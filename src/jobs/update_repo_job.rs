use actix::prelude::*;
use diesel::pg::PgConnection;
use log::info;
use serde_json::json;
use std::collections::HashSet;
use std::fs::{self};
use std::iter::FromIterator;
use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc;
use std::time;
use walkdir::WalkDir;

use crate::config::{Config, RepoConfig};
use crate::deltas::{DeltaGenerator, DeltaRequest, DeltaRequestSync};
use crate::errors::{JobError, JobResult};
use crate::models::{Job, UpdateRepoJob};
use crate::ostree;

use super::job_executor::JobExecutor;
use super::job_instance::{InvalidJobInstance, JobInstance};
use super::utils::{add_gpg_args, do_command};

#[derive(Debug)]
pub struct UpdateRepoJobInstance {
    pub delta_generator: Addr<DeltaGenerator>,
    pub job_id: i32,
    pub repo: String,
}

impl UpdateRepoJobInstance {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(job: Job, delta_generator: Addr<DeltaGenerator>) -> Box<dyn JobInstance> {
        if let Ok(update_repo_job) = serde_json::from_str::<UpdateRepoJob>(&job.contents) {
            Box::new(UpdateRepoJobInstance {
                delta_generator,
                job_id: job.id,
                repo: update_repo_job.repo,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse publish job"))
        }
    }

    fn calculate_deltas(
        &self,
        repoconfig: &RepoConfig,
    ) -> (HashSet<ostree::Delta>, HashSet<ostree::Delta>) {
        let repo_path = repoconfig.get_abs_repo_path();

        let mut wanted_deltas = HashSet::new();
        let refs = ostree::list_refs(&repo_path, "");

        for ref_name in refs {
            let depth = repoconfig.get_delta_depth_for_ref(&ref_name);

            if depth > 0 {
                let ref_deltas = ostree::calc_deltas_for_ref(&repo_path, &ref_name, depth);
                for ref_delta in ref_deltas {
                    wanted_deltas.insert(ref_delta);
                }
            }
        }
        let old_deltas = HashSet::from_iter(ostree::list_deltas(&repo_path).iter().cloned());

        let missing_deltas = wanted_deltas.difference(&old_deltas).cloned().collect();
        let unwanted_deltas = old_deltas.difference(&wanted_deltas).cloned().collect();

        (missing_deltas, unwanted_deltas)
    }

    fn generate_deltas(
        &self,
        deltas: &HashSet<ostree::Delta>,
        repoconfig: &RepoConfig,
        conn: &PgConnection,
    ) -> JobResult<()> {
        job_log_and_info!(self.job_id, conn, "Generating deltas");

        let (tx, rx) = mpsc::channel();

        /* We can't use a regular .send() here, as that requres a current task which is
         * not available in a sync actor like this. Instead we use the non-blocking
         * do_send and implement returns using a mpsc::channel.
         */

        for delta in deltas.iter() {
            self.delta_generator.do_send(DeltaRequestSync {
                delta_request: DeltaRequest {
                    repo: repoconfig.name.clone(),
                    delta: delta.clone(),
                },
                tx: tx.clone(),
            })
        }

        for (delta, result) in rx.iter().take(deltas.len()) {
            let message = match result {
                Ok(()) => format!(" {delta}"),
                Err(e) => format!(" failed to generate {delta}: {e}"),
            };
            job_log_and_info!(self.job_id, conn, &message);
        }

        job_log_and_info!(self.job_id, conn, "All deltas generated");

        Ok(())
    }

    fn retire_deltas(
        &self,
        deltas: &HashSet<ostree::Delta>,
        repoconfig: &RepoConfig,
        conn: &PgConnection,
    ) -> JobResult<()> {
        job_log_and_info!(self.job_id, conn, "Cleaning out old deltas");
        let repo_path = repoconfig.get_abs_repo_path();
        let deltas_dir = repo_path.join("deltas");
        let tmp_deltas_dir = repo_path.join("deltas/.tmp");
        fs::create_dir_all(&tmp_deltas_dir)?;

        let now = time::SystemTime::now();
        let now_filetime = filetime::FileTime::from_system_time(now);

        /* Instead of directly removing the deltas we move them to a different
         * directory which is *also* used as a source for the delta dir when accessed
         * via http. This way we avoid them disappearing while possibly in use, yet
         * ensure they are not picked up for the new summary file */

        for delta in deltas {
            let src = delta.delta_path(&repo_path)?;
            let dst = delta.tmp_delta_path(&repo_path)?;
            let dst_parent = dst.parent().unwrap();
            fs::create_dir_all(dst_parent)?;

            job_log_and_info!(
                self.job_id,
                conn,
                &format!(
                    " Queuing delta {:?} for deletion",
                    src.strip_prefix(&deltas_dir).unwrap()
                ),
            );

            if dst.exists() {
                fs::remove_dir_all(&dst)?;
            }
            fs::rename(&src, &dst)?;

            /* Update mtime so we can use it to trigger deletion */
            filetime::set_file_times(&dst, now_filetime, now_filetime)?;
        }

        /* Delete all temporary deltas older than one hour */
        let to_delete = WalkDir::new(&tmp_deltas_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| {
                if let Ok(metadata) = e.metadata() {
                    if let Ok(mtime) = metadata.modified() {
                        if let Ok(since) = now.duration_since(mtime) {
                            return since.as_secs() > 60 * 60;
                        }
                    }
                };
                false
            })
            .map(|e| e.path().to_path_buf())
            .collect::<Vec<PathBuf>>();

        for dir in to_delete {
            job_log_and_info!(
                self.job_id,
                conn,
                &format!(
                    " Deleting old delta {:?}",
                    dir.strip_prefix(&tmp_deltas_dir).unwrap()
                ),
            );
            fs::remove_dir_all(&dir)?;
        }

        Ok(())
    }

    fn update_appstream(
        &self,
        config: &Config,
        repoconfig: &RepoConfig,
        conn: &PgConnection,
    ) -> JobResult<()> {
        job_log_and_info!(self.job_id, conn, "Regenerating appstream branches");
        let repo_path = repoconfig.get_abs_repo_path();

        let mut cmd = Command::new("flatpak");
        cmd.arg("build-update-repo").arg("--no-update-summary");
        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);
        cmd.arg(&repo_path);

        do_command(cmd)?;
        Ok(())
    }

    fn update_summary(
        &self,
        config: &Config,
        repoconfig: &RepoConfig,
        conn: &PgConnection,
    ) -> JobResult<()> {
        job_log_and_info!(self.job_id, conn, "Updating summary");
        let repo_path = repoconfig.get_abs_repo_path();

        let mut cmd = Command::new("flatpak");
        cmd.arg("build-update-repo").arg("--no-update-appstream");
        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);
        cmd.arg(&repo_path);

        do_command(cmd)?;
        Ok(())
    }

    fn run_post_publish(&self, repoconfig: &RepoConfig, conn: &PgConnection) -> JobResult<()> {
        if let Some(post_publish_script) = &repoconfig.post_publish_script {
            let repo_path = repoconfig.get_abs_repo_path();
            let mut cmd = Command::new(post_publish_script);
            cmd.arg(&repoconfig.name).arg(&repo_path);
            job_log_and_info!(self.job_id, conn, "Running post-publish script");
            do_command(cmd)?;
        };
        Ok(())
    }

    fn extract_appstream(&self, repoconfig: &RepoConfig, conn: &PgConnection) -> JobResult<()> {
        job_log_and_info!(self.job_id, conn, "Extracting appstream branches");
        let repo_path = repoconfig.get_abs_repo_path();
        let appstream_dir = repo_path.join("appstream");
        let appstream_refs = ostree::list_refs(&repoconfig.path, "appstream");
        for appstream_ref in appstream_refs {
            let arch = appstream_ref.split('/').nth(1).unwrap();
            let mut cmd = Command::new("ostree");
            cmd.arg(&format!("--repo={}", &repoconfig.path.to_str().unwrap()))
                .arg("checkout")
                .arg("--user-mode")
                .arg("--union")
                .arg("--bareuseronly-dirs")
                .arg(&appstream_ref)
                .arg(appstream_dir.join(arch));
            do_command(cmd)?;
        }
        Ok(())
    }
}

impl JobInstance for UpdateRepoJobInstance {
    fn get_job_id(&self) -> i32 {
        self.job_id
    }

    fn order(&self) -> i32 {
        2 /* Delay updates after publish so they can be chunked. */
    }

    fn handle_job(
        &mut self,
        executor: &JobExecutor,
        conn: &PgConnection,
    ) -> JobResult<serde_json::Value> {
        info!(
            "#{}: Handling Job UpdateRepo: repo: {}",
            &self.job_id, &self.repo
        );

        // Get repo config
        let config = &executor.config;
        let repoconfig = config
            .get_repoconfig(&self.repo)
            .map_err(|_e| JobError::new(&format!("Can't find repo {}", &self.repo)))?;

        self.update_appstream(config, repoconfig, conn)?;

        let (missing_deltas, unwanted_deltas) = self.calculate_deltas(repoconfig);
        self.generate_deltas(&missing_deltas, repoconfig, conn)?;
        self.retire_deltas(&unwanted_deltas, repoconfig, conn)?;

        self.extract_appstream(repoconfig, conn)?;

        self.update_summary(config, repoconfig, conn)?;

        self.run_post_publish(repoconfig, conn)?;

        Ok(json!({}))
    }
}
