use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use log::{error, info};
use std::fmt::Write as _;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::str;

use crate::config::{Config, RepoConfig};
use crate::errors::{JobError, JobResult};
use crate::models::Job;
use crate::schema::*;

use super::job_queue::queue_update_job;

pub fn generate_flatpakref(
    ref_name: &str,
    maybe_build_id: Option<i32>,
    config: &Config,
    repoconfig: &RepoConfig,
) -> (String, String) {
    let parts: Vec<&str> = ref_name.split('/').collect();

    let filename = format!("{}.flatpakref", parts[1]);
    let app_id = &parts[1];
    let branch = &parts[3];

    let is_runtime = if parts[0] == "runtime" {
        "true"
    } else {
        "false"
    };

    let (url, maybe_gpg_content) = match maybe_build_id {
        Some(build_id) => (
            format!("{}/build-repo/{}", config.base_url, build_id),
            &config.build_gpg_key_content,
        ),
        None => (repoconfig.get_base_url(config), &repoconfig.gpg_key_content),
    };

    let title = if let Some(build_id) = maybe_build_id {
        format!("{} build nr {}", parts[1], build_id)
    } else {
        let reponame = match &repoconfig.suggested_repo_name {
            Some(suggested_name) => suggested_name,
            None => &repoconfig.name,
        };
        format!("{} from {}", app_id, reponame)
    };

    let mut contents = format!(
        r#"[Flatpak Ref]
Name={}
Branch={}
Title={}
IsRuntime={}
Url={}
"#,
        app_id, branch, title, is_runtime, url
    );

    /* We only want to deploy the collection ID if the flatpakref is being generated for the main
     * repo not a build repo.
     */
    if let Some(collection_id) = &repoconfig.collection_id {
        if repoconfig.deploy_collection_id && maybe_build_id.is_none() {
            writeln!(contents, "DeployCollectionID={}", collection_id).unwrap();
        }
    };

    if maybe_build_id.is_none() {
        if let Some(suggested_name) = &repoconfig.suggested_repo_name {
            writeln!(contents, "SuggestRemoteName={}", suggested_name).unwrap();
        }
    }

    if let Some(gpg_content) = maybe_gpg_content {
        writeln!(contents, "GPGKey={}", gpg_content).unwrap();
    }

    if let Some(runtime_repo_url) = &repoconfig.runtime_repo_url {
        writeln!(contents, "RuntimeRepo={}\n", runtime_repo_url).unwrap();
    }

    (filename, contents)
}

pub fn add_gpg_args(
    cmd: &mut Command,
    maybe_gpg_key: &Option<String>,
    maybe_gpg_homedir: &Option<String>,
) {
    if let Some(gpg_homedir) = maybe_gpg_homedir {
        cmd.arg(format!("--gpg-homedir={}", gpg_homedir));
    };

    if let Some(key) = maybe_gpg_key {
        cmd.arg(format!("--gpg-sign={}", key));
    };
}

pub fn job_log(job_id: i32, conn: &PgConnection, output: &str) {
    if let Err(e) = diesel::update(jobs::table)
        .filter(jobs::id.eq(job_id))
        .set((jobs::log.eq(jobs::log.concat(&output)),))
        .execute(conn)
    {
        error!("Error appending to job {} log: {}", job_id, e.to_string());
    }
}

pub fn job_log_and_info(job_id: i32, conn: &PgConnection, output: &str) {
    info!("#{}: {}", job_id, output);
    job_log(job_id, conn, &format!("{}\n", output));
}

pub fn job_log_and_error(job_id: i32, conn: &PgConnection, output: &str) {
    error!("#{}: {}", job_id, output);
    job_log(job_id, conn, &format!("{}\n", output));
}

pub fn do_command(mut cmd: Command) -> JobResult<()> {
    let output = unsafe {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .pre_exec(|| {
                // Setsid in the child to avoid SIGINT on server killing
                // child and breaking the graceful shutdown
                libc::setsid();
                Ok(())
            })
            .output()
            .map_err(|e| JobError::new(&format!("Failed to run {:?}: {}", &cmd, e)))?
    };

    if !output.status.success() {
        return Err(JobError::new(&format!(
            "Command {:?} exited unsuccesfully: {}",
            &cmd,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

pub fn schedule_update_job(
    config: &Config,
    repoconfig: &RepoConfig,
    conn: &PgConnection,
    job_id: i32,
) -> Result<Job, DieselError> {
    /* Create update repo job */
    let delay = config.delay_update_secs;
    let (is_new, update_job) = queue_update_job(delay, conn, &repoconfig.name, Some(job_id))?;
    if is_new {
        job_log_and_info(
            job_id,
            conn,
            &format!(
                "Queued repository update job {}{}",
                update_job.id,
                match delay {
                    0 => "".to_string(),
                    _ => format!(" in {} secs", delay),
                }
            ),
        );
    } else {
        job_log_and_info(
            job_id,
            conn,
            &format!("Piggy-backed on existing update job {}", update_job.id),
        );
    }

    Ok(update_job)
}