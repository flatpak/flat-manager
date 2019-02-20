use actix::prelude::*;
use actix::{Actor, SyncContext};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::result::{Error as DieselError};
use diesel;
use serde_json;
use std::str;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path;
use std::process::{Command, Stdio};
use std::sync::{Arc};
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time;
use std::os::unix::process::CommandExt;
use libc;
use std::collections::HashMap;

use app::{RepoConfig, Config};
use errors::{JobError, JobResult};
use models::{NewJob, Job, JobDependency, JobKind, CommitJob, PublishJob, UpdateRepoJob, JobStatus, job_dependencies_with_status, RepoState, PublishedState };
use models;
use schema::*;
use schema;

pub struct JobExecutor {
    pub config: Arc<Config>,
    pub pool: Pool<ConnectionManager<PgConnection>>,
}

impl Actor for JobExecutor {
    type Context = SyncContext<Self>;
}

fn generate_flatpakref(ref_name: &String,
                       maybe_build_id: Option<i32>,
                       config: &Arc<Config>,
                       repoconfig: &RepoConfig) -> (String, String) {
    let parts: Vec<&str> = ref_name.split('/').collect();

    let filename = format!("{}.flatpakref", parts[1]);
    let app_id = &parts[1];
    let branch = &parts[3];

    let (url, maybe_gpg_content) = match maybe_build_id {
        Some(build_id) => (
            format!("{}/build-repo/{}", config.base_url, build_id),
            &config.build_gpg_key_content
        ),
        None => (
            repoconfig.get_base_url (&config),
            &repoconfig.gpg_key_content
        ),
    };

    let title = if let Some(build_id) = maybe_build_id {
        format!("{} build nr {}", parts[1], build_id)
    } else {
        let reponame = match &repoconfig.suggested_repo_name {
            Some(suggested_name) => &suggested_name,
            None => &repoconfig.name,
        };
        format!("{} from {}", app_id, reponame)
    };

    let mut contents = format!(r#"[Flatpak Ref]
Name={}
Branch={}
Title={}
IsRuntime=false
Url={}
"#, app_id, branch, title, url);

    if maybe_build_id == None {
        if let Some(suggested_name) = &repoconfig.suggested_repo_name {
            contents.push_str(&format!("SuggestRemoteName={}\n", suggested_name));
        }
    }

    if let Some(gpg_content) = maybe_gpg_content {
        contents.push_str(&format!("GPGKey={}\n", gpg_content))
    }

    if let Some(runtime_repo_url) = &repoconfig.runtime_repo_url {
        contents.push_str(&format!("RuntimeRepo={}\n", runtime_repo_url));
    }

    (filename, contents)
}

fn queue_update_job (conn: &PgConnection,
                     repo: &str,
                     starting_job_id: i32) -> JobResult<Job>
{
    /* This depends on there only being one writer to the job status,
     * so if this ever changes this needs to be a transaction with
     * higher isolation level. */

    let mut old_update_started_job = None;
    let mut old_update_new_job = None;

    /* First look for an existing active (i.e. unstarted or running) update job matching the repo */
    let existing_update_jobs =
        jobs::table
        .order(jobs::id.desc())
        .filter(jobs::kind.eq(JobKind::UpdateRepo.to_db()))
        .filter(jobs::status.le(JobStatus::Started as i16))
        .get_results::<Job>(conn)?;

    for existing_update_job in existing_update_jobs {
        if let Ok(data) = serde_json::from_str::<UpdateRepoJob>(&existing_update_job.contents) {
            if data.repo == repo {
                if existing_update_job.status == JobStatus::New as i16 {
                    old_update_new_job = Some(existing_update_job);
                } else {
                    old_update_started_job = Some(existing_update_job);
                }
                break
            }
        }
    }

    /* We found the last queued active update job for this repo.
     * If it was not started we piggy-back on it, if it was started
     * we make the new job depend on it to ensure we only run one
     * update job per repo in parallel */

    let update_job = match old_update_new_job {
        Some(job) => job,
        None => {
            /* Create a new job */
            diesel::insert_into(schema::jobs::table)
                .values(NewJob {
                    kind: JobKind::UpdateRepo.to_db(),
                    contents: json!(UpdateRepoJob {
                        repo: repo.to_string(),
                    }).to_string(),
                })
                .get_result::<Job>(conn)?
        },
    };

    /* Make new job depend previous started update for this repo (if any) */
    if let Some(previous_started_job) = old_update_started_job {
        diesel::insert_into(schema::job_dependencies::table)
            .values(JobDependency {
                job_id: update_job.id,
                depends_on: previous_started_job.id,
            })
            .execute(conn)?;
    }

    diesel::insert_into(schema::job_dependencies::table)
        .values(JobDependency {
            job_id: update_job.id,
            depends_on: starting_job_id,
        })
        .execute(conn)?;

    Ok(update_job)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CommandOutputSource {
    Stdout,
    Stderr,
}

impl CommandOutputSource {
    fn prefix(&self) -> &str {
        match self {
            CommandOutputSource::Stdout => "|",
            CommandOutputSource::Stderr => ">",
        }
    }
}

#[derive(Debug)]
enum CommandOutput {
    Data(CommandOutputSource, Vec<u8>),
    Closed(CommandOutputSource),
}

fn send_reads<T: Read>(sender: Sender<CommandOutput>, source: CommandOutputSource, mut reader: T) {
    let mut buffer = [0; 4096];
    loop {
        match reader.read(&mut buffer) {
            Ok(num_read) => {
                if num_read == 0 {
                    sender.send(CommandOutput::Closed(source)).unwrap();
                    return;
                } else {
                    let data = buffer[0..num_read].to_vec();
                    sender.send(CommandOutput::Data(source,data)).unwrap();
                }
            },
            Err(e) => {
                error!("Error reading from Command {:?} {}", source, e);
                sender.send(CommandOutput::Closed(source)).unwrap();
                break;
            }
        }
    }
}

fn append_job_log(job_id: i32, conn: &PgConnection, output: &str) {
    if let Err(e) = diesel::update(jobs::table)
        .filter(jobs::id.eq(job_id))
        .set((jobs::log.eq(jobs::log.concat(&output)),))
        .execute(conn) {
            error!("Error appending to job {} log: {}", job_id, e.to_string());
        }
}

fn add_gpg_args(cmd: &mut Command, maybe_gpg_key: &Option<String>, maybe_gpg_homedir: &Option<String>) {
    if let Some(gpg_homedir) = maybe_gpg_homedir {
        cmd
            .arg(format!("--gpg-homedir={}", gpg_homedir));
    };

    if let Some(key) = maybe_gpg_key {
        cmd
            .arg(format!("--gpg-sign={}", key));
    };
}

fn run_command(mut cmd: Command, job_id: i32, conn: &PgConnection) -> JobResult<(bool, String, String)>
{
    info!("/ Running: {:?}", cmd);
    append_job_log(job_id, conn, &format!("Running: {:?}\n", cmd));
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .before_exec (|| {
            // Setsid in the child to avoid SIGINT on server killing
            // child and breaking the graceful shutdown
            unsafe { libc::setsid() };
            Ok(())
        })
        .spawn()
        .or_else(|e| Err(JobError::new(&format!("Can't start command: {}", e))))?;

    let (sender1, receiver) = channel();
    let sender2 = sender1.clone();

    let stdout_reader = child.stdout.take().unwrap();
    let stdout_thread = thread::spawn(move || send_reads(sender1, CommandOutputSource::Stdout, stdout_reader));

    let stderr_reader = child.stderr.take().unwrap();
    let stderr_thread = thread::spawn(move || send_reads(sender2, CommandOutputSource::Stderr, stderr_reader));

    let mut remaining = 2;
    let mut stderr = Vec::new();
    let mut log = Vec::<u8>::new();
    while remaining > 0 {
        match receiver.recv() {
            Ok(CommandOutput::Data(source, v)) => {
                let output = String::from_utf8_lossy(&v);
                append_job_log(job_id, conn, &output);
                 for line in output.split_terminator("\n") {
                    info!("{} {}", source.prefix(), line);
                }
                log.extend(&v);
                if source == CommandOutputSource::Stderr {
                    stderr.extend(&v);
                }
            },
            Ok(CommandOutput::Closed(_)) => remaining -= 1,
            Err(_e) => break,
        }
    }
    stdout_thread.join().unwrap();
    stderr_thread.join().unwrap();

    let status = child.wait().or_else(|e| Err(JobError::new(&format!("Can't wait for command: {}", e))))?;

    let code = status.code().unwrap_or(-1);
    if code != 0 {
        append_job_log(job_id, conn, &format!("status {:?}\n", code))
    }
    info!("\\ status {:?}", status.code().unwrap_or(-1));

    Ok((status.success(),
        String::from_utf8_lossy(&log).to_string(),
        String::from_utf8_lossy(&stderr).to_string()))
}

fn do_commit_build_refs (job_id: i32,
                         build_id: i32,
                         build_refs: &Vec<models::BuildRef>,
                         endoflife: &Option<String>,
                         config: &Arc<Config>,
                         repoconfig: &RepoConfig,
                         conn: &PgConnection)  -> JobResult<serde_json::Value> {
    let build_repo_path = config.build_repo_base.join(build_id.to_string());
    let upload_path = build_repo_path.join("upload");

    let mut src_repo_arg = OsString::from("--src-repo=");
    src_repo_arg.push(&upload_path);

    let mut commits = HashMap::new();

    for build_ref in build_refs.iter() {
        let mut src_ref_arg = String::from("--src-ref=");
        src_ref_arg.push_str(&build_ref.commit);

        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-commit-from")
            .arg("--timestamp=NOW")     // All builds have the same timestamp, not when the individual builds finished
            .arg("--no-update-summary") // We update it once at the end
            .arg("--untrusted")         // Verify that the uploaded objects are correct
            .arg("--force")             // Always generate a new commit even if nothing changed
            .arg("--disable-fsync");    // There is a sync in flatpak build-update-repo, so avoid it here

        add_gpg_args(&mut cmd, &config.build_gpg_key, &config.gpg_homedir);

        if let Some(endoflife) = &endoflife {
            cmd
                .arg(format!("--end-of-life={}", endoflife));
        };

        cmd
            .arg(&src_repo_arg)
            .arg(&src_ref_arg)
            .arg(&build_repo_path)
            .arg(&build_ref.ref_name);

        let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
        if !success {
            return Err(JobError::new(&format!("Failed to build commit for ref {}: {}", &build_ref.ref_name, stderr.trim())))
        }

        let commit = parse_ostree_ref(&build_repo_path, &build_ref.ref_name)?;
        commits.insert(build_ref.ref_name.to_string(), commit);

        if build_ref.ref_name.starts_with("app/") {
            let (filename, contents) = generate_flatpakref(&build_ref.ref_name, Some(build_id), config, repoconfig);
            let path = build_repo_path.join(&filename);
            File::create(&path)?.write_all(contents.as_bytes())?;
        }
    }

    info!("running build-update-repo");

    let mut cmd = Command::new("flatpak");
    cmd
        .arg("build-update-repo")
        .arg(&build_repo_path);

    add_gpg_args(&mut cmd, &config.build_gpg_key, &config.gpg_homedir);

    let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
    if !success {
        return Err(JobError::new(&format!("Failed to updaterepo: {}", stderr.trim())))
    }

    info!("Removing upload directory");

    fs::remove_dir_all(&upload_path)?;

    Ok(json!({ "refs": commits}))
}

fn parse_ostree_ref (repo_path: &path::PathBuf, ref_name: &String) ->JobResult<String> {
    let mut repo_arg = OsString::from("--repo=");
    repo_arg.push(&repo_path);

    match Command::new("ostree")
        .arg("rev-parse")
        .arg(repo_arg)
        .arg(ref_name)
        .output() {
            Ok(output) => {
                if output.status.success() {
                    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
                } else {
                    Err(JobError::new(&format!("Can't find commit for ref {} build refs: {}", ref_name, String::from_utf8_lossy(&output.stderr).trim())))
                }

            },
            Err(e) => Err(JobError::new(&format!("Can't find commit for ref {} build refs: {}", ref_name, e.to_string())))
        }
}

fn list_ostree_refs (repo_path: &path::PathBuf, prefix: &str) ->JobResult<Vec<String>> {
    let mut repo_arg = OsString::from("--repo=");
    repo_arg.push(&repo_path);

    match Command::new("ostree")
        .arg("refs")
        .arg(repo_arg)
        .arg(prefix)
        .output() {
            Ok(output) => {
                if output.status.success() {
                    Ok(String::from_utf8_lossy(&output.stdout).split_whitespace().map(|s| s.to_string()).collect())
                } else {
                    Err(JobError::new(&format!("Can't list refs: {}", String::from_utf8_lossy(&output.stderr).trim())))
                }
            },
            Err(e) => Err(JobError::new(&format!("Can't list refs: {}", e.to_string())))
        }
}


fn handle_commit_job (executor: &JobExecutor, conn: &PgConnection, job_id: i32, job: &CommitJob) -> JobResult<serde_json::Value> {
    // Get build details
    let build_data = builds::table
        .filter(builds::id.eq(job.build))
        .get_result::<models::Build>(conn)
        .or_else(|_e| Err(JobError::new("Can't load build")))?;

    // Get repo config
    let repoconfig = executor.config.get_repoconfig(&build_data.repo)
        .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &build_data.repo))))?;

    // Get the uploaded refs from db
    let build_refs = build_refs::table
        .filter(build_refs::build_id.eq(job.build))
        .get_results::<models::BuildRef>(conn)
        .or_else(|_e| Err(JobError::new("Can't load build refs")))?;

    if build_refs.len() == 0 {
        return Err(JobError::new("No refs in build"));
    }

    // Do the actual work

    let res = do_commit_build_refs(job_id, job.build, &build_refs, &job.endoflife, &executor.config, repoconfig, conn);

    // Update the build repo state in db

    let new_repo_state = match &res {
        Ok(_) => RepoState::Ready,
        Err(e) => RepoState::Failed(e.to_string()),
    };

    conn.transaction::<models::Build, DieselError, _>(|| {
        let current_build = builds::table
            .filter(builds::id.eq(job.build))
            .get_result::<models::Build>(conn)?;
        let current_repo_state = RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
        if !current_repo_state.same_state_as(&RepoState::Verifying) {
            // Something weird was happening, we expected this build to be in the verifying state
            return Err(DieselError::RollbackTransaction)
        };
        let (val, reason) = RepoState::to_db(&new_repo_state);
        diesel::update(builds::table)
            .filter(builds::id.eq(job.build))
            .set((builds::repo_state.eq(val),
                  builds::repo_state_reason.eq(reason)))
            .get_result::<models::Build>(conn)
    })?;

    res
}


fn do_publish (job_id: i32,
               build_id: i32,
               build: &models::Build,
               build_refs: &Vec<models::BuildRef>,
               config: &Arc<Config>,
               repoconfig: &RepoConfig,
               conn: &PgConnection)  -> JobResult<serde_json::Value> {
    let build_repo_path = config.build_repo_base.join(build_id.to_string());

    let mut src_repo_arg = OsString::from("--src-repo=");
    src_repo_arg.push(&build_repo_path);

    // Import commit and modify refs

    let mut cmd = Command::new("flatpak");
    cmd
        .arg("build-commit-from")
        .arg("--force")             // Always generate a new commit even if nothing changed
        .arg("--no-update-summary"); // We update it separately

    add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);

    if let Some(collection_id) = &repoconfig.collection_id {
        for ref extra_id in build.extra_ids.iter() {
            cmd.arg(format!("--extra-collection-id={}.{}", collection_id, extra_id));
        }
    }

    cmd
        .arg(&src_repo_arg)
        .arg(&repoconfig.path);

    let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
    if !success {
        return Err(JobError::new(&format!("Failed to publish repo: {}", stderr.trim())));
    }

    let appstream_dir = repoconfig.path.join("appstream");
    fs::create_dir_all(&appstream_dir)?;

    let screenshots_dir = repoconfig.path.join("screenshots");
    fs::create_dir_all(&screenshots_dir)?;

    let mut commits = HashMap::new();
    for build_ref in build_refs.iter() {
        if build_ref.ref_name.starts_with("app/") || build_ref.ref_name.starts_with("runtime/") {
            let commit = parse_ostree_ref(&repoconfig.path, &build_ref.ref_name)?;
            commits.insert(build_ref.ref_name.to_string(), commit);
        }

        if build_ref.ref_name.starts_with("app/") {
            let (filename, contents) = generate_flatpakref(&build_ref.ref_name, None, config, repoconfig);
            let path = appstream_dir.join(&filename);
            info!("generating {}", &filename);
            let old_contents = fs::read_to_string(&path).unwrap_or_default();
            if contents != old_contents {
                File::create(&path)?.write_all(contents.as_bytes())?;
            }
        }
    }

    for build_ref in build_refs.iter() {
        if build_ref.ref_name.starts_with("screenshots/") {
            info!("extracting screenshots for {}", build_ref.ref_name);
            let mut cmd = Command::new("ostree");
            cmd
                .arg(&format!("--repo={}", &build_repo_path.to_str().unwrap()))
                .arg("checkout")
                .arg("--user-mode")
                .arg("--union")
                .arg(&build_ref.ref_name)
                .arg(&screenshots_dir);
            let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
            if !success {
                return Err(JobError::new(&format!("Failed to extract screenshots: {}", stderr.trim())));
            }
        }
    }

    /* Create update repo job */
    let update_job = queue_update_job (conn, &repoconfig.name, job_id)?;
    append_job_log(job_id, conn, &format!("Queued repository update job {}\nFollow logs at: {}/status/{}\n",
                                          update_job.id, config.base_url, update_job.id));
    info!("Queued repository update job {}", update_job.id);

    Ok(json!({ "refs": commits}))
}

fn handle_publish_job (executor: &JobExecutor, conn: &PgConnection,  job_id: i32, job: &PublishJob) -> JobResult<serde_json::Value> {
    // Get build details
    let build_data = builds::table
        .filter(builds::id.eq(job.build))
        .get_result::<models::Build>(conn)
        .or_else(|_e| Err(JobError::new("Can't load build")))?;

    // Get repo config
    let repoconfig = executor.config.get_repoconfig(&build_data.repo)
        .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &build_data.repo))))?;

    // Get the uploaded refs from db
    let build_refs = build_refs::table
        .filter(build_refs::build_id.eq(job.build))
        .get_results::<models::BuildRef>(conn)
        .or_else(|_e| Err(JobError::new("Can't load build refs")))?;
    if build_refs.len() == 0 {
        return Err(JobError::new("No refs in build"));
    }

    // Do the actual work
    let res = do_publish(job_id, job.build, &build_data, &build_refs, &executor.config, repoconfig, conn);

    // Update the publish repo state in db

    let new_published_state = match &res {
        Ok(_) => PublishedState::Published,
        Err(e) => PublishedState::Failed(e.to_string()),
    };

    conn.transaction::<models::Build, DieselError, _>(|| {
        let current_build = builds::table
            .filter(builds::id.eq(job.build))
            .get_result::<models::Build>(conn)?;
        let current_published_state = PublishedState::from_db(current_build.published_state, &current_build.published_state_reason);
        if !current_published_state.same_state_as(&PublishedState::Publishing) {
            // Something weird was happening, we expected this build to be in the publishing state
            error!("Unexpected publishing state {:?}", current_published_state);
            return Err(DieselError::RollbackTransaction)
        };
        let (val, reason) = PublishedState::to_db(&new_published_state);
        diesel::update(builds::table)
            .filter(builds::id.eq(job.build))
            .set((builds::published_state.eq(val),
                  builds::published_state_reason.eq(reason)))
            .get_result::<models::Build>(conn)
    })?;

    res
}

fn handle_update_repo_job (executor: &JobExecutor, conn: &PgConnection,  job_id: i32, job: UpdateRepoJob) -> JobResult<serde_json::Value> {
    // Get repo config
    let config = &executor.config;
    let repoconfig = executor.config.get_repoconfig(&job.repo)
        .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &job.repo))))?;

    info!("running flatpak build-update-repo");

    let mut cmd = Command::new("flatpak");
    cmd
        .arg("build-update-repo")
        .arg("--generate-static-deltas");

    add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);

    cmd
        .arg(&repoconfig.path);

    let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
    if !success {
        return Err(JobError::new(&format!("Failed to update repo: {}", stderr.trim())));
    }

    if let Some(post_publish_script) = &repoconfig.post_publish_script {
        let mut full_path = std::env::current_dir()?;
        full_path.push(&repoconfig.path);
        let mut cmd = Command::new(post_publish_script);
        cmd
            .arg(&repoconfig.name)
            .arg(&full_path);
        let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
        if !success {
            return Err(JobError::new(&format!("Failed to run post-update-script: {}", stderr.trim())));
        }
    }

    let appstream_dir = repoconfig.path.join("appstream");
    let appstream_arches = list_ostree_refs (&repoconfig.path, "appstream")?;
    for arch in appstream_arches {
        let mut cmd = Command::new("ostree");
        cmd
            .arg(&format!("--repo={}", &repoconfig.path.to_str().unwrap()))
            .arg("checkout")
            .arg("--user-mode")
            .arg("--union")
            .arg(&format!("appstream/{}", arch))
            .arg(appstream_dir.join(arch));
        let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
        if !success {
            return Err(JobError::new(&format!("Failed to extract appstream: {}", stderr.trim())));
        }
    };

    Ok(json!({ }))
}

fn handle_job (executor: &JobExecutor, conn: &PgConnection, job: &Job) {
    let handler_res = match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => {
            if let Ok(commit_job) = serde_json::from_str::<CommitJob>(&job.contents) {
                info!("Handling Commit Job {}: {:?}", job.id, commit_job);
                handle_commit_job (executor, conn, job.id, &commit_job)
            } else {
                Err(JobError::new("Can't parse commit job"))
            }
        },
        Some(JobKind::Publish) => {
            if let Ok(publish_job) = serde_json::from_str::<PublishJob>(&job.contents) {
                info!("Handling Publish Job {}: {:?}", job.id, publish_job);
                handle_publish_job (executor, conn, job.id, &publish_job)
            } else {
                Err(JobError::new("Can't parse publish job"))
            }
        },
        Some(JobKind::UpdateRepo) => {
            if let Ok(update_repo_job) = serde_json::from_str::<UpdateRepoJob>(&job.contents) {
                info!("Handling UpdateRepo Job {}: {:?}", job.id, update_repo_job);
                handle_update_repo_job (executor, conn, job.id, update_repo_job)
            } else {
                Err(JobError::new("Can't parse publish job"))
            }
        },
        _ => {
            Err(JobError::new("Unknown job type"))
        }
    };
    let (new_status, new_results) = match handler_res {
        Ok(json) =>  (JobStatus::Ended, json.to_string()),
        Err(e) => {
            error!("Job {} failed: {}", job.id, e.to_string());
            (JobStatus::Broken, json!(e.to_string()).to_string())
        }
    };
    let update_res =
        diesel::update(jobs::table)
        .filter(jobs::id.eq(job.id))
        .set((jobs::status.eq(new_status as i16),
              jobs::results.eq(new_results)))
        .execute(conn);
    if let Err(e) = update_res {
        error!("handle_job: Error updating job {}", e);
    }
}

fn process_one_job (executor: &JobExecutor, conn: &PgConnection) -> bool {
    use diesel::dsl::exists;
    use diesel::dsl::not;

    let new_job = conn.transaction::<models::Job, _, _>(|| {
        let maybe_new_job = jobs::table
            .filter(jobs::status.eq(JobStatus::New as i16)
                    .and(
                        not(exists(
                            job_dependencies_with_status::table.filter(
                                job_dependencies_with_status::job_id.eq(jobs::id)
                                    .and(job_dependencies_with_status::dependant_status.le(JobStatus::Started as i16))
                            )
                        ))
                    )
            )
            .get_result::<models::Job>(conn);
        if let Ok(new_job) = maybe_new_job {
            diesel::update(jobs::table)
                .filter(jobs::id.eq(new_job.id))
                .set((jobs::status.eq(JobStatus::Started as i16),))
                .get_result::<models::Job>(conn)
        } else {
            maybe_new_job
        }
    });

    match new_job {
        Ok(job) => {
            handle_job (&executor, conn, &job);
            true
        },
        Err(diesel::NotFound) => {
            false
        },
        Err(e) => {
            error!("Unexpected db error processing job: {}", e);
            false
        },
    }
}

pub struct StopJobs();

impl Message for StopJobs {
    type Result = Result<(), ()>;
}

impl Handler<StopJobs> for JobExecutor {
    type Result = Result<(), ()>;

    fn handle(&mut self, _msg: StopJobs, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
        Ok(())
    }
}

pub struct ProcessOneJob();

impl Message for ProcessOneJob {
    type Result = Result<bool, ()>;
}

impl Handler<ProcessOneJob> for JobExecutor {
    type Result = Result<bool, ()>;

    fn handle(&mut self, _msg: ProcessOneJob, _ctx: &mut Self::Context) -> Self::Result {
        let conn = &self.pool.get().map_err(|_e| ())?;
        Ok(process_one_job (&self, conn))
    }
}


// We have an async JobQueue object that wraps the sync JobExecutor, because
// that way we can respond to incomming requests immediately and decide in
// what order to handle them. In particular, we want to prioritize stop
// operations and exit cleanly with outstanding jobs for next run

pub struct JobQueue {
    executor: Addr<JobExecutor>,
    running: bool,
    processing_job: bool,
    jobs_queued: bool,
}

impl JobQueue {
    fn kick(&mut self, ctx: &mut Context<Self>) {
        if !self.running {
            return
        }
        if self.processing_job {
            self.jobs_queued = true;
        } else {
            self.processing_job = true;
            self.jobs_queued = false;

            ctx.spawn(
                self.executor
                    .send (ProcessOneJob())
                    .into_actor(self)
                    .then(|result, queue, ctx| {
                        queue.processing_job = false;

                        if queue.running {
                            let processed_job = match result {
                                Ok(Ok(true)) => true,
                                Ok(Ok(false)) => false,
                                res => {
                                    error!("Unexpected ProcessOneJob result {:?}", res);
                                    false
                                },
                            };

                            // If we ran a job, or a job was queued, kick again
                            if queue.jobs_queued || processed_job {
                                queue.kick(ctx);
                            } else  {
                                // We send a ProcessJobs message each time we added something to the
                                // db, but case something external modifes the db we have a 10 sec
                                // polling loop here.  Ideally this should be using NOTIFY/LISTEN
                                // postgre, but diesel/pq-sys does not currently support it.

                                ctx.run_later(time::Duration::new(10, 0), move |queue, ctx| {
                                    queue.kick(ctx);
                                });
                            }

                        }
                        actix::fut::ok(())
                    })
            );
        }
    }
}

impl Actor for JobQueue {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.kick(ctx); // Run any jobs in db
    }
}

pub struct ProcessJobs();

impl Message for ProcessJobs {
    type Result = Result<(), ()>;
}

impl Handler<ProcessJobs> for JobQueue {
    type Result = Result<(), ()>;

    fn handle(&mut self, _msg: ProcessJobs, ctx: &mut Self::Context) -> Self::Result {
        self.kick(ctx);
        Ok(())
    }
}

pub struct StopJobQueue();

impl Message for StopJobQueue {
    type Result = Result<(), ()>;
}

impl Handler<StopJobQueue> for JobQueue {
    type Result = ActorResponse<JobQueue, (), ()>;

    fn handle(&mut self, _msg: StopJobQueue, _ctx: &mut Self::Context) -> Self::Result {
        self.running = false;
        ActorResponse::async(
            self.executor
                .send (StopJobs())
                .into_actor(self)
                .then(|_result, _queue, _ctx| {
                    actix::fut::ok(())
                }))
    }
}


pub fn start_job_executor(config: Arc<Config>,
                          pool: Pool<ConnectionManager<PgConnection>>) -> Addr<JobQueue> {
    let config_copy = config.clone();
    let jobs_addr = SyncArbiter::start(1, move || JobExecutor {
        config: config_copy.clone(),
        pool: pool.clone()
    });
    JobQueue {
        executor: jobs_addr.clone(),
        running: true,
        processing_job: false,
        jobs_queued: false,
    }.start()
}


pub fn cleanup_started_jobs(pool: &Pool<ConnectionManager<PgConnection>>) -> Result<(), diesel::result::Error> {
    let conn = &pool.get().unwrap();
    {
        use schema::builds::dsl::*;
        let (verifying, _) = RepoState::Verifying.to_db();
        let (purging, _) = RepoState::Purging.to_db();
        let (failed, failed_reason) = RepoState::Failed("Server was restarted during job".to_string()).to_db();
        let n_updated =
            diesel::update(builds)
            .filter(repo_state.eq(verifying).or(repo_state.eq(purging)))
            .set((repo_state.eq(failed),
                  repo_state_reason.eq(failed_reason)))
            .execute(conn)?;
        if n_updated != 0 {
            error!("Marked {} builds as failed due to in progress jobs on startup", n_updated);
        }
        let (publishing, _) = PublishedState::Publishing.to_db();
        let (failed_publish, failed_publish_reason) = PublishedState::Failed("Server was restarted during publish".to_string()).to_db();
        let n_updated2 =
            diesel::update(builds)
            .filter(published_state.eq(publishing))
            .set((published_state.eq(failed_publish),
                  published_state_reason.eq(failed_publish_reason)))
            .execute(conn)?;
        if n_updated2 != 0 {
            error!("Marked {} builds as failed to publish due to in progress jobs on startup", n_updated2);
        }
    };
    {
        use schema::jobs::dsl::*;
        let n_updated =
            diesel::update(jobs)
            .filter(status.eq(JobStatus::Started as i16))
            .set((status.eq(JobStatus::Broken as i16),))
            .execute(conn)?;
        if n_updated != 0 {
            error!("Marked {} jobs as broken due to being started already at startup", n_updated);
        }
    };
    Ok(())
}
