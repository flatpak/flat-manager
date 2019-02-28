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
use std::process::{Command, Stdio};
use std::sync::{Arc};
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time;
use std::os::unix::process::CommandExt;
use libc;
use std::collections::HashMap;

use ostree;
use app::{RepoConfig, Config};
use errors::{JobError, JobResult};
use models::{NewJob, Job, JobDependency, JobKind, CommitJob, PublishJob, UpdateRepoJob, JobStatus, job_dependencies_with_status, RepoState, PublishedState };
use models;
use schema::*;
use schema;

fn generate_flatpakref(ref_name: &String,
                       maybe_build_id: Option<i32>,
                       config: &Config,
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
                        start_time: time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs(),
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

pub struct JobExecutor {
    pub config: Arc<Config>,
    pub pool: Pool<ConnectionManager<PgConnection>>,
}

impl Actor for JobExecutor {
    type Context = SyncContext<Self>;
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


fn new_job_instance(job: Job) -> Box<JobInstance> {
    match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => CommitJobInstance::new(job),
        Some(JobKind::Publish) => PublishJobInstance::new(job),
        Some(JobKind::UpdateRepo) => UpdateRepoJobInstance::new(job),
        _ => InvalidJobInstance::new(job, JobError::new("Unknown job type")),
    }
}

pub trait JobInstance {
    fn get_job_id (&self) -> i32;
    fn should_start_job (&self, _config: &Config) -> bool {
        true
    }
    fn order (&self) -> i32 {
        0
    }
    fn handle_job (&mut self, executor: &JobExecutor, conn: &PgConnection) -> JobResult<serde_json::Value>;
}

struct InvalidJobInstance {
    pub job_id: i32,
    pub error: JobError,
}

impl InvalidJobInstance {
    fn new(job: Job,
           error: JobError) -> Box<JobInstance> {
        Box::new(InvalidJobInstance {
            job_id: job.id,
            error: error,
        })
    }
}

impl JobInstance for InvalidJobInstance {
    fn get_job_id (&self) -> i32 {
        self.job_id
    }

    fn handle_job (&mut self, _executor: &JobExecutor, _conn: &PgConnection) -> JobResult<serde_json::Value> {
        Err(self.error.clone())
    }
}


#[derive(Debug)]
struct CommitJobInstance {
    pub job_id: i32,
    pub build_id: i32,
    pub endoflife: Option<String>,
}

impl CommitJobInstance {
    fn new(job: Job) -> Box<JobInstance> {
        if let Ok(commit_job) = serde_json::from_str::<CommitJob>(&job.contents) {
            Box::new(CommitJobInstance {
                job_id: job.id,
                build_id: commit_job.build,
                endoflife: commit_job.endoflife,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse commit job"))
        }
    }


    fn do_commit_build_refs (&self,
                             build_refs: &Vec<models::BuildRef>,
                             config: &Config,
                             repoconfig: &RepoConfig,
                             conn: &PgConnection)  -> JobResult<serde_json::Value> {
        let build_repo_path = config.build_repo_base.join(self.build_id.to_string());
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

            if let Some(endoflife) = &self.endoflife {
                cmd
                    .arg(format!("--end-of-life={}", endoflife));
            };

            cmd
                .arg(&src_repo_arg)
                .arg(&src_ref_arg)
                .arg(&build_repo_path)
                .arg(&build_ref.ref_name);

            let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
            if !success {
                return Err(JobError::new(&format!("Failed to build commit for ref {}: {}", &build_ref.ref_name, stderr.trim())))
            }

            let commit = ostree::parse_ref(&build_repo_path, &build_ref.ref_name)?;
            commits.insert(build_ref.ref_name.to_string(), commit);

            if build_ref.ref_name.starts_with("app/") {
                let (filename, contents) = generate_flatpakref(&build_ref.ref_name, Some(self.build_id), config, repoconfig);
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

        let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
        if !success {
            return Err(JobError::new(&format!("Failed to updaterepo: {}", stderr.trim())))
        }

        info!("Removing upload directory");

        fs::remove_dir_all(&upload_path)?;

        Ok(json!({ "refs": commits}))
    }
}

impl JobInstance for CommitJobInstance {
    fn get_job_id (&self) -> i32 {
        self.job_id
    }

    fn handle_job (&mut self, executor: &JobExecutor, conn: &PgConnection) -> JobResult<serde_json::Value> {
        info!("Handling Commit Job {}: {:?}", self.job_id, self);

        let config = &executor.config;

        // Get build details
        let build_data = builds::table
            .filter(builds::id.eq(self.build_id))
            .get_result::<models::Build>(conn)
            .or_else(|_e| Err(JobError::new("Can't load build")))?;

        // Get repo config
        let repoconfig = config.get_repoconfig(&build_data.repo)
            .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &build_data.repo))))?;

        // Get the uploaded refs from db
        let build_refs = build_refs::table
            .filter(build_refs::build_id.eq(self.build_id))
            .get_results::<models::BuildRef>(conn)
            .or_else(|_e| Err(JobError::new("Can't load build refs")))?;

        if build_refs.len() == 0 {
            return Err(JobError::new("No refs in build"));
        }

        // Do the actual work

        let res = self.do_commit_build_refs(&build_refs, config, repoconfig, conn);

        // Update the build repo state in db

        let new_repo_state = match &res {
            Ok(_) => RepoState::Ready,
            Err(e) => RepoState::Failed(e.to_string()),
        };

        conn.transaction::<models::Build, DieselError, _>(|| {
            let current_build = builds::table
                .filter(builds::id.eq(self.build_id))
                .get_result::<models::Build>(conn)?;
            let current_repo_state = RepoState::from_db(current_build.repo_state, &current_build.repo_state_reason);
            if !current_repo_state.same_state_as(&RepoState::Verifying) {
                // Something weird was happening, we expected this build to be in the verifying state
                return Err(DieselError::RollbackTransaction)
            };
            let (val, reason) = RepoState::to_db(&new_repo_state);
            diesel::update(builds::table)
                .filter(builds::id.eq(self.build_id))
                .set((builds::repo_state.eq(val),
                      builds::repo_state_reason.eq(reason)))
                .get_result::<models::Build>(conn)
        })?;

        res
    }
}


#[derive(Debug)]
struct PublishJobInstance {
    pub job_id: i32,
    pub build_id: i32,
}

impl PublishJobInstance {
    fn new(job: Job) -> Box<JobInstance> {
        if let Ok(publish_job) = serde_json::from_str::<PublishJob>(&job.contents) {
            Box::new(PublishJobInstance {
                job_id: job.id,
                build_id: publish_job.build,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse publish job"))
        }
    }

    fn do_publish (&self,
                   build: &models::Build,
                   build_refs: &Vec<models::BuildRef>,
                   config: &Config,
                   repoconfig: &RepoConfig,
                   conn: &PgConnection)  -> JobResult<serde_json::Value> {
        let build_repo_path = config.build_repo_base.join(self.build_id.to_string());

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

        let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
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
                let commit = ostree::parse_ref(&repoconfig.path, &build_ref.ref_name)?;
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
                let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
                if !success {
                    return Err(JobError::new(&format!("Failed to extract screenshots: {}", stderr.trim())));
                }
            }
        }

        /* Create update repo job */
        let update_job = queue_update_job (conn, &repoconfig.name, self.job_id)?;
        append_job_log(self.job_id, conn, &format!("Queued repository update job {}\nFollow logs at: {}/status/{}\n",
                                              update_job.id, config.base_url, update_job.id));
        info!("Queued repository update job {}", update_job.id);

        Ok(json!({ "refs": commits}))
    }
}

impl JobInstance for PublishJobInstance {
    fn get_job_id (&self) -> i32 {
        self.job_id
    }

    fn order (&self) -> i32 {
        1 /* Delay publish after commits (and other normal ops). because the
            commits may generate more publishes. */
    }

    fn handle_job (&mut self, executor: &JobExecutor, conn: &PgConnection) -> JobResult<serde_json::Value> {
        info!("Handling Publish Job {}: {:?}", self.job_id, self);

        let config = &executor.config;

        // Get build details
        let build_data = builds::table
            .filter(builds::id.eq(self.build_id))
            .get_result::<models::Build>(conn)
            .or_else(|_e| Err(JobError::new("Can't load build")))?;

        // Get repo config
        let repoconfig = config.get_repoconfig(&build_data.repo)
            .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &build_data.repo))))?;

        // Get the uploaded refs from db
        let build_refs = build_refs::table
        .filter(build_refs::build_id.eq(self.build_id))
            .get_results::<models::BuildRef>(conn)
            .or_else(|_e| Err(JobError::new("Can't load build refs")))?;
        if build_refs.len() == 0 {
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
            let current_published_state = PublishedState::from_db(current_build.published_state, &current_build.published_state_reason);
            if !current_published_state.same_state_as(&PublishedState::Publishing) {
                // Something weird was happening, we expected this build to be in the publishing state
                error!("Unexpected publishing state {:?}", current_published_state);
                return Err(DieselError::RollbackTransaction)
            };
            let (val, reason) = PublishedState::to_db(&new_published_state);
            diesel::update(builds::table)
                .filter(builds::id.eq(self.build_id))
                .set((builds::published_state.eq(val),
                      builds::published_state_reason.eq(reason)))
                .get_result::<models::Build>(conn)
        })?;

        res
    }
}

#[derive(Debug)]
struct UpdateRepoJobInstance {
    pub job_id: i32,
    pub repo: String,
    pub start_time: u64,
}

impl UpdateRepoJobInstance {
    fn new(job: Job) -> Box<JobInstance> {
        if let Ok(update_repo_job) = serde_json::from_str::<UpdateRepoJob>(&job.contents) {
            Box::new(UpdateRepoJobInstance {
                job_id: job.id,
                repo: update_repo_job.repo,
                start_time: update_repo_job.start_time,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse publish job"))
        }
    }
}

impl JobInstance for UpdateRepoJobInstance {
    fn get_job_id (&self) -> i32 {
        self.job_id
    }

    fn should_start_job (&self, config: &Config) -> bool {
        /* We always wait a bit before starting an update to allow
         * more jobs to merge */
        time::SystemTime::now().duration_since(time::UNIX_EPOCH).unwrap().as_secs() - self.start_time > config.delay_update_secs
    }

    fn order (&self) -> i32 {
        2 /* Delay updates after publish so they can be chunked. */
    }

    fn handle_job (&mut self, executor: &JobExecutor, conn: &PgConnection) -> JobResult<serde_json::Value> {
        info!("Handling UpdateRepo Job {}: {:?}", self.job_id, self);
        // Get repo config
        let config = &executor.config;
        let repoconfig = config.get_repoconfig(&self.repo)
            .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &self.repo))))?;

        info!("running flatpak build-update-repo");

        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-update-repo")
            .arg("--generate-static-deltas");

        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);

        cmd
            .arg(&repoconfig.path);

        let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
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
            let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
            if !success {
                return Err(JobError::new(&format!("Failed to run post-update-script: {}", stderr.trim())));
            }
        }

        let appstream_dir = repoconfig.path.join("appstream");
        let appstream_arches = ostree::list_refs (&repoconfig.path, "appstream");
        for arch in appstream_arches {
            let mut cmd = Command::new("ostree");
            cmd
                .arg(&format!("--repo={}", &repoconfig.path.to_str().unwrap()))
                .arg("checkout")
                .arg("--user-mode")
                .arg("--union")
                .arg(&format!("appstream/{}", arch))
                .arg(appstream_dir.join(arch));
            let (success, _log, stderr) = run_command(cmd, self.job_id, conn)?;
            if !success {
                return Err(JobError::new(&format!("Failed to extract appstream: {}", stderr.trim())));
            }
        };

        Ok(json!({ }))
    }
}

fn process_one_job (executor: &mut JobExecutor, conn: &PgConnection) -> bool {
    use diesel::dsl::exists;
    use diesel::dsl::not;

    /* Find next job (if any) and mark it started */
    let new_instance = conn.transaction::<Box<JobInstance>, _, _>(|| {
        let mut new_instances : Vec<Box<JobInstance>> = jobs::table
            .order(jobs::id)
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
            .get_results::<models::Job>(conn)?
            .into_iter()
            .map(move |job| new_job_instance(job))
            .collect();

        new_instances.sort_by(|a, b| a.order().cmp(&b.order()));

        for new_instance in new_instances {
            if new_instance.should_start_job(&executor.config) {
                diesel::update(jobs::table)
                    .filter(jobs::id.eq(new_instance.get_job_id()))
                    .set((jobs::status.eq(JobStatus::Started as i16),))
                    .execute(conn)?;
                return Ok(new_instance)
            }
        }

        Err(diesel::NotFound)
    });

    match new_instance {
        Ok(mut instance) => {
            let (new_status, new_results) =
                match instance.handle_job(executor, conn) {
                    Ok(json) =>  (JobStatus::Ended, json.to_string()),
                    Err(e) => {
                        error!("Job {} failed: {}", instance.get_job_id(), e.to_string());
                        (JobStatus::Broken, json!(e.to_string()).to_string())
                    }
                };

            let update_res =
                diesel::update(jobs::table)
                .filter(jobs::id.eq(instance.get_job_id()))
                .set((jobs::status.eq(new_status as i16),
                      jobs::results.eq(new_results)))
                .execute(conn);
            if let Err(e) = update_res {
                error!("handle_job: Error updating job {}", e);
            }
            true /* We handled a job */
        },
        Err(diesel::NotFound) => {
            false /* We didn't handle a job */
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

    fn handle(&mut self, _msg: StopJobs, ctx: &mut SyncContext<JobExecutor>) -> Self::Result {
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
        Ok(process_one_job (self, conn))
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
