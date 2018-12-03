use actix::prelude::*;
use actix::{Actor, SyncContext};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::result::{Error as DieselError};
use diesel;
use serde_json;
use std::env;
use std::str;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path;
use std::process::{Command, Stdio};
use std::sync::{Arc};
use std::sync::mpsc::{channel, Sender};
use std::thread;
use std::time;
use std::os::unix::process::CommandExt;
use libc;
use std::collections::HashMap;

use app::Config;
use errors::{JobError, JobResult};
use models::{Job, JobKind, CommitJob, PublishJob, JobStatus, job_dependencies_with_status, RepoState, PublishedState };
use models;
use schema::*;

pub struct JobExecutor {
    pub config: Arc<Config>,
    pub pool: Pool<ConnectionManager<PgConnection>>,
}

impl Actor for JobExecutor {
    type Context = SyncContext<Self>;
}

fn generate_flatpakref(ref_name: &String, maybe_build_id: Option<i32>, config: &Arc<Config>) -> (String, String) {
    let parts: Vec<&str> = ref_name.split('/').collect();

    let filename = format!("{}.flatpakref", parts[1]);

    let (url, maybe_gpg_content) = match maybe_build_id {
        Some(build_id) => (format!("{}/build-repo/{}", config.base_url, build_id), &config.build_gpg_key_content),
        None => (format!("{}/repo", config.base_url), &config.main_gpg_key_content),
    };

    let gpg_line = match maybe_gpg_content {
        Some(gpg_content) => format!("GPGKey={}\n", gpg_content),
        None => "".to_string(),
    };

    let contents = format!(r#"
[Flatpak Ref]
Name={}
Branch={}
Title={} from flathub
Url={}
RuntimeRepo=https://dl.flathub.org/repo/flathub.flatpakrepo
SuggestRemoteName=flathub
IsRuntime=false
{}"#,
                           parts[1],
                           parts[3],
                           parts[1],
                           url,
                           gpg_line);
    (filename, contents)
}

fn init_ostree_repo(repo_path: &path::PathBuf, parent_repo_path: &path::PathBuf, build_id: i32, opt_collection_id: &Option<String>) -> io::Result<()> {
    let parent_repo_absolute_path = env::current_dir()?.join(parent_repo_path);

    for &d in ["extensions",
               "objects",
               "refs/heads",
               "refs/mirrors",
               "refs/remotes",
               "state",
               "tmp/cache"].iter() {
        fs::create_dir_all(repo_path.join(d))?;
    }

    let mut file = File::create(repo_path.join("config"))?;
    file.write_all(format!(r#"
[core]
repo_version=1
mode=archive-z2
{}parent={}"#,
                           match opt_collection_id {
                               Some(collection_id) => format!("collection-id={}.Build{}\n", collection_id, build_id),
                               _ => "".to_string(),
                           },
                           parent_repo_absolute_path.display()).as_bytes())?;
    Ok(())
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
                         conn: &PgConnection)  -> JobResult<serde_json::Value> {
    let build_repo_path = config.build_repo_base_path.join(build_id.to_string());
    let upload_path = build_repo_path.join("upload");

    init_ostree_repo (&build_repo_path, &config.repo_path, build_id, &config.collection_id)?;
    init_ostree_repo (&upload_path, &config.repo_path, build_id, &None)?;

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
            let (filename, contents) = generate_flatpakref(&build_ref.ref_name, Some(build_id), config);
            let mut file = File::create(build_repo_path.join(filename))?;
            file.write_all(contents.as_bytes())?;
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
    // Get the uploaded refs from db

    let build_refs = build_refs::table
        .filter(build_refs::build_id.eq(job.build))
        .get_results::<models::BuildRef>(conn)
        .or_else(|_e| Err(JobError::new("Can't load build refs")))?;
    if build_refs.len() == 0 {
        return Err(JobError::new("No refs in build"));
    }

    // Do the actual work

    let res = do_commit_build_refs(job_id, job.build, &build_refs, &&job.endoflife, &executor.config, conn);

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
               build_refs: &Vec<models::BuildRef>,
               config: &Arc<Config>,
               conn: &PgConnection)  -> JobResult<serde_json::Value> {
    let build_repo_path = config.build_repo_base_path.join(build_id.to_string());

    let mut src_repo_arg = OsString::from("--src-repo=");
    src_repo_arg.push(&build_repo_path);

    // Import commit and modify refs

    let mut cmd = Command::new("flatpak");
    cmd
        .arg("build-commit-from")
        .arg("--no-update-summary"); // We update it separately

    add_gpg_args(&mut cmd, &config.main_gpg_key, &config.gpg_homedir);

    cmd
        .arg(&src_repo_arg)
        .arg(&config.repo_path);

    let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
    if !success {
        return Err(JobError::new(&format!("Failed to publish repo: {}", stderr.trim())));
    }

    let appstream_dir = config.repo_path.join("appstream");
    fs::create_dir_all(&appstream_dir)?;

    let screenshots_dir = config.repo_path.join("screenshots");
    fs::create_dir_all(&screenshots_dir)?;

    let mut commits = HashMap::new();
    for build_ref in build_refs.iter() {
        println!("build_ref {:?}", build_ref);
        if build_ref.ref_name.starts_with("app/") || build_ref.ref_name.starts_with("runtime/") {
            let commit = parse_ostree_ref(&config.repo_path, &build_ref.ref_name)?;
            commits.insert(build_ref.ref_name.to_string(), commit);
        }

        if build_ref.ref_name.starts_with("app/") {
            let (filename, contents) = generate_flatpakref(&build_ref.ref_name, None, config);
            info!("generating {}", filename);
            let mut file = File::create(appstream_dir.join(filename))?;
            file.write_all(contents.as_bytes())?;
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

    // Update repo

    info!("running flatpak build-update-repo");

    let mut cmd = Command::new("flatpak");
    cmd
        .arg("build-update-repo")
        .arg("--generate-static-deltas");

    add_gpg_args(&mut cmd, &config.main_gpg_key, &config.gpg_homedir);

    cmd
        .arg(&config.repo_path);

    let (success, _log, stderr) = run_command(cmd, job_id, conn)?;
    if !success {
        return Err(JobError::new(&format!("Failed to update repo: {}", stderr.trim())));
    }

    // TODO: PURGE summary and summary.sig files here

    let appstream_arches = list_ostree_refs (&config.repo_path, "appstream")?;
    for arch in appstream_arches {
        let mut cmd = Command::new("ostree");
        cmd
            .arg(&format!("--repo={}", &config.repo_path.to_str().unwrap()))
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

    Ok(json!({ "refs": commits}))
}

fn handle_publish_job (executor: &JobExecutor, conn: &PgConnection,  job_id: i32, job: &PublishJob) -> JobResult<serde_json::Value> {
    // Get the uploaded refs from db

    let build_refs = build_refs::table
        .filter(build_refs::build_id.eq(job.build))
        .get_results::<models::BuildRef>(conn)
        .or_else(|_e| Err(JobError::new("Can't load build refs")))?;
    if build_refs.len() == 0 {
        return Err(JobError::new("No refs in build"));
    }

    // Do the actual work

    let res = do_publish(job_id, job.build, &build_refs, &executor.config, conn);

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
