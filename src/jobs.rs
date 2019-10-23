use actix::prelude::*;
use actix::{Actor, SyncContext};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::result::{Error as DieselError};
use diesel::result::DatabaseErrorKind::SerializationFailure;
use diesel;
use filetime;
use serde_json;
use std::cell::RefCell;
use std::str;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::{Arc};
use std::path::PathBuf;
use std::time;
use std::os::unix::process::CommandExt;
use libc;
use std::collections::{HashMap,HashSet};
use std::iter::FromIterator;
use walkdir::WalkDir;
use std::sync::mpsc;

use ostree;
use app::{RepoConfig, Config};
use errors::{JobError, JobResult};
use models::{NewJob, Job, JobDependency, JobKind, CommitJob, PublishJob, UpdateRepoJob, JobStatus, job_dependencies_with_status, RepoState, PublishedState };
use deltas::{DeltaGenerator, DeltaRequest, DeltaRequestSync};
use models;
use schema::*;
use schema;

/**************************************************************************
 * Job handling - theory of operations.
 *
 * The job queue is stored in the database, and we regularly (on a
 * timer and triggered via in-process ProcessJobs messages) check for new
 * jobs and execute them.
 *
 * All jobs have a status which is:
 *   New - queued but not started
 *   Started - set when we start working on a job
 *   Ended - set when the job is done
 *   Broken - set when we get some internal error working on a job,
 *            or if an old job was marked "Started" already on startup.
 *
 * All jobs are run on single-threaded blocking actors, but we have
 * multiple of those. One per configured repo, and one for the builds.
 * This way we avoid any races with multiple things modifying a single
 * repo, but still allow concurrent build commits with a repo update.
 *
 * There is also an async JobQueue actor which manages the sync ones,
 * handling the queue of jobs and other messages such as StopJobs to
 * shut down things.
 *
 * Since job status is changed on multiple thread all the
 * modification/access to that is serialized in the db using serialized
 * transactions.
 *
 ************************************************************************/

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

    /* We only want to deploy the collection ID if the flatpakref is being generated for the main
     * repo not a build repo.
     */
    if let Some(collection_id) = &repoconfig.collection_id {
        if repoconfig.deploy_collection_id && maybe_build_id == None {
            contents.push_str(&format!("DeployCollectionID={}\n", collection_id));
        }
    };

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

fn queue_update_job (delay_secs: u64,
                     conn: &PgConnection,
                     repo: &str,
                     starting_job_id: Option<i32>) -> Result<(bool,Job), DieselError>
{
    /* We wrap everything in a serializable transaction, because if something else
     * starts the job while we're adding dependencies to it the dependencies will be
     * ignored.
     */

    let transaction_result =
        conn
        .build_transaction()
        .serializable()
        .deferrable()
        .run(|| {
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

            let (is_new, update_job) = match old_update_new_job {
                Some(job) => (false, job),
                None => {
                    /* Create a new job */
                    let new_job =
                        diesel::insert_into(schema::jobs::table)
                        .values(NewJob {
                            kind: JobKind::UpdateRepo.to_db(),
                            repo: Some(repo.to_string()),
                            start_after: Some(time::SystemTime::now() + time::Duration::new(delay_secs, 0)),
                            contents: json!(UpdateRepoJob {
                                repo: repo.to_string()
                            }).to_string(),
                        })
                        .get_result::<Job>(conn)?;
                    (true, new_job)
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

            if let Some(depends_on) = starting_job_id {
                diesel::insert_into(schema::job_dependencies::table)
                    .values(JobDependency {
                        job_id: update_job.id,
                        depends_on: depends_on,
                    })
                    .execute(conn)?;
            }

            Ok((is_new, update_job))
        });

    /* Retry on serialization failure */
    match transaction_result {
        Err(DieselError::DatabaseError(SerializationFailure, _)) => queue_update_job (delay_secs, conn, repo, starting_job_id),
        _ => transaction_result
    }
}

pub struct JobExecutor {
    pub repo: Option<String>,
    pub config: Arc<Config>,
    pub delta_generator: Addr<DeltaGenerator>,
    pub pool: Pool<ConnectionManager<PgConnection>>,
}

impl Actor for JobExecutor {
    type Context = SyncContext<Self>;
}

fn job_log(job_id: i32, conn: &PgConnection, output: &str) {
    if let Err(e) = diesel::update(jobs::table)
        .filter(jobs::id.eq(job_id))
        .set((jobs::log.eq(jobs::log.concat(&output)),))
        .execute(conn) {
            error!("Error appending to job {} log: {}", job_id, e.to_string());
        }
}

fn job_log_and_info(job_id: i32, conn: &PgConnection, output: &str) {
    info!("#{}: {}", job_id, output);
    job_log(job_id, conn, &format!("{}\n", output));
}

fn job_log_and_error(job_id: i32, conn: &PgConnection, output: &str) {
    error!("#{}: {}", job_id, output);
    job_log(job_id, conn, &format!("{}\n", output));
}

fn do_command(mut cmd: Command) -> JobResult<()>
{
    let output =
        unsafe {
            cmd
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .pre_exec (|| {
                    // Setsid in the child to avoid SIGINT on server killing
                    // child and breaking the graceful shutdown
                    libc::setsid();
                    Ok(())
                })
                .output()
                .map_err(|e| JobError::new(&format!("Failed to run {:?}: {}", &cmd, e)))?
        };

    if !output.status.success() {
        return Err(JobError::new(&format!("Command {:?} exited unsuccesfully: {}", &cmd, String::from_utf8_lossy(&output.stderr))))
    }
    Ok(())
}

fn new_job_instance(executor: &JobExecutor, job: Job) -> Box<dyn JobInstance> {
    match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => CommitJobInstance::new(job),
        Some(JobKind::Publish) => PublishJobInstance::new(job),
        Some(JobKind::UpdateRepo) => UpdateRepoJobInstance::new(job, executor.delta_generator.clone()),
        _ => InvalidJobInstance::new(job, JobError::new("Unknown job type")),
    }
}

pub trait JobInstance {
    fn get_job_id (&self) -> i32;
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
           error: JobError) -> Box<dyn JobInstance> {
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
    pub endoflife_rebase: Option<String>,
}

impl CommitJobInstance {
    fn new(job: Job) -> Box<dyn JobInstance> {
        if let Ok(commit_job) = serde_json::from_str::<CommitJob>(&job.contents) {
            Box::new(CommitJobInstance {
                job_id: job.id,
                build_id: commit_job.build,
                endoflife: commit_job.endoflife,
                endoflife_rebase: commit_job.endoflife_rebase,
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

        let endoflife_rebase_arg = if let Some(endoflife_rebase) = &self.endoflife_rebase {
            if let Some(app_ref) = build_refs.iter().filter(|app_ref| app_ref.ref_name.starts_with("app/")).nth(0) {
                Some(format!("--end-of-life-rebase={}={}", app_ref.ref_name.split('/').nth(1).unwrap(), endoflife_rebase))
            } else {
                None
            }
        } else {
            None
        };

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

            if let Some(endoflife_rebase_arg) = &endoflife_rebase_arg {
                cmd
                    .arg(&endoflife_rebase_arg);
            };

            cmd
                .arg(&src_repo_arg)
                .arg(&src_ref_arg)
                .arg(&build_repo_path)
                .arg(&build_ref.ref_name);

            job_log_and_info(self.job_id, conn, &format!("Committing ref {} ({})", build_ref.ref_name, build_ref.commit));
            do_command(cmd)?;

            let commit = ostree::parse_ref(&build_repo_path, &build_ref.ref_name)?;
            commits.insert(build_ref.ref_name.to_string(), commit);

            if build_ref.ref_name.starts_with("app/") {
                let (filename, contents) = generate_flatpakref(&build_ref.ref_name, Some(self.build_id), config, repoconfig);
                let path = build_repo_path.join(&filename);
                File::create(&path)?.write_all(contents.as_bytes())?;
            }
        }


        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-update-repo")
            .arg(&build_repo_path);

        add_gpg_args(&mut cmd, &config.build_gpg_key, &config.gpg_homedir);

        job_log_and_info(self.job_id, conn, "running build-update-repo");
        do_command(cmd)?;

        job_log_and_info(self.job_id, conn, "Removing upload directory");
        fs::remove_dir_all(&upload_path)?;

        Ok(json!({ "refs": commits}))
    }
}

impl JobInstance for CommitJobInstance {
    fn get_job_id (&self) -> i32 {
        self.job_id
    }

    fn handle_job (&mut self, executor: &JobExecutor, conn: &PgConnection) -> JobResult<serde_json::Value> {
        info!("#{}: Handling Job Commit: build: {}, end-of-life: {}, eol-rebase: {}",
              &self.job_id, &self.build_id, self.endoflife.as_ref().unwrap_or(&"".to_string()), self.endoflife_rebase.as_ref().unwrap_or(&"".to_string()));

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
    fn new(job: Job) -> Box<dyn JobInstance> {
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

        job_log_and_info(self.job_id, conn,
                         &format!("Importing build to repo {}", repoconfig.name));
        do_command(cmd)?;

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
                job_log_and_info (self.job_id, conn, &format!("generating {}", &filename));
                let old_contents = fs::read_to_string(&path).unwrap_or_default();
                if contents != old_contents {
                    File::create(&path)?.write_all(contents.as_bytes())?;
                }
            }
        }

        for build_ref in build_refs.iter() {
            if build_ref.ref_name.starts_with("screenshots/") {
                job_log_and_info (self.job_id, conn, &format!("extracting {}", build_ref.ref_name));
                let mut cmd = Command::new("ostree");
                cmd
                    .arg(&format!("--repo={}", &build_repo_path.to_str().unwrap()))
                    .arg("checkout")
                    .arg("--user-mode")
                    .arg("--bareuseronly-dirs")
                    .arg("--union")
                    .arg(&build_ref.ref_name)
                    .arg(&screenshots_dir);
                do_command(cmd)?;
            }
        }

        /* Create update repo job */
        let delay = config.delay_update_secs;
        let (is_new, update_job) = queue_update_job (delay, conn, &repoconfig.name, Some(self.job_id))?;
        if is_new {
            job_log_and_info(self.job_id, conn,
                                    &format!("Queued repository update job {}{}",
                                             update_job.id, match delay {
                                                 0 => "".to_string(),
                                                 _ => format!(" in {} secs", delay),
                                             }));
        } else {
            job_log_and_info(self.job_id, conn,
                             &format!("Piggy-backed on existing update job {}", update_job.id));
        }

        Ok(json!({
            "refs": commits,
            "update-repo-job": update_job.id,
        }))
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
        info!("#{}: Handling Job Publish: build: {}",
              &self.job_id, &self.build_id);

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
    pub delta_generator: Addr<DeltaGenerator>,
    pub job_id: i32,
    pub repo: String,
}

impl UpdateRepoJobInstance {
    fn new(job: Job, delta_generator: Addr<DeltaGenerator>) -> Box<dyn JobInstance> {
        if let Ok(update_repo_job) = serde_json::from_str::<UpdateRepoJob>(&job.contents) {
            Box::new(UpdateRepoJobInstance {
                delta_generator: delta_generator,
                job_id: job.id,
                repo: update_repo_job.repo,
            })
        } else {
            InvalidJobInstance::new(job, JobError::new("Can't parse publish job"))
        }
    }

    fn calculate_deltas(&self, repoconfig: &RepoConfig) -> (HashSet<ostree::Delta>, HashSet<ostree::Delta>) {
        let repo_path = repoconfig.get_abs_repo_path();

        let mut wanted_deltas = HashSet::new();
        let refs = ostree::list_refs (&repo_path, "");

        for ref_name in refs {
            let depth = repoconfig.get_delta_depth_for_ref(&ref_name);

            if depth > 0 {
                let ref_deltas = ostree::calc_deltas_for_ref(&repo_path, &ref_name, depth);
                for ref_delta in ref_deltas {
                    wanted_deltas.insert(ref_delta);
                }
            }
        }
        let old_deltas = HashSet::from_iter(ostree::list_deltas (&repo_path).iter().cloned());

        let missing_deltas = wanted_deltas.difference(&old_deltas).cloned().collect();
        let unwanted_deltas = old_deltas.difference(&wanted_deltas).cloned().collect();

        (missing_deltas, unwanted_deltas)
    }

    fn generate_deltas(&self,
                       deltas: &HashSet<ostree::Delta>,
                       repoconfig: &RepoConfig,
                       conn: &PgConnection) -> JobResult<()> {
        job_log_and_info(self.job_id, conn, "Generating deltas");

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
                Ok(()) => format!(" {}", delta.to_string()),
                Err(e) => format!(" failed to generate {}: {}", delta.to_string(), e),
            };
            job_log_and_info(self.job_id, conn, &message);
        }

        job_log_and_info(self.job_id, conn, "All deltas generated");

        Ok(())
    }

    fn retire_deltas(&self,
                     deltas: &HashSet<ostree::Delta>,
                     repoconfig: &RepoConfig,
                     conn: &PgConnection) -> JobResult<()> {
        job_log_and_info(self.job_id, conn, "Cleaning out old deltas");
        let repo_path = repoconfig.get_abs_repo_path();
        let deltas_dir = repo_path.join("deltas");
        let tmp_deltas_dir = repo_path.join("tmp/deltas");
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
            fs::create_dir_all(&dst_parent)?;

            job_log_and_info(self.job_id, conn,
                                    &format!(" Queuing delta {:?} for deletion", src.strip_prefix(&deltas_dir).unwrap()));

            if dst.exists() {
                fs::remove_dir_all(&dst)?;
            }
            fs::rename(&src, &dst)?;

            /* Update mtime so we can use it to trigger deletion */
            filetime::set_file_times(&dst, now_filetime, now_filetime)?;
        }

        /* Delete all temporary deltas older than one hour */
        let to_delete =
            WalkDir::new(&tmp_deltas_dir)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| {
                if let Ok(metadata) = e.metadata() {
                    if let Ok(mtime) = metadata.modified() {
                        if let Ok(since) = now.duration_since(mtime) {
                            return since.as_secs() > 60 * 60
                        }
                    }
                };
                false
            })
            .map(|e| e.path().to_path_buf())
            .collect::<Vec<PathBuf>>();

        for dir in to_delete {
            job_log_and_info(self.job_id, conn,
                             &format!(" Deleting old delta {:?}", dir.strip_prefix(&tmp_deltas_dir).unwrap()));
            fs::remove_dir_all(&dir)?;
        }

        Ok(())
    }

    fn update_appstream (&self,
                         config: &Config,
                         repoconfig: &RepoConfig,
                         conn: &PgConnection) -> JobResult<()> {
        job_log_and_info(self.job_id, conn, "Regenerating appstream branches");
        let repo_path = repoconfig.get_abs_repo_path();

        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-update-repo")
            .arg("--no-update-summary");
        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);
        cmd
            .arg(&repo_path);

        do_command(cmd)?;
        Ok(())
    }

    fn update_summary (&self,
                       config: &Config,
                       repoconfig: &RepoConfig,
                       conn: &PgConnection) -> JobResult<()> {
        job_log_and_info(self.job_id, conn, "Updating summary");
        let repo_path = repoconfig.get_abs_repo_path();

        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-update-repo")
            .arg("--no-update-appstream");
        add_gpg_args(&mut cmd, &repoconfig.gpg_key, &config.gpg_homedir);
        cmd
            .arg(&repo_path);

        do_command(cmd)?;
        Ok(())
    }

    fn run_post_publish (&self,
                         repoconfig: &RepoConfig,
                         conn: &PgConnection) -> JobResult<()> {
        if let Some(post_publish_script) = &repoconfig.post_publish_script {
            let repo_path = repoconfig.get_abs_repo_path();
            let mut cmd = Command::new(post_publish_script);
            cmd
                .arg(&repoconfig.name)
                .arg(&repo_path);
            job_log_and_info(self.job_id, conn, "Running post-publish script");
            do_command(cmd)?;
        };
        Ok(())
    }

    fn extract_appstream (&self,
                          repoconfig: &RepoConfig,
                          conn: &PgConnection) -> JobResult<()> {
        job_log_and_info(self.job_id, conn, "Extracting appstream branches");
        let repo_path = repoconfig.get_abs_repo_path();
        let appstream_dir = repo_path.join("appstream");
        let appstream_refs = ostree::list_refs (&repoconfig.path, "appstream");
        for appstream_ref in appstream_refs {
            let arch = appstream_ref.split("/").nth(1).unwrap();
            let mut cmd = Command::new("ostree");
            cmd
                .arg(&format!("--repo={}", &repoconfig.path.to_str().unwrap()))
                .arg("checkout")
                .arg("--user-mode")
                .arg("--union")
                .arg("--bareuseronly-dirs")
                .arg(&appstream_ref)
                .arg(appstream_dir.join(arch));
            do_command(cmd)?;
        };
        Ok(())
    }
}


impl JobInstance for UpdateRepoJobInstance {
    fn get_job_id (&self) -> i32 {
        self.job_id
    }

    fn order (&self) -> i32 {
        2 /* Delay updates after publish so they can be chunked. */
    }

    fn handle_job (&mut self, executor: &JobExecutor, conn: &PgConnection) -> JobResult<serde_json::Value> {
        info!("#{}: Handling Job UpdateRepo: repo: {}",
              &self.job_id, &self.repo);

        // Get repo config
        let config = &executor.config;
        let repoconfig = config.get_repoconfig(&self.repo)
            .or_else(|_e| Err(JobError::new(&format!("Can't find repo {}", &self.repo))))?;

        self.update_appstream(config, repoconfig, conn)?;

        let (missing_deltas, unwanted_deltas) = self.calculate_deltas(repoconfig);
        self.generate_deltas(&missing_deltas, repoconfig, conn)?;
        self.retire_deltas(&unwanted_deltas, repoconfig, conn)?;

        self.update_summary(config, repoconfig, conn)?;

        self.run_post_publish(repoconfig, conn)?;

        self.extract_appstream(repoconfig, conn)?;

        Ok(json!({ }))
    }
}

fn pick_next_job (executor: &mut JobExecutor, conn: &PgConnection) -> Result<Box<dyn JobInstance>, DieselError> {
    use diesel::dsl::exists;
    use diesel::dsl::not;
    use diesel::dsl::now;

    /* Find next job (if any) and mark it started */

    let for_repo = executor.repo.clone();
    let transaction_result =
        conn
        .build_transaction()
        .serializable()
        .deferrable()
        .run(|| {
            let ready_job_filter = jobs::status.eq(JobStatus::New as i16)
                .and(jobs::start_after.is_null().or(jobs::start_after.lt(now)))
                .and(
                    not(exists(
                        job_dependencies_with_status::table.filter(
                            job_dependencies_with_status::job_id.eq(jobs::id)
                                .and(job_dependencies_with_status::dependant_status.le(JobStatus::Started as i16))
                        )
                    )));

            let mut new_instances : Vec<Box<dyn JobInstance>> = match for_repo {
                None => {
                    jobs::table
                        .order(jobs::id)
                        .filter(ready_job_filter.and(jobs::repo.is_null()))
                        .get_results::<models::Job>(conn)?
                        .into_iter()
                        .map(|job| new_job_instance(executor, job))
                        .collect()
                },
                Some(repo) => {
                    jobs::table
                        .order(jobs::id)
                        .filter(ready_job_filter.and(jobs::repo.eq(repo)))
                        .get_results::<models::Job>(conn)?
                        .into_iter()
                        .map(|job| new_job_instance(executor, job))
                        .collect()
                },
            };

            /* Sort by prio */
            new_instances.sort_by(|a, b| a.order().cmp(&b.order()));

            /* Handle the first, if any */
            for new_instance in new_instances {
                diesel::update(jobs::table)
                    .filter(jobs::id.eq(new_instance.get_job_id()))
                    .set((jobs::status.eq(JobStatus::Started as i16),))
                    .execute(conn)?;
                return Ok(new_instance)
            }

            Err(diesel::NotFound)
        });

    /* Retry on serialization failure */
    match transaction_result {
        Err(DieselError::DatabaseError(SerializationFailure, _)) => pick_next_job (executor, conn),
        _ => transaction_result
    }
}


fn process_one_job (executor: &mut JobExecutor, conn: &PgConnection) -> bool {
    let new_instance = pick_next_job(executor, conn);

    match new_instance {
        Ok(mut instance) => {
            let (new_status, new_results) =
                match instance.handle_job(executor, conn) {
                    Ok(json) =>  {
                        info!("#{}: Job succeeded", instance.get_job_id());
                        (JobStatus::Ended, json.to_string())
                    },
                    Err(e) => {
                        job_log_and_error(instance.get_job_id(), conn,
                                          &format!("Job failed: {}", e.to_string()));
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

struct ExecutorInfo {
    addr: Addr<JobExecutor>,
    processing_job: bool,
    job_queued: bool,
}

pub struct JobQueue {
    executors: HashMap<Option<String>,RefCell<ExecutorInfo>>,
    running: bool,
}

impl JobQueue {
    fn kick(&mut self, repo: &Option<String>, ctx: &mut Context<Self>) {
        let mut info = match self.executors.get(repo) {
            None => {
                error!("Got process jobs for non existing executor");
                return
            },
            Some(executor_info) => executor_info.borrow_mut(),
        };

        if !self.running {
            return
        }
        if info.processing_job {
            info.job_queued = true;
        } else {
            info.processing_job = true;
            info.job_queued = false;

            let repo = repo.clone();
            ctx.spawn(
                info.addr
                    .send (ProcessOneJob())
                    .into_actor(self)
                    .then(|result, queue, ctx| {
                        let job_queued = {
                            let mut info = queue.executors.get(&repo).unwrap().borrow_mut();
                            info.processing_job = false;
                            info.job_queued
                        };

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
                            if job_queued || processed_job {
                                queue.kick(&repo, ctx);
                            } else  {
                                // We send a ProcessJobs message each time we added something to the
                                // db, but case something external modifes the db we have a 10 sec
                                // polling loop here.  Ideally this should be using NOTIFY/LISTEN
                                // postgre, but diesel/pq-sys does not currently support it.

                                ctx.run_later(time::Duration::new(10, 0), move |queue, ctx| {
                                    queue.kick(&repo, ctx);
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
        // Run any jobs in db
        let repos = self.executors.keys().cloned().collect::<Vec<_>>();
        for repo in repos {
            self.kick(&repo, ctx);
        }
    }
}

pub struct ProcessJobs(pub Option<String>);

impl Message for ProcessJobs {
    type Result = Result<(), ()>;
}

impl Handler<ProcessJobs> for JobQueue {
    type Result = Result<(), ()>;

    fn handle(&mut self, msg: ProcessJobs, ctx: &mut Self::Context) -> Self::Result {
        self.kick(&msg.0, ctx);
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

        let executors : Vec<Addr<JobExecutor>> = self.executors.values().map(|info| info.borrow().addr.clone()).collect();
        ActorResponse::async(
            futures::stream::iter_ok(executors).into_actor(self)
                .map(|executor: Addr<JobExecutor>, job_queue, _ctx| {
                    executor
                        .send (StopJobs())
                        .into_actor(job_queue)
                        .then(|_result, _job_queue, _ctx| {
                            actix::fut::ok::<_,(),_>(())
                        })
                })
                .finish()
        )
    }
}

fn start_executor(repo: &Option<String>,
                  config: &Arc<Config>,
                  delta_generator: &Addr<DeltaGenerator>,
                  pool: &Pool<ConnectionManager<PgConnection>>) -> RefCell<ExecutorInfo>
{
    let config_copy = config.clone();
    let delta_generator_copy = delta_generator.clone();
    let pool_copy = pool.clone();
    let repo_clone = repo.clone();
    RefCell::new(ExecutorInfo {
        addr: SyncArbiter::start(1, move || JobExecutor {
            repo: repo_clone.clone(),
            config: config_copy.clone(),
            delta_generator: delta_generator_copy.clone(),
            pool: pool_copy.clone()
        }),
        processing_job: false,
        job_queued: false,
    })
}


pub fn start_job_executor(config: Arc<Config>,
                          delta_generator: Addr<DeltaGenerator>,
                          pool: Pool<ConnectionManager<PgConnection>>) -> Addr<JobQueue> {
    let mut executors = HashMap::new();
    executors.insert(None,
                     start_executor(&None, &config, &delta_generator, &pool));

    for repo in config.repos.keys().cloned() {
        executors.insert(Some(repo.clone()),
                         start_executor(&Some(repo.clone()), &config, &delta_generator, &pool));
    }
    JobQueue {
        executors: executors,
        running: true,
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
        let updated =
            diesel::update(jobs)
            .filter(status.eq(JobStatus::Started as i16))
            .set((status.eq(JobStatus::Broken as i16),))
            .get_results::<Job>(conn)?;
        if !updated.is_empty() {
            error!("Marked {} jobs as broken due to being started already at startup", updated.len());
            /* For any repo that had an update-repo marked broken, queue a new job */
            for job in updated.iter() {
                let mut queue_update_for_repos = HashSet::new();
                if job.kind == JobKind::UpdateRepo.to_db() {
                    if let Ok(data) = serde_json::from_str::<UpdateRepoJob>(&job.contents) {
                        queue_update_for_repos.insert(data.repo);
                    }
                }
                for reponame in queue_update_for_repos {
                    info!("Queueing new update job for repo {:?}", reponame);
                    let _update_job = queue_update_job (0, conn, &reponame, None);
                }
            }
        }
    };
    Ok(())
}
