use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::result::{Error as DieselError};
use diesel;
use serde_json;
use std::env;
use std::ffi::OsString;
use std::fs::File;
use std::fs;
use std::io::Write;
use std::io;
use std::path;
use std::process::{Command};
use std::sync::Arc;
use std::sync::mpsc;
use std::{thread, time};

use app::Config;
use errors::{WorkerError, WorkerResult};
use models::{Job, JobKind, CommitJob, PublishJob, JobStatus, job_dependencies_with_status, RepoState, PublishedState };
use models;
use schema::*;

struct Worker {
    pub config: Arc<Config>,
    pub pool: Pool<ConnectionManager<PgConnection>>,
    pub wakeup_channel: mpsc::Receiver<()>,
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

fn do_commit_build_refs (build_id: i32,
                      build_refs: &Vec<models::BuildRef>,
                      endoflife: &Option<String>,
                      config: &Arc<Config>)  -> WorkerResult<serde_json::Value> {
    let build_repo_path = config.build_repo_base_path.join(build_id.to_string());
    let upload_path = build_repo_path.join("upload");

    init_ostree_repo (&build_repo_path, &config.repo_path, build_id, &config.collection_id)?;
    init_ostree_repo (&upload_path, &config.repo_path, build_id, &None)?;

    let mut src_repo_arg = OsString::from("--src-repo=");
    src_repo_arg.push(&upload_path);

    for build_ref in build_refs.iter() {
        let mut src_ref_arg = String::from("--src-ref=");
        src_ref_arg.push_str(&build_ref.commit);

        let mut cmd = Command::new("flatpak");
        cmd
            .arg("build-commit-from")
            .arg("--timestamp=NOW")     // All builds have the same timestamp, not when the individual builds finished
            .arg("--no-update-summary") // We update it once at the end
            .arg("--untrusted")         // Verify that the uploaded objects are correct
            .arg("--disable-fsync");    // There is a sync in flatpak build-update-repo, so avoid it here

        if let Some(gpg_homedir) = &config.gpg_homedir {
            cmd
                .arg(format!("--gpg-homedir=={}", gpg_homedir));
        };

        if let Some(key) = &config.build_gpg_key {
            cmd
                .arg(format!("--gpg-sign=={}", key));
        };

        if let Some(endoflife) = &endoflife {
            cmd
                .arg("--force")         // Even if the content is the same, the metadata is not, so ensure its updated
                .arg(format!("--end-of-life={}", endoflife));
        };

        println!("running {:?} {:?} {:?} {:?} {:?}",
                 cmd,
                 &src_repo_arg,
                 &src_ref_arg,
                 &build_repo_path,
                 &build_ref.ref_name);

        let output = cmd
            .arg(&src_repo_arg)
            .arg(&src_ref_arg)
            .arg(&build_repo_path)
            .arg(&build_ref.ref_name)
            .output()?;
        if !output.status.success() {
            return Err(WorkerError::new(&format!("Failed to build commit for ref {}: {}", &build_ref.ref_name, String::from_utf8_lossy(&output.stderr))));
        }

        if build_ref.ref_name.starts_with("app/") {
            let parts: Vec<&str> = build_ref.ref_name.split('/').collect();
            let mut file = File::create(build_repo_path.join(format!("{}.flatpakref", parts[1])))?;
            // TODO: We should also add GPGKey here if state.build_gpg_key is set
            file.write_all(format!(r#"
[Flatpak Ref]
Name={}
Branch={}
Url={}/build-repo/{}
RuntimeRepo=https://dl.flathub.org/repo/flathub.flatpakrepo
IsRuntime=false
"#,
                                   parts[1],
                                   parts[3],
                                   config.base_url,
                                   build_id).as_bytes())?;
        }
    }

    println!("running build-update-repo");

    let output = Command::new("flatpak")
        .arg("build-update-repo")
        .arg(&build_repo_path)
        .output()?;
    if !output.status.success() {
        return Err(WorkerError::new(&format!("Failed to update repo: {}", String::from_utf8_lossy(&output.stderr))));
    }

    println!("remove upload path");

    fs::remove_dir_all(&upload_path)?;

    Ok(json!({}))
}


fn handle_commit_job (worker: &Worker, conn: &PgConnection, job: &CommitJob) -> WorkerResult<serde_json::Value> {
    println!("commit: {:?}", job);

    // Get the uploaded refs from db

    let build_refs = build_refs::table
        .filter(build_refs::build_id.eq(job.build))
        .get_results::<models::BuildRef>(conn)
        .or_else(|_e| Err(WorkerError::new("Can't load build refs")))?;
    println!("refs: {:?}", build_refs);
    if build_refs.len() == 0 {
        return Err(WorkerError::new("No refs in build"));
    }

    // Do the actual work

    let res = do_commit_build_refs(job.build, &build_refs, &&job.endoflife, &worker.config);

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

fn do_publish (build_id: i32,
               config: &Arc<Config>)  -> WorkerResult<serde_json::Value> {
    let build_repo_path = config.build_repo_base_path.join(build_id.to_string());

    let mut src_repo_arg = OsString::from("--src-repo=");
    src_repo_arg.push(&build_repo_path);

    let mut cmd = Command::new("flatpak");
    cmd
        .arg("build-commit-from")
        .arg("--no-update-summary"); // We update it separately

        if let Some(gpg_homedir) = &config.gpg_homedir {
            cmd
                .arg(format!("--gpg-homedir=={}", gpg_homedir));
        };

    if let Some(key) = &config.build_gpg_key {
        cmd
            .arg(format!("--gpg-sign=={}", key));
    };

    println!("running {:?} {:?} {:?}",
             cmd,
             &src_repo_arg,
             &config.repo_path);

    let output = cmd
        .arg(&src_repo_arg)
        .arg(&config.repo_path)
        .output()?;
    if !output.status.success() {
        return Err(WorkerError::new(&format!("Failed to publish repo: {}", String::from_utf8_lossy(&output.stderr))));
    }

    println!("running flatpak build-update-repo");

    let output = Command::new("flatpak")
        .arg("build-update-repo")
        .arg(&config.repo_path)
        .output()
        .map_err(|e| WorkerError::new(&format!("Failed to update repo: {}", e.to_string())))
        ?;
    if !output.status.success() {
        return Err(WorkerError::new(&format!("Failed to update repo: {}", String::from_utf8_lossy(&output.stderr))));
    }
    Ok(json!({}))
}

fn handle_publish_job (worker: &Worker, conn: &PgConnection, job: &PublishJob) -> WorkerResult<serde_json::Value> {
    println!("publish: {:?}", job);

    // Do the actual work

    let res = do_publish(job.build, &worker.config);

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
            println!("Unexpected publishing state2 {:?}", current_published_state);
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


fn handle_job (worker: &Worker, conn: &PgConnection, job: &Job) {
    println!("jobs: {:?}", job);
    let handler_res = match JobKind::from_db(job.kind) {
        Some(JobKind::Commit) => {
            if let Ok(commit_job) = serde_json::from_value::<CommitJob>(job.contents.clone()) {
                handle_commit_job (worker, conn, &commit_job)
            } else {
                Err(WorkerError::new("Can't parse commit job"))
            }
        },
        Some(JobKind::Publish) => {
            if let Ok(publish_job) = serde_json::from_value::<PublishJob>(job.contents.clone()) {
                handle_publish_job (worker, conn, &publish_job)
            } else {
                Err(WorkerError::new("Can't parse publish job"))
            }
        },
        _ => {
            println!("Uknown job type {}", job.id);
            Err(WorkerError::new("Unknown job type"))
        }
    };
    let (new_status, new_results) = match handler_res {
        Ok(json) =>  (JobStatus::Ended, json),
        Err(e) => (JobStatus::Broken, json!(e.to_string()))
    };
    let update_res =
        diesel::update(jobs::table)
        .filter(jobs::id.eq(job.id))
        .set((jobs::status.eq(new_status as i16),
              jobs::results.eq(new_results)))
        .execute(conn);
    if let Err(e) = update_res {
        println!("handle_job: Error updating job {}", e);
    }
}

fn worker_thread (worker: Worker) {
    use diesel::dsl::exists;
    use diesel::dsl::not;

    loop {
        let conn = &worker.pool.get().unwrap();

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

        if let Ok(job) = new_job {
            handle_job (&worker, conn, &job);
        } else {
            // diesel/pq-sys does not currently support NOTIFY/LISTEN, so we use an in-process channel
            // and a 3 sec poll for now
            println!("No jobs, sleeping");
            let _r = worker.wakeup_channel.recv_timeout(time::Duration::from_secs(3));
        }

    }
}

pub fn start_worker(config: &Arc<Config>,
                    pool: Pool<ConnectionManager<PgConnection>>,
                    wakeup_channel: mpsc::Receiver<()> ) {
    let worker = Worker {
        config: config.clone(),
        pool: pool,
        wakeup_channel: wakeup_channel,
    };
    thread::spawn(move || worker_thread (worker));
}
