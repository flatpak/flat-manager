use actix::prelude::*;
use actix::Actor;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind::SerializationFailure;
use diesel::result::Error as DieselError;
use log::{error, info};
use serde_json::json;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::str;
use std::time;

use crate::models::{
    Job, JobDependency, JobKind, JobStatus, NewJob, PublishedState, RepoState, UpdateRepoJob,
};
use crate::schema;
use crate::schema::*;
use crate::Pool;

use super::job_executor::{JobExecutor, ProcessOneJob, StopJobs};

// We have an async JobQueue object that wraps the sync JobExecutor, because
// that way we can respond to incomming requests immediately and decide in
// what order to handle them. In particular, we want to prioritize stop
// operations and exit cleanly with outstanding jobs for next run

pub struct ExecutorInfo {
    pub addr: Addr<JobExecutor>,
    pub processing_job: bool,
    pub job_queued: bool,
}

pub struct JobQueue {
    pub executors: HashMap<Option<String>, RefCell<ExecutorInfo>>,
    pub running: bool,
}

impl JobQueue {
    fn kick(&mut self, repo: &Option<String>, ctx: &mut Context<Self>) {
        let mut info = match self.executors.get(repo) {
            None => {
                error!("Got process jobs for non existing executor");
                return;
            }
            Some(executor_info) => executor_info.borrow_mut(),
        };

        if !self.running {
            return;
        }
        if info.processing_job {
            info.job_queued = true;
        } else {
            info.processing_job = true;
            info.job_queued = false;

            let repo = repo.clone();
            ctx.spawn(info.addr.send(ProcessOneJob()).into_actor(self).then(
                |result, queue, ctx| {
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
                            }
                        };

                        // If we ran a job, or a job was queued, kick again
                        if job_queued || processed_job {
                            queue.kick(&repo, ctx);
                        } else {
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
                },
            ));
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
        // Stop assigning jobs to executors
        self.running = false;

        // Send each executor a StopJobs message and wait for it to be run. Since the executors are single-threaded,
        // and we aren't assigning any more jobs, we know that no jobs are in progress on an executor when its StopJobs
        // message responds.
        let stop_jobs = self
            .executors
            .values()
            .map(|info| info.borrow().addr.send(StopJobs()).map_err(|_| ()));

        // Wait for all the above futures to resolve
        ActorResponse::r#async(
            futures::stream::futures_unordered(stop_jobs)
                .into_actor(self)
                .finish(),
        )
    }
}

pub fn cleanup_started_jobs(pool: &Pool) -> Result<(), diesel::result::Error> {
    let conn = &pool.get().unwrap();
    {
        use schema::builds::dsl::*;
        let (verifying, _) = RepoState::Committing.to_db();
        let (purging, _) = RepoState::Purging.to_db();
        let (failed, failed_reason) =
            RepoState::Failed("Server was restarted during job".to_string()).to_db();
        let n_updated = diesel::update(builds)
            .filter(repo_state.eq(verifying).or(repo_state.eq(purging)))
            .set((repo_state.eq(failed), repo_state_reason.eq(failed_reason)))
            .execute(conn)?;
        if n_updated != 0 {
            error!(
                "Marked {} builds as failed due to in progress jobs on startup",
                n_updated
            );
        }
        let (publishing, _) = PublishedState::Publishing.to_db();
        let (failed_publish, failed_publish_reason) =
            PublishedState::Failed("Server was restarted during publish".to_string()).to_db();
        let n_updated2 = diesel::update(builds)
            .filter(published_state.eq(publishing))
            .set((
                published_state.eq(failed_publish),
                published_state_reason.eq(failed_publish_reason),
            ))
            .execute(conn)?;
        if n_updated2 != 0 {
            error!(
                "Marked {} builds as failed to publish due to in progress jobs on startup",
                n_updated2
            );
        }
    };
    {
        use schema::jobs::dsl::*;
        let updated = diesel::update(jobs)
            .filter(status.eq(JobStatus::Started as i16))
            .set((status.eq(JobStatus::Broken as i16),))
            .get_results::<Job>(conn)?;
        if !updated.is_empty() {
            error!(
                "Marked {} jobs as broken due to being started already at startup",
                updated.len()
            );
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
                    let _update_job = queue_update_job(0, conn, &reponame, None);
                }
            }
        }
    };
    Ok(())
}

pub fn queue_update_job(
    delay_secs: u64,
    conn: &PgConnection,
    repo: &str,
    starting_job_id: Option<i32>,
) -> Result<(bool, Job), DieselError> {
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
                        depends_on,
                    })
                    .execute(conn)?;
            }

            Ok((is_new, update_job))
        });

    /* Retry on serialization failure */
    match transaction_result {
        Err(DieselError::DatabaseError(SerializationFailure, _)) => {
            queue_update_job(delay_secs, conn, repo, starting_job_id)
        }
        _ => transaction_result,
    }
}
