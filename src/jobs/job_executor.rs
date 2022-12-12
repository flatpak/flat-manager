use actix::prelude::*;
use actix::{Actor, SyncContext};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::DatabaseErrorKind::SerializationFailure;
use diesel::result::Error as DieselError;
use log::{error, info};
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::app::Config;
use crate::deltas::DeltaGenerator;
use crate::jobs::job_instance::new_job_instance;
use crate::models;
use crate::models::{job_dependencies_with_status, JobStatus};
use crate::schema::*;
use crate::Pool;

use super::job_instance::JobInstance;
use super::job_queue::{ExecutorInfo, JobQueue};
use super::utils::job_log_and_error;

pub struct JobExecutor {
    pub repo: Option<String>,
    pub config: Arc<Config>,
    pub delta_generator: Addr<DeltaGenerator>,
    pub pool: Pool,
}

impl Actor for JobExecutor {
    type Context = SyncContext<Self>;
}

fn pick_next_job(
    executor: &mut JobExecutor,
    conn: &PgConnection,
) -> Result<Box<dyn JobInstance>, DieselError> {
    use diesel::dsl::exists;
    use diesel::dsl::not;
    use diesel::dsl::now;

    /* Find next job (if any) and mark it started */

    let for_repo = executor.repo.clone();
    let transaction_result = conn
        .build_transaction()
        .serializable()
        .deferrable()
        .run(|| {
            let ready_job_filter = jobs::status
                .eq(JobStatus::New as i16)
                .and(jobs::start_after.is_null().or(jobs::start_after.lt(now)))
                .and(not(exists(
                    job_dependencies_with_status::table.filter(
                        job_dependencies_with_status::job_id.eq(jobs::id).and(
                            job_dependencies_with_status::dependant_status
                                .le(JobStatus::Started as i16),
                        ),
                    ),
                )));

            let mut new_instances: Vec<Box<dyn JobInstance>> = match for_repo {
                None => jobs::table
                    .order(jobs::id)
                    .filter(ready_job_filter.and(jobs::repo.is_null()))
                    .get_results::<models::Job>(conn)?
                    .into_iter()
                    .map(|job| new_job_instance(executor, job))
                    .collect(),
                Some(repo) => jobs::table
                    .order(jobs::id)
                    .filter(ready_job_filter.and(jobs::repo.eq(repo)))
                    .get_results::<models::Job>(conn)?
                    .into_iter()
                    .map(|job| new_job_instance(executor, job))
                    .collect(),
            };

            /* Sort by prio */
            new_instances.sort_by_key(|a| a.order());

            /* Handle the first, if any */
            if let Some(new_instance) = new_instances.into_iter().next() {
                diesel::update(jobs::table)
                    .filter(jobs::id.eq(new_instance.get_job_id()))
                    .set((jobs::status.eq(JobStatus::Started as i16),))
                    .execute(conn)?;
                return Ok(new_instance);
            }

            Err(diesel::NotFound)
        });

    /* Retry on serialization failure */
    match transaction_result {
        Err(DieselError::DatabaseError(SerializationFailure, _)) => pick_next_job(executor, conn),
        _ => transaction_result,
    }
}

fn process_one_job(executor: &mut JobExecutor, conn: &PgConnection) -> bool {
    let new_instance = pick_next_job(executor, conn);

    match new_instance {
        Ok(mut instance) => {
            let (new_status, new_results) = match instance.handle_job(executor, conn) {
                Ok(json) => {
                    info!("#{}: Job succeeded", instance.get_job_id());
                    (JobStatus::Ended, json.to_string())
                }
                Err(e) => {
                    job_log_and_error(instance.get_job_id(), conn, &format!("Job failed: {}", e));
                    (
                        JobStatus::Broken,
                        json!({"error-message": e.to_string()}).to_string(),
                    )
                }
            };

            let update_res = diesel::update(jobs::table)
                .filter(jobs::id.eq(instance.get_job_id()))
                .set((
                    jobs::status.eq(new_status as i16),
                    jobs::results.eq(new_results),
                ))
                .execute(conn);
            if let Err(e) = update_res {
                error!("handle_job: Error updating job {}", e);
            }
            true /* We handled a job */
        }
        Err(diesel::NotFound) => {
            false /* We didn't handle a job */
        }
        Err(e) => {
            error!("Unexpected db error processing job: {}", e);
            false
        }
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
        Ok(process_one_job(self, conn))
    }
}

fn start_executor(
    repo: &Option<String>,
    config: &Arc<Config>,
    delta_generator: &Addr<DeltaGenerator>,
    pool: &Pool,
) -> RefCell<ExecutorInfo> {
    let config_copy = config.clone();
    let delta_generator_copy = delta_generator.clone();
    let pool_copy = pool.clone();
    let repo_clone = repo.clone();
    RefCell::new(ExecutorInfo {
        addr: SyncArbiter::start(1, move || JobExecutor {
            repo: repo_clone.clone(),
            config: config_copy.clone(),
            delta_generator: delta_generator_copy.clone(),
            pool: pool_copy.clone(),
        }),
        processing_job: false,
        job_queued: false,
    })
}

pub fn start_job_executor(
    config: Arc<Config>,
    delta_generator: Addr<DeltaGenerator>,
    pool: Pool,
) -> Addr<JobQueue> {
    let mut executors = HashMap::new();
    executors.insert(
        None,
        start_executor(&None, &config, &delta_generator, &pool),
    );

    for repo in config.repos.keys() {
        executors.insert(
            Some(repo.clone()),
            start_executor(&Some(repo.clone()), &config, &delta_generator, &pool),
        );
    }
    JobQueue {
        executors,
        running: true,
    }
    .start()
}
