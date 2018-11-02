use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use std::{thread, time};

use models;
use models::{Job, JobStatus, job_dependencies_with_status, };
use schema::*;

fn handle_job (job: &Job) {
    println!("jobs: {:?}", job);
}

pub fn start_handling_jobs(pool: Pool<ConnectionManager<PgConnection>>) {
    use diesel::dsl::exists;

    loop {
        let conn = &pool.get().unwrap();

        if let Ok(new_job) = jobs::table
            .filter(jobs::status.eq(JobStatus::New as i16)
                    .and(
                        exists(
                            job_dependencies_with_status::table.filter(
                                job_dependencies_with_status::job_id.eq(jobs::id)
                                    .and(job_dependencies_with_status::dependant_status.le(JobStatus::Started as i16))
                            )
                        )
                    )
            )
            .get_result::<models::Job>(conn)  {
                handle_job (&new_job);
            } else  {
                // diesel/pq-sys does not currently support NOTIFY/LISTEN, so we poll for now
                println!("No jobs, sleeping");
                thread::sleep(time::Duration::from_secs(1));
            }

    }
}
