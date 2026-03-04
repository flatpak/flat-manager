use actix_web::web::{Data, Path};
use actix_web::{HttpResponse, Result};

use std::env;

use crate::db::*;
use crate::errors::ApiError;
use crate::models::{Job, JobKind, JobStatus};
use askama::Template;

use super::build::JobPathParams;

#[derive(Template)]
#[template(path = "job.html")]
struct JobStatusData {
    id: i32,
    kind: String,
    status: String,
    contents: String,
    results: String,
    log: String,
    finished: bool,
}

fn job_status_data(job: Job) -> JobStatusData {
    JobStatusData {
        id: job.id,
        kind: JobKind::from_db(job.kind).map_or("Unknown".to_string(), |k| format!("{k:?}")),
        status: JobStatus::from_db(job.status).map_or("Unknown".to_string(), |s| format!("{s:?}")),
        contents: job.contents,
        results: job.results.unwrap_or_default(),
        log: job.log,
        finished: job.status >= JobStatus::Ended as i16,
    }
}

pub async fn job_status(
    params: Path<JobPathParams>,
    db: Data<Db>,
) -> Result<HttpResponse, ApiError> {
    let job = db.lookup_job(params.id, None).await?;
    let s = job_status_data(job).render().unwrap();
    Ok(HttpResponse::Ok().content_type("text/html").body(s))
}

#[derive(Template)]
#[template(path = "status.html")]
struct Status {
    jobs: Vec<JobStatusData>,
    version: String,
}

pub async fn status(db: Data<Db>) -> Result<HttpResponse, ApiError> {
    let jobs = db.list_active_jobs().await?;

    let s = Status {
        jobs: jobs.into_iter().map(job_status_data).collect(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    }
    .render()
    .unwrap();
    Ok(HttpResponse::Ok().content_type("text/html").body(s))
}
