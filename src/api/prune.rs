use actix_web::{web, HttpRequest, HttpResponse};
use futures3::TryFutureExt;
use serde::Deserialize;

use crate::db::Db;
use crate::errors::ApiError;
use crate::tokens::{ClaimsScope, ClaimsValidator};

#[derive(Deserialize)]
pub struct PruneArgs {
    repo: String,
}

pub fn handle_prune(
    args: web::Json<PruneArgs>,
    db: web::Data<Db>,
    req: HttpRequest,
) -> impl futures::Future<Item = HttpResponse, Error = ApiError> {
    Box::pin(handle_prune_async(args, db, req)).compat()
}

async fn handle_prune_async(
    args: web::Json<PruneArgs>,
    db: web::Data<Db>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    // Verify token has TokenManagement scope
    req.has_token_claims("", ClaimsScope::TokenManagement)?;

    // Create a new prune job
    let job = db.get_ref().start_prune_job(args.repo.clone()).await?;

    Ok(HttpResponse::Ok().json(job))
}
